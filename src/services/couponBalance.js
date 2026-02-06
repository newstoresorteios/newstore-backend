// src/services/couponBalance.js
// ÚNICO ponto autorizado a creditar saldo em users.coupon_value_cents.
// Regras:
// - Somente quando payments.status final = 'approved'
// - Idempotente e concorrente-safe (anti-duplicação via UPDATE + ledger UNIQUE)
// - Não quebra fluxos existentes: NO-OP quando já creditado / não aprovado

import { query as defaultQuery } from "../db.js";

function isDebugEnabled() {
  const v = String(process.env.DEBUG_COUPON || "").toLowerCase().trim();
  return v === "1" || v === "true" || v === "yes" || v === "on";
}

function safeJsonMeta(meta) {
  try {
    if (!meta || typeof meta !== "object") return "{}";
    return JSON.stringify(meta);
  } catch {
    return "{}";
  }
}

/**
 * Credita saldo do usuário quando um payment estiver approved.
 *
 * @param {string|number} paymentId
 * @param {object} options
 * @param {'PIX'|'CARD'|'VINDI'} options.channel
 * @param {'mercadopago_webhook'|'pix_status_poll'|'vindi_webhook'|'reconcile_sync'} options.source
 * @param {string|null} options.runTraceId
 * @param {object|null} options.meta
 * @param {number|null} options.unitCents
 * @returns {Promise<{
 *  ok:boolean,
 *  action:'credited'|'noop'|'invalid_amount'|'not_supported'|'error'|'invalid_payment_id',
 *  reason?:string|null,
 *  history_rows:number,
 *  user_rows:number,
 *  payment_rows:number,
 *  user_id?:number|null,
 *  status?:string|null,
 *  delta_cents?:number|null,
 *  qty?:number|null,
 *  unit_cents?:number|null,
 *  already_in_ledger?:boolean,
 *  already_credited?:boolean,
 *  errCode?:string|null,
 *  errMsg?:string|null
 * }>}
 */
export async function creditCouponOnApprovedPayment(paymentId, options = {}) {
  const pid = paymentId != null ? String(paymentId) : "";
  const channel = options?.channel != null ? String(options.channel) : null;
  const runTraceId = options?.runTraceId != null ? String(options.runTraceId) : null;

  const unitCents = Number(
    options?.unitCents ??
      options?.meta?.unit_cents ??
      process.env.COUPON_UNIT_CENTS ??
      5500
  );
  const unit = Number.isFinite(unitCents) && unitCents > 0 ? Math.trunc(unitCents) : 5500;

  const metaJson = safeJsonMeta({
    ...(options?.meta && typeof options.meta === "object" ? options.meta : null),
    source: options?.source || null,
  });

  // Permite reuso de transação (ex.: webhook Vindi) sem alterar contrato de endpoints.
  const q = options?.pgClient?.query ? options.pgClient.query.bind(options.pgClient) : defaultQuery;

  if (!pid) {
    return {
      ok: false,
      action: "invalid_payment_id",
      reason: "missing_payment_id",
      history_rows: 0,
      user_rows: 0,
      payment_rows: 0,
      user_id: null,
      status: null,
      delta_cents: null,
      qty: null,
      unit_cents: unit,
      already_in_ledger: false,
      already_credited: false,
      errCode: null,
      errMsg: null,
    };
  }

  // 1 SQL atômico (CTE) com parâmetros tipados (evita 42P08):
  // $1 payment_id (text)
  // $2 unit (int4)
  // $3 channel (text)
  // $4 run_trace_id (text)
  // $5 meta (jsonb)
  //
  // Regras:
  // - delta = qty(numbers) * unit; fallback: amount_cents se qty<=0 (se >0)
  // - Idempotência: payments.coupon_credited + UNIQUE(payment_id,event_type) + guardrail NOT EXISTS no ledger
  // - Não depende de colunas opcionais (reservation_id no histórico = NULL)
  const sql = `
    WITH pi AS (
      SELECT
        id,
        user_id,
        amount_cents,
        numbers,
        draw_id,
        lower(status) AS status_l,
        coupon_credited
      FROM public.payments
      WHERE id = $1
      LIMIT 1
    ),
    p AS (
      UPDATE public.payments pay
         SET coupon_credited = true,
             coupon_credited_at = now()
        FROM pi
       WHERE pay.id = pi.id
         AND pi.status_l IN ('approved','paid','pago')
         AND lower(pay.status) IN ('approved','paid','pago')
         AND COALESCE(pi.coupon_credited, false) = false
         AND COALESCE(pay.coupon_credited, false) = false
         AND (
           CASE
             WHEN COALESCE(array_length(pi.numbers,1),0) > 0
               THEN (COALESCE(array_length(pi.numbers,1),0) * $2::int4)
             ELSE COALESCE(pi.amount_cents,0)
           END
         ) > 0
         AND NOT EXISTS (
           SELECT 1
           FROM public.coupon_balance_history h
           WHERE h.payment_id = pi.id
             AND h.event_type = 'CREDIT_PURCHASE'
         )
       RETURNING
         pay.id,
         pi.user_id,
         pi.draw_id,
         COALESCE(array_length(pi.numbers,1),0)::int AS qty,
         (
           CASE
             WHEN COALESCE(array_length(pi.numbers,1),0) > 0
               THEN (COALESCE(array_length(pi.numbers,1),0) * $2::int4)
             ELSE COALESCE(pi.amount_cents,0)
           END
         )::int AS delta_cents,
         pi.status_l AS payment_status
    ),
    u AS (
      UPDATE public.users usr
         SET coupon_value_cents = usr.coupon_value_cents + p.delta_cents,
             coupon_updated_at  = now()
        FROM p
       WHERE usr.id = p.user_id
       RETURNING
         usr.id AS user_id,
         (usr.coupon_value_cents - p.delta_cents) AS balance_before_cents,
         usr.coupon_value_cents AS balance_after_cents,
         p.id AS payment_id,
         p.delta_cents,
         p.draw_id,
         p.qty,
         p.payment_status
    ),
    h AS (
      INSERT INTO public.coupon_balance_history
        (user_id, payment_id, delta_cents, balance_before_cents, balance_after_cents,
         event_type, channel, status, draw_id, reservation_id, run_trace_id, meta)
      SELECT
        u.user_id,
        u.payment_id,
        u.delta_cents,
        u.balance_before_cents,
        u.balance_after_cents,
        'CREDIT_PURCHASE',
        $3::text,
        u.payment_status,
        u.draw_id,
        NULL::text,
        $4::text,
        (
          COALESCE($5::jsonb, '{}'::jsonb)
          || jsonb_build_object('unit_cents', $2::int4, 'qty', u.qty, 'channel', $3::text)
        )
      FROM u
      ON CONFLICT DO NOTHING
      RETURNING id
    ),
    p2 AS (
      UPDATE public.payments pay
         SET coupon_credited = true,
             coupon_credited_at = now()
        FROM pi
       WHERE pay.id = pi.id
         AND pi.status_l IN ('approved','paid','pago')
         AND lower(pay.status) IN ('approved','paid','pago')
         AND COALESCE(pay.coupon_credited, false) = false
         AND EXISTS (
           SELECT 1
           FROM public.coupon_balance_history hh
           WHERE hh.payment_id = pi.id
             AND hh.event_type = 'CREDIT_PURCHASE'
         )
       RETURNING pay.id
    )
    SELECT
      (SELECT count(*)::int FROM p) AS credited_rows,
      (SELECT count(*)::int FROM h) AS history_rows,
      (SELECT count(*)::int FROM u) AS user_rows,
      ((SELECT count(*)::int FROM p) + (SELECT count(*)::int FROM p2)) AS payment_rows,
      (SELECT pi.user_id::int FROM pi) AS user_id,
      (SELECT pi.status_l::text FROM pi) AS status_l,
      (SELECT COALESCE(array_length(pi.numbers,1),0)::int FROM pi) AS qty,
      $2::int4 AS unit_cents,
      (SELECT (
        CASE
          WHEN COALESCE(array_length(pi.numbers,1),0) > 0 THEN (COALESCE(array_length(pi.numbers,1),0) * $2::int4)
          ELSE COALESCE(pi.amount_cents,0)
        END
      )::int FROM pi) AS delta_cents,
      (SELECT COALESCE(EXISTS(
        SELECT 1
        FROM public.coupon_balance_history hh
        WHERE hh.payment_id = pi.id
          AND hh.event_type = 'CREDIT_PURCHASE'
      ), false) FROM pi) AS already_in_ledger,
      (SELECT COALESCE(pi.coupon_credited, false) FROM pi) AS already_credited
  `;

  try {
    const { rows } = await q(sql, [pid, unit, channel, runTraceId, metaJson]);
    const credited_rows = rows?.[0]?.credited_rows ?? 0;
    const history_rows = rows?.[0]?.history_rows ?? 0;
    const user_rows = rows?.[0]?.user_rows ?? 0;
    const payment_rows = rows?.[0]?.payment_rows ?? 0;
    const user_id = rows?.[0]?.user_id ?? null;
    const status = rows?.[0]?.status_l ?? null;
    const qty = rows?.[0]?.qty ?? null;
    const unit_cents = rows?.[0]?.unit_cents ?? null;
    const delta_cents = rows?.[0]?.delta_cents ?? null;
    const already_in_ledger = !!rows?.[0]?.already_in_ledger;
    const already_credited = !!rows?.[0]?.already_credited;

    const isFinal = status != null && ["approved", "paid", "pago"].includes(String(status));
    const noNumbers = isFinal && (Number(qty || 0) <= 0);
    const invalidAmount = isFinal && Number(delta_cents || 0) <= 0;

    let action = "noop";
    let reason = null;

    if (credited_rows === 1 && history_rows === 1 && user_rows === 1) {
      action = "credited";
      reason = null;
    } else if (!isFinal) {
      action = "noop";
      reason = "not_final";
    } else if (invalidAmount) {
      action = "invalid_amount";
      reason = noNumbers ? "no_numbers" : "zero_delta";
    } else if (already_in_ledger) {
      action = "noop";
      reason = "already_in_ledger";
    } else if (already_credited) {
      action = "noop";
      reason = "already_credited";
    }

    if (isDebugEnabled()) {
      // logs enxutos, sem poluir produção
      // eslint-disable-next-line no-console
      console.log(
        "[coupon.credit]",
        `payment=${pid}`,
        `channel=${channel}`,
        `credited_rows=${credited_rows}`,
        `history_rows=${history_rows}`,
        `user_rows=${user_rows}`,
        `payment_rows=${payment_rows}`,
        `user_id=${user_id ?? ""}`,
        `status=${status ?? ""}`,
        `qty=${qty ?? ""}`,
        `unit=${unit_cents ?? ""}`,
        `delta=${delta_cents ?? ""}`,
        `action=${action}`,
        `reason=${reason ?? ""}`
      );
    }

    return {
      ok: true,
      action,
      reason,
      history_rows,
      user_rows,
      payment_rows,
      user_id,
      status,
      delta_cents,
      qty,
      unit_cents,
      already_in_ledger,
      already_credited,
      errCode: null,
      errMsg: null,
    };
  } catch (e) {
    // Segurança operacional: não quebrar fluxo existente caso migration ainda não tenha rodado
    const code = e?.code || null;
    const msg = e?.message || String(e);
    const action = code === "42P01" || code === "42703" || code === "42804" ? "not_supported" : "error";

    // WARN mínimo (sempre) para rastrear em produção sem flood
    // eslint-disable-next-line no-console
    console.warn("[coupon.credit] WARN", {
      action,
      paymentId: pid,
      userId: null,
      channel,
      source: options?.source || null,
      runTraceId: runTraceId || null,
      unit_cents: unit,
      errCode: code,
      errMsg: msg,
    });

    return {
      ok: false,
      action,
      reason: null,
      history_rows: 0,
      user_rows: 0,
      payment_rows: 0,
      user_id: null,
      status: null,
      delta_cents: null,
      qty: null,
      unit_cents: unit,
      already_in_ledger: false,
      already_credited: false,
      errCode: code,
      errMsg: msg,
    };
  }
}

