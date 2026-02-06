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

  let unitCents = Number(
    options?.unitCents ??
      options?.meta?.unit_cents ??
      process.env.COUPON_UNIT_CENTS ??
      5500
  );
  if (!Number.isFinite(unitCents) || unitCents <= 0) unitCents = 5500;

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
      unit_cents: unitCents,
      already_in_ledger: false,
      already_credited: false,
      errCode: null,
      errMsg: null,
    };
  }

  // 1 SQL atômico (CTE):
  // - GATE de idempotência: coupon_balance_history UNIQUE(payment_id,event_type)
  // - Lock no usuário (FOR UPDATE) para balance_before correto
  // - NUNCA marca payments.coupon_credited=true sem:
  //   (a) ter inserido ledger nesta execução, OU
  //   (b) já existir ledger CREDIT_PURCHASE para este payment
  // - Não depende de colunas opcionais em payments (ex.: reservation_id)
  const sql = `
    WITH pi AS (
      SELECT
        id,
        user_id,
        numbers,
        lower(status) AS status_l,
        coupon_credited
      FROM public.payments
      WHERE id = $1
      LIMIT 1
    ),
    calc AS (
      SELECT
        pi.*,
        COALESCE(array_length(pi.numbers, 1), 0)::int AS qty,
        $5::int AS unit_cents,
        (COALESCE(array_length(pi.numbers, 1), 0) * $5::int)::int AS delta_cents
      FROM pi
    ),
    ul AS (
      SELECT
        u.id AS user_id,
        COALESCE(u.coupon_value_cents, 0)::int AS balance_before_cents
      FROM public.users u
      JOIN calc c ON c.user_id = u.id
      FOR UPDATE
    ),
    h AS (
      INSERT INTO public.coupon_balance_history
        (user_id, payment_id, delta_cents, balance_before_cents, balance_after_cents,
         event_type, channel, status, draw_id, reservation_id, run_trace_id, meta)
      SELECT
        ul.user_id,
        c.id,
        c.delta_cents,
        ul.balance_before_cents,
        (ul.balance_before_cents + c.delta_cents),
        'CREDIT_PURCHASE',
        $2,
        c.status_l,
        NULL::int4,
        NULL::text,
        $3,
        ($4::jsonb || jsonb_build_object(
          'unit_cents', c.unit_cents,
          'qty', c.qty,
          'source', ($4::jsonb->>'source'),
          'channel', $2
        ))
      FROM calc c
      JOIN ul ON true
      WHERE c.status_l IN ('approved','paid','pago')
        AND COALESCE(c.coupon_credited, false) = false
        AND c.qty > 0
        AND c.delta_cents > 0
      ON CONFLICT DO NOTHING
      RETURNING user_id, payment_id, delta_cents
    ),
    u_upd AS (
      UPDATE public.users usr
         SET coupon_value_cents = usr.coupon_value_cents + h.delta_cents,
             coupon_updated_at  = now()
        FROM h
       WHERE usr.id = h.user_id
       RETURNING usr.id AS user_id
    ),
    p_upd AS (
      UPDATE public.payments pay
         SET coupon_credited = true,
             coupon_credited_at = now()
        FROM calc c
       WHERE pay.id = c.id
         AND lower(pay.status) IN ('approved','paid','pago')
         AND COALESCE(pay.coupon_credited, false) = false
         AND (
           EXISTS (SELECT 1 FROM h)
           OR EXISTS (
             SELECT 1
             FROM public.coupon_balance_history hh
             WHERE hh.payment_id = c.id
               AND hh.event_type = 'CREDIT_PURCHASE'
           )
         )
      RETURNING pay.id
    )
    SELECT
      (SELECT count(*)::int FROM h) AS history_rows,
      (SELECT count(*)::int FROM u_upd) AS user_rows,
      (SELECT count(*)::int FROM p_upd) AS payment_rows,
      (SELECT c.user_id::int FROM calc c LIMIT 1) AS user_id,
      (SELECT c.status_l::text FROM calc c LIMIT 1) AS status_l,
      (SELECT c.qty::int FROM calc c LIMIT 1) AS qty,
      (SELECT c.unit_cents::int FROM calc c LIMIT 1) AS unit_cents,
      (SELECT c.delta_cents::int FROM calc c LIMIT 1) AS delta_cents,
      (SELECT COALESCE(EXISTS(
        SELECT 1
        FROM public.coupon_balance_history hh
        WHERE hh.payment_id = pi.id
          AND hh.event_type = 'CREDIT_PURCHASE'
      ), false) FROM pi) AS already_in_ledger,
      (SELECT COALESCE(pi.coupon_credited, false) FROM pi) AS already_credited
  `;

  try {
    const { rows } = await q(sql, [pid, channel, runTraceId, metaJson, unitCents]);
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

    let action = "noop";
    let reason = null;

    if (history_rows === 1 && user_rows === 1) {
      action = "credited";
      reason = null;
    } else if (!isFinal) {
      action = "noop";
      reason = "not_final";
    } else if (noNumbers) {
      action = "noop";
      reason = "no_numbers";
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
      unit_cents: unitCents,
      already_in_ledger: false,
      already_credited: false,
      errCode: code,
      errMsg: msg,
    };
  }
}

