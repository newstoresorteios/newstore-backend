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
 * @returns {Promise<{ok:boolean, credited_rows:number, history_rows:number, action:string, reason?:string|null, invalid_amount?:boolean, delta_cents?:number|null, qty?:number|null, unit_cents?:number|null, computed_delta_cents?:number|null, computed_qty?:number|null, computed_unit_cents?:number|null, already_in_ledger?:boolean}>}
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
    return { ok: false, credited_rows: 0, history_rows: 0, action: "invalid_payment_id" };
  }

  // 1 SQL atômico (CTE):
  // - trava crédito com payments.coupon_credited=false + status=approved
  // - calcula delta por qty*n (qty = array_length(numbers,1))
  // - NO-OP seguro quando qty<=0 (reason=no_numbers)
  // - soma users.coupon_value_cents
  // - insere ledger (ON CONFLICT DO NOTHING)
  const sql = `
    WITH pi AS (
      SELECT id, user_id, amount_cents, draw_id, reservation_id, numbers,
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
        CASE
          WHEN COALESCE(array_length(pi.numbers, 1), 0) > 0
            THEN (COALESCE(array_length(pi.numbers, 1), 0) * $5::int)
          ELSE 0
        END::int AS delta_cents
      FROM pi
    ),
    p AS (
      UPDATE public.payments pay
         SET coupon_credited = true,
             coupon_credited_at = now()
        FROM calc
       WHERE pay.id = calc.id
         AND calc.status_l IN ('approved','paid','pago')
         AND lower(pay.status) IN ('approved','paid','pago')
         AND pay.coupon_credited = false
         AND calc.coupon_credited = false
         AND calc.delta_cents > 0
         AND NOT EXISTS (
           SELECT 1
           FROM public.coupon_balance_history h
           WHERE h.payment_id = calc.id
             AND h.event_type = 'CREDIT_PURCHASE'
         )
       RETURNING
         pay.id,
         calc.user_id,
         calc.draw_id,
         calc.reservation_id,
         calc.qty,
         calc.unit_cents,
         calc.delta_cents,
         calc.status_l AS payment_status
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
         p.qty,
         p.unit_cents,
         p.draw_id,
         p.reservation_id,
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
        $2,
        u.payment_status,
        u.draw_id,
        u.reservation_id,
        $3,
        ($4::jsonb || jsonb_build_object(
          'unit_cents', u.unit_cents,
          'qty', u.qty,
          'source', ($4::jsonb->>'source'),
          'channel', $2
        ))
      FROM u
      ON CONFLICT DO NOTHING
      RETURNING id
    )
    SELECT
      (SELECT count(*)::int FROM p) AS credited_rows,
      (SELECT count(*)::int FROM h) AS history_rows,
      (SELECT delta_cents::int FROM calc LIMIT 1) AS computed_delta_cents,
      (SELECT qty::int FROM calc LIMIT 1) AS computed_qty,
      (SELECT unit_cents::int FROM calc LIMIT 1) AS computed_unit_cents,
      (SELECT COALESCE(EXISTS(
        SELECT 1
        FROM public.coupon_balance_history h
        WHERE h.payment_id = pi.id
          AND h.event_type = 'CREDIT_PURCHASE'
      ), false) FROM pi) AS already_in_ledger,
      (SELECT COALESCE((calc.status_l IN ('approved','paid','pago') AND calc.coupon_credited = false AND calc.qty <= 0), false) FROM calc) AS no_numbers
  `;

  try {
    const { rows } = await q(sql, [pid, channel, runTraceId, metaJson, unitCents]);
    const credited_rows = rows?.[0]?.credited_rows ?? 0;
    const history_rows = rows?.[0]?.history_rows ?? 0;
    const computed_delta_cents = rows?.[0]?.computed_delta_cents ?? null;
    const computed_qty = rows?.[0]?.computed_qty ?? null;
    const computed_unit_cents = rows?.[0]?.computed_unit_cents ?? null;
    const already_in_ledger = !!rows?.[0]?.already_in_ledger;
    const no_numbers = !!rows?.[0]?.no_numbers;
    const reason = no_numbers ? "no_numbers" : null;

    if (isDebugEnabled()) {
      // logs enxutos, sem poluir produção
      // eslint-disable-next-line no-console
      console.log(
        "[coupon.credit]",
        `payment=${pid}`,
        `channel=${channel}`,
        `credited_rows=${credited_rows}`,
        `history_rows=${history_rows}`,
        `qty=${computed_qty ?? ""}`,
        `unit=${computed_unit_cents ?? ""}`,
        `delta=${computed_delta_cents ?? ""}`,
        `reason=${reason ?? ""}`,
        `already_in_ledger=${already_in_ledger ? "1" : "0"}`
      );
    }

    return {
      ok: true,
      credited_rows,
      history_rows,
      action: reason || (credited_rows === 1 ? "credited" : "noop"),
      reason,
      delta_cents: computed_delta_cents,
      qty: computed_qty,
      unit_cents: computed_unit_cents,
      computed_delta_cents,
      computed_qty,
      computed_unit_cents,
      already_in_ledger,
    };
  } catch (e) {
    // Segurança operacional: não quebrar fluxo existente caso migration ainda não tenha rodado
    const code = e?.code || null;
    if (code === "42P01" || code === "42703") {
      // table/column missing
      if (isDebugEnabled()) {
        // eslint-disable-next-line no-console
        console.warn("[coupon.credit] not_supported", { payment: pid, code, msg: e?.message });
      }
      return {
        ok: false,
        credited_rows: 0,
        history_rows: 0,
        action: "not_supported",
        reason: null,
        delta_cents: null,
        qty: null,
        unit_cents: unitCents,
        computed_delta_cents: null,
        computed_qty: null,
        computed_unit_cents: unitCents,
        already_in_ledger: false,
      };
    }

    if (isDebugEnabled()) {
      // eslint-disable-next-line no-console
      console.warn("[coupon.credit] error", { payment: pid, code, msg: e?.message });
    }
    return {
      ok: false,
      credited_rows: 0,
      history_rows: 0,
      action: "error",
      reason: null,
      delta_cents: null,
      qty: null,
      unit_cents: unitCents,
      computed_delta_cents: null,
      computed_qty: null,
      computed_unit_cents: unitCents,
      already_in_ledger: false,
    };
  }
}

