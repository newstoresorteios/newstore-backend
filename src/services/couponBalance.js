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
 * @returns {Promise<{ok:boolean, credited_rows:number, history_rows:number, action:string, invalid_amount?:boolean}>}
 */
export async function creditCouponOnApprovedPayment(paymentId, options = {}) {
  const pid = paymentId != null ? String(paymentId) : "";
  const channel = options?.channel != null ? String(options.channel) : null;
  const runTraceId = options?.runTraceId != null ? String(options.runTraceId) : null;
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
  // - valida amount_cents > 0 para não creditar errado
  // - soma users.coupon_value_cents
  // - insere ledger (ON CONFLICT DO NOTHING)
  const sql = `
    WITH pi AS (
      SELECT id, user_id, amount_cents, draw_id, reservation_id,
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
         AND pi.status_l = 'approved'
         AND pi.coupon_credited = false
         AND COALESCE(pi.amount_cents, 0) > 0
       RETURNING pay.id, pi.user_id, pi.amount_cents, pi.draw_id, pi.reservation_id
    ),
    u AS (
      UPDATE public.users usr
         SET coupon_value_cents = usr.coupon_value_cents + p.amount_cents,
             coupon_updated_at  = now()
        FROM p
       WHERE usr.id = p.user_id
       RETURNING
         usr.id AS user_id,
         (usr.coupon_value_cents - p.amount_cents) AS balance_before_cents,
         usr.coupon_value_cents AS balance_after_cents,
         p.id AS payment_id,
         p.amount_cents,
         p.draw_id,
         p.reservation_id
    ),
    h AS (
      INSERT INTO public.coupon_balance_history
        (user_id, payment_id, delta_cents, balance_before_cents, balance_after_cents,
         event_type, channel, status, draw_id, reservation_id, run_trace_id, meta)
      SELECT
        u.user_id,
        u.payment_id,
        u.amount_cents,
        u.balance_before_cents,
        u.balance_after_cents,
        'CREDIT_PURCHASE',
        $2,
        'approved',
        u.draw_id,
        u.reservation_id,
        $3,
        $4::jsonb
      FROM u
      ON CONFLICT DO NOTHING
      RETURNING id
    )
    SELECT
      (SELECT count(*)::int FROM p) AS credited_rows,
      (SELECT count(*)::int FROM h) AS history_rows,
      (SELECT COALESCE((pi.status_l = 'approved' AND pi.coupon_credited = false AND COALESCE(pi.amount_cents,0) <= 0), false) FROM pi) AS invalid_amount
  `;

  try {
    const { rows } = await q(sql, [pid, channel, runTraceId, metaJson]);
    const credited_rows = rows?.[0]?.credited_rows ?? 0;
    const history_rows = rows?.[0]?.history_rows ?? 0;
    const invalid_amount = !!rows?.[0]?.invalid_amount;

    if (isDebugEnabled()) {
      // logs enxutos, sem poluir produção
      // eslint-disable-next-line no-console
      console.log("[coupon.credit]", `payment=${pid}`, `channel=${channel}`, `credited_rows=${credited_rows}`, `history_rows=${history_rows}`);
    }

    if (invalid_amount) {
      return { ok: false, credited_rows, history_rows, action: "invalid_amount", invalid_amount: true };
    }

    return { ok: true, credited_rows, history_rows, action: credited_rows === 1 ? "credited" : "noop" };
  } catch (e) {
    // Segurança operacional: não quebrar fluxo existente caso migration ainda não tenha rodado
    const code = e?.code || null;
    if (code === "42P01" || code === "42703") {
      // table/column missing
      if (isDebugEnabled()) {
        // eslint-disable-next-line no-console
        console.warn("[coupon.credit] not_supported", { payment: pid, code, msg: e?.message });
      }
      return { ok: false, credited_rows: 0, history_rows: 0, action: "not_supported" };
    }

    if (isDebugEnabled()) {
      // eslint-disable-next-line no-console
      console.warn("[coupon.credit] error", { payment: pid, code, msg: e?.message });
    }
    return { ok: false, credited_rows: 0, history_rows: 0, action: "error" };
  }
}

