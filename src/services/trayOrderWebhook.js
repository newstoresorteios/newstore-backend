// src/services/trayOrderWebhook.js
//
// Fase G (P0, item 32/34-ish do pedido): reconciliacao de gasto DIRETO do
// cupom individual na Tray (fora da Loja NS) — o gap que permitia um
// cliente gastar o cupom no checkout normal da Tray sem que
// users.coupon_value_cents (nosso saldo canonico) fosse debitado, e que o
// proximo login (ensureTrayCouponForUser) "ressuscitasse" um cupom Tray
// ja gasto com o valor antigo, ainda maior, do nosso banco.
//
// Auditoria (Fase A/G, ver skills/cupons e skills/webhooks do
// tray-api-ai-plugin, fonte oficial): a Tray NAO expoe um contador de
// "usos restantes" via GET /discount_coupons/:id — o UNICO jeito
// documentado de saber que um cupom foi usado e observar o pedido onde ele
// foi aplicado (campos coupon_code/discount), via consulta apos o webhook
// de escopo `order` (insert/update). Por isso este arquivo NUNCA confia no
// payload do webhook sozinho (que so traz seller_id/scope_id/act) — sempre
// busca o pedido real via GET /orders/:id/full antes de mexer em saldo.
//
// Ativacao: o escopo `order` precisa ser habilitado via chamado no suporte
// Tray informando a URL deste endpoint. Ate isso acontecer, esta rota
// nunca recebe trafego real — fica pronta e testada, documentada como
// BLOQUEADA POR CONFIGURACAO EXTERNA no relatorio.

import { query as defaultQuery, getPool } from "../db.js";
import { applyCouponLedgerEntry, CouponLedgerError } from "./couponLedger.js";
import { getTrayOrderFull } from "./trayOrderClient.js";

async function defaultWithTransaction(fn) {
  const pool = await getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const out = await fn(client);
    await client.query("COMMIT");
    return out;
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    client.release();
  }
}

const REQUIRED_FIELDS = ["seller_id", "scope_id", "scope_name", "act"];

export class TrayWebhookError extends Error {
  constructor(code, { status = 400 } = {}) {
    super(code);
    this.name = "TrayWebhookError";
    this.code = code;
    this.status = status;
  }
}

/**
 * Valida o payload form-urlencoded documentado
 * (seller_id, scope_id, scope_name, act, app_code, url_notification).
 * Nunca aceita JSON nem outro formato — o formato oficial e sempre
 * application/x-www-form-urlencoded.
 */
export function parseOrderWebhookPayload(body) {
  const b = body && typeof body === "object" ? body : {};
  for (const field of REQUIRED_FIELDS) {
    if (b[field] == null || String(b[field]).trim() === "") {
      throw new TrayWebhookError("webhook_field_missing", { status: 400 });
    }
  }
  return {
    sellerId: String(b.seller_id).trim(),
    scopeId: String(b.scope_id).trim(),
    scopeName: String(b.scope_name).trim(),
    act: String(b.act).trim(),
  };
}

function assertExpectedSeller(sellerId, deps) {
  const expected = String(deps.expectedSellerId || "").trim();
  if (!expected) return; // nao configurado ainda — nao bloqueia, so nao valida (item documentado no relatorio)
  if (expected !== String(sellerId).trim()) {
    throw new TrayWebhookError("webhook_seller_mismatch", { status: 401 });
  }
}

async function findUserByCouponCode(query, couponCode) {
  const { rows } = await query(
    `select id, coalesce(coupon_value_cents,0)::int as coupon_value_cents
       from public.users where coupon_code = $1 limit 1`,
    [couponCode]
  );
  return rows[0] || null;
}

function resolveDeps(deps = {}) {
  return {
    query: deps.query || defaultQuery,
    withTransaction: deps.withTransaction || defaultWithTransaction,
    getTrayOrderFull: deps.getTrayOrderFull || getTrayOrderFull,
    expectedSellerId: deps.expectedSellerId ?? process.env.TRAY_WEBHOOK_SELLER_ID,
  };
}

/**
 * Ponto de entrada do webhook de pedido. Idempotente por orderId (reusa a
 * UNIQUE de coupon_balance_history.idempotency_key — mesmo mecanismo da
 * saga de resgate, nao um novo).
 *
 * So age quando: scope_name === 'order', o pedido tem coupon_code que bate
 * com um users.coupon_code EXATO, e discount > 0 (cupom efetivamente
 * aplicado, nao so digitado sem efeito). Zera o saldo local inteiro — o
 * cupom individual e single-use (usage_counter_limit=1, ver
 * trayCouponEnsure.js), entao uma vez aplicado em QUALQUER pedido Tray ele
 * nunca pode ser usado de novo, independente do valor exato do desconto.
 */
export async function handleTrayOrderWebhook(rawBody, deps = {}) {
  const d = resolveDeps(deps);
  const payload = parseOrderWebhookPayload(rawBody);
  assertExpectedSeller(payload.sellerId, d);

  if (payload.scopeName !== "order") {
    return { handled: false, reason: "scope_not_order" };
  }

  const order = await d.getTrayOrderFull(payload.scopeId);
  if (!order.couponCode || !(order.discount > 0)) {
    return { handled: false, reason: "no_coupon_discount_applied" };
  }

  const user = await findUserByCouponCode(d.query, order.couponCode);
  if (!user) {
    return { handled: false, reason: "coupon_code_not_ours" };
  }

  if (user.coupon_value_cents <= 0) {
    return { handled: false, reason: "balance_already_zero" };
  }

  try {
    const result = await applyCouponLedgerEntry(
      {
        userId: user.id,
        operation: "debit",
        amountCents: user.coupon_value_cents,
        eventType: "DIRECT_TRAY_SPEND",
        idempotencyKey: `tray-order-webhook:${payload.scopeId}`,
        meta: { tray_order_id: payload.scopeId, coupon_code: order.couponCode, discount: order.discount },
      },
      { query: d.query, withTransaction: d.withTransaction }
    );
    return { handled: true, replayed: result.replayed, userId: user.id, balance_cents: result.balance_cents };
  } catch (e) {
    // Corrida legitima: outra operacao (ex.: resgate Loja NS) ja mudou o
    // saldo entre a leitura acima e o debito. Nao e uma falha do webhook —
    // o saldo ja reflete OUTRA reducao valida; nao ha nada a corrigir.
    if (e instanceof CouponLedgerError && (e.code === "insufficient_balance" || e.code === "coupon_expired")) {
      return { handled: false, reason: "balance_changed_concurrently" };
    }
    throw e;
  }
}
