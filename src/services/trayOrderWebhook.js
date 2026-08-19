// src/services/trayOrderWebhook.js
//
// Fase G (P0, item 32/34-ish do pedido): reconciliacao de gasto DIRETO do
// cupom individual na Tray (fora da Loja NS) — o gap que permitia um
// cliente gastar o cupom no checkout normal da Tray sem que
// users.coupon_value_cents (nosso saldo canonico) fosse debitado, e que o
// proximo login (ensureTrayCouponForUser) "ressuscitasse" um cupom Tray
// ja gasto com o valor antigo, ainda maior, do nosso banco.
//
// Auditoria (rodada 2, GET /discount_coupons/:id real contra producao —
// 17 cupons reais amostrados): a Tray DOCUMENTA `usage_counter`/`usage_sum`
// no GET, mas o mecanismo de controle de reuso configurado para os cupons
// da NewStore e por CONTAGEM, nao por valor: usage_counter_limit=1 e
// usage_counter_limit_customer=1 em TODOS os cupons amostrados (o cupom so
// pode ser APLICADO uma unica vez, nunca reaplicado depois). `usage_sum`/
// `usage_sum_limit` existem no schema mas aparecem sempre zerados/inativos
// para essa configuracao especifica, e a documentacao curada da Tray
// (skills/cupons) nunca os menciona — nao ha evidencia de que sejam usados
// aqui. Nenhum dos 17 cupons amostrados ja foi gasto (usage_counter=0 em
// todos), entao NAO existe um exemplo real de "cupom usado" para observar
// diretamente o valor de desconto concedido numa aplicacao unica.
//
// Por isso: o cupom so pode ser aplicado UMA vez (contagem), mas o VALOR do
// desconto concedido nessa unica aplicacao pode ser MENOR que o `value`
// total do cupom (comportamento padrao de cupom de valor fixo: desconto =
// min(value, subtotal do pedido)). Debitamos exatamente o `discount` do
// pedido — NUNCA zeramos o saldo inteiro as cegas. Isso e seguro nos dois
// cenarios possiveis: se o consumo for sempre integral, discount==saldo e
// o resultado e identico a zerar; se for parcial, o saldo remanescente fica
// corretamente preservado (e continua resgatavel via Loja NS, que nao usa
// o cupom Tray — cria pedido direto por customer_id).
//
// A Tray NAO expoe um contador de "usos restantes" de forma diretamente
// comparavel a um evento de webhook — o unico jeito documentado de saber
// que um cupom foi usado E QUANTO foi descontado e observar o pedido onde
// ele foi aplicado (campos coupon_code/discount), via consulta apos o
// webhook de escopo `order` (insert/update). Por isso este arquivo NUNCA
// confia no payload do webhook sozinho (que so traz seller_id/scope_id/act)
// — sempre busca o pedido real via GET /orders/:id/full antes de mexer em
// saldo.
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
 * aplicado, nao so digitado sem efeito). Debita EXATAMENTE o `discount` do
 * pedido (parsing monetario seguro, nunca float ingenuo) — nunca zera o
 * saldo inteiro as cegas. Se o desconto exceder o saldo local (inconsistencia
 * financeira real), NUNCA mascara com Math.max(0,...): marca a anomalia e
 * nao debita nada, para investigacao manual.
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

  if (order.discountCents == null || order.discountCents <= 0) {
    // discount>0 mas nao conseguimos parsear o valor monetario com
    // seguranca -- nunca adivinhar, nunca debitar um valor inventado.
    return { handled: false, reason: "discount_unparseable", anomaly: true };
  }

  const user = await findUserByCouponCode(d.query, order.couponCode);
  if (!user) {
    return { handled: false, reason: "coupon_code_not_ours" };
  }

  if (user.coupon_value_cents <= 0) {
    return { handled: false, reason: "balance_already_zero" };
  }

  // Nao pre-checa discount > saldo aqui: applyCouponLedgerEntry ja resolve
  // idempotencia (replay) ANTES de checar saldo, dentro da MESMA transacao
  // atomica. Pre-checar aqui fora da transacao classificaria erroneamente
  // um retry legitimo (apos um credito novo ja ter reduzido a "folga" do
  // saldo) como anomalia financeira, quando na verdade e so um replay que
  // o ledger resolveria corretamente. O ledger NUNCA mascara com
  // Math.max(0,...) — se faltar saldo de verdade, ele lanca
  // insufficient_balance e nada e debitado (ver catch abaixo).
  try {
    const result = await applyCouponLedgerEntry(
      {
        userId: user.id,
        operation: "debit",
        amountCents: order.discountCents,
        eventType: "DIRECT_TRAY_SPEND",
        idempotencyKey: `tray-order-webhook:${payload.scopeId}`,
        meta: { tray_order_id: payload.scopeId, coupon_code: order.couponCode, discount: order.discount, discount_cents: order.discountCents },
      },
      { query: d.query, withTransaction: d.withTransaction }
    );
    return { handled: true, replayed: result.replayed, userId: user.id, balance_cents: result.balance_cents };
  } catch (e) {
    // insufficient_balance cobre dois cenarios que, do lado de fora, sao
    // indistinguiveis com seguranca: (a) corrida legitima -- outra operacao
    // (ex.: resgate Loja NS) ja reduziu o saldo entre a leitura e o debito;
    // (b) anomalia financeira real -- a Tray registrou um desconto maior
    // que qualquer saldo que autorizamos. Em AMBOS os casos o comportamento
    // seguro e identico: nada e debitado, nada e mascarado com
    // Math.max(0,...), o saldo fica intocado para investigacao manual.
    if (e instanceof CouponLedgerError && (e.code === "insufficient_balance" || e.code === "coupon_expired")) {
      return { handled: false, reason: "balance_changed_concurrently" };
    }
    throw e;
  }
}
