// src/services/trayPaymentClient.js
//
// Payment REAL do resgate na Tray — POST /payments.
//
// REGRA DE NEGOCIO (2026-09-09): supera a decisao de 31/08 ("o fluxo nao cria
// Payment na Tray; has_payment pode continuar 0"). A partir daqui, todo
// resgate NOVO liquidado com NSCreditos precisa de um Payment de verdade no
// pedido Tray, e o resgate so vira `confirmed` quando o proprio pedido
// devolve `has_payment === "1"`.
//
// O QUE ESTA CAMADA NAO FAZ:
//   - NAO converte NSCreditos em reais. NSCreditos sao ledger interno da
//     NewStore; o `payment.value` e SEMPRE o valor monetario factual do
//     pedido Tray (`Order.total`, lido por GET /orders/:id pelo chamador).
//   - NAO define `has_payment` manualmente no payload do Order. Quem marca
//     isso e a propria Tray, ao receber o Payment.
//   - NAO inventa gateway: `method` representa "NSCreditos", nunca
//     pix/boleto/cartao.
//   - NAO duplica OAuth/HTTP: leitura por trayCatalogGet, escrita por
//     trayMutationRequest (allow-list TRAY_REDEMPTION_PAYMENT_CREATE/POST).
//
// IDEMPOTENCIA: a Tray nao documenta idempotency-key em POST /payments, e nao
// assumimos que exista. A identidade do pagamento e um MARKER deterministico
// derivado do redemption_id, gravado em `payment.note`. Antes de qualquer
// POST procuramos esse marker em GET /payments?order_id=... — e depois de um
// resultado AMBIGUO (timeout/rede) a unica pergunta legitima e "a Tray
// persistiu?", respondida por um novo GET. Nunca um POST repetido as cegas.

import { trayCatalogGet, TrayCatalogError, TRAY_MAX_LIMIT } from "./trayCatalogClient.js";
import { trayMutationRequest } from "./trayMutationClient.js";
import { LOJA_NS_ORDER_DEFAULTS } from "./trayOrderClient.js";

/**
 * Forma de liquidacao declarada no Payment. Mesma string ja usada em
 * `Order.payment_form` (LOJA_NS_ORDER_DEFAULTS) — uma unica fonte de verdade.
 * ATENCAO: `Order.payment_form = "NSCréditos"` NAO representa pagamento
 * confirmado; quem representa e o Payment criado aqui.
 */
export const TRAY_REDEMPTION_PAYMENT_METHOD = LOJA_NS_ORDER_DEFAULTS.payment_form;

/** Prefixo do marker deterministico gravado em `payment.note`. */
export const TRAY_REDEMPTION_PAYMENT_MARKER_PREFIX = "LOJA_NS_REDEMPTION";

const SAO_PAULO_TIME_ZONE = "America/Sao_Paulo";

/** Timeout/rede: a Tray pode ou nao ter persistido. Nunca repetir as cegas. */
const AMBIGUOUS_TRAY_CODES = new Set(["tray_timeout", "tray_unreachable"]);

/**
 * Identidade deterministica do Payment do resgate. Mesmo redemption_id =>
 * mesmo marker, sempre — e por isso um replay encontra o pagamento existente
 * em vez de criar outro.
 */
export function buildRedemptionPaymentMarker(redemptionId) {
  const id = String(redemptionId ?? "").trim();
  if (!id) throw new TrayCatalogError("redemption_id_missing", { status: 400 });
  return `${TRAY_REDEMPTION_PAYMENT_MARKER_PREFIX}:${id}`;
}

/**
 * Valor monetario canonico para COMPARACAO entre o total do pedido e o valor
 * de um Payment ja existente.
 *
 * Aceita casas decimais extras SOMENTE quando sao zeros ("489.9900" ===
 * "489.99"): isso e o mesmo dinheiro escrito com outra precisao. Qualquer
 * outra coisa devolve "" — e comparar com "" nunca resulta em igualdade, ou
 * seja, na duvida a comparacao FALHA (fail-closed) em vez de arredondar.
 * Nunca ha aritmetica de float aqui.
 */
export function normalizeTrayPaymentValue(raw) {
  if (raw == null || raw === "") return "";
  const s = String(raw).trim().replace(",", ".");
  const m = /^(\d+)(?:\.(\d+))?$/.exec(s);
  if (!m) return "";
  const frac = m[2] || "";
  const cents = frac.slice(0, 2).padEnd(2, "0");
  // Precisao extra so e aceitavel se nao carregar dinheiro nenhum.
  if (frac.length > 2 && /[^0]/.test(frac.slice(2))) return "";
  return `${String(Number(m[1]))}.${cents}`;
}

/** Os dois valores representam exatamente o mesmo dinheiro? */
function sameMoney(a, b) {
  const left = normalizeTrayPaymentValue(a);
  const right = normalizeTrayPaymentValue(b);
  return Boolean(left) && left === right;
}

/**
 * Data do pagamento em YYYY-MM-DD no fuso da loja (America/Sao_Paulo) —
 * usar UTC gravaria o dia seguinte para qualquer resgate feito a noite no
 * Brasil. Mesmo idioma ja usado nas notificacoes de saldo.
 */
export function resolveTrayPaymentDate(now = new Date()) {
  const when = now instanceof Date && !Number.isNaN(now.getTime()) ? now : new Date();
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: SAO_PAULO_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(when);
  const byType = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${byType.year}-${byType.month}-${byType.day}`;
}

/**
 * Desembrulha `{ Payment: {...} }` e tambem o objeto ja cru.
 *
 * CONTRATO FACTUAL (auditoria read-only da loja real, 2026-09-09): o Payment
 * devolvido pela Tray tem os campos
 *   created, modified, id, order_id, payment_method_id, payment_place,
 *   value, date, note
 * — ou seja, **nao existe `method` na leitura**. O rotulo do meio de pagamento
 * vem em `payment_place`; `method` so aparece no corpo que ENVIAMOS no POST.
 * Por isso `method` aqui e derivado de `payment_place` (com fallback para
 * `method`, caso a Tray algum dia passe a ecoar o campo enviado).
 *
 * A correspondencia do resgate NAO depende disso: ela usa order_id + marker
 * em `note` + valor, que existem de fato nos dois lados.
 */
export function normalizeTrayPayment(entry) {
  const raw = entry && typeof entry === "object" && entry.Payment && typeof entry.Payment === "object" ? entry.Payment : entry;
  if (!raw || typeof raw !== "object") return null;
  const label = raw.payment_place ?? raw.method;
  return {
    id: raw.id != null ? String(raw.id) : null,
    orderId: raw.order_id != null ? String(raw.order_id) : null,
    method: label != null ? String(label) : null,
    paymentMethodId: raw.payment_method_id != null ? String(raw.payment_method_id) : null,
    value: raw.value != null ? String(raw.value) : null,
    date: raw.date != null ? String(raw.date) : null,
    note: raw.note != null ? String(raw.note) : "",
    raw,
  };
}

/**
 * GET /payments?order_id=... — leitura pura pelo mesmo cliente read-only do
 * catalogo (mesma auth, mesmo timeout, mesma trava de somente-GET).
 *
 * Formato desconhecido NAO vira lista vazia: responder "nao existe Payment"
 * sem ter certeza levaria a um POST duplicado. Falha fechado.
 */
export async function listTrayPaymentsByOrder(orderId, options = {}) {
  const id = String(orderId ?? "").trim();
  if (!id) throw new TrayCatalogError("order_id_missing", { status: 400 });

  // `limit` explicito: o default da Tray nesta rota e 30 (maxLimit 50,
  // confirmado na loja real). Um pedido de resgate tem 0 ou 1 pagamento, mas
  // paginar por engano seria ler "nao existe Payment" e duplicar o POST.
  const body = await trayCatalogGet("/payments", { order_id: id, limit: TRAY_MAX_LIMIT }, options);

  const rows = Array.isArray(body)
    ? body
    : Array.isArray(body?.Payments)
      ? body.Payments
      : Array.isArray(body?.Payment)
        ? body.Payment
        : body?.Payment && typeof body.Payment === "object"
          ? [body.Payment]
          : null;

  if (!rows) {
    throw new TrayCatalogError("tray_payment_list_invalid", {
      status: 502,
      publicDetails: { order_id: id, keys: body && typeof body === "object" ? Object.keys(body).slice(0, 10) : null },
    });
  }

  return rows.map(normalizeTrayPayment).filter(Boolean);
}

/**
 * Procura o Payment DESTE resgate na listagem do pedido.
 *
 * Validamos os tres eixos exigidos: `order_id`, o marker em `note` e o valor
 * esperado. Marker certo com valor divergente NAO e aceito em silencio nem
 * "corrigido" com um segundo Payment — e um estado que precisa de auditoria
 * humana (`tray_payment_value_mismatch`).
 *
 * @returns {object|null} o Payment normalizado, ou null se este resgate ainda
 *   nao tem pagamento neste pedido.
 */
export function findExistingRedemptionPayment({ payments, orderId, redemptionId, expectedValue } = {}) {
  const marker = buildRedemptionPaymentMarker(redemptionId);
  const id = String(orderId ?? "").trim();

  const mine = (Array.isArray(payments) ? payments : []).filter(
    (p) => p && String(p.orderId ?? "") === id && String(p.note ?? "").includes(marker)
  );
  if (!mine.length) return null;

  const matching = mine.filter((p) => sameMoney(p.value, expectedValue));
  if (!matching.length) {
    throw new TrayCatalogError("tray_payment_value_mismatch", {
      status: 502,
      publicDetails: {
        tray_order_id: id,
        expected_value: normalizeTrayPaymentValue(expectedValue) || String(expectedValue ?? ""),
        found_values: mine.map((p) => String(p.value ?? "")),
      },
    });
  }

  return matching[0];
}

/**
 * POST /payments — cadastro do pagamento do resgate.
 *
 * Estrutura oficial da Tray, enviada exatamente como esta:
 *   { payment: { order_id, method, value, date, note } }
 *
 * `value` chega pronto do chamador e vem de `Order.total` (GET /orders/:id).
 * Se nao for um valor monetario reconhecivel, falhamos ANTES da rede — um
 * pagamento com valor errado corrompe um pedido real.
 */
export async function createTrayRedemptionPayment({ orderId, redemptionId, value, date } = {}, options = {}) {
  const id = String(orderId ?? "").trim();
  if (!id) throw new TrayCatalogError("order_id_missing", { status: 400 });

  const note = buildRedemptionPaymentMarker(redemptionId);

  const amount = normalizeTrayPaymentValue(value);
  if (!amount) {
    throw new TrayCatalogError("tray_payment_value_invalid", { status: 400, publicDetails: { tray_order_id: id } });
  }

  const paymentDate = /^\d{4}-\d{2}-\d{2}$/.test(String(date || "")) ? String(date) : resolveTrayPaymentDate();

  // ENVELOPE `Payment` MAIUSCULO. Provado contra a loja real (smoke
  // controlado 2026-09-09, pedido 25894): com `{ payment: {...} }` a Tray
  // responde 400 e as `causes` vem como
  //   Payment.value / Payment.method / Payment.order_id =
  //   "Este campo nao pode ser deixado em branco."
  // ou seja, ela NAO enxerga o objeto minusculo e trata tudo como ausente.
  // Mesmo padrao ja conhecido nesta API em `Order` e `ProductsSold`.
  // As `causes` tambem confirmam que `method` e um campo valido de ESCRITA
  // (na LEITURA o rotulo volta como `payment_place`).
  const body = {
    Payment: {
      order_id: id,
      method: TRAY_REDEMPTION_PAYMENT_METHOD,
      value: amount,
      date: paymentDate,
      note,
    },
  };

  return trayMutationRequest("TRAY_REDEMPTION_PAYMENT_CREATE", "POST", "/payments", body, options);
}

/**
 * Garante EXATAMENTE UM Payment do resgate no pedido Tray.
 *
 *   1. GET /payments?order_id=... — ja existe pagamento deste resgate?
 *        SIM -> reutiliza, zero POST.
 *   2. NAO -> POST /payments (uma unica vez).
 *   3. Confirma por GET que o Payment existe de verdade.
 *
 * Resultado AMBIGUO no POST (timeout/rede): NUNCA repetimos o POST. Fazemos
 * um GET e perguntamos a Tray se ela persistiu:
 *   - encontrou -> criacao confirmada, seguimos;
 *   - nao encontrou -> `tray_payment_unconfirmed`, que o chamador transforma
 *     em reconciliacao (pedido preservado, creditos preservados, nenhuma
 *     compensacao automatica).
 *
 * @returns {Promise<{payment: object, created: boolean, reconciled: boolean}>}
 */
export async function ensureTrayRedemptionPayment({ orderId, redemptionId, value, date } = {}, options = {}) {
  const id = String(orderId ?? "").trim();
  if (!id) throw new TrayCatalogError("order_id_missing", { status: 400 });
  const marker = buildRedemptionPaymentMarker(redemptionId);

  const find = async () =>
    findExistingRedemptionPayment({
      payments: await listTrayPaymentsByOrder(id, options),
      orderId: id,
      redemptionId,
      expectedValue: value,
    });

  const existing = await find();
  if (existing) return { payment: existing, created: false, reconciled: false };

  let ambiguous = false;
  try {
    await createTrayRedemptionPayment({ orderId: id, redemptionId, value, date }, options);
  } catch (e) {
    // Erro deterministico (400/401/404/5xx) sobe como esta: sabemos que nao
    // ha pagamento pendente de descoberta. So timeout/rede e ambiguo.
    if (!(e instanceof TrayCatalogError && AMBIGUOUS_TRAY_CODES.has(e.code))) throw e;
    ambiguous = true;
  }

  // Confirmacao factual — vale tanto para o POST aceito quanto para o
  // ambiguo. Nenhum segundo POST sai daqui em hipotese alguma.
  let confirmed = null;
  try {
    confirmed = await find();
  } catch (e) {
    if (e instanceof TrayCatalogError && e.code === "tray_payment_value_mismatch") throw e;
    throw new TrayCatalogError("tray_payment_unconfirmed", {
      status: 502,
      publicDetails: { operation: "TRAY_REDEMPTION_PAYMENT_CREATE", tray_order_id: id, marker, cause: e?.code || null },
    });
  }

  if (!confirmed) {
    throw new TrayCatalogError("tray_payment_unconfirmed", {
      status: 502,
      publicDetails: {
        operation: "TRAY_REDEMPTION_PAYMENT_CREATE",
        tray_order_id: id,
        marker,
        cause: ambiguous ? "tray_timeout" : "tray_payment_not_found_after_create",
      },
    });
  }

  return { payment: confirmed, created: true, reconciled: ambiguous };
}
