// src/services/trayOrderClient.js
//
// Criação de pedido Tray REAL — POST /orders. Única mutação de pedido
// autorizada nesta camada (ver ALLOWED_MUTATIONS em trayCatalogClient.js).
//
// Contrato (schema oficial Tray, tray-tecnologia/tray-api-ai-plugin,
// skills/pedidos/schemas/pedido.create.json — cita
// https://developers.tray.com.br/#api-de-pedidos):
//   required: ["customer_id", "products"]
//   demais campos (shipping_method, shipping_cost, payment_method) são
//   OPCIONAIS — por isso NUNCA enviamos payment_method: não existe valor
//   documentado para "resgate sem cobrança real" (pix/boleto/cartão seriam
//   invenção), e o schema confirma que o campo pode simplesmente ser
//   omitido. Frete está fora do escopo desta fase (decisão do responsável)
//   — nunca enviamos shipping_method/shipping_cost.
//
// Identificação do resgate: campo oficial `notes` ("Observações livres do
// pedido") — é onde a Loja NS se identifica para quem olhar o pedido na
// Tray, sem usar nenhum campo fora do schema.
//
// Variação (P0 resolvido nesta rodada): a pagina real de docs
// (developers.tray.com.br, secao "Cadastrar Pedido#post", exemplo
// ProductsSold) confirma product_id e variant_id como campos SEPARADOS —
// nunca a variacao substituindo o produto. Cada item de `products` envia
// product_id sempre, e variant_id somente quando o item tem variacao.

import { trayMutationRequest } from "./trayMutationClient.js";
import { trayCatalogGet, TrayCatalogError } from "./trayCatalogClient.js";

let cachedOperationalStatus = null;

// Campos obrigatorios do Order descobertos empiricamente (400 real:
// "Este campo nao pode ser deixado em branco" para shipment e point_sale).
// Os valores abaixo sao DECISAO DE PRODUTO da Loja NS, nao invencao:
//
//   point_sale     origem factual do pedido (nao "PARTICULAR"/"LOJA VIRTUAL",
//                  que sao so exemplos da doc).
//   shipment       preenche o campo textual obrigatorio. NAO implementa frete:
//                  transportadora/cotacao/prazo/etiqueta/valor efetivo seguem
//                  sob responsabilidade da Tray.
//   shipment_value a NewStore nao cobra nem calcula frete nesta integracao.
//   payment_form   o beneficio foi quitado pelo saldo interno NSCreditos --
//                  nunca um meio de pagamento ficticio (PIX/cartao/boleto).
//
// Limites da doc: point_sale 45, shipment 100, payment_form 50.
// Auditoria read-only de 50 pedidos reais desta loja (total 707):
//   point_sale: "LOJA VIRTUAL" (45), "PARTICULAR" (5) -- nenhuma convencao
//               para pedido externo/API, entao usamos a origem factual.
//   shipment:   "Sedex" (49), "" (1) -- nenhuma convencao de "pendente",
//               entao usamos um rotulo explicito de logistica pendente.
//   shipment_value: "0.00" aparece em pedidos reais -- valor aceito.
export const LOJA_NS_ORDER_DEFAULTS = Object.freeze({
  point_sale: "LOJA NS",
  shipment: "PENDENTE TRAY",
  shipment_value: "0.00",
  payment_form: "NSCréditos",
});

// Tipo de pessoa no Customer da Tray. Confirmado lendo o Customer real
// 24858 desta loja: type "0" + cnpj vazio = pessoa fisica. Enviar "1" faz a
// Tray tratar como pessoa juridica e exigir cnpj (400 real observado).
export const TRAY_CUSTOMER_TYPE_PF = "0";

/**
 * Brasil em ISO-3 ("BRA"), como a estrutura oficial de POST /orders usa.
 * Aceita as variacoes que podem estar gravadas internamente (BR, Brasil,
 * BRASIL). Qualquer outro valor passa adiante em maiusculas — nunca
 * "adivinhamos" um pais diferente do que o usuario cadastrou.
 */
export function normalizeTrayCountry(raw) {
  const s = String(raw ?? "").trim();
  if (!s) return "";
  if (/^(br|bra|brasil|brazil)$/i.test(s)) return "BRA";
  return s.toUpperCase();
}

/**
 * @param {object} params
 * @param {string|number} params.customerId ID Tray do cliente (nunca users.id)
 * @param {{name?:string, email?:string, cpf?:string}} [params.customer] dados factuais do cliente
 * @param {Array<{trayProductId:string, trayVariantId?:string|null, quantity:number}>} params.items
 * @param {string} params.notes texto livre identificando o resgate (redemption_id, coupon_code)
 */
/**
 * session_id estavel derivado do redemption: se o POST der timeout, existe
 * uma identidade externa deterministica para procurar/reconciliar o pedido.
 * NUNCA aleatorio/timestamp -- isso nao correlacionaria com nada. Sem PII:
 * e so o UUID do redemption em hex. Pedidos reais desta loja usam 26 chars.
 */
/**
 * birth_date no formato que a Tray documenta: YYYY-MM-DD. Aceita Date (o
 * driver pg devolve Date para colunas `date`) ou string ja no formato.
 * Devolve "" quando nao da pra derivar com seguranca -- nunca inventa data.
 */
export function normalizeTrayBirthDate(raw) {
  if (!raw) return "";
  if (raw instanceof Date && !Number.isNaN(raw.getTime())) {
    // getUTC* evita que o fuso empurre a data um dia pra tras/frente.
    const y = raw.getUTCFullYear();
    const m = String(raw.getUTCMonth() + 1).padStart(2, "0");
    const d = String(raw.getUTCDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  }
  const s = String(raw).trim();
  const m = s.match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : "";
}

/**
 * Valor monetario no formato que a Tray aceita em ProductsSold ("299.99").
 * A Tray recusa com "Por favor, forneça um valor monetário válido." quando o
 * campo vem vazio/malformado. Devolve "" se nao der pra derivar com seguranca
 * -- nunca inventa preco (preco errado corrompe o total de um pedido real).
 */
export function normalizeTrayMoney(raw) {
  if (raw == null || raw === "") return "";
  const s = String(raw).trim().replace(",", ".");
  if (!/^\d+(\.\d{1,2})?$/.test(s)) return "";
  const n = Number(s);
  if (!Number.isFinite(n) || n < 0) return "";
  return n.toFixed(2);
}

export function buildTraySessionId(redemptionId) {
  const hex = String(redemptionId ?? "").replace(/[^a-zA-Z0-9]/g, "");
  return hex ? hex.slice(0, 26) : "";
}

export async function createTrayOrder({ customerId, customer, items, notes, address, sessionId }, options = {}) {
  const cid = Number(customerId);
  if (!Number.isFinite(cid) || cid <= 0) throw new TrayCatalogError("customer_id_invalid", { status: 400 });

  const list = Array.isArray(items) ? items : [];
  if (!list.length) throw new TrayCatalogError("order_items_empty", { status: 400 });

  const products = list.map((item) => {
    const productId = Number(item.trayProductId);
    const quantity = Number(item.quantity);
    if (!Number.isFinite(productId) || productId <= 0) throw new TrayCatalogError("order_item_product_id_invalid", { status: 400 });
    if (!Number.isFinite(quantity) || quantity <= 0) throw new TrayCatalogError("order_item_quantity_invalid", { status: 400 });

    // M7 (prova real): a Tray exige price E original_price em cada item --
    // "Por favor, forneça um valor monetário válido." quando ausentes. O valor
    // e o preco monetario REAL do catalogo Tray (dominio separado dos
    // NSCreditos), nunca convertido a partir do preco em creditos.
    const price = normalizeTrayMoney(item.trayPrice);
    if (!price) throw new TrayCatalogError("order_item_price_invalid", { status: 400, publicDetails: { product_id: productId } });

    const line = { product_id: productId, quantity, price, original_price: price };
    if (item.trayVariantId != null && String(item.trayVariantId).trim() !== "") {
      const variantId = Number(item.trayVariantId);
      if (!Number.isFinite(variantId) || variantId <= 0) throw new TrayCatalogError("order_item_variant_id_invalid", { status: 400 });
      line.variant_id = variantId;
    }
    return line;
  });

  // ENDERECO DE ENTREGA DO RESGATE — quem manda e a NewStore.
  //
  // Regra de negocio: o pedido carrega o endereco que o cliente escolheu
  // NAQUELE resgate (user_addresses), nao o que estiver cadastrado na Tray.
  // Se o cliente mudou de endereco depois, o historico do pedido nao muda.
  //
  // Posicao provada empiricamente (M7):
  //   Order.CustomerAddress          -> a Tray NAO le (reporta tudo em branco)
  //   Order.Customer.CustomerAddress -> a Tray LE
  //
  // Mas o bloco Customer com dados de identidade (cpf/name/email/birth_date)
  // faz a Tray tentar CADASTRAR o cliente e colidir:
  //   causes.Customer.cpf = "Está em uso em outro cadastro."
  //
  // Solucao: Customer carrega SOMENTE o endereco. Sem identidade nao ha o que
  // colidir, e o cliente segue identificado por Order.customer_id (o schema
  // oficial de criacao exige exatamente customer_id, sem propriedade Customer).
  // Cadastro de cliente continua exclusivo do resolver / POST /customers.
  //
  // Falhamos ANTES da rede se faltar campo obrigatorio -- nunca enviar em
  // branco, nunca inventar endereco. Isso NAO e frete: nenhum valor ou
  // transportadora e calculado aqui; a logistica fica com os vendedores.
  const customerAddress = {
    address: String(address?.street ?? "").trim(),
    number: String(address?.number ?? "").trim(),
    complement: String(address?.complement ?? "").trim(),
    neighborhood: String(address?.neighborhood ?? "").trim(),
    city: String(address?.city ?? "").trim(),
    state: String(address?.state ?? "").trim(),
    zip_code: String(address?.zipcode ?? "").replace(/\D/g, ""),
    // ISO-3 so no boundary da Tray; user_addresses.country nao muda.
    country: normalizeTrayCountry(address?.country),
    // type "1" = endereco de entrega.
    type: "1",
  };
  const missingAddress = ["address", "number", "neighborhood", "city", "state", "zip_code", "country"].filter(
    (k) => !customerAddress[k]
  );
  if (missingAddress.length) {
    throw new TrayCatalogError("order_address_incomplete", { status: 400, publicDetails: { missing: missingAddress } });
  }

  // IDENTIDADE vem da Tray (Customer canonico), ENDERECO vem da NewStore.
  // Nao enviamos Order.customer_id junto: seria um segundo modelo de
  // identidade no mesmo payload. O contrato documentado do POST /orders leva
  // o Order.Customer completo, e o tray_customer_id serve internamente para
  // localizar esse Customer canonico.
  const phone = String(customer?.phone || "").replace(/\D/g, "");
  const cellphone = String(customer?.cellphone || "").replace(/\D/g, "");
  const trayPhone = phone || cellphone;

  const body = {
    Order: {
      point_sale: LOJA_NS_ORDER_DEFAULTS.point_sale,
      ...(sessionId ? { session_id: String(sessionId) } : {}),
      shipment: LOJA_NS_ORDER_DEFAULTS.shipment,
      shipment_value: LOJA_NS_ORDER_DEFAULTS.shipment_value,
      payment_form: LOJA_NS_ORDER_DEFAULTS.payment_form,
      Customer: {
        ...(customer?.type ? { type: String(customer.type) } : { type: TRAY_CUSTOMER_TYPE_PF }),
        ...(customer?.name ? { name: String(customer.name) } : {}),
        ...(customer?.cpf ? { cpf: String(customer.cpf).replace(/\D/g, "") } : {}),
        ...(customer?.email ? { email: String(customer.email) } : {}),
        ...(customer?.birth_date ? { birth_date: normalizeTrayBirthDate(customer.birth_date) } : {}),
        ...(trayPhone ? { phone: trayPhone } : {}),
        // rg/gender so quando a propria Tray ja os tem -- nunca inventados.
        ...(customer?.rg ? { rg: String(customer.rg) } : {}),
        ...(customer?.gender ? { gender: String(customer.gender) } : {}),
        CustomerAddress: [customerAddress],
      },
      // M7 (prova real): a chave do container de itens e `ProductsSold`, nao
      // `products`. Enviando `products` a Tray responde 400 "Pedido nao tem
      // produtos." — ela simplesmente nao encontra os itens. `ProductsSold` e
      // o nome usado tanto no exemplo oficial de "Cadastrar Pedido#post"
      // quanto no GET /orders/:id real desta loja. Fica em Order, NUNCA em
      // Order.Customer.
      ProductsSold: products,
      // Identificacao do resgate. `notes` nunca foi recusado pela Tray, mas o
      // GET /orders real desta loja expoe `store_note`/`customer_note` (nao
      // `notes`) -- mandamos os dois para que a Loja NS seja realmente
      // identificavel no painel. Sem PII: so origem + redemption_id.
      notes: String(notes || "").slice(0, 1000),
      store_note: String(notes || "").slice(0, 1000),
      // Deliberadamente ausentes (nunca preventivos): partner_id
      // -- a Tray ainda nao o exigiu. Tambem ausentes price/original_price
      // nos itens: a Tray nunca os pediu e mandar um preco errado corromperia
      // o total de um pedido real; sem eles ela usa o preco do proprio
      // catalogo. Se qualquer um passar a ser exigido, o meta sanitizado do
      // evento mostra o campo exato — nunca fabricar valor.
    },
  };

  const result = await trayMutationRequest("TRAY_ORDER_CREATE", "POST", "/orders", body, options);

  const orderId = result?.id ?? result?.Order?.id ?? result?.order_id ?? result?.order?.id ?? null;
  if (!orderId) {
    throw new TrayCatalogError("tray_order_id_missing", { status: 502, publicDetails: { tray_body: result } });
  }

  return { orderId: String(orderId), raw: result };
}

/**
 * Converte uma string monetaria ("50.00", "1234,50") para centavos SEM
 * multiplicacao de float (item 6 do pedido: "nunca float ingenuo para
 * cents"). Devolve null se o formato nao for reconhecido — o chamador
 * decide o que fazer, nunca assume zero silenciosamente.
 */
export function parseMoneyStringToCents(raw) {
  const s = String(raw ?? "").trim().replace(",", ".");
  if (!s) return null;
  const m = /^(-?\d+)(?:\.(\d{1,2}))?$/.exec(s);
  if (!m) return null;
  const negative = m[1].startsWith("-");
  const wholeCents = Math.abs(Number(m[1])) * 100;
  const fracCents = Number((m[2] || "").padEnd(2, "0"));
  const cents = wholeCents + fracCents;
  return negative ? -cents : cents;
}

/**
 * GET /orders/:id — leitura pura do pedido.
 *
 * ACHADO FACTUAL (auditoria read-only contra a loja real, 2026-08-25):
 * `GET /orders/:id/full` responde **404** para TODOS os pedidos desta loja,
 * enquanto `GET /orders/:id` devolve o objeto `Order` completo — incluindo os
 * campos de logística (shipment_integrator, shipment_date, sending_code,
 * tracking_url, has_shipment, is_traceable, estimated_delivery_date).
 * Por isso a leitura de acompanhamento usa `/orders/:id`.
 *
 * `getTrayOrderFull` abaixo continua INTOCADO: ele pertence ao reconciliador
 * do webhook (dinheiro), e mexer nele está fora do escopo desta tarefa. O
 * 404 do `/full` está registrado no relatório para tratamento próprio.
 *
 * Reusa `trayCatalogGet` — mesmo cliente, mesma autenticação, mesmo timeout,
 * mesma trava de somente-leitura. Nenhum client Tray novo.
 */
export async function getTrayOrder(orderId, options = {}) {
  const id = String(orderId || "").trim();
  if (!id) throw new TrayCatalogError("order_id_missing", { status: 400 });

  const body = await trayCatalogGet(`/orders/${encodeURIComponent(id)}`, {}, options);
  const order = body?.Order ?? body?.order ?? null;
  if (!order || typeof order !== "object") {
    throw new TrayCatalogError("tray_invalid_response", { status: 502 });
  }
  return { raw: order };
}

function normalizeTrayStatusName(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .replace(/\s+/g, " ")
    .toUpperCase();
}

/**
 * Caminhos documentados pela Tray para a listagem de status de pedido. A
 * conta desta loja e a autoridade: tentamos na ordem, SEMPRE em GET, e
 * paramos no primeiro que devolver status de verdade. Nenhum ID e assumido
 * aqui — quem escolhe e resolveTrayOperationalStatus, pelo nome real.
 */
const TRAY_ORDER_STATUS_PATHS = ["/order_status", "/orders/statuses"];

/** A listagem aparece como { OrderStatuses: [{ OrderStatus: {...} }] } ou array cru. */
function extractTrayStatuses(body) {
  const rows = Array.isArray(body)
    ? body
    : Array.isArray(body?.OrderStatuses)
      ? body.OrderStatuses
      : Array.isArray(body?.OrderStatus)
        ? body.OrderStatus
        : [];
  return rows
    .map((entry) => entry?.OrderStatus ?? entry)
    .filter((entry) => entry && typeof entry === "object")
    // O rotulo aparece como `status` no pedido e como `name` na listagem de
    // status — aceitamos os dois formatos factuais, sem inventar um terceiro.
    .map((entry) => ({
      id: String(entry.id ?? "").trim(),
      status: String(entry.status ?? entry.name ?? "").trim(),
    }))
    .filter((entry) => entry.id && entry.status);
}

export async function listTrayOrderStatuses(options = {}) {
  for (const statusPath of TRAY_ORDER_STATUS_PATHS) {
    let body;
    try {
      // eslint-disable-next-line no-await-in-loop
      body = await trayCatalogGet(statusPath, { limit: 50, page: 1 }, options);
    } catch (e) {
      // 404 = esta conta nao expoe a listagem neste caminho; qualquer outro
      // erro (auth, rate limit, 5xx, timeout) sobe e vira reconciliacao.
      if (e instanceof TrayCatalogError && Number(e.status) === 404) continue;
      throw e;
    }
    const statuses = extractTrayStatuses(body);
    if (statuses.length) return statuses;
  }
  return [];
}

export async function resolveTrayOperationalStatus(options = {}) {
  if (options.cache !== false && cachedOperationalStatus) return cachedOperationalStatus;

  const statuses = await listTrayOrderStatuses(options);
  const matches = statuses.filter((entry) => normalizeTrayStatusName(entry.status) === "A ENVIAR");
  if (matches.length !== 1) {
    throw new TrayCatalogError(matches.length ? "tray_operational_status_ambiguous" : "tray_operational_status_not_found", {
      status: 502,
      publicDetails: {
        expected_status: "A ENVIAR",
        available_statuses: statuses.map(({ id, status }) => ({ id, status })),
      },
    });
  }

  const target = { id: matches[0].id, status: matches[0].status };
  if (options.cache !== false) cachedOperationalStatus = target;
  return target;
}

/** Timeout/rede: a Tray pode ou nao ter aplicado a mutation. Nunca repetir as cegas. */
const AMBIGUOUS_TRAY_CODES = new Set(["tray_timeout", "tray_unreachable"]);

function unconfirmedStatusError(orderId, targetStatus, cause) {
  return new TrayCatalogError("tray_order_status_unconfirmed", {
    status: 502,
    publicDetails: {
      operation: "TRAY_ORDER_STATUS_UPDATE",
      tray_order_id: orderId,
      target_status_id: targetStatus.id,
      cause,
    },
  });
}

export async function updateTrayOrderStatus({ orderId, statusId } = {}, options = {}) {
  const id = String(orderId || "").trim();
  const targetId = String(statusId || "").trim();
  if (!id) throw new TrayCatalogError("order_id_missing", { status: 400 });
  if (!targetId) throw new TrayCatalogError("order_status_id_missing", { status: 400 });

  return trayMutationRequest(
    "TRAY_ORDER_STATUS_UPDATE",
    "PUT",
    `/orders/${encodeURIComponent(id)}`,
    { Order: { status_id: targetId } },
    options
  );
}

function trayOrderHasOperationalStatus(order, orderId, targetStatus) {
  if (String(order?.id || "") !== String(orderId)) return false;
  const statusIdMatches = String(order?.OrderStatus?.id || "") === String(targetStatus.id);
  const statusNameMatches =
    normalizeTrayStatusName(order?.OrderStatus?.status || order?.status) === normalizeTrayStatusName(targetStatus.status);
  return statusIdMatches || statusNameMatches;
}

/**
 * Avanca UM pedido ja criado para o status operacional real da loja.
 *
 * O pedido ja existe na Tray quando esta funcao roda: qualquer falha aqui e
 * um caso de reconciliacao, NUNCA de recriar pedido ou devolver credito.
 * Por isso o timeout do PUT nao vira retry cego -- a unica pergunta legitima
 * e "a Tray aplicou?", e quem responde e o GET /orders/:id (nunca /full,
 * que responde 404 nesta loja).
 *
 * Este fluxo NAO cria Payment na Tray: NSCreditos sao liquidados dentro da
 * NewStore (ledger de cupom) e o status serve so para liberar a operacao/
 * separacao. `has_payment` pode continuar 0 -- decisao de negocio, nao bug.
 */
export async function advanceTrayOrderToOperationalStatus({ orderId } = {}, options = {}) {
  const id = String(orderId || "").trim();
  if (!id) throw new TrayCatalogError("order_id_missing", { status: 400 });

  // Fail-closed: sem status operacional identificado com seguranca, nenhuma
  // mutation sai daqui (nada de status arbitrario num pedido real).
  const targetStatus = await resolveTrayOperationalStatus(options);

  let ambiguous = false;
  try {
    await updateTrayOrderStatus({ orderId: id, statusId: targetStatus.id }, options);
  } catch (e) {
    if (!(e instanceof TrayCatalogError && AMBIGUOUS_TRAY_CODES.has(e.code))) throw e;
    ambiguous = true;
  }

  let order = null;
  try {
    ({ raw: order } = await getTrayOrder(id, options));
  } catch (e) {
    if (!ambiguous) throw e;
    throw unconfirmedStatusError(id, targetStatus, e.code);
  }

  if (!trayOrderHasOperationalStatus(order, id, targetStatus)) {
    if (ambiguous) throw unconfirmedStatusError(id, targetStatus, "tray_timeout");
    throw new TrayCatalogError("tray_order_status_verification_failed", {
      status: 502,
      publicDetails: {
        operation: "TRAY_ORDER_STATUS_UPDATE",
        tray_order_id: id,
        target_status_id: targetStatus.id,
      },
    });
  }

  return {
    targetStatus,
    order,
    hasPayment: order?.has_payment ?? null,
  };
}

/**
 * GET /orders/:id/full — leitura pura, usada pelo reconciliador do webhook
 * de pedido (Fase G) para confirmar coupon_code/discount de um pedido antes
 * de agir sobre o saldo local. Nunca confia so no payload do webhook (que
 * so traz o id) — sempre busca o dado oficial na Tray.
 * @returns {Promise<{couponCode: string|null, discount: number, discountCents: number|null}>}
 */
export async function getTrayOrderFull(orderId, options = {}) {
  const id = String(orderId || "").trim();
  if (!id) throw new TrayCatalogError("order_id_missing", { status: 400 });

  const body = await trayCatalogGet(`/orders/${encodeURIComponent(id)}/full`, {}, options);
  const order = body?.Order ?? body?.order ?? null;
  if (!order || typeof order !== "object") {
    throw new TrayCatalogError("tray_invalid_response", { status: 502 });
  }

  const couponCode = order.coupon_code != null && String(order.coupon_code).trim() !== "" ? String(order.coupon_code).trim() : null;
  const discountRaw = order.discount;
  const discount = discountRaw == null ? 0 : Number(String(discountRaw).replace(",", "."));
  const discountCents = discountRaw == null ? 0 : parseMoneyStringToCents(discountRaw);

  return { couponCode, discount: Number.isFinite(discount) ? discount : 0, discountCents, raw: order };
}
