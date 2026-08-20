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

    const line = { product_id: productId, quantity };
    if (item.trayVariantId != null && String(item.trayVariantId).trim() !== "") {
      const variantId = Number(item.trayVariantId);
      if (!Number.isFinite(variantId) || variantId <= 0) throw new TrayCatalogError("order_item_variant_id_invalid", { status: 400 });
      line.variant_id = variantId;
    }
    return line;
  });

  // M7 (prova real): a Tray recusa o pedido com 400 e
  // causes.CustomerAddress[campo]="Este campo nao pode ser deixado em branco"
  // quando o bloco de endereco nao chega onde ela espera. O JSON oficial de
  // POST /orders aninha o endereco em Order.Customer.CustomerAddress[] --
  // NAO em Order.CustomerAddress (essa posicao foi testada e a Tray continua
  // reportando os campos como em branco).
  // Falhamos ANTES da rede se algum campo obrigatorio estiver vazio -- nunca
  // enviar em branco, nunca inventar valor. Isso NAO e frete: nenhum
  // valor/transportadora e calculado aqui.
  const customerAddress = {
    address: String(address?.street ?? "").trim(),
    number: String(address?.number ?? "").trim(),
    complement: String(address?.complement ?? "").trim(),
    neighborhood: String(address?.neighborhood ?? "").trim(),
    city: String(address?.city ?? "").trim(),
    state: String(address?.state ?? "").trim(),
    zip_code: String(address?.zipcode ?? "").replace(/\D/g, ""),
    // O JSON oficial usa ISO-3 ("BRA"). Normalizamos SO no boundary da Tray:
    // o armazenamento interno (user_addresses.country) continua como esta.
    country: normalizeTrayCountry(address?.country),
    // type "1" = endereco de entrega, conforme a estrutura oficial.
    type: "1",
  };
  const missingAddress = ["address", "number", "neighborhood", "city", "state", "zip_code", "country", "type"].filter(
    (k) => !customerAddress[k]
  );
  if (missingAddress.length) {
    throw new TrayCatalogError("order_address_incomplete", { status: 400, publicDetails: { missing: missingAddress } });
  }

  // birth_date e exigido pela Tray no Customer inline. Falha ANTES da rede se
  // o perfil nao tiver uma data utilizavel -- nunca inventar/estimar.
  const customerBirthDate = normalizeTrayBirthDate(customer?.birthDate);
  if (customer?.birthDate && !customerBirthDate) {
    throw new TrayCatalogError("order_customer_birth_date_invalid", { status: 400 });
  }
  const customerPhone = String(customer?.phone ?? "").replace(/\D/g, "");

  const body = {
    Order: {
      customer_id: cid,
      point_sale: LOJA_NS_ORDER_DEFAULTS.point_sale,
      ...(sessionId ? { session_id: String(sessionId) } : {}),
      shipment: LOJA_NS_ORDER_DEFAULTS.shipment,
      shipment_value: LOJA_NS_ORDER_DEFAULTS.shipment_value,
      payment_form: LOJA_NS_ORDER_DEFAULTS.payment_form,
      Customer: {
        ...(customer?.name ? { name: String(customer.name).trim() } : {}),
        ...(customer?.email ? { email: String(customer.email).trim().toLowerCase() } : {}),
        // M7 (prova real): a Tray valida cpf E birth_date no Customer inline
        // do POST /orders. customer_id sozinho NAO substitui esses campos --
        // o contrato documentado de criacao leva o Customer completo.
        ...(customer?.cpf ? { cpf: String(customer.cpf).replace(/\D/g, "") } : {}),
        ...(customerBirthDate ? { birth_date: customerBirthDate } : {}),
        ...(customerPhone ? { phone: customerPhone } : {}),
        type: TRAY_CUSTOMER_TYPE_PF,
        CustomerAddress: [customerAddress],
      },
      // M7 (prova real): a chave do container de itens e `ProductsSold`, nao
      // `products`. Enviando `products` a Tray responde 400 "Pedido nao tem
      // produtos." — ela simplesmente nao encontra os itens. `ProductsSold` e
      // o nome usado tanto no exemplo oficial de "Cadastrar Pedido#post"
      // quanto no GET /orders/:id real desta loja. Fica em Order, NUNCA em
      // Order.Customer.
      ProductsSold: products,
      notes: String(notes || "").slice(0, 1000),
      // Deliberadamente ausentes (nunca preventivos): partner_id e session_id
      // -- a Tray ainda nao os exigiu. Tambem ausentes price/original_price
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
