// src/services/trayRedemptionOrder.js
//
// UNICO ponto de integracao entre a saga de resgate (rewardRedemption.js) e
// a Tray real. Sem inventar payment_type nem frete (ver trayOrderClient.js
// para o contrato completo e a justificativa documental).
//
// Passos:
//   1. Resolver o customer_id Tray do usuario (trayCustomerResolver.js):
//      cache -> busca por e-mail E por cpf -> reconcilia -> cria
//      (POST /customers) se o perfil estiver completo (birth_date + cpf,
//      M7.1: cpf provado obrigatorio pra esta loja via teste controlado
//      real). Bloqueio isolado e deterministico quando o perfil esta
//      incompleto ou ha ambiguidade/conflito de identidade — nunca um
//      bloqueio geral do resgate, nunca uma mutacao Tray sem necessidade.
//   2. Criar o pedido real (trayOrderClient.js), identificando o resgate
//      via o campo oficial `notes` — nunca via payment_method inventado.

import { resolveTrayCustomerId, TrayCustomerProfileIncompleteError, TrayCustomerIdentityConflictError } from "./trayCustomerResolver.js";
import { createTrayOrder, buildTraySessionId } from "./trayOrderClient.js";
import { getTrayCustomerById } from "./trayCustomerClient.js";
import { TrayCatalogError } from "./trayCatalogClient.js";
import { fetchTrayProduct, fetchTrayVariants } from "./trayCatalogClient.js";

export class TrayOrderNotImplementedError extends Error {
  constructor(reason = "tray_order_contract_pending") {
    super(reason);
    this.name = "TrayOrderNotImplementedError";
    this.code = reason;
    this.ambiguous = false;
  }
}

export { TrayCustomerProfileIncompleteError, TrayCustomerIdentityConflictError };

/**
 * Bloqueio ISOLADO e deterministico: mais de um Customer Tray tem
 * exatamente o mesmo e-mail. Nunca escolhe arbitrariamente (item 11).
 * Nenhuma mutacao ocorreu, seguro compensar.
 */
export class TrayCustomerAmbiguousError extends Error {
  constructor(reason = "tray_customer_ambiguous") {
    super(reason);
    this.name = "TrayCustomerAmbiguousError";
    this.code = reason;
    this.ambiguous = false;
  }
}

export class TrayOrderAmbiguousError extends Error {
  constructor(reason = "tray_order_result_ambiguous") {
    super(reason);
    this.name = "TrayOrderAmbiguousError";
    this.code = reason;
    // Timeout ou resposta inconclusiva NA MUTACAO de pedido: nao sabemos se
    // o pedido foi criado do lado Tray. Nunca compensar automaticamente
    // aqui (item 34) — precisa reconciliar.
    this.ambiguous = true;
  }
}

function formatNsCredits(value) {
  const amount = Number(value);
  if (!Number.isFinite(amount)) return "0";
  return Number.isInteger(amount) ? String(amount) : amount.toFixed(2).replace(".", ",");
}

export function buildRedemptionStoreNote({ redemptionId, items } = {}) {
  const lines = (Array.isArray(items) ? items : []).map((item) => {
    const quantity = Number(item?.quantity);
    const unitCredits = Number(
      item?.current_nscredits_price ??
      item?.nscredits_unit_price ??
      item?.nscredits_unit_price_snapshot
    );
    const itemTotal = quantity * unitCredits;
    const variant = item?.tray_variant_id != null && String(item.tray_variant_id).trim() !== ""
      ? ` | variant_id=${item.tray_variant_id}`
      : "";
    return `- product_id=${item?.tray_product_id}${variant} | quantidade=${quantity} | NSCréditos unitários=${formatNsCredits(unitCredits)} | total=${formatNsCredits(itemTotal)} NSCréditos`;
  });
  const total = (Array.isArray(items) ? items : []).reduce((sum, item) => {
    const quantity = Number(item?.quantity);
    const unitCredits = Number(
      item?.current_nscredits_price ??
      item?.nscredits_unit_price ??
      item?.nscredits_unit_price_snapshot
    );
    return sum + quantity * unitCredits;
  }, 0);

  return [
    "RESGATE LOJA NS",
    "",
    "Forma de liquidação: NSCréditos",
    "Pagamento monetário: NÃO APLICÁVEL",
    "",
    `NSCréditos utilizados: ${formatNsCredits(total)}`,
    "",
    "Itens:",
    ...lines,
    "",
    `Total do resgate: ${formatNsCredits(total)} NSCréditos`,
    `redemption_id=${redemptionId}`,
    "",
    "Pedido liquidado integralmente através de NSCréditos.",
    "Não houve cobrança via PIX, cartão, boleto ou dinheiro.",
  ].join("\n");
}

/**
 * @param {object} params
 * @param {number} params.userId id NewStore do usuario (chave de cache/lock do customer_id Tray)
 * @param {string} params.redemptionId
 * @param {object} params.userProfile { name, email, birthDate, cpf, phone } — perfil NewStore completo
 * @param {Array<{tray_product_id:string, tray_variant_id?:string|null, quantity:number}>} params.items
 * @param {object} params.couponSnapshot { coupon_code, tray_coupon_id }
 * @throws {TrayCustomerProfileIncompleteError} sem Customer existente e perfil sem birth_date/cpf
 * @throws {TrayCustomerAmbiguousError} mais de um Customer Tray com o mesmo e-mail/cpf
 * @throws {TrayCustomerIdentityConflictError} e-mail e cpf apontam pra identidades incompativeis
 * @throws {TrayOrderAmbiguousError} timeout/rede instavel NA CRIACAO do pedido (nunca compensar sozinho)
 * @throws {TrayCatalogError} demais falhas deterministicas (400/401/404/5xx) — seguro compensar
 */
export async function createTrayRedemptionOrder(params, options = {}) {
  const { userId, redemptionId, items, userProfile, address } = params || {};

  const email = String(userProfile?.email || "").trim();
  if (!email) throw new TrayCustomerProfileIncompleteError(["email"]);

  let customerId;
  try {
    customerId = await resolveTrayCustomerId(
      userId,
      {
        name: userProfile?.name || "",
        email,
        birthDate: userProfile?.birthDate || null,
        cpf: userProfile?.cpf || null,
        phone: userProfile?.phone || null,
      },
      options
    );
  } catch (e) {
    if (e instanceof TrayCatalogError && e.code === "tray_customer_ambiguous") {
      throw new TrayCustomerAmbiguousError(e.code);
    }
    throw e;
  }

  // Preco monetario REAL da Tray para cada item. Dominio totalmente separado
  // dos NSCreditos: nunca convertemos credito em reais. Lido live do catalogo
  // (GET) imediatamente antes do pedido -- se a Tray nao devolver um preco
  // utilizavel, o createTrayOrder falha ANTES da rede em vez de inventar.
  const orderItems = [];
  for (const item of Array.isArray(items) ? items : []) {
    const trayProduct = await fetchTrayProduct(item.tray_product_id, options);
    let trayPrice = trayProduct?.price;

    if (item.tray_variant_id != null && String(item.tray_variant_id).trim() !== "") {
      const variants = await fetchTrayVariants(item.tray_product_id, options).catch(() => []);
      const variant = (Array.isArray(variants) ? variants : []).find(
        (v) => String(v?.id) === String(item.tray_variant_id)
      );
      // A variacao manda no preco quando ela existe e tem preco proprio.
      if (variant?.price != null && String(variant.price).trim() !== "") trayPrice = variant.price;
    }

    orderItems.push({
      trayProductId: item.tray_product_id,
      trayVariantId: item.tray_variant_id,
      quantity: item.quantity,
      trayPrice,
    });
  }

  const notes = buildRedemptionStoreNote({ redemptionId, items });

  // IDENTIDADE CANONICA: quando ja existe um Customer Tray, a identidade do
  // Order.Customer vem da PROPRIA Tray, nunca remontada com os dados da
  // NewStore. Remontar faz a Tray enxergar um cadastro novo (o e-mail diverge
  // do dela) e recusar com cpf "Está em uso em outro cadastro.".
  // O endereco de entrega continua sendo o escolhido na NewStore.
  const canonical = await getTrayCustomerById(customerId, options);

  // Gate de identidade: o CPF tem que ser o mesmo dos dois lados. E-mail pode
  // divergir -- o vinculo users.tray_customer_id ja foi reconciliado
  // explicitamente (ver trayCustomerResolver). CPF diferente significa que o
  // mapeamento esta errado: aborta ANTES de qualquer mutation.
  const localCpf = String(userProfile?.cpf || "").replace(/\D/g, "");
  if (!canonical.cpf || !localCpf || canonical.cpf !== localCpf) {
    throw new TrayCustomerIdentityConflictError("tray_customer_cpf_mismatch", { customerId: String(customerId) });
  }

  try {
    const result = await createTrayOrder(
      {
        customerId,
        customer: canonical,
        items: orderItems,
        notes,
        address,
        // Correlacao estavel para reconciliacao em caso de timeout.
        sessionId: buildTraySessionId(redemptionId),
      },
      options
    );
    return { orderId: result.orderId };
  } catch (e) {
    if (e instanceof TrayCatalogError && (e.code === "tray_timeout" || e.code === "tray_unreachable")) {
      throw new TrayOrderAmbiguousError(e.code);
    }
    throw e;
  }
}
