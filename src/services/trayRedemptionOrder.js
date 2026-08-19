// src/services/trayRedemptionOrder.js
//
// UNICO ponto de integracao entre a saga de resgate (rewardRedemption.js) e
// a Tray real. Sem inventar payment_type nem frete (ver trayOrderClient.js
// para o contrato completo e a justificativa documental).
//
// Passos:
//   1. Resolver o customer_id Tray do usuario (trayCustomerResolver.js):
//      cache -> busca por e-mail -> cria (POST /customers) se o perfil
//      estiver completo (birth_date). Bloqueio isolado e deterministico
//      quando o perfil esta incompleto ou ha ambiguidade — nunca um
//      bloqueio geral do resgate, nunca uma mutacao Tray sem necessidade.
//   2. Criar o pedido real (trayOrderClient.js), identificando o resgate
//      via o campo oficial `notes` — nunca via payment_method inventado.

import { resolveTrayCustomerId, TrayCustomerProfileIncompleteError } from "./trayCustomerResolver.js";
import { createTrayOrder } from "./trayOrderClient.js";
import { TrayCatalogError } from "./trayCatalogClient.js";

export class TrayOrderNotImplementedError extends Error {
  constructor(reason = "tray_order_contract_pending") {
    super(reason);
    this.name = "TrayOrderNotImplementedError";
    this.code = reason;
    this.ambiguous = false;
  }
}

export { TrayCustomerProfileIncompleteError };

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

function buildNotes({ redemptionId, couponSnapshot }) {
  const code = couponSnapshot?.coupon_code || "sem-cupom";
  return `Resgate Loja NS / redemption_id=${redemptionId} / coupon_code=${code}`;
}

/**
 * @param {object} params
 * @param {number} params.userId id NewStore do usuario (chave de cache/lock do customer_id Tray)
 * @param {string} params.redemptionId
 * @param {object} params.userProfile { name, email, birthDate, phone } — perfil NewStore completo
 * @param {Array<{tray_product_id:string, tray_variant_id?:string|null, quantity:number}>} params.items
 * @param {object} params.couponSnapshot { coupon_code, tray_coupon_id }
 * @throws {TrayCustomerProfileIncompleteError} sem Customer existente e perfil sem birth_date
 * @throws {TrayCustomerAmbiguousError} mais de um Customer Tray com o mesmo e-mail
 * @throws {TrayOrderAmbiguousError} timeout/rede instavel NA CRIACAO do pedido (nunca compensar sozinho)
 * @throws {TrayCatalogError} demais falhas deterministicas (400/401/404/5xx) — seguro compensar
 */
export async function createTrayRedemptionOrder(params, options = {}) {
  const { userId, redemptionId, items, userProfile, couponSnapshot } = params || {};

  const email = String(userProfile?.email || "").trim();
  if (!email) throw new TrayCustomerProfileIncompleteError(["email"]);

  let customerId;
  try {
    customerId = await resolveTrayCustomerId(
      userId,
      { name: userProfile?.name || "", email, birthDate: userProfile?.birthDate || null, phone: userProfile?.phone || null },
      options
    );
  } catch (e) {
    if (e instanceof TrayCatalogError && e.code === "tray_customer_ambiguous") {
      throw new TrayCustomerAmbiguousError(e.code);
    }
    throw e;
  }

  const orderItems = (Array.isArray(items) ? items : []).map((item) => ({
    trayProductId: item.tray_product_id,
    trayVariantId: item.tray_variant_id,
    quantity: item.quantity,
  }));

  const notes = buildNotes({ redemptionId, couponSnapshot });

  try {
    const result = await createTrayOrder({ customerId, items: orderItems, notes }, options);
    return { orderId: result.orderId };
  } catch (e) {
    if (e instanceof TrayCatalogError && (e.code === "tray_timeout" || e.code === "tray_unreachable")) {
      throw new TrayOrderAmbiguousError(e.code);
    }
    throw e;
  }
}
