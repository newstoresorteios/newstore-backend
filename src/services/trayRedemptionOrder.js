// src/services/trayRedemptionOrder.js
//
// UNICO ponto de integracao entre a saga de resgate (rewardRedemption.js) e
// a Tray real. Fase D: implementado de verdade — sem inventar payment_type
// nem frete (ver trayOrderClient.js para o contrato completo e a
// justificativa documental).
//
// Passos:
//   1. Resolver o customer_id Tray do usuario por e-mail (SOMENTE LEITURA,
//      trayCustomerClient.js). Nunca cria um cliente novo aqui: criar
//      cliente na Tray exige birth_date (POST /customers), campo que a
//      NewStore nao coleta em nenhum lugar do cadastro. Ver
//      TrayCustomerNotFoundError abaixo — bloqueio isolado e documentado,
//      nao um bloqueio geral do resgate.
//   2. Criar o pedido real (trayOrderClient.js), identificando o resgate
//      via o campo oficial `notes` — nunca via payment_method inventado.

import { findTrayCustomerByEmail } from "./trayCustomerClient.js";
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

/**
 * Bloqueio ISOLADO e deterministico: o usuario nao tem (ou nao pudemos
 * confirmar) um cliente Tray correspondente por e-mail. Nunca criamos um
 * cliente novo aqui — POST /customers exige birth_date, que a NewStore nao
 * coleta. Como nenhuma chamada de mutacao Tray ocorreu, e seguro compensar
 * (devolver os creditos) imediatamente.
 */
export class TrayCustomerNotFoundError extends Error {
  constructor(reason = "tray_customer_not_found") {
    super(reason);
    this.name = "TrayCustomerNotFoundError";
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
 * @param {string} params.redemptionId
 * @param {string} params.userEmail e-mail do usuario, usado para localizar o customer_id Tray
 * @param {Array<{tray_product_id:string, tray_variant_id?:string|null, quantity:number}>} params.items
 * @param {object} params.couponSnapshot { coupon_code, tray_coupon_id }
 * @throws {TrayCustomerNotFoundError} sem cliente Tray correspondente (bloqueio isolado, sem mutacao)
 * @throws {TrayOrderAmbiguousError} timeout/rede instavel NA CRIACAO do pedido (nunca compensar sozinho)
 * @throws {TrayCatalogError} demais falhas deterministicas (400/401/404/5xx) — seguro compensar
 */
export async function createTrayRedemptionOrder(params, options = {}) {
  const { redemptionId, items, userEmail, couponSnapshot } = params || {};

  const email = String(userEmail || "").trim();
  if (!email) throw new TrayCustomerNotFoundError("tray_customer_email_missing");

  // GET puro — nenhuma mutacao ocorre aqui, entao qualquer falha (inclusive
  // timeout/rede) e deterministicamente segura de propagar e compensar.
  const customer = await findTrayCustomerByEmail(email, options);
  if (!customer) throw new TrayCustomerNotFoundError("tray_customer_not_found");

  const orderItems = (Array.isArray(items) ? items : []).map((item) => ({
    trayProductId: item.tray_product_id,
    trayVariantId: item.tray_variant_id,
    quantity: item.quantity,
  }));

  const notes = buildNotes({ redemptionId, couponSnapshot });

  try {
    const result = await createTrayOrder({ customerId: customer.id, items: orderItems, notes }, options);
    return { orderId: result.orderId };
  } catch (e) {
    if (e instanceof TrayCatalogError && (e.code === "tray_timeout" || e.code === "tray_unreachable")) {
      throw new TrayOrderAmbiguousError(e.code);
    }
    throw e;
  }
}
