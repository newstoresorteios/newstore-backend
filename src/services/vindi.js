// backend/src/services/vindi.js
// Integração com Vindi API v1
import crypto from "node:crypto";

const VINDI_BASE =
  (process.env.VINDI_API_BASE_URL && process.env.VINDI_API_BASE_URL.replace(/\/+$/, "")) ||
  "https://app.vindi.com.br/api/v1";

const VINDI_API_KEY = process.env.VINDI_API_KEY || "";
const VINDI_DEFAULT_PAYMENT_METHOD = process.env.VINDI_DEFAULT_PAYMENT_METHOD || "credit_card";
const VINDI_DEFAULT_GATEWAY = process.env.VINDI_DEFAULT_GATEWAY || "pagarme";

/* ------------------------------------------------------- *
 * Logging estruturado (sem segredos)
 * ------------------------------------------------------- */
const LP = "[vindi]";
const log = (msg, extra = null) => console.log(`${LP} ${msg}`, extra ? JSON.stringify(extra) : "");
const warn = (msg, extra = null) => console.warn(`${LP} ${msg}`, extra ? JSON.stringify(extra) : "");
const err = (msg, extra = null) => console.error(`${LP} ${msg}`, extra ? JSON.stringify(extra) : "");

/**
 * Constrói header de autenticação Basic Auth (RFC2617)
 * Formato: base64("API_KEY:")
 */
function buildAuthHeader() {
  if (!VINDI_API_KEY) {
    throw new Error("VINDI_API_KEY não configurado no servidor.");
  }
  const authString = `${VINDI_API_KEY}:`;
  const encoded = Buffer.from(authString).toString("base64");
  return `Basic ${encoded}`;
}

/**
 * Faz requisição HTTP para a API Vindi
 * @param {string} method - GET, POST, PUT, DELETE
 * @param {string} path - Caminho da API (ex: /customers)
 * @param {object|null} body - Body da requisição (será serializado como JSON)
 * @param {object} options - { timeoutMs }
 * @returns {Promise<object>} Resposta JSON da API
 */
async function vindiRequest(method, path, body = null, { timeoutMs = 30000 } = {}) {
  if (!VINDI_API_KEY) {
    const error = new Error("VINDI_API_KEY não configurado no servidor.");
    error.status = 503;
    throw error;
  }

  const url = `${VINDI_BASE}${path.startsWith("/") ? path : `/${path}`}`;
  log(`chamando Vindi API: ${method} ${url}`);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  let response;
  try {
    response = await fetch(url, {
      method,
      headers: {
        Authorization: buildAuthHeader(),
        "Content-Type": "application/json",
        Accept: "application/json",
        "User-Agent": "lancaster-backend/1.0",
      },
      body: body != null ? JSON.stringify(body) : undefined,
      signal: controller.signal,
    });
  } catch (e) {
    clearTimeout(timeout);
    if (e?.name === "AbortError") {
      throw new Error(`Vindi ${method} ${path} timeout após ${timeoutMs}ms`);
    }
    throw e;
  }
  clearTimeout(timeout);

  const text = await response.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }

  if (!response.ok) {
    const errorMsg =
      json?.errors?.[0]?.message ||
      json?.error ||
      json?.message ||
      `Vindi ${method} ${path} falhou (${response.status})`;

    const error = new Error(errorMsg);
    error.status = response.status;
    error.response = json;
    error.path = path;
    error.url = url;
    
    // Log do erro (sem dados sensíveis)
    err(`Vindi API erro: ${method} ${url}`, {
      status: response.status,
      error_message: errorMsg,
      errors_count: json?.errors?.length || 0,
    });
    
    throw error;
  }

  return json;
}

/**
 * Garante/retorna um customer na Vindi (procura por email; senão cria)
 * @param {object} params - { email, name, code?, cpfCnpj? }
 * @returns {Promise<{customerId: string}>}
 */
export async function ensureCustomer({ email, name, code, cpfCnpj }) {
  if (!email) {
    throw new Error("email é obrigatório para ensureCustomer");
  }

  try {
    // Busca por email
    const search = await vindiRequest("GET", `/customers?query=email:${encodeURIComponent(email)}`);
    if (search?.customers?.length > 0) {
      const customer = search.customers[0];
      log("customer encontrado", { customerId: customer.id, email });
      return { customerId: customer.id };
    }

    // Cria novo customer
    const createBody = {
      name: name || "Cliente",
      email,
    };
    if (code) {
      createBody.code = String(code);
    }
    if (cpfCnpj) {
      createBody.registry_code = String(cpfCnpj).replace(/\D+/g, "");
    }

    const created = await vindiRequest("POST", "/customers", createBody);
    log("customer criado", { customerId: created.customer?.id, email });
    return { customerId: created.customer?.id };
  } catch (e) {
    err("ensureCustomer falhou", { email, msg: e?.message, status: e?.status });
    throw e;
  }
}

/**
 * Cria um payment_profile (cartão) na Vindi usando gateway_token
 * @param {object} params - { customerId, gatewayToken, holderName, docNumber?, phone? }
 * @returns {Promise<{paymentProfileId: string, lastFour?: string, cardType?: string}>}
 */
export async function createPaymentProfile({ customerId, gatewayToken, holderName, docNumber, phone }) {
  if (!customerId || !gatewayToken || !holderName) {
    throw new Error("customerId, gatewayToken e holderName são obrigatórios");
  }

  try {
    const body = {
      customer_id: customerId,
      holder_name: holderName,
      payment_method_code: VINDI_DEFAULT_PAYMENT_METHOD,
      payment_company_code: VINDI_DEFAULT_GATEWAY,
      gateway_token: gatewayToken,
    };

    if (docNumber) {
      body.registry_code = String(docNumber).replace(/\D+/g, "");
    }
    if (phone) {
      body.phone = String(phone).replace(/\D+/g, "");
    }

    const created = await vindiRequest("POST", "/payment_profiles", body);
    const profile = created.payment_profile;

    log("payment_profile criado", {
      paymentProfileId: profile?.id,
      customerId,
      lastFour: profile?.last_four,
    });

    return {
      paymentProfileId: profile?.id,
      lastFour: profile?.last_four || null,
      cardType: profile?.card_type || null,
    };
  } catch (e) {
    err("createPaymentProfile falhou", {
      customerId,
      msg: e?.message,
      status: e?.status,
    });
    throw e;
  }
}

/**
 * Cria um payment_profile (cartão) na Vindi usando dados do cartão diretamente (API privada)
 * @param {object} params - { customerId, holderName, cardNumber, cardExpiration, cardCvv, paymentCompanyCode?, docNumber?, phone? }
 * @returns {Promise<{paymentProfileId: string, lastFour?: string, cardType?: string, paymentCompanyCode?: string}>}
 */
export async function createPaymentProfileWithCardData({ 
  customerId, 
  holderName, 
  cardNumber, 
  cardExpiration, 
  cardCvv, 
  paymentCompanyCode,
  docNumber,
  phone 
}) {
  if (!customerId || !holderName || !cardNumber || !cardExpiration || !cardCvv) {
    throw new Error("customerId, holderName, cardNumber, cardExpiration e cardCvv são obrigatórios");
  }

  try {
    const body = {
      customer_id: customerId,
      holder_name: holderName,
      card_number: String(cardNumber).replace(/\D+/g, ""),
      card_expiration: cardExpiration, // Formato MM/YYYY
      card_cvv: String(cardCvv).replace(/\D+/g, ""),
      payment_method_code: "credit_card",
    };

    if (paymentCompanyCode) {
      body.payment_company_code = String(paymentCompanyCode).trim().toLowerCase();
    }
    if (docNumber) {
      body.registry_code = String(docNumber).replace(/\D+/g, "");
    }
    if (phone) {
      body.phone = String(phone).replace(/\D+/g, "");
    }

    const created = await vindiRequest("POST", "/payment_profiles", body);
    const profile = created.payment_profile;

    // Mascara cartão para log (apenas last4)
    const last4 = profile?.last_four || (cardNumber.length >= 4 ? String(cardNumber).replace(/\D+/g, "").slice(-4) : "****");

    log("payment_profile criado com dados do cartão", {
      paymentProfileId: profile?.id,
      customerId,
      lastFour,
      cardType: profile?.card_type || null,
    });

    return {
      paymentProfileId: profile?.id,
      lastFour: last4 || null,
      cardType: profile?.card_type || null,
      paymentCompanyCode: profile?.payment_company?.code || paymentCompanyCode || null,
    };
  } catch (e) {
    err("createPaymentProfileWithCardData falhou", {
      customerId,
      msg: e?.message,
      status: e?.status,
    });
    throw e;
  }
}

/**
 * Cria uma bill (fatura) na Vindi
 * @param {object} params - { customerId, amount, description, metadata?, paymentProfileId, dueAt? }
 * @returns {Promise<{billId: string, status: string}>}
 */
export async function createBill({ customerId, amount, description, metadata, paymentProfileId, dueAt }) {
  if (!customerId || !amount || !paymentProfileId) {
    throw new Error("customerId, amount e paymentProfileId são obrigatórios");
  }

  try {
    // Converte centavos para reais (Vindi usa decimal)
    const amountDecimal = Number((Number(amount) / 100).toFixed(2));

    const body = {
      customer_id: customerId,
      payment_method_code: VINDI_DEFAULT_PAYMENT_METHOD,
      bill_items: [
        {
          product_id: null, // pode ser null se não tiver produto cadastrado
          description: description || "Autopay",
          quantity: 1,
          pricing_schema: {
            price: amountDecimal,
          },
        },
      ],
      payment_profile_id: paymentProfileId,
    };

    if (metadata) {
      body.metadata = metadata;
    }
    if (dueAt) {
      body.due_at = dueAt;
    } else {
      // Por padrão, vence hoje (cobrança imediata)
      body.due_at = new Date().toISOString().split("T")[0];
    }

    const created = await vindiRequest("POST", "/bills", body);
    const bill = created.bill;

    log("bill criada", {
      billId: bill?.id,
      customerId,
      amount: amountDecimal,
      status: bill?.status,
    });

    return {
      billId: bill?.id,
      status: bill?.status,
      chargeId: bill?.charges?.[0]?.id || null,
    };
  } catch (e) {
    err("createBill falhou", {
      customerId,
      amount,
      msg: e?.message,
      status: e?.status,
    });
    throw e;
  }
}

/**
 * Cobra uma bill (POST /bills/{id}/charge)
 * Nota: Alguns gateways cobram automaticamente ao criar a bill, mas este método garante a cobrança
 * @param {string} billId
 * @returns {Promise<{chargeId: string, status: string}>}
 */
export async function chargeBill(billId) {
  if (!billId) {
    throw new Error("billId é obrigatório");
  }

  try {
    const result = await vindiRequest("POST", `/bills/${billId}/charge`, {});
    const charge = result.charge;

    log("bill cobrada", {
      billId,
      chargeId: charge?.id,
      status: charge?.status,
    });

    return {
      chargeId: charge?.id,
      status: charge?.status,
      billStatus: result.bill?.status,
    };
  } catch (e) {
    err("chargeBill falhou", {
      billId,
      msg: e?.message,
      status: e?.status,
    });
    throw e;
  }
}

/**
 * Estorna um charge (POST /charges/{id}/refund)
 * @param {string} chargeId
 * @param {boolean} cancelBill - Se true, cancela a bill associada
 * @returns {Promise<{refundId: string, status: string}>}
 */
export async function refundCharge(chargeId, cancelBill = true) {
  if (!chargeId) {
    throw new Error("chargeId é obrigatório");
  }

  try {
    const body = {};
    if (cancelBill) {
      body.cancel_bill = true;
    }

    const result = await vindiRequest("POST", `/charges/${chargeId}/refund`, body);
    const refund = result.refund || result.charge;

    log("charge estornado", {
      chargeId,
      refundId: refund?.id,
      status: refund?.status,
    });

    return {
      refundId: refund?.id,
      status: refund?.status,
    };
  } catch (e) {
    err("refundCharge falhou", {
      chargeId,
      msg: e?.message,
      status: e?.status,
    });
    throw e;
  }
}

/**
 * Busca informações de uma bill
 * @param {string} billId
 * @returns {Promise<object>}
 */
export async function getBill(billId) {
  if (!billId) {
    throw new Error("billId é obrigatório");
  }

  try {
    const result = await vindiRequest("GET", `/bills/${billId}`);
    return result.bill;
  } catch (e) {
    err("getBill falhou", {
      billId,
      msg: e?.message,
      status: e?.status,
    });
    throw e;
  }
}

/**
 * Busca informações de um charge
 * @param {string} chargeId
 * @returns {Promise<object>}
 */
export async function getCharge(chargeId) {
  if (!chargeId) {
    throw new Error("chargeId é obrigatório");
  }

  try {
    const result = await vindiRequest("GET", `/charges/${chargeId}`);
    return result.charge;
  } catch (e) {
    err("getCharge falhou", {
      chargeId,
      msg: e?.message,
      status: e?.status,
    });
    throw e;
  }
}

/**
 * Interpreta webhook da Vindi e retorna evento normalizado
 * @param {object} payload - Payload do webhook
 * @returns {object} - { type, billId?, chargeId?, status, metadata? }
 */
export function parseWebhook(payload) {
  if (!payload || typeof payload !== "object") {
    throw new Error("Payload inválido");
  }

  const type = payload.type || payload.event_type || "unknown";
  const data = payload.data || payload;

  // Mapeia tipos de eventos comuns da Vindi
  const eventMap = {
    bill_paid: "bill.paid",
    bill_failed: "bill.failed",
    bill_canceled: "bill.canceled",
    charge_rejected: "charge.rejected",
    charge_refunded: "charge.refunded",
    charge_paid: "charge.paid",
  };

  const normalizedType = eventMap[type] || type;

  const result = {
    type: normalizedType,
    billId: data.bill?.id || data.bill_id || null,
    chargeId: data.charge?.id || data.charge_id || null,
    status: data.bill?.status || data.charge?.status || data.status || null,
    metadata: data.metadata || {},
  };

  log("webhook parseado", {
    originalType: type,
    normalizedType: result.type,
    billId: result.billId,
    chargeId: result.chargeId,
  });

  return result;
}

/**
 * Associa gateway_token a um payment_profile existente (janela de 5 minutos)
 * @param {object} params - { customerId, gatewayToken }
 * @returns {Promise<{paymentProfileId: string, lastFour?: string, cardType?: string}>}
 */
export async function associateGatewayToken({ customerId, gatewayToken }) {
  if (!customerId || !gatewayToken) {
    throw new Error("customerId e gatewayToken são obrigatórios");
  }

  try {
    // O gateway_token deve ser associado dentro de 5 minutos após a tokenização
    // A Vindi permite associar via POST /payment_profiles com gateway_token
    const body = {
      customer_id: customerId,
      gateway_token: gatewayToken,
      payment_method_code: VINDI_DEFAULT_PAYMENT_METHOD,
      payment_company_code: VINDI_DEFAULT_GATEWAY,
    };

    const created = await vindiRequest("POST", "/payment_profiles", body);
    const profile = created.payment_profile;

    log("gateway_token associado", {
      paymentProfileId: profile?.id,
      customerId,
      lastFour: profile?.last_four,
      cardType: profile?.card_type,
    });

    return {
      paymentProfileId: profile?.id,
      lastFour: profile?.last_four || null,
      cardType: profile?.card_type || null,
    };
  } catch (e) {
    err("associateGatewayToken falhou", {
      customerId,
      msg: e?.message,
      status: e?.status,
    });
    throw e;
  }
}

export default {
  ensureCustomer,
  createPaymentProfile,
  createPaymentProfileWithCardData,
  createBill,
  chargeBill,
  refundCharge,
  getBill,
  getCharge,
  parseWebhook,
  associateGatewayToken,
};

