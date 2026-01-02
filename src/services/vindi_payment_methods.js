// backend/src/services/vindi_payment_methods.js
// Helper para obter e cachear payment_methods da Vindi e resolver payment_company_id

const VINDI_BASE =
  (process.env.VINDI_API_BASE_URL && process.env.VINDI_API_BASE_URL.replace(/\/+$/, "")) ||
  "https://app.vindi.com.br/api/v1";

const VINDI_API_KEY = process.env.VINDI_API_KEY || "";

/* ------------------------------------------------------- *
 * Logging estruturado
 * ------------------------------------------------------- */
const LP = "[vindiPaymentMethods]";
const log = (msg, extra = null) => console.log(`${LP} ${msg}`, extra ? JSON.stringify(extra) : "");
const warn = (msg, extra = null) => console.warn(`${LP} ${msg}`, extra ? JSON.stringify(extra) : "");
const err = (msg, extra = null) => console.error(`${LP} ${msg}`, extra ? JSON.stringify(extra) : "");

// Cache em memória: paymentMethodCode -> { companyCode -> { id, code, name? } }
let paymentMethodsCache = {
  data: null,
  expiresAt: 0,
  TTL_MS: 10 * 60 * 1000, // 10 minutos
};

/**
 * Constrói header de autenticação Basic Auth para API privada
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
 * Obtém payment_methods da Vindi via GET /payment_methods
 * Usa cache em memória (TTL 10 minutos)
 * @returns {Promise<object>} Estrutura: { paymentMethodCode -> { companyCode -> { id, code, name? } } }
 */
async function fetchPaymentMethods() {
  // Retorna cache se ainda válido
  if (paymentMethodsCache.data && Date.now() < paymentMethodsCache.expiresAt) {
    return paymentMethodsCache.data;
  }

  if (!VINDI_API_KEY) {
    warn("VINDI_API_KEY não configurado, não é possível obter payment_methods");
    return null;
  }

  try {
    const url = `${VINDI_BASE}/payment_methods`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);

    let response;
    try {
      response = await fetch(url, {
        method: "GET",
        headers: {
          Authorization: buildAuthHeader(),
          Accept: "application/json",
          "User-Agent": "lancaster-backend/1.0",
        },
        signal: controller.signal,
      });
    } catch (e) {
      clearTimeout(timeout);
      if (e?.name === "AbortError") {
        throw new Error("Vindi GET /payment_methods timeout após 10s");
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
      err("Falha ao obter payment_methods da Vindi", {
        status: response.status,
        error: json?.errors?.[0]?.message || json?.error || "unknown",
      });
      return null;
    }

    // Constrói lookup: paymentMethodCode -> { companyCode -> { id, code, name? } }
    const lookup = {};

    if (json?.payment_methods && Array.isArray(json.payment_methods)) {
      json.payment_methods.forEach(method => {
        const methodCode = method?.code;
        if (!methodCode) return;

        if (!lookup[methodCode]) {
          lookup[methodCode] = {};
        }

        if (method?.payment_companies && Array.isArray(method.payment_companies)) {
          method.payment_companies.forEach(company => {
            const companyCode = company?.code?.toLowerCase();
            if (companyCode && company?.id) {
              lookup[methodCode][companyCode] = {
                id: company.id,
                code: company.code,
                name: company.name || null,
              };
            }
          });
        }
      });
    }

    // Atualiza cache
    paymentMethodsCache.data = lookup;
    paymentMethodsCache.expiresAt = Date.now() + paymentMethodsCache.TTL_MS;

    log("payment_methods atualizados do cache Vindi", {
      methods_count: Object.keys(lookup).length,
      methods: Object.keys(lookup),
    });

    return lookup;
  } catch (e) {
    err("Erro ao obter payment_methods da Vindi", {
      msg: e?.message,
    });
    return null;
  }
}

/**
 * Resolve payment_company_id a partir de payment_method_code e payment_company_code
 * @param {object} params - { payment_method_code, payment_company_code }
 * @returns {Promise<number|null>} payment_company_id ou null se não encontrado
 */
export async function resolvePaymentCompanyId({ payment_method_code, payment_company_code }) {
  if (!payment_method_code || !payment_company_code) {
    return null;
  }

  const lookup = await fetchPaymentMethods();
  if (!lookup) {
    return null;
  }

  const methodLookup = lookup[payment_method_code];
  if (!methodLookup) {
    return null;
  }

  const companyCode = String(payment_company_code).trim().toLowerCase();
  const company = methodLookup[companyCode];

  if (company?.id) {
    log("payment_company_id resolvido", {
      payment_method_code,
      payment_company_code: companyCode,
      payment_company_id: company.id,
    });
    return company.id;
  }

  return null;
}

/**
 * Obtém lista de payment_company_codes válidos para um payment_method_code
 * @param {string} payment_method_code - Ex: "credit_card"
 * @returns {Promise<string[]>} Array de códigos válidos
 */
export async function getValidPaymentCompanyCodes(payment_method_code = "credit_card") {
  const lookup = await fetchPaymentMethods();
  if (!lookup) {
    // Fallback para lista padrão
    return ["visa", "mastercard", "elo", "american_express", "diners_club", "hipercard", "hiper"];
  }

  const methodLookup = lookup[payment_method_code];
  if (!methodLookup) {
    return [];
  }

  return Object.keys(methodLookup).map(code => methodLookup[code].code || code);
}

/**
 * Limpa o cache (útil para testes ou forçar refresh)
 */
export function clearCache() {
  paymentMethodsCache.data = null;
  paymentMethodsCache.expiresAt = 0;
}

export default {
  resolvePaymentCompanyId,
  getValidPaymentCompanyCodes,
  clearCache,
};

