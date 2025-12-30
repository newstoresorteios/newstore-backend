// backend/src/services/vindi_public.js
// Integração com Vindi Public API (tokenização de cartão)
// Esta API é chamada do backend para gerar gateway_token a partir de dados do cartão

const VINDI_PUBLIC_BASE =
  (process.env.VINDI_PUBLIC_BASE_URL && process.env.VINDI_PUBLIC_BASE_URL.replace(/\/+$/, "")) ||
  "https://app.vindi.com.br/api/v1";

const VINDI_PUBLIC_KEY = process.env.VINDI_PUBLIC_KEY || "";

/* ------------------------------------------------------- *
 * Logging estruturado (sem segredos)
 * ------------------------------------------------------- */
const LP = "[vindiPublic]";
const log = (msg, extra = null) => console.log(`${LP} ${msg}`, extra ? JSON.stringify(extra) : "");
const warn = (msg, extra = null) => console.warn(`${LP} ${msg}`, extra ? JSON.stringify(extra) : "");
const err = (msg, extra = null) => console.error(`${LP} ${msg}`, extra ? JSON.stringify(extra) : "");

/**
 * Constrói header de autenticação Basic Auth para Public API
 * Formato: base64("PUBLIC_KEY:")
 */
function buildPublicAuthHeader() {
  if (!VINDI_PUBLIC_KEY) {
    throw new Error("VINDI_PUBLIC_KEY não configurado no servidor.");
  }
  const authString = `${VINDI_PUBLIC_KEY}:`;
  const encoded = Buffer.from(authString).toString("base64");
  return `Basic ${encoded}`;
}

/**
 * Tokeniza cartão via Vindi Public API
 * @param {object} payload - { holder_name, card_number, card_expiration_month, card_expiration_year, card_cvv, payment_method_code?, document_number? }
 * @returns {Promise<{gatewayToken: string, paymentProfile: object}>}
 */
export async function tokenizeCardPublic(payload) {
  if (!VINDI_PUBLIC_KEY) {
    const error = new Error("VINDI_PUBLIC_KEY não configurado no servidor.");
    error.status = 503;
    throw error;
  }

  // Validações obrigatórias
  if (!payload?.holder_name || !payload?.card_number || !payload?.card_expiration_month || !payload?.card_expiration_year || !payload?.card_cvv) {
    const error = new Error("Campos obrigatórios: holder_name, card_number, card_expiration_month, card_expiration_year, card_cvv");
    error.status = 422;
    throw error;
  }

  // Normalizações
  const cleanCardNumber = String(payload.card_number).replace(/\D+/g, "");
  
  // Month: 1-12
  let month = Number(payload.card_expiration_month);
  if (month < 1 || month > 12) {
    const error = new Error("card_expiration_month deve ser entre 1 e 12");
    error.status = 422;
    throw error;
  }
  const normalizedMonth = String(month).padStart(2, "0");

  // Year: aceitar "YY" e converter para "20YY", ou aceitar "YYYY"
  // A Vindi espera "YYYY" no body (não "YY")
  let year = String(payload.card_expiration_year);
  let normalizedYear;
  if (year.length === 2) {
    // "YY" -> "20YY"
    normalizedYear = `20${year}`;
  } else if (year.length === 4) {
    // "YYYY" -> usar como está
    normalizedYear = year;
  } else {
    const error = new Error("card_expiration_year deve ter 2 ou 4 dígitos");
    error.status = 422;
    throw error;
  }

  try {
    const body = {
      holder_name: String(payload.holder_name).slice(0, 120),
      card_number: cleanCardNumber,
      card_expiration_month: normalizedMonth,
      card_expiration_year: normalizedYear, // Vindi espera "YYYY", não "YY"
      card_cvv: String(payload.card_cvv).slice(0, 4),
      payment_method_code: payload.payment_method_code || "credit_card",
    };

    if (payload.document_number) {
      body.document_number = String(payload.document_number).replace(/\D+/g, "").slice(0, 18);
    }

    const url = `${VINDI_PUBLIC_BASE}/public/payment_profiles`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 30000);

    let response;
    try {
      response = await fetch(url, {
        method: "POST",
        headers: {
          Authorization: buildPublicAuthHeader(),
          "Content-Type": "application/json",
          Accept: "application/json",
          "User-Agent": "lancaster-backend/1.0",
        },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
    } catch (e) {
      clearTimeout(timeout);
      if (e?.name === "AbortError") {
        throw new Error("Vindi Public API timeout após 30s");
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
      // Se Vindi retornar JSON com errors[0].message, usar essa mensagem
      // Prioriza mensagens mais específicas do array de erros
      let errorMsg = null;
      
      if (json?.errors && Array.isArray(json.errors) && json.errors.length > 0) {
        // Busca a primeira mensagem disponível no array de erros
        const firstError = json.errors.find(e => e.message);
        if (firstError) {
          errorMsg = firstError.message;
        } else if (json.errors[0]) {
          // Se não tem message, tenta usar o erro como string
          errorMsg = String(json.errors[0]);
        }
      }
      
      // Fallback para outros formatos de erro
      if (!errorMsg) {
        errorMsg = json?.error || json?.message || `Vindi Public API falhou (${response.status})`;
      }

      const error = new Error(errorMsg);
      error.status = response.status; // Preserva status original (401/422 etc)
      error.response = json;
      throw error;
    }

    const paymentProfile = json?.payment_profile || json;
    const gatewayToken = paymentProfile?.gateway_token || json?.gateway_token;

    if (!gatewayToken) {
      const error = new Error("Vindi não retornou gateway_token");
      error.status = 500;
      throw error;
    }

    log("tokenização bem-sucedida", {
      hasToken: !!gatewayToken,
      // NÃO logar dados sensíveis
    });

    return {
      gatewayToken,
      paymentProfile: paymentProfile || {},
    };
  } catch (e) {
    err("tokenizeCardPublic falhou", {
      status: e?.status,
      msg: e?.message,
      // NÃO logar dados do cartão
    });
    throw e;
  }
}

export default {
  tokenizeCardPublic,
};

