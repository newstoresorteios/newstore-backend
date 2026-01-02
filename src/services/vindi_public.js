// backend/src/services/vindi_public.js
// Integração com Vindi Public API (tokenização de cartão)
// Esta API é chamada do backend para gerar gateway_token a partir de dados do cartão

const VINDI_PUBLIC_BASE =
  (process.env.VINDI_PUBLIC_BASE_URL && process.env.VINDI_PUBLIC_BASE_URL.replace(/\/+$/, "")) ||
  "https://app.vindi.com.br/api/v1";

const VINDI_PUBLIC_KEY = process.env.VINDI_PUBLIC_KEY || "";
const VINDI_DEFAULT_GATEWAY = process.env.VINDI_DEFAULT_GATEWAY || "pagarme";

/* ------------------------------------------------------- *
 * Logging estruturado (sem segredos)
 * ------------------------------------------------------- */
const LP = "[vindiPublic]";
const log = (msg, extra = null) => console.log(`${LP} ${msg}`, extra ? JSON.stringify(extra) : "");
const warn = (msg, extra = null) => console.warn(`${LP} ${msg}`, extra ? JSON.stringify(extra) : "");
const err = (msg, extra = null) => console.error(`${LP} ${msg}`, extra ? JSON.stringify(extra) : "");

/**
 * Mascara número do cartão para logs (mostra apenas últimos 4 dígitos)
 */
function maskCardNumber(cardNumber) {
  if (!cardNumber) return "****";
  const clean = String(cardNumber).replace(/\D+/g, "");
  if (clean.length < 4) return "****";
  return `****${clean.slice(-4)}`;
}

/**
 * Detecta bandeira do cartão pelo número (algoritmo de Luhn + prefixos)
 * Retorna: { brand: string, payment_company_code: string }
 */
function detectCardBrand(cardNumber) {
  const clean = String(cardNumber).replace(/\D+/g, "");
  
  // Visa: começa com 4
  if (clean.startsWith("4")) {
    return { brand: "visa", payment_company_code: "visa" };
  }
  
  // Mastercard: 51-55 ou 2221-2720
  if (/^5[1-5]/.test(clean) || /^2[2-7]/.test(clean)) {
    return { brand: "mastercard", payment_company_code: "mastercard" };
  }
  
  // Amex: 34 ou 37
  if (/^3[47]/.test(clean)) {
    return { brand: "american_express", payment_company_code: "american_express" };
  }
  
  // Elo: vários prefixos
  if (/^(4011|4312|4389|4514|4573|5041|5066|5067|5090|6278|6362|6363|6500|6504|6505|6507|6509|6516|6550)/.test(clean)) {
    return { brand: "elo", payment_company_code: "elo" };
  }
  
  // Hipercard: 38 ou 60
  if (/^(38|60)/.test(clean)) {
    return { brand: "hipercard", payment_company_code: "hipercard" };
  }
  
  // Default: visa
  return { brand: "visa", payment_company_code: "visa" };
}

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

  // Validações obrigatórias: aceita card_expiration OU (card_expiration_month + card_expiration_year)
  if (!payload?.holder_name || !payload?.card_number || (!payload?.card_expiration && (!payload?.card_expiration_month || !payload?.card_expiration_year)) || !payload?.card_cvv) {
    const error = new Error("Campos obrigatórios: holder_name, card_number, card_expiration/card_expiration_month+year, card_cvv");
    error.status = 422;
    throw error;
  }

  // Normalizações
  const cleanCardNumber = String(payload.card_number).replace(/\D+/g, "");
  
  // Detecta bandeira e payment_company_code
  const { brand, payment_company_code } = detectCardBrand(cleanCardNumber);
  
  // Expiration: aceita MM/YYYY ou campos separados
  let normalizedMonth, normalizedYear;
  
  if (payload.card_expiration) {
    // Formato MM/YYYY
    const parts = String(payload.card_expiration).split("/");
    if (parts.length !== 2) {
      const error = new Error("card_expiration deve estar no formato MM/YYYY");
      error.status = 422;
      throw error;
    }
    normalizedMonth = parts[0].padStart(2, "0");
    normalizedYear = parts[1];
  } else {
    // Campos separados
    let month = Number(payload.card_expiration_month);
    if (month < 1 || month > 12) {
      const error = new Error("card_expiration_month deve ser entre 1 e 12");
      error.status = 422;
      throw error;
    }
    normalizedMonth = String(month).padStart(2, "0");

    let year = String(payload.card_expiration_year);
    if (year.length === 2) {
      normalizedYear = `20${year}`;
    } else if (year.length === 4) {
      normalizedYear = year;
    } else {
      const error = new Error("card_expiration_year deve ter 2 ou 4 dígitos");
      error.status = 422;
      throw error;
    }
  }

  const cardExpiration = `${normalizedMonth}/${normalizedYear}`;

  try {
    const body = {
      holder_name: String(payload.holder_name).slice(0, 120),
      card_number: cleanCardNumber,
      card_expiration: cardExpiration, // formato esperado pela Vindi Public API
      card_cvv: String(payload.card_cvv).slice(0, 4),
      payment_method_code: payload.payment_method_code || "credit_card",
    };

    // Para Visa/Master a Vindi detecta automaticamente, mas é recomendado enviar.
    // Para Elo/Amex/Diners/Hipercard é obrigatório/fortemente recomendado.
    const pcc = payload.payment_company_code || payment_company_code;
    if (pcc) body.payment_company_code = pcc;
    
    // Log do payload mascarado (antes da chamada)
    const maskedCard = maskCardNumber(cleanCardNumber);
    log("chamando Vindi Public API", {
      holder_name: payload.holder_name,
      card_last4: maskedCard.slice(-4),
      card_expiration: cardExpiration,
      brand,
      payment_company_code: pcc || null,
      has_cvv: !!payload.card_cvv,
    });

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
    
    // Log da resposta (mascarada)
    if (response.ok) {
      log("Vindi Public API resposta OK", {
        status: response.status,
        has_gateway_token: !!json?.payment_profile?.gateway_token,
        payment_profile_id: json?.payment_profile?.id,
        card_last4: json?.payment_profile?.last_four || null,
        brand: json?.payment_profile?.card_type || brand,
      });
    } else {
      const errorMessages = json?.errors?.map(e => e.message).filter(Boolean) || [];
      err("Vindi Public API erro", {
        status: response.status,
        error_count: json?.errors?.length || 0,
        error_messages: errorMessages,
      });
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

