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
 * Mascara número do cartão para logs (ex: 6504********5236)
 */
function maskCardNumber(cardNumber) {
  if (!cardNumber) return "****";
  const clean = String(cardNumber).replace(/\D+/g, "");
  if (clean.length < 4) return "****";
  if (clean.length <= 8) return `****${clean.slice(-4)}`;
  // Mostra primeiros 4 e últimos 4, mascarando o meio
  const first4 = clean.slice(0, 4);
  const last4 = clean.slice(-4);
  const middle = "*".repeat(Math.max(0, clean.length - 8));
  return `${first4}${middle}${last4}`;
}

/**
 * Detecta bandeira do cartão pelo número (BIN/prefixos)
 * Retorna: { brandCode: string } onde brandCode ∈ ["visa","mastercard","elo","american_express","diners_club","hipercard"]
 */
function detectCardBrand(cardNumber) {
  const clean = String(cardNumber).replace(/\D+/g, "");
  
  if (!clean || clean.length < 4) {
    return null; // Não detectado
  }
  
  // Elo: prefixos comuns (incluindo 636368, 6504, etc)
  // Prefixos Elo: 4011, 4312, 4389, 4514, 4573, 5041, 5066, 5067, 5090, 6278, 6362, 6363, 636368, 6500, 6504, 6505, 6507, 6509, 6516, 6550
  if (/^(4011|4312|4389|4514|4573|5041|5066|5067|5090|6278|6362|6363|636368|6500|6504|6505|6507|6509|6516|6550)/.test(clean)) {
    return { brandCode: "elo" };
  }
  
  // Visa: começa com 4
  if (clean.startsWith("4")) {
    return { brandCode: "visa" };
  }
  
  // Mastercard: 51-55 ou 2221-2720
  if (/^5[1-5]/.test(clean) || /^2[2-7]/.test(clean)) {
    return { brandCode: "mastercard" };
  }
  
  // American Express: 34 ou 37
  if (/^3[47]/.test(clean)) {
    return { brandCode: "american_express" };
  }
  
  // Diners Club: 30, 36, 38 (mas 38 pode ser Hipercard também)
  // Priorizamos Diners Club para 30 e 36, 38 fica para Hipercard
  if (/^(30|36)/.test(clean)) {
    return { brandCode: "diners_club" };
  }
  
  // Hipercard: 38 ou 60
  if (/^(38|60)/.test(clean)) {
    return { brandCode: "hipercard" };
  }
  
  // Não detectado
  return null;
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
  
  // Detecta bandeira (se não fornecida no payload)
  const detectedBrand = detectCardBrand(cleanCardNumber);
  const brandCode = payload.payment_company_code || (detectedBrand?.brandCode);
  
  // Valida se temos payment_company_code válido
  const validBrandCodes = ["visa", "mastercard", "elo", "american_express", "diners_club", "hipercard"];
  if (!brandCode || !validBrandCodes.includes(brandCode)) {
    const maskedCard = maskCardNumber(cleanCardNumber);
    const error = new Error(`Bandeira do cartão não detectada ou inválida. Forneça payment_company_code válido (${validBrandCodes.join(", ")})`);
    error.status = 422;
    err("bandeira não detectada", {
      card_masked: maskedCard,
      detected_brand: detectedBrand?.brandCode || null,
      provided_payment_company_code: payload.payment_company_code || null,
    });
    throw error;
  }
  
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
    normalizedMonth = parts[0].replace(/\D+/g, "").padStart(2, "0");
    let yearPart = parts[1].replace(/\D+/g, "");
    // Normaliza ano para 4 dígitos
    if (yearPart.length === 2) {
      normalizedYear = `20${yearPart}`;
    } else if (yearPart.length === 4) {
      normalizedYear = yearPart;
    } else {
      const error = new Error("card_expiration: ano deve ter 2 ou 4 dígitos (formato MM/YYYY)");
      error.status = 422;
      throw error;
    }
  } else {
    // Campos separados
    let month = Number(payload.card_expiration_month);
    if (month < 1 || month > 12) {
      const error = new Error("card_expiration_month deve ser entre 1 e 12");
      error.status = 422;
      throw error;
    }
    normalizedMonth = String(month).padStart(2, "0");

    let year = String(payload.card_expiration_year).replace(/\D+/g, "");
    if (year.length === 2) {
      normalizedYear = `20${year}`;
    } else if (year.length === 4) {
      normalizedYear = year;
    } else {
      const error = new Error("card_expiration_year deve ter 2 ou 4 dígitos");
      error.status = 422;
      throw error;
    }
    
    // Garante que o ano seja 4 dígitos
    if (normalizedYear.length !== 4) {
      const error = new Error("card_expiration_year deve resultar em ano com 4 dígitos");
      error.status = 422;
      throw error;
    }
    
    // Garante que o ano seja 4 dígitos
    if (normalizedYear.length !== 4) {
      const error = new Error("card_expiration_year deve resultar em ano com 4 dígitos");
      error.status = 422;
      throw error;
    }
  }

  const cardExpiration = `${normalizedMonth}/${normalizedYear}`;

  try {
    // Constrói form data (x-www-form-urlencoded)
    const form = new URLSearchParams();
    form.set("allow_as_fallback", "true");
    form.set("holder_name", String(payload.holder_name).slice(0, 120));
    form.set("card_number", cleanCardNumber);
    form.set("card_expiration", cardExpiration); // formato MM/YYYY esperado pela Vindi Public API
    form.set("card_cvv", String(payload.card_cvv).slice(0, 4));
    form.set("payment_method_code", payload.payment_method_code || "credit_card");
    form.set("payment_company_code", brandCode); // Sempre envia payment_company_code válido
    
    if (payload.document_number) {
      form.set("document_number", String(payload.document_number).replace(/\D+/g, "").slice(0, 18));
    }
    
    // Log do payload mascarado (antes da chamada)
    const maskedCard = maskCardNumber(cleanCardNumber);
    log("chamando Vindi Public API", {
      holder_name: payload.holder_name,
      card_masked: maskedCard,
      card_expiration: cardExpiration,
      payment_company_code: brandCode,
      payment_method_code: payload.payment_method_code || "credit_card",
      has_cvv: !!payload.card_cvv,
      has_document_number: !!payload.document_number,
    });

    const url = `${VINDI_PUBLIC_BASE}/public/payment_profiles`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 30000);

    let response;
    try {
      response = await fetch(url, {
        method: "POST",
        headers: {
          Authorization: buildPublicAuthHeader(),
          "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
          Accept: "application/json",
          "User-Agent": "lancaster-backend/1.0",
        },
        body: form.toString(),
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
        card_type: json?.payment_profile?.card_type || null,
        payment_company_code_sent: brandCode,
      });
    } else {
      const errorMessages = json?.errors?.map(e => e.message).filter(Boolean) || [];
      const errorParameters = json?.errors?.map(e => e.parameter).filter(Boolean) || [];
      err("Vindi Public API erro", {
        status: response.status,
        error_count: json?.errors?.length || 0,
        error_messages: errorMessages,
        error_parameters: errorParameters,
      });
    }

    if (!response.ok) {
      // Se Vindi retornar JSON com errors[0].message, usar essa mensagem
      // Prioriza mensagens mais específicas do array de erros
      let errorMsg = null;
      const errorsWithDetails = [];
      
      if (json?.errors && Array.isArray(json.errors) && json.errors.length > 0) {
        // Captura todos os erros com message e parameter
        json.errors.forEach(e => {
          if (e.message || e.parameter) {
            errorsWithDetails.push({
              message: e.message || null,
              parameter: e.parameter || null,
            });
          }
        });
        
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
      error.response = {
        ...json,
        errors: errorsWithDetails.length > 0 ? errorsWithDetails : json?.errors || [],
      };
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

/**
 * Função de validação/teste para detecção de bandeiras
 * @param {string} cardNumber - Número do cartão (pode ter formatação)
 * @returns {object|null} - { brandCode: string } ou null se não detectado
 * 
 * Exemplos:
 * - "6363680000000000" => { brandCode: "elo" }
 * - "6504123456789012" => { brandCode: "elo" }
 * - "4111111111111111" => { brandCode: "visa" }
 * - "5555555555554444" => { brandCode: "mastercard" }
 */
export function validateCardBrand(cardNumber) {
  return detectCardBrand(cardNumber);
}

export default {
  tokenizeCardPublic,
  validateCardBrand,
};

