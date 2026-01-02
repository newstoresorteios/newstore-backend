// backend/src/services/vindi_public.js
// Integração com Vindi Public API (tokenização de cartão)
// Esta API é chamada do backend para gerar gateway_token a partir de dados do cartão

const VINDI_PUBLIC_BASE =
  (process.env.VINDI_PUBLIC_BASE_URL && process.env.VINDI_PUBLIC_BASE_URL.replace(/\/+$/, "")) ||
  "https://app.vindi.com.br/api/v1";

const VINDI_PUBLIC_KEY = process.env.VINDI_PUBLIC_KEY || "";
const VINDI_DEFAULT_GATEWAY = process.env.VINDI_DEFAULT_GATEWAY || "pagarme";
const VINDI_API_KEY = process.env.VINDI_API_KEY || ""; // Para GET /payment_methods

// Cache em memória para payment_company_codes válidos da Vindi
let paymentCompanyCodesCache = {
  codes: null,
  expiresAt: 0,
  TTL_MS: 60 * 60 * 1000, // 1 hora
};

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
 * IMPORTANTE: Prioriza Elo antes de Visa devido à sobreposição de prefixos
 * Retorna: { brandCode: string } onde brandCode ∈ ["visa","mastercard","elo","american_express","diners_club","hipercard","hiper"]
 */
export function detectPaymentCompanyCode(cardNumber) {
  const clean = String(cardNumber).replace(/\D+/g, "");
  
  if (!clean || clean.length < 4) {
    return null; // Não detectado
  }
  
  // PRIORIDADE 1: Elo - DEVE vir antes de Visa porque Elo tem prefixos que começam com "4"
  // Prefixos Elo completos: 4011, 4312, 4389, 4514, 4573, 5041, 5066, 5067, 5090, 6278, 6362, 6363, 636368, 6500, 6504, 6505, 6507, 6509, 6516, 6550
  if (/^(4011|4312|4389|4514|4573|5041|5066|5067|5090|6278|6362|6363|636368|6500|6504|6505|6507|6509|6516|6550)/.test(clean)) {
    return { brandCode: "elo" };
  }
  
  // PRIORIDADE 2: Hipercard/Hiper - antes de Diners Club (sobreposição com 38)
  if (/^(38|60)/.test(clean)) {
    return { brandCode: "hipercard" }; // ou "hiper" dependendo da conta
  }
  
  // PRIORIDADE 3: Diners Club (30, 36)
  if (/^(30|36)/.test(clean)) {
    return { brandCode: "diners_club" };
  }
  
  // PRIORIDADE 4: American Express (34, 37)
  if (/^3[47]/.test(clean)) {
    return { brandCode: "american_express" };
  }
  
  // PRIORIDADE 5: Mastercard (51-55 ou 2221-2720)
  if (/^5[1-5]/.test(clean) || /^2[2-7]/.test(clean)) {
    return { brandCode: "mastercard" };
  }
  
  // PRIORIDADE 6: Visa (começa com 4) - ÚLTIMA porque Elo tem prefixos que começam com 4
  if (clean.startsWith("4")) {
    return { brandCode: "visa" };
  }
  
  // Não detectado
  return null;
}

// Alias para compatibilidade
function detectCardBrand(cardNumber) {
  return detectPaymentCompanyCode(cardNumber);
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
 * Constrói header de autenticação Basic Auth para API privada
 * Formato: base64("API_KEY:")
 */
function buildPrivateAuthHeader() {
  if (!VINDI_API_KEY) {
    throw new Error("VINDI_API_KEY não configurado no servidor.");
  }
  const authString = `${VINDI_API_KEY}:`;
  const encoded = Buffer.from(authString).toString("base64");
  return `Basic ${encoded}`;
}

/**
 * Obtém lista de payment_company_codes válidos da Vindi via GET /payment_methods
 * Usa cache em memória (TTL 1 hora)
 * @returns {Promise<string[]>} Array de códigos válidos (ex: ["visa", "mastercard", "elo", ...])
 */
async function getValidPaymentCompanyCodes() {
  // Retorna cache se ainda válido
  if (paymentCompanyCodesCache.codes && Date.now() < paymentCompanyCodesCache.expiresAt) {
    return paymentCompanyCodesCache.codes;
  }
  
  // Se não tem VINDI_API_KEY, retorna lista padrão conhecida
  if (!VINDI_API_KEY) {
    warn("VINDI_API_KEY não configurado, usando lista padrão de payment_company_codes");
    const defaultCodes = ["visa", "mastercard", "elo", "american_express", "diners_club", "hipercard", "hiper"];
    paymentCompanyCodesCache.codes = defaultCodes;
    paymentCompanyCodesCache.expiresAt = Date.now() + paymentCompanyCodesCache.TTL_MS;
    return defaultCodes;
  }
  
  try {
    const url = `${VINDI_PUBLIC_BASE}/payment_methods`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);
    
    let response;
    try {
      response = await fetch(url, {
        method: "GET",
        headers: {
          Authorization: buildPrivateAuthHeader(),
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
      warn("Falha ao obter payment_methods da Vindi, usando lista padrão", {
        status: response.status,
        error: json?.errors?.[0]?.message || json?.error || "unknown",
      });
      const defaultCodes = ["visa", "mastercard", "elo", "american_express", "diners_club", "hipercard", "hiper"];
      paymentCompanyCodesCache.codes = defaultCodes;
      paymentCompanyCodesCache.expiresAt = Date.now() + paymentCompanyCodesCache.TTL_MS;
      return defaultCodes;
    }
    
    // Extrai payment_company_codes únicos de payment_methods
    const codes = new Set();
    if (json?.payment_methods && Array.isArray(json.payment_methods)) {
      json.payment_methods.forEach(method => {
        if (method?.payment_companies && Array.isArray(method.payment_companies)) {
          method.payment_companies.forEach(company => {
            if (company?.code) {
              codes.add(company.code.toLowerCase());
            }
          });
        }
      });
    }
    
    // Se não encontrou nenhum, usa lista padrão
    const validCodes = codes.size > 0 ? Array.from(codes) : ["visa", "mastercard", "elo", "american_express", "diners_club", "hipercard", "hiper"];
    
    // Atualiza cache
    paymentCompanyCodesCache.codes = validCodes;
    paymentCompanyCodesCache.expiresAt = Date.now() + paymentCompanyCodesCache.TTL_MS;
    
    log("payment_company_codes atualizados do cache Vindi", {
      count: validCodes.length,
      codes: validCodes,
    });
    
    return validCodes;
  } catch (e) {
    err("Erro ao obter payment_company_codes da Vindi, usando lista padrão", {
      msg: e?.message,
    });
    const defaultCodes = ["visa", "mastercard", "elo", "american_express", "diners_club", "hipercard", "hiper"];
    paymentCompanyCodesCache.codes = defaultCodes;
    paymentCompanyCodesCache.expiresAt = Date.now() + paymentCompanyCodesCache.TTL_MS;
    return defaultCodes;
  }
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
  
  // Obtém lista de payment_company_codes válidos da Vindi (com cache)
  const validBrandCodes = await getValidPaymentCompanyCodes();
  let brandCode = null;
  let brandCodeSource = null;
  
  // PRIORIDADE 1: payment_company_code do frontend (se fornecido e válido)
  if (payload.payment_company_code) {
    const providedCode = String(payload.payment_company_code).trim().toLowerCase();
    if (validBrandCodes.includes(providedCode)) {
      brandCode = providedCode;
      brandCodeSource = "frontend";
    } else {
      warn("payment_company_code do frontend inválido, tentando detecção automática", {
        provided: providedCode,
        valid_codes: validBrandCodes,
      });
    }
  }
  
  // PRIORIDADE 2: Detecção automática local (se não veio do frontend ou é inválido)
  if (!brandCode) {
    const detectedBrand = detectPaymentCompanyCode(cleanCardNumber);
    if (detectedBrand?.brandCode) {
      // Valida se o código detectado está na lista da Vindi
      if (validBrandCodes.includes(detectedBrand.brandCode)) {
        brandCode = detectedBrand.brandCode;
        brandCodeSource = "detected";
      } else {
        // Código detectado não está disponível na conta Vindi
        warn("bandeira detectada não disponível na conta Vindi", {
          detected: detectedBrand.brandCode,
          available: validBrandCodes,
        });
      }
    }
  }
  
  // Valida se temos payment_company_code válido (obrigatório para credit_card)
  const paymentMethodCode = payload.payment_method_code || "credit_card";
  if (paymentMethodCode === "credit_card" && (!brandCode || !validBrandCodes.includes(brandCode))) {
    const maskedCard = maskCardNumber(cleanCardNumber);
    const error = new Error("Não foi possível detectar a bandeira. Informe a bandeira e tente novamente.");
    error.status = 422;
    error.details = [{
      parameter: "payment_company_code",
      message: `Bandeira não detectada ou inválida. Códigos válidos: ${validBrandCodes.join(", ")}`,
    }];
    err("bandeira não detectada ou inválida", {
      card_masked: maskedCard,
      provided_payment_company_code: payload.payment_company_code || null,
      detected_brand: detectPaymentCompanyCode(cleanCardNumber)?.brandCode || null,
      valid_codes: validBrandCodes,
    });
    throw error;
  }
  
  // Expiration: aceita MM/YY, MM/YYYY ou campos separados
  // A Vindi espera MM/YY (2 dígitos do ano)
  let normalizedMonth, normalizedYear2Digits;
  
  if (payload.card_expiration) {
    // Formato MM/YY ou MM/YYYY
    const parts = String(payload.card_expiration).split("/");
    if (parts.length !== 2) {
      const error = new Error("card_expiration deve estar no formato MM/YY ou MM/YYYY");
      error.status = 422;
      throw error;
    }
    normalizedMonth = parts[0].replace(/\D+/g, "").padStart(2, "0");
    let yearPart = parts[1].replace(/\D+/g, "");
    
    // Normaliza para 2 dígitos do ano (YY)
    if (yearPart.length === 2) {
      normalizedYear2Digits = yearPart;
    } else if (yearPart.length === 4) {
      // Pega os últimos 2 dígitos do ano de 4 dígitos
      normalizedYear2Digits = yearPart.slice(-2);
    } else {
      const error = new Error("card_expiration: ano deve ter 2 ou 4 dígitos (formato MM/YY ou MM/YYYY)");
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
    let fullYear;
    if (year.length === 2) {
      fullYear = `20${year}`;
    } else if (year.length === 4) {
      fullYear = year;
    } else {
      const error = new Error("card_expiration_year deve ter 2 ou 4 dígitos");
      error.status = 422;
      throw error;
    }
    
    // Pega os últimos 2 dígitos do ano (YY)
    normalizedYear2Digits = fullYear.slice(-2);
  }

  // Formato final: MM/YY (conforme documentação Vindi)
  const cardExpiration = `${normalizedMonth}/${normalizedYear2Digits}`;

  try {
    // Constrói form data (x-www-form-urlencoded)
    const form = new URLSearchParams();
    form.set("allow_as_fallback", "true");
    form.set("holder_name", String(payload.holder_name).slice(0, 120));
    form.set("card_number", cleanCardNumber);
    form.set("card_expiration", cardExpiration); // formato MM/YY conforme documentação Vindi
    form.set("card_cvv", String(payload.card_cvv).slice(0, 4));
    form.set("payment_method_code", paymentMethodCode);
    // Sempre envia payment_company_code quando for credit_card (obrigatório para Elo/Hipercard/Hiper)
    if (paymentMethodCode === "credit_card" && brandCode) {
      form.set("payment_company_code", brandCode);
    }
    
    if (payload.document_number) {
      form.set("document_number", String(payload.document_number).replace(/\D+/g, "").slice(0, 18));
    }
    
    // Log do payload mascarado (antes da chamada) - mostra payment_company_code efetivo
    const maskedCard = maskCardNumber(cleanCardNumber);
    log("chamando Vindi Public API", {
      user_id: payload.user_id || null,
      holder_name: payload.holder_name,
      card_masked: maskedCard,
      card_expiration: cardExpiration,
      payment_company_code: brandCode, // Código efetivo que será enviado
      payment_method_code: paymentMethodCode,
      payment_company_code_source: brandCodeSource || "unknown",
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
  return detectPaymentCompanyCode(cardNumber);
}

export default {
  tokenizeCardPublic,
  validateCardBrand,
  detectPaymentCompanyCode,
};

