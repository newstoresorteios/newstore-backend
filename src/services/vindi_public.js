// backend/src/services/vindi_public.js
// Integração com Vindi Public API (tokenização de cartão)
// Esta API é chamada do backend para gerar gateway_token a partir de dados do cartão

/* ------------------------------------------------------- *
 * Helper: Normaliza URL base da Vindi Public API
 * ------------------------------------------------------- */
function normalizeBaseUrl(envValue, fallback) {
  if (!envValue) {
    return fallback;
  }
  
  const trimmed = String(envValue).trim();
  
  // Se não começa com http, logar warning e usar fallback
  if (!trimmed.startsWith("http://") && !trimmed.startsWith("https://")) {
    console.warn(`[vindiPublic] VINDI_PUBLIC_BASE_URL inválida (não começa com http): "${trimmed}". Usando fallback.`);
    return fallback;
  }
  
  // Remove trailing slashes
  return trimmed.replace(/\/+$/, "");
}

// Base URL da Vindi Public API
// Configurável via VINDI_PUBLIC_BASE_URL ou VINDI_PUBLIC_URL
// Produção: https://app.vindi.com.br/api/v1
// Sandbox: https://sandbox-app.vindi.com.br/api/v1
const rawPublicBaseUrl = process.env.VINDI_PUBLIC_BASE_URL || process.env.VINDI_PUBLIC_URL;
const VINDI_PUBLIC_BASE = normalizeBaseUrl(
  rawPublicBaseUrl,
  "https://app.vindi.com.br/api/v1"
);

// Log diagnóstico no boot (sem expor secrets)
const publicBaseUrlHost = VINDI_PUBLIC_BASE ? new URL(VINDI_PUBLIC_BASE).host : "N/A";
console.log(`[vindiPublic] VINDI_PUBLIC_BASE configurado: ${publicBaseUrlHost}`);

// Sanity-check de env: VINDI_PUBLIC_KEY
const rawPublicKey = process.env.VINDI_PUBLIC_KEY || "";
const VINDI_PUBLIC_KEY = String(rawPublicKey).trim();
const VINDI_PUBLIC_KEY_SET = !!VINDI_PUBLIC_KEY;
console.log(`[vindiPublic] VINDI_PUBLIC_KEY setado: ${VINDI_PUBLIC_KEY_SET}`);

const VINDI_DEFAULT_GATEWAY = process.env.VINDI_DEFAULT_GATEWAY || "pagarme";

// Sanity-check de env: VINDI_API_KEY (para GET /payment_methods)
const rawApiKey = process.env.VINDI_API_KEY || "";
const VINDI_API_KEY = String(rawApiKey).trim();
const VINDI_API_KEY_SET = !!VINDI_API_KEY;

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
    const error = new Error("VINDI_PUBLIC_KEY não configurado no servidor.");
    error.status = 503;
    error.code = "VINDI_PUBLIC_CONFIG_ERROR";
    throw error;
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
    error.code = "VINDI_PUBLIC_CONFIG_ERROR";
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
  
  // payment_company_code já vem normalizado do handler da rota
  // Se não veio, tenta detectar (mas não bloqueia se não detectar)
  const paymentMethodCode = payload.payment_method_code || "credit_card";
  const validCodes = ["visa", "mastercard", "elo", "american_express", "diners_club", "hipercard", "hiper"];
  let brandCode = null;
  let brandCodeSource = null;
  
  // PRIORIDADE 1: payment_company_code do payload (vindo da rota)
  if (payload.payment_company_code) {
    const providedCode = String(payload.payment_company_code).trim().toLowerCase();
    if (validCodes.includes(providedCode)) {
      brandCode = providedCode;
      brandCodeSource = "frontend";
    } else {
      warn("payment_company_code inválido (formato), tentando detecção", {
        provided: providedCode,
        valid_codes: validCodes,
      });
    }
  }
  
  // PRIORIDADE 2: Detecção automática local (se não veio ou é inválido)
  if (!brandCode && paymentMethodCode === "credit_card") {
    const detectedBrand = detectPaymentCompanyCode(cleanCardNumber);
    if (detectedBrand?.brandCode && validCodes.includes(detectedBrand.brandCode)) {
      brandCode = detectedBrand.brandCode;
      brandCodeSource = "backend-detected";
    }
  }
  
  // Log se não conseguiu determinar payment_company_code (mas não bloqueia - Vindi tentará detecção)
  if (!brandCode && paymentMethodCode === "credit_card") {
    warn("payment_company_code não determinado - Vindi tentará detecção automática", {
      card_masked: maskCardNumber(cleanCardNumber),
      provided: payload.payment_company_code || null,
      detected: detectPaymentCompanyCode(cleanCardNumber)?.brandCode || null,
    });
  }
  
  // Expiration: aceita MM/YYYY (sempre normalizado para 4 dígitos do ano)
  // A rota já normaliza para MM/YYYY, mas garantimos aqui também
  let cardExpiration = null;
  
  if (payload.card_expiration) {
    // Formato MM/YYYY (já normalizado pela rota)
    const parts = String(payload.card_expiration).split("/");
    if (parts.length === 2) {
      const month = parts[0].replace(/\D+/g, "").padStart(2, "0");
      let yearPart = parts[1].replace(/\D+/g, "");
      
      // Garante que o ano seja 4 dígitos
      if (yearPart.length === 2) {
        // MM/YY: assume 20YY se YY <= 79, senão 19YY
        const yy = parseInt(yearPart, 10);
        yearPart = yy <= 79 ? `20${yearPart.padStart(2, "0")}` : `19${yearPart.padStart(2, "0")}`;
      } else if (yearPart.length !== 4) {
        const error = new Error("card_expiration: ano deve ter 2 ou 4 dígitos (formato MM/YY ou MM/YYYY)");
        error.status = 422;
        throw error;
      }
      
      cardExpiration = `${month}/${yearPart}`;
    } else {
      const error = new Error("card_expiration deve estar no formato MM/YYYY");
      error.status = 422;
      throw error;
    }
  } else {
    const error = new Error("card_expiration é obrigatório (formato MM/YYYY)");
    error.status = 422;
    throw error;
  }

  try {
    // Constrói form data (x-www-form-urlencoded) - campos exatos esperados pela Vindi
    const form = new URLSearchParams();
    
    // allow_as_fallback sempre true
    form.set("allow_as_fallback", payload.allow_as_fallback !== false ? "true" : "false");
    
    form.set("holder_name", String(payload.holder_name).slice(0, 120));
    form.set("card_number", cleanCardNumber);
    form.set("card_expiration", cardExpiration); // formato MM/YYYY
    form.set("card_cvv", String(payload.card_cvv).slice(0, 4));
    form.set("payment_method_code", paymentMethodCode);
    
    // Prioriza payment_company_code do payload (vindo da rota), senão usa brandCode detectado
    const finalPaymentCompanyCode = payload.payment_company_code || brandCode;
    if (paymentMethodCode === "credit_card" && finalPaymentCompanyCode) {
      form.set("payment_company_code", finalPaymentCompanyCode);
    }
    
    // Envia payment_company_id SOMENTE se for um número válido (não null, undefined, 0, "", etc)
    const paymentCompanyId = payload.payment_company_id;
    const hasValidPaymentCompanyId = paymentCompanyId != null && 
                                     paymentCompanyId !== "" && 
                                     !isNaN(Number(paymentCompanyId)) && 
                                     Number(paymentCompanyId) > 0;
    
    if (hasValidPaymentCompanyId) {
      form.set("payment_company_id", String(paymentCompanyId));
    }
    
    if (payload.document_number) {
      form.set("document_number", String(payload.document_number).replace(/\D+/g, "").slice(0, 18));
    }
    
    // Log do payload final que será enviado à Vindi (mascarado)
    const maskedCard = maskCardNumber(cleanCardNumber);
    // Mascara: primeiros 4 + últimos 4
    const cardMasked = cleanCardNumber.length >= 8
      ? `${cleanCardNumber.slice(0, 4)}${"*".repeat(Math.max(0, cleanCardNumber.length - 8))}${cleanCardNumber.slice(-4)}`
      : maskCardNumber(cleanCardNumber);
    
    // Log do payload final que será enviado à Vindi (mascarado)
    const logPayload = {
      user_id: payload.user_id || null,
      holder_name: payload.holder_name,
      card_masked: cardMasked,
      card_expiration: cardExpiration,
      payment_method_code: paymentMethodCode,
      allow_as_fallback: form.get("allow_as_fallback"),
      payment_company_code_received: payload.payment_company_code || null,
      payment_company_code_sent: finalPaymentCompanyCode || null,
      payment_company_code_source: payload.payment_company_code ? "frontend" : (brandCode ? "backend-detected" : "none"),
    };
    
    // Só inclui payment_company_id no log se for válido
    if (hasValidPaymentCompanyId) {
      logPayload.payment_company_id_sent = String(paymentCompanyId);
    } else {
      logPayload.payment_company_id_sent = null;
      logPayload.payment_company_id_received = payload.payment_company_id || null;
    }
    
    logPayload.has_cvv = !!payload.card_cvv;
    logPayload.has_document_number = !!payload.document_number;
    
    log("chamando Vindi Public API - request final", logPayload);
    log("endpoint Vindi Public API", { url: `${VINDI_PUBLIC_BASE}/public/payment_profiles` });

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
        const error = new Error("Vindi Public API timeout após 30s");
        error.status = 502;
        error.code = "VINDI_TIMEOUT";
        throw error;
      }
      
      // Se fetch lançar TypeError "Failed to parse URL", retornar erro padronizado
      if (e instanceof TypeError && e.message?.includes("Failed to parse URL")) {
        const error = new Error("Configuração Vindi Public inválida (VINDI_PUBLIC_BASE_URL).");
        error.status = 502;
        error.code = "VINDI_PUBLIC_CONFIG_ERROR";
        error.provider_status = null;
        err(`Vindi Public URL parse error`, {
          url,
          error_message: e.message,
        });
        throw error;
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
        endpoint: `${VINDI_PUBLIC_BASE}/public/payment_profiles`,
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
      
      // Log detalhado para 422 com payment_company_id
      const hasPaymentCompanyIdError = errorParameters.includes("payment_company_id");
      const errorLog = {
        status: response.status,
        error_count: json?.errors?.length || 0,
        error_messages: errorMessages,
        error_parameters: errorParameters,
      };
      
      if (hasPaymentCompanyIdError) {
        errorLog.payload_contains_payment_company_id = hasValidPaymentCompanyId;
        errorLog.payment_company_id_value = hasValidPaymentCompanyId ? String(paymentCompanyId) : null;
        errorLog.payment_company_code_sent = finalPaymentCompanyCode || null;
      }
      
      errorLog.endpoint = `${VINDI_PUBLIC_BASE}/public/payment_profiles`;
      err("Vindi Public API erro", errorLog);
    }

    if (!response.ok) {
      // Captura erros completos da Vindi (especialmente 422)
      let errorMsg = null;
      const errorsWithDetails = [];
      
      if (json?.errors && Array.isArray(json.errors) && json.errors.length > 0) {
        // Captura todos os erros com message e parameter
        json.errors.forEach(e => {
          const detail = {
            message: e.message || null,
            parameter: e.parameter || null,
          };
          // Formata mensagem para frontend: "campo: <parameter> - <message>"
          if (detail.parameter && detail.message) {
            detail.formatted = `campo: ${detail.parameter} - ${detail.message}`;
          }
          errorsWithDetails.push(detail);
        });
        
        // Busca a primeira mensagem disponível no array de erros
        const firstError = json.errors.find(e => e.message);
        if (firstError) {
          errorMsg = firstError.message;
        } else if (json.errors[0]) {
          errorMsg = String(json.errors[0]);
        }
      }
      
      // Fallback para outros formatos de erro
      if (!errorMsg) {
        errorMsg = json?.error || json?.message || `Vindi Public API falhou (${response.status})`;
      }

      const error = new Error(errorMsg);
      error.provider_status = response.status;
      error.response = {
        ...json,
        errors: errorsWithDetails.length > 0 ? errorsWithDetails : json?.errors || [],
      };
      
      // Se Vindi responder 401/403: NÃO propagar 401 para o client
      // Retornar 502 com code VINDI_AUTH_ERROR
      if (response.status === 401 || response.status === 403) {
        error.status = 502; // Não retornar 401 para o client
        error.code = "VINDI_AUTH_ERROR";
        error.message = "Falha de autenticação na Vindi (verifique VINDI_PUBLIC_KEY/BASE_URL).";
        throw error;
      }
      
      // Para 422, mantém status 422 mas adiciona code
      if (response.status === 422) {
        error.status = 422;
        error.code = "VINDI_VALIDATION_ERROR";
        if (errorsWithDetails.length > 0) {
          error.details = errorsWithDetails;
        }
        throw error;
      }
      
      // Para demais erros, retornar 502 com code VINDI_UPSTREAM_ERROR
      error.status = 502;
      error.code = "VINDI_UPSTREAM_ERROR";
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

