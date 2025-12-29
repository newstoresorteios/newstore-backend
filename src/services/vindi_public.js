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
 * @param {object} params - { holderName, cardNumber, cardExpirationMonth, cardExpirationYear, cardCvv, documentNumber? }
 * @returns {Promise<{gatewayToken: string}>}
 */
export async function tokenizeCard({
  holderName,
  cardNumber,
  cardExpirationMonth,
  cardExpirationYear,
  cardCvv,
  documentNumber,
}) {
  if (!VINDI_PUBLIC_KEY) {
    throw new Error("VINDI_PUBLIC_KEY não configurado no servidor.");
  }

  // Validações
  if (!holderName || !cardNumber || !cardExpirationMonth || !cardExpirationYear || !cardCvv) {
    throw new Error("Campos obrigatórios: holderName, cardNumber, cardExpirationMonth, cardExpirationYear, cardCvv");
  }

  // Limpa número do cartão (remove espaços/hífens)
  const cleanCardNumber = String(cardNumber).replace(/\D+/g, "");

  try {
    const body = {
      holder_name: String(holderName).slice(0, 120),
      card_number: cleanCardNumber,
      card_expiration_month: String(cardExpirationMonth).padStart(2, "0").slice(0, 2),
      card_expiration_year: String(cardExpirationYear).slice(-2), // últimos 2 dígitos do ano
      card_cvv: String(cardCvv).slice(0, 4),
    };

    if (documentNumber) {
      body.document_number = String(documentNumber).replace(/\D+/g, "").slice(0, 18);
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
      const errorMsg =
        json?.errors?.[0]?.message ||
        json?.error ||
        json?.message ||
        `Vindi Public API falhou (${response.status})`;

      const error = new Error(errorMsg);
      error.status = response.status;
      error.response = json;
      throw error;
    }

    const gatewayToken = json?.payment_profile?.gateway_token || json?.gateway_token;

    if (!gatewayToken) {
      throw new Error("Vindi não retornou gateway_token");
    }

    log("tokenização bem-sucedida", {
      hasToken: !!gatewayToken,
      // NÃO logar dados sensíveis
    });

    return { gatewayToken };
  } catch (e) {
    err("tokenizeCard falhou", {
      status: e?.status,
      msg: e?.message,
      // NÃO logar dados do cartão
    });
    throw e;
  }
}

export default {
  tokenizeCard,
};

