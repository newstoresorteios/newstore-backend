// backend/src/services/mercadopago.js
// ESM
import crypto from "node:crypto";

const MP_BASE =
  (process.env.MP_BASE_URL && process.env.MP_BASE_URL.replace(/\/+$/, "")) ||
  "https://api.mercadopago.com";

// Busca o token sempre que precisar (permite trocar env e redeploy sem cache em const)
function getAccessToken() {
  return (
    process.env.MERCADOPAGO_ACCESS_TOKEN ||
    process.env.MP_ACCESS_TOKEN ||
    process.env.REACT_APP_MP_ACCESS_TOKEN || // fallback (não recomendado, mas ajuda se ficou errado)
    ""
  );
}

function ensureToken() {
  if (!getAccessToken()) {
    throw new Error(
      "MP_ACCESS_TOKEN/MERCADOPAGO_ACCESS_TOKEN não configurado no servidor."
    );
  }
}

async function mpFetch(
  method,
  path,
  body,
  extraHeaders = {},
  { timeoutMs = 15000 } = {}
) {
  ensureToken();

  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);

  let res;
  try {
    res = await fetch(`${MP_BASE}${path}`, {
      method,
      headers: {
        Authorization: `Bearer ${getAccessToken()}`,
        "Content-Type": "application/json",
        Accept: "application/json",
        "User-Agent": "newstore-autopay/1.0",
        ...extraHeaders,
      },
      body: body != null ? JSON.stringify(body) : undefined,
      signal: controller.signal,
    });
  } catch (e) {
    clearTimeout(t);
    if (e?.name === "AbortError") {
      throw new Error(`MercadoPago ${method} ${path} timeout`);
    }
    throw e;
  }
  clearTimeout(t);

  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }

  if (!res.ok) {
    const cause =
      Array.isArray(json?.cause) && json.cause.length
        ? `: ${json.cause.map(c => c?.description || c?.message).filter(Boolean).join(" | ")}`
        : "";
    const msg =
      json?.message ||
      json?.error?.message ||
      json?.error ||
      `MercadoPago ${method} ${path} falhou (${res.status})${cause}`;
    const err = new Error(msg);
    err.status = res.status;
    err.response = json;
    throw err;
  }

  return json;
}

/**
 * Garante/retorna um customer no MP (procura por email; senão cria).
 * Retorna: { customerId }
 */
export async function mpEnsureCustomer({ user, doc_number, name }) {
  ensureToken();
  const email = user?.email || undefined;

  if (email) {
    const found = await mpFetch(
      "GET",
      `/v1/customers/search?email=${encodeURIComponent(email)}`
    );
    const hit = found?.results?.[0];
    if (hit?.id) return { customerId: hit.id };
  }

  const created = await mpFetch("POST", "/v1/customers", {
    email,
    first_name: name || user?.name || "Cliente",
    description: user?.id ? `user:${user.id}` : undefined,
    identification: doc_number
      ? {
          type: String(doc_number).length > 11 ? "CNPJ" : "CPF",
          number: String(doc_number),
        }
      : undefined,
  });

  return { customerId: created.id };
}

/**
 * Salva um cartão no customer a partir de um card_token (gerado no front).
 * Retorna: { cardId, brand, last4 }
 */
export async function mpSaveCard({ customerId, card_token }) {
  const card = await mpFetch("POST", `/v1/customers/${customerId}/cards`, {
    token: card_token,
  });

  const brand =
    card?.payment_method?.id ||
    card?.payment_method?.name ||
    card?.issuer?.name ||
    null;

  const last4 = card?.last_four_digits || null;

  return { cardId: card.id, brand, last4 };
}

/**
 * Cobra usando cartão salvo:
 * 1) Cria um card_token a partir de (customer_id, card_id)
 * 2) Cria o payment com esse token
 * Retorna: { status, paymentId }
 *
 * Opcional: passe { security_code } se sua conta exigir CVV na tokenização do cartão salvo.
 */
export async function mpChargeCard({
  customerId,
  cardId,
  amount_cents,
  description,
  metadata,
  security_code, // opcional
}) {
  // 1) token a partir do cartão salvo
  const cardTok = await mpFetch("POST", "/v1/card_tokens", {
    customer_id: customerId,
    card_id: cardId,
    security_code: security_code || undefined,
  });

  // 2) pagamento
  const amount = Number(
    (Math.round(Number(amount_cents || 0)) / 100).toFixed(2)
  );

  const idempotencyKey = crypto.randomUUID();

  // IMPORTANTE: NÃO enviar "currency_id" aqui; com token de cartão o MP rejeita esse campo.
  const pay = await mpFetch(
    "POST",
    "/v1/payments",
    {
      transaction_amount: amount,
      description: description || "AutoPay",
      token: cardTok.id,
      installments: 1,
      payer: { type: "customer", id: customerId },
      metadata: metadata || {},
      statement_descriptor: process.env.MP_STATEMENT || undefined,
      binary_mode: true, // evita pagamentos "pendentes" quando possível
    },
    { "X-Idempotency-Key": idempotencyKey }
  );

  return { status: pay.status, paymentId: pay.id };
}

export default { mpEnsureCustomer, mpSaveCard, mpChargeCard };
