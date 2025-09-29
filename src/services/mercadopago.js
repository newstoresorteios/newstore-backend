// backend/src/services/mercadopago.js
// ESM

const MP_BASE = "https://api.mercadopago.com";
const ACCESS_TOKEN =
  process.env.MP_ACCESS_TOKEN ||
  process.env.MERCADOPAGO_ACCESS_TOKEN ||
  "";

// exige access token configurado
function ensureToken() {
  if (!ACCESS_TOKEN) {
    throw new Error("MP_ACCESS_TOKEN (ou MERCADOPAGO_ACCESS_TOKEN) não configurado no servidor.");
  }
}

async function mpFetch(method, path, body) {
  ensureToken();
  const r = await fetch(`${MP_BASE}${path}`, {
    method,
    headers: {
      "Authorization": `Bearer ${ACCESS_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok) {
    const msg = j?.message || j?.error || j?.cause?.[0]?.description || `${method} ${path} falhou`;
    const e = new Error(msg);
    e.response = j;
    e.status = r.status;
    throw e;
  }
  return j;
}

/**
 * Garante/retorna um customer no MP.
 * - Tenta achar por e-mail; se não achar, cria.
 * Retorna: { customerId }
 */
export async function mpEnsureCustomer({ user, doc_number, name }) {
  ensureToken();
  const email = user?.email || undefined;

  if (email) {
    const found = await mpFetch("GET", `/v1/customers/search?email=${encodeURIComponent(email)}`);
    const hit = found?.results?.[0];
    if (hit?.id) return { customerId: hit.id };
  }

  const created = await mpFetch("POST", "/v1/customers", {
    email,
    first_name: name || user?.name || "Cliente",
    description: user?.id ? `user:${user.id}` : undefined,
    identification: doc_number
      ? { type: String(doc_number).length > 11 ? "CNPJ" : "CPF", number: String(doc_number) }
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
  return {
    cardId: card.id,
    brand: card.payment_method?.id || card.issuer?.name || null,
    last4: card.last_four_digits || null,
  };
}

/**
 * Cobra usando cartão salvo:
 * 1) Cria um card_token a partir de (customer_id, card_id)
 * 2) Cria o payment com esse token
 * Retorna: { status, paymentId }
 */
export async function mpChargeCard({
  customerId,
  cardId,
  amount_cents,
  description,
  metadata,
}) {
  // 1) token a partir do cartão salvo
  // Obs.: Algumas contas podem exigir security_code. Se seu contrato exigir,
  // adicione { security_code: "123" } no payload abaixo.
  const cardTok = await mpFetch("POST", "/v1/card_tokens", {
    customer_id: customerId,
    card_id: cardId,
  });

  // 2) pagamento
  const amount = Math.round(Number(amount_cents || 0)) / 100;
  const pay = await mpFetch("POST", "/v1/payments", {
    transaction_amount: amount,
    description: description || "AutoPay",
    token: cardTok.id,
    installments: 1,
    payer: { type: "customer", id: customerId },
    metadata: metadata || {},
  });

  return { status: pay.status, paymentId: pay.id };
}

export default { mpEnsureCustomer, mpSaveCard, mpChargeCard };
