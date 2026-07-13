// src/services/notifications/brevoContacts.js
// Upsert de contatos Brevo para templates WhatsApp com atributos {{FIRSTNAME}}, {{WHATSAPP}}, etc.

import { normalizePhoneBR } from "./brevoWhatsApp.js";

export { normalizePhoneBR };

const BREVO_CONTACT_ATTRS = new Set([
  "FIRSTNAME",
  "LASTNAME",
  "WHATSAPP",
  "SMS",
  "EMAIL",
]);

export function normalizeEmail(email) {
  if (email == null) return null;
  const s = String(email).trim().toLowerCase();
  if (!s || !s.includes("@")) return null;
  return s;
}

function getBaseUrl() {
  return (process.env.BREVO_WHATSAPP_BASE_URL || "https://api.brevo.com/v3").replace(
    /\/+$/,
    ""
  );
}

function resolveContactEmail({ email, phone, extId }) {
  const normalizedEmail = normalizeEmail(email);
  if (normalizedEmail) return normalizedEmail;

  const normalizedPhone = normalizePhoneBR(phone);
  if (normalizedPhone) {
    return `whatsapp+${normalizedPhone}@contact.newstore.local`;
  }

  if (extId != null && String(extId).trim() !== "") {
    const safe = String(extId).trim().replace(/[^a-zA-Z0-9._-]/g, "_");
    return `ext+${safe}@contact.newstore.local`;
  }

  return null;
}

async function parseResponseBody(res) {
  const text = await res.text();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch {
    return { raw: text };
  }
}

export async function upsertBrevoContactForWhatsApp({
  email = null,
  phone = null,
  firstName = "",
  messageText = "",
  extraAttributes = {},
  extId = null,
} = {}) {
  const apiKey = String(process.env.BREVO_API_KEY || "").trim();
  if (!apiKey) {
    return {
      ok: false,
      error: "missing_brevo_api_key",
      statusCode: null,
      response: null,
      attributes: null,
    };
  }

  const normalizedPhone = normalizePhoneBR(phone);
  const resolvedEmail = resolveContactEmail({ email, phone, extId });
  if (!resolvedEmail) {
    return {
      ok: false,
      error: "missing_contact_identifier",
      statusCode: null,
      response: null,
      attributes: null,
    };
  }

  const attributes = {
    FIRSTNAME: firstName != null ? String(firstName) : "",
    WHATSAPP: messageText != null ? String(messageText) : "",
    ...(extraAttributes && typeof extraAttributes === "object"
      ? extraAttributes
      : {}),
  };

  if (normalizedPhone && !attributes.SMS) {
    attributes.SMS = normalizedPhone;
  }

  console.log("[brevo.contacts] upsert:start", {
    has_email: Boolean(normalizeEmail(email)),
    has_phone: Boolean(normalizedPhone),
    has_firstName: Boolean(firstName && String(firstName).trim()),
    message_length: messageText ? String(messageText).length : 0,
    attribute_keys: Object.keys(attributes),
  });

  const payload = {
    email: resolvedEmail,
    updateEnabled: true,
    attributes,
  };

  if (extId != null && String(extId).trim() !== "") {
    payload.ext_id = String(extId).trim();
  }

  const baseUrl = getBaseUrl();
  const timeoutMs = Number(process.env.BREVO_WHATSAPP_TIMEOUT_MS) || 15000;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(`${baseUrl}/contacts`, {
      method: "POST",
      headers: {
        accept: "application/json",
        "content-type": "application/json",
        "api-key": apiKey,
      },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });

    clearTimeout(timer);
    const body = await parseResponseBody(res);
    const statusCode = res.status;
    const ok = statusCode === 200 || statusCode === 201 || statusCode === 204;

    if (ok) {
      console.log("[brevo.contacts] upsert:done", {
        statusCode,
        ok: true,
        response_keys: body && typeof body === "object" ? Object.keys(body) : [],
      });
      return {
        ok: true,
        statusCode,
        response: body,
        attributes,
        contact_email: resolvedEmail,
      };
    }

    console.warn("[brevo.contacts] upsert:failed", {
      statusCode,
      error: body?.message || body?.code || "brevo_contact_upsert_failed",
      reason: body?.reason || null,
      response: body,
    });

    return {
      ok: false,
      statusCode,
      error: body?.message || body?.code || "brevo_contact_upsert_failed",
      reason: body?.reason || null,
      response: body,
      attributes,
      contact_email: resolvedEmail,
    };
  } catch (error) {
    clearTimeout(timer);
    console.warn("[brevo.contacts] upsert:failed", {
      statusCode: null,
      error: error?.message || "brevo_contact_upsert_failed",
      reason: null,
      response: null,
    });
    return {
      ok: false,
      statusCode: null,
      error: error?.message || "brevo_contact_upsert_failed",
      reason: null,
      response: null,
      attributes,
      contact_email: resolvedEmail,
    };
  }
}

export function isBrevoContactAttributeName(name) {
  return BREVO_CONTACT_ATTRS.has(String(name || "").toUpperCase());
}
