// src/services/notifications/brevoWhatsApp.js
// ESM — envio WhatsApp via Brevo (sem expor BREVO_API_KEY)

export function normalizePhoneBR(phone) {
  if (phone == null) return null;
  const digits = String(phone).replace(/\D/g, "");
  if (!digits) return null;

  if (digits.startsWith("55") && (digits.length === 12 || digits.length === 13)) {
    return digits;
  }
  if (digits.length === 10 || digits.length === 11) {
    return `55${digits}`;
  }
  return null;
}

export function isTruthy(value) {
  if (value === true) return true;
  const s = String(value ?? "").trim().toLowerCase();
  return s === "true" || s === "1" || s === "yes" || s === "on";
}

function isExplicitlyFalse(value) {
  if (value === false) return true;
  const s = String(value ?? "").trim().toLowerCase();
  return s === "false" || s === "0" || s === "no" || s === "off";
}

export function isTestModeActive() {
  const v = process.env.NOTIFICATION_TEST_MODE;
  if (v === undefined || v === null || String(v).trim() === "") return true;
  return !isExplicitlyFalse(v);
}

export function isAllowRealRecipients() {
  return isTruthy(process.env.NOTIFICATION_ALLOW_REAL_RECIPIENTS);
}

export function shouldForceTestRecipient() {
  return isTestModeActive() || !isAllowRealRecipients();
}

export function isWhatsAppEnabled() {
  return (
    isTruthy(process.env.BREVO_WHATSAPP_ENABLED) &&
    !!String(process.env.BREVO_API_KEY || "").trim() &&
    !!String(process.env.BREVO_WHATSAPP_SENDER_NUMBER || "").trim()
  );
}

export function getTestRecipient() {
  return normalizePhoneBR(process.env.NOTIFICATION_TEST_WHATSAPP_TO);
}

export function resolveRecipientForCurrentMode(originalRecipient) {
  const normalizedOriginal = normalizePhoneBR(originalRecipient);
  const originalRaw =
    originalRecipient != null ? String(originalRecipient).trim() : null;

  if (!shouldForceTestRecipient()) {
    if (!normalizedOriginal) {
      return {
        ok: false,
        reason: "invalid_recipient",
        recipient: null,
        recipient_original: originalRaw,
        recipient_forced: false,
      };
    }
    return {
      ok: true,
      recipient: normalizedOriginal,
      recipient_original: normalizedOriginal,
      recipient_forced: false,
    };
  }

  const testRecipient = getTestRecipient();
  if (!testRecipient) {
    return {
      ok: false,
      reason: "missing_test_recipient",
      recipient: null,
      recipient_original: normalizedOriginal || originalRaw,
      recipient_forced: true,
    };
  }

  return {
    ok: true,
    recipient: testRecipient,
    recipient_original: normalizedOriginal || originalRaw,
    recipient_forced: true,
  };
}

function isNumericTemplateId(templateId) {
  if (templateId == null || templateId === "") return false;
  return /^\d+$/.test(String(templateId).trim());
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

export async function sendBrevoWhatsAppTemplate({
  to,
  templateId,
  params,
  templateKey,
  correlationId,
}) {
  void correlationId;

  let recipient = null;
  let recipient_original = null;
  let recipient_forced = false;

  if (!isWhatsAppEnabled()) {
    return {
      ok: false,
      skipped: true,
      reason: "whatsapp_disabled",
      provider: "brevo",
      channel: "whatsapp",
      recipient,
      recipient_original,
      recipient_forced,
    };
  }

  const resolved = resolveRecipientForCurrentMode(to);
  recipient = resolved.recipient;
  recipient_original = resolved.recipient_original;
  recipient_forced = resolved.recipient_forced;

  if (!resolved.ok) {
    return {
      ok: false,
      skipped: true,
      reason: resolved.reason || "invalid_recipient",
      provider: "brevo",
      channel: "whatsapp",
      recipient,
      recipient_original,
      recipient_forced,
    };
  }

  if (!isNumericTemplateId(templateId)) {
    return {
      ok: false,
      skipped: true,
      reason: "missing_template_id",
      provider: "brevo",
      channel: "whatsapp",
      recipient,
      recipient_original,
      recipient_forced,
    };
  }

  const baseUrl = (
    process.env.BREVO_WHATSAPP_BASE_URL || "https://api.brevo.com/v3"
  ).replace(/\/+$/, "");
  const timeoutMs = Number(process.env.BREVO_WHATSAPP_TIMEOUT_MS) || 15000;
  const senderNumber = normalizePhoneBR(process.env.BREVO_WHATSAPP_SENDER_NUMBER);

  const payload = {
    senderNumber,
    contactNumbers: [recipient],
    templateId: Number(templateId),
    params: params || {},
  };

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  console.log("[brevo.whatsapp] send:start", {
    templateKey: templateKey || null,
    templateId: Number(templateId),
    recipient_forced: resolved.recipient_forced,
    hasParams: Boolean(params && Object.keys(params).length),
    senderConfigured: Boolean(process.env.BREVO_WHATSAPP_SENDER_NUMBER),
  });

  try {
    const res = await fetch(`${baseUrl}/whatsapp/sendMessage`, {
      method: "POST",
      headers: {
        accept: "application/json",
        "content-type": "application/json",
        "api-key": process.env.BREVO_API_KEY,
      },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });

    clearTimeout(timer);
    const body = await parseResponseBody(res);
    const statusCode = res.status;

    if (statusCode === 200 || statusCode === 201 || statusCode === 202) {
      console.log("[brevo.whatsapp] send:accepted", {
        statusCode,
        messageId: body?.messageId || body?.id || null,
        recipient_forced: resolved.recipient_forced,
        templateId: Number(templateId),
        templateKey: templateKey || null,
      });
      return {
        ok: true,
        skipped: false,
        provider: "brevo",
        channel: "whatsapp",
        statusCode,
        messageId: body.messageId || body.id || null,
        provider_status: "accepted",
        delivery_confirmed: false,
        response: body,
        recipient,
        recipient_original,
        recipient_forced,
      };
    }

    console.warn("[brevo.whatsapp] send:failed", {
      statusCode,
      templateId: Number(templateId),
      templateKey: templateKey || null,
      recipient_forced: resolved.recipient_forced,
      error: body?.message || body?.code || "brevo_request_failed",
    });

    return {
      ok: false,
      skipped: false,
      provider: "brevo",
      channel: "whatsapp",
      statusCode,
      error: body.message || body.code || "brevo_request_failed",
      response: body,
      recipient,
      recipient_original,
      recipient_forced,
    };
  } catch (error) {
    clearTimeout(timer);
    console.error("[brevo.whatsapp] send:error", {
      templateId: Number(templateId),
      templateKey: templateKey || null,
      recipient_forced: resolved.recipient_forced,
      error: error?.message,
    });
    return {
      ok: false,
      skipped: false,
      provider: "brevo",
      channel: "whatsapp",
      error: error?.message || "brevo_request_failed",
      response: null,
      recipient,
      recipient_original,
      recipient_forced,
    };
  }
}
