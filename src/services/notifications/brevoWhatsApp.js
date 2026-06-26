// src/services/notifications/brevoWhatsApp.js
// ESM — envio WhatsApp via Brevo (sem expor BREVO_API_KEY)

import { assertWhatsAppAllowed } from "./whatsappSafetyGuard.js";
import { isWhatsAppConsentRequired } from "./communicationConsent.js";

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

export function isAdminTestCustomRecipientsEnabled() {
  return isTruthy(process.env.NOTIFICATION_ADMIN_TEST_CUSTOM_RECIPIENTS_ENABLED);
}

export function getAllowedAdminTestRecipients() {
  const raw = String(process.env.NOTIFICATION_ADMIN_TEST_ALLOWED_RECIPIENTS || "");
  return raw
    .split(",")
    .map((s) => normalizePhoneBR(s.trim()))
    .filter(Boolean);
}

export function isRecipientAllowedForAdminTest(phone) {
  const normalized = normalizePhoneBR(phone);
  if (!normalized) return false;
  const allowed = getAllowedAdminTestRecipients();
  if (!allowed.length) return true;
  return allowed.includes(normalized);
}

function canUseAdminTestCustomRecipient(originalRecipient, options = {}) {
  const normalized = normalizePhoneBR(originalRecipient);
  return (
    options.context === "admin_test" &&
    options.allowAdminTestCustomRecipient === true &&
    isAdminTestCustomRecipientsEnabled() &&
    !!normalized &&
    isRecipientAllowedForAdminTest(originalRecipient)
  );
}

export function resolveRecipientForCurrentMode(originalRecipient, options = {}) {
  const normalizedOriginal = normalizePhoneBR(originalRecipient);
  const originalRaw =
    originalRecipient != null ? String(originalRecipient).trim() : null;

  if (canUseAdminTestCustomRecipient(originalRecipient, options)) {
    return {
      ok: true,
      recipient: normalizedOriginal,
      recipient_original: normalizedOriginal,
      recipient_forced: false,
      recipient_mode: "admin_test_custom",
    };
  }

  if (!shouldForceTestRecipient()) {
    if (!normalizedOriginal) {
      return {
        ok: false,
        reason: "invalid_recipient",
        recipient: null,
        recipient_original: originalRaw,
        recipient_forced: false,
        recipient_mode: null,
      };
    }
    return {
      ok: true,
      recipient: normalizedOriginal,
      recipient_original: normalizedOriginal,
      recipient_forced: false,
      recipient_mode: "real",
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
      recipient_mode: "forced_test_recipient",
    };
  }

  return {
    ok: true,
    recipient: testRecipient,
    recipient_original: normalizedOriginal || originalRaw,
    recipient_forced: true,
    recipient_mode: "forced_test_recipient",
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
  allowAdminTestCustomRecipient = false,
  context = "system",
  source,
  isAutomation = false,
  isCampaign = false,
  consentChecked = false,
}) {
  void correlationId;

  const resolvedSource = source || context || "system";
  const resolvedIsCampaign =
    isCampaign ||
    context === "manual_send" ||
    context === "manual_send_selected";

  try {
    assertWhatsAppAllowed({
      source: resolvedSource,
      isAutomation,
      isCampaign: resolvedIsCampaign,
    });
  } catch (err) {
    return {
      ok: false,
      skipped: true,
      reason: err.code || err.message,
      provider: "brevo",
      channel: "whatsapp",
      recipient: null,
      recipient_original: null,
      recipient_forced: false,
      recipient_mode: null,
    };
  }

  let recipient = null;
  let recipient_original = null;
  let recipient_forced = false;
  let recipient_mode = null;

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
      recipient_mode,
    };
  }

  const resolved = resolveRecipientForCurrentMode(to, {
    allowAdminTestCustomRecipient,
    context,
  });
  recipient = resolved.recipient;
  recipient_original = resolved.recipient_original;
  recipient_forced = resolved.recipient_forced;
  recipient_mode = resolved.recipient_mode ?? null;

  console.log("[brevo.whatsapp] recipient resolved", {
    context,
    recipient_forced: resolved.recipient_forced,
    recipient_mode: resolved.recipient_mode || null,
    has_original_recipient: Boolean(to),
    custom_admin_test_enabled: isAdminTestCustomRecipientsEnabled(),
  });

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
      recipient_mode,
    };
  }

  if (
    isWhatsAppConsentRequired() &&
    resolved.recipient_forced !== true &&
    consentChecked !== true
  ) {
    return {
      ok: false,
      skipped: true,
      reason: "whatsapp_consent_unknown",
      provider: "brevo",
      channel: "whatsapp",
      recipient,
      recipient_original,
      recipient_forced,
      recipient_mode,
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
      recipient_mode,
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
      const messageId = body?.messageId || body?.id || null;
      console.log("[brevo.whatsapp] send:accepted", {
        statusCode,
        messageId,
        templateId: Number(templateId),
        templateKey: templateKey || null,
        recipient_forced: resolved.recipient_forced,
        provider_status: "accepted",
        delivery_status: "unknown",
        error: null,
        reason: null,
        response_keys:
          body && typeof body === "object" ? Object.keys(body) : [],
      });
      return {
        ok: true,
        skipped: false,
        provider: "brevo",
        channel: "whatsapp",
        statusCode,
        messageId,
        provider_status: "accepted",
        delivery_status: "unknown",
        delivery_confirmed: false,
        response: body,
        recipient,
        recipient_original,
        recipient_forced,
        recipient_mode,
        error: null,
        reason: null,
      };
    }

    const httpError = body?.message || body?.code || "brevo_request_failed";
    console.warn("[brevo.whatsapp] send:failed", {
      statusCode,
      templateId: Number(templateId),
      templateKey: templateKey || null,
      recipient_forced: resolved.recipient_forced,
      error: httpError,
      reason: body?.reason || null,
      response: body,
    });

    return {
      ok: false,
      skipped: false,
      provider: "brevo",
      channel: "whatsapp",
      statusCode,
      error: httpError,
      reason: body?.reason || null,
      response: body,
      recipient,
      recipient_original,
      recipient_forced,
      recipient_mode,
    };
  } catch (error) {
    clearTimeout(timer);
    console.error("[brevo.whatsapp] send:error", {
      templateId: Number(templateId),
      templateKey: templateKey || null,
      recipient_forced: resolved.recipient_forced,
      error: error?.message || null,
      reason: null,
    });
    return {
      ok: false,
      skipped: false,
      provider: "brevo",
      channel: "whatsapp",
      error: error?.message || "brevo_request_failed",
      reason: null,
      response: null,
      recipient,
      recipient_original,
      recipient_forced,
      recipient_mode,
    };
  }
}
