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

function last4(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return digits ? digits.slice(-4) : null;
}

function shortProviderError(value) {
  const text = String(value || "brevo_request_failed").trim();
  return text.replace(/[\r\n\t]+/g, " ").slice(0, 180);
}

function safeUrlHost(value) {
  try {
    return new URL(String(value || "")).host || null;
  } catch {
    return null;
  }
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

export function getWhatsAppProviderReadiness() {
  const brevoWhatsappEnabled = isTruthy(process.env.BREVO_WHATSAPP_ENABLED);
  const hasBrevoApiKey = !!String(process.env.BREVO_API_KEY || "").trim();
  const hasSenderNumber = !!normalizePhoneBR(process.env.BREVO_WHATSAPP_SENDER_NUMBER);
  const notificationWhatsappCampaignEnabled = isTruthy(process.env.NOTIFICATION_WHATSAPP_CAMPAIGN_ENABLED);
  const testMode = isTestModeActive();
  const hasTestTo = !!getTestRecipient();

  let reason = null;
  if (!brevoWhatsappEnabled) reason = "whatsapp_provider_disabled";
  else if (!hasBrevoApiKey) reason = "brevo_api_key_missing";
  else if (!hasSenderNumber) reason = "brevo_sender_number_missing";

  return {
    ok: !reason,
    reason,
    brevo_whatsapp_enabled: brevoWhatsappEnabled,
    notification_whatsapp_campaign_enabled: notificationWhatsappCampaignEnabled,
    test_mode: testMode,
    has_test_to: hasTestTo,
    has_brevo_api_key: hasBrevoApiKey,
    has_sender_number: hasSenderNumber,
  };
}

export function isWhatsAppEnabled() {
  return getWhatsAppProviderReadiness().ok;
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
      reason: "whatsapp_test_number_missing",
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

  const readiness = getWhatsAppProviderReadiness();
  if (!readiness.ok) {
    return {
      ok: false,
      skipped: true,
      reason: readiness.reason,
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
      reason: "brevo_template_id_missing",
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
  };
  if (params && typeof params === "object" && Object.keys(params).length) {
    payload.params = params;
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  console.log("[brevo.whatsapp] send:start", {
    templateKey: templateKey || null,
    templateId: Number(templateId),
    recipient_forced: resolved.recipient_forced,
    hasParams: Boolean(params && Object.keys(params).length),
    senderConfigured: Boolean(process.env.BREVO_WHATSAPP_SENDER_NUMBER),
  });

  console.log("[brevo-whatsapp] prepared_request", {
    template_key: templateKey || null,
    provider_template_id: Number(templateId),
    has_api_key: Boolean(process.env.BREVO_API_KEY),
    has_sender_number: Boolean(senderNumber),
    sender_last4: last4(senderNumber),
    recipient_forced: resolved.recipient_forced,
    recipient_last4: last4(recipient),
    test_mode: isTestModeActive(),
    params_keys: params && typeof params === "object" ? Object.keys(params) : [],
    has_authorize_url: Boolean(params?.authorize_url),
    has_decline_url: Boolean(params?.decline_url),
    authorize_host: safeUrlHost(params?.authorize_url),
    decline_host: safeUrlHost(params?.decline_url),
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
      console.log("[brevo-whatsapp] accepted", {
        provider_message_id_present: Boolean(messageId),
        provider_template_id: Number(templateId),
        recipient_forced: resolved.recipient_forced,
        recipient_last4: last4(recipient),
        sent_at_present: true,
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

    const httpError = shortProviderError(body?.message || body?.code || "brevo_request_failed");
    const providerReason = shortProviderError(body?.reason || httpError);
    console.warn("[brevo-whatsapp] provider_rejected", {
      http_status: statusCode,
      reason: httpError,
      provider_template_id: Number(templateId),
      sender_last4: last4(senderNumber),
      recipient_forced: resolved.recipient_forced,
      recipient_last4: last4(recipient),
      test_mode: isTestModeActive(),
      has_api_key: Boolean(process.env.BREVO_API_KEY),
      has_sender_number: Boolean(senderNumber),
    });
    console.warn("[brevo.whatsapp] send:failed", {
      statusCode,
      templateId: Number(templateId),
      templateKey: templateKey || null,
      recipient_forced: resolved.recipient_forced,
      error: httpError,
      reason: providerReason,
      response_keys: body && typeof body === "object" ? Object.keys(body) : [],
    });

    return {
      ok: false,
      skipped: false,
      provider: "brevo",
      channel: "whatsapp",
      statusCode,
      error: httpError,
      reason: body?.reason ? providerReason : null,
      response: { statusCode, body },
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
