function coded(code) {
  const err = new Error(code);
  err.code = code;
  return err;
}

function isTruthy(value) {
  if (value === true) return true;
  const s = String(value ?? "").trim().toLowerCase();
  return s === "true" || s === "1" || s === "yes" || s === "on";
}

export function assertWhatsAppAllowed({
  source = "manual",
  isAutomation = false,
  isCampaign = false,
} = {}) {
  if (!isTruthy(process.env.BREVO_WHATSAPP_ENABLED)) {
    throw coded("whatsapp_provider_disabled");
  }
  if (isAutomation && !isTruthy(process.env.NOTIFICATION_WHATSAPP_AUTOMATION_ENABLED)) {
    throw coded("whatsapp_automation_blocked");
  }
  if (isCampaign && !isTruthy(process.env.NOTIFICATION_WHATSAPP_CAMPAIGN_ENABLED)) {
    throw coded("whatsapp_campaign_disabled");
  }
  if (
    source === "push_fallback" &&
    !isTruthy(process.env.NOTIFICATION_WHATSAPP_FALLBACK_ENABLED)
  ) {
    throw coded("whatsapp_push_fallback_blocked");
  }
  return true;
}
