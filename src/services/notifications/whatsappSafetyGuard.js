function coded(code) {
  const err = new Error(code);
  err.code = code;
  return err;
}

export function assertWhatsAppAllowed({
  source = "manual",
  isAutomation = false,
  isCampaign = false,
} = {}) {
  if (process.env.BREVO_WHATSAPP_ENABLED !== "true") {
    throw coded("whatsapp_disabled");
  }
  if (isAutomation && process.env.NOTIFICATION_WHATSAPP_AUTOMATION_ENABLED !== "true") {
    throw coded("whatsapp_automation_blocked");
  }
  if (isCampaign && process.env.NOTIFICATION_WHATSAPP_CAMPAIGN_ENABLED !== "true") {
    throw coded("whatsapp_campaign_blocked");
  }
  if (
    source === "push_fallback" &&
    process.env.NOTIFICATION_WHATSAPP_FALLBACK_ENABLED !== "true"
  ) {
    throw coded("whatsapp_push_fallback_blocked");
  }
  return true;
}
