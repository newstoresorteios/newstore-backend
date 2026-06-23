export function assertWhatsAppAllowed({ source, isAutomation, isCampaign }) {
  if (process.env.BREVO_WHATSAPP_ENABLED !== "true") {
    const err = new Error("whatsapp_disabled");
    err.code = "whatsapp_disabled";
    throw err;
  }

  if (isAutomation && process.env.NOTIFICATION_WHATSAPP_AUTOMATION_ENABLED !== "true") {
    const err = new Error("whatsapp_automation_disabled");
    err.code = "whatsapp_automation_disabled";
    throw err;
  }

  if (isCampaign && process.env.NOTIFICATION_WHATSAPP_CAMPAIGN_ENABLED !== "true") {
    const err = new Error("whatsapp_campaign_disabled");
    err.code = "whatsapp_campaign_disabled";
    throw err;
  }

  if (source === "push_fallback" && process.env.NOTIFICATION_WHATSAPP_FALLBACK_ENABLED !== "true") {
    const err = new Error("whatsapp_fallback_disabled");
    err.code = "whatsapp_fallback_disabled";
    throw err;
  }

  return true;
}
