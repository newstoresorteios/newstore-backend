export function coded(code) {
  const err = new Error(code);
  err.code = code;
  return err;
}

export function assertPushSingleDeviceMode({
  source = "manual_test",
  isAudience = false,
  isEngine = false,
  isMassSend = false,
  isCampaign = false,
} = {}) {
  if (process.env.PUSH_ENABLED !== "true") throw coded("push_disabled");
  if (process.env.PUSH_MODE !== "single_device_test") {
    throw coded("push_mode_not_single_device_test");
  }
  if (process.env.PUSH_TEST_ONLY !== "true") throw coded("push_test_only_required");
  if (
    process.env.NODE_ENV === "production" &&
    process.env.PUSH_ALLOW_PRODUCTION_SEND !== "true"
  ) {
    throw coded("push_production_send_blocked");
  }

  const normalizedSource = String(source || "").trim().toLowerCase();
  if (
    process.env.PUSH_ALLOW_DB_RECIPIENT_LOOKUP !== "true" &&
    ["db_lookup", "audience", "campaign", "engine"].includes(normalizedSource)
  ) {
    throw coded("push_db_recipient_lookup_blocked");
  }
  if (isAudience && process.env.PUSH_ALLOW_AUDIENCE !== "true") {
    throw coded("push_audience_blocked");
  }
  if (isEngine && process.env.PUSH_ALLOW_ENGINE_EVENTS !== "true") {
    throw coded("push_engine_blocked");
  }
  if (isMassSend && process.env.PUSH_ALLOW_ADMIN_MASS_SEND !== "true") {
    throw coded("push_mass_send_blocked");
  }
  if (isCampaign && process.env.PUSH_ALLOW_CAMPAIGNS !== "true") {
    throw coded("push_campaign_blocked");
  }
  if (!String(process.env.PUSH_TEST_SUBSCRIPTION_ID || "").trim()) {
    throw coded("push_test_subscription_id_missing");
  }
  return true;
}

export function assertAllowedTestSubscription({ subscriptionId } = {}) {
  const allowedId = String(process.env.PUSH_TEST_SUBSCRIPTION_ID || "").trim();
  if (!allowedId) throw coded("push_test_subscription_id_missing");
  if (String(subscriptionId || "").trim() !== allowedId) {
    throw coded("push_subscription_not_allowed_in_test_mode");
  }
  return true;
}
