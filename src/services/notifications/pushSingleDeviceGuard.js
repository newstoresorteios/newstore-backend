export function coded(code) {
  const err = new Error(code);
  err.code = code;
  return err;
}

export function getAllowedTestSubscriptionIds() {
  const ids = [
    process.env.PUSH_TEST_SUBSCRIPTION_ID,
    process.env.PUSH_TEST_SUBSCRIPTION_IDS,
  ]
    .flatMap((value) => String(value || "").split(","))
    .map((value) => String(value || "").trim())
    .filter(Boolean);

  return Array.from(new Set(ids));
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
  if (
    process.env.NODE_ENV === "production" &&
    [
      process.env.PUSH_ALLOW_DB_RECIPIENT_LOOKUP,
      process.env.PUSH_ALLOW_AUDIENCE,
      process.env.PUSH_ALLOW_ENGINE_EVENTS,
      process.env.PUSH_ALLOW_ADMIN_MASS_SEND,
      process.env.PUSH_ALLOW_CAMPAIGNS,
    ].some((value) => value === "true")
  ) {
    throw coded("push_production_unsafe_flags_blocked");
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
  const allowedIds = getAllowedTestSubscriptionIds();
  const maxDevices = Math.max(1, Number(process.env.PUSH_TEST_MAX_DEVICES || 2) || 2);
  if (allowedIds.length > maxDevices) {
    throw coded("push_test_too_many_subscriptions_configured");
  }
  if (!allowedIds.length) {
    throw coded("push_test_subscription_id_missing");
  }
  return true;
}

export function assertPushCurrentDeviceMode({
  source = "current_device_test",
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
  if (
    process.env.NODE_ENV === "production" &&
    [
      process.env.PUSH_ALLOW_DB_RECIPIENT_LOOKUP,
      process.env.PUSH_ALLOW_AUDIENCE,
      process.env.PUSH_ALLOW_ENGINE_EVENTS,
      process.env.PUSH_ALLOW_ADMIN_MASS_SEND,
      process.env.PUSH_ALLOW_CAMPAIGNS,
    ].some((value) => value === "true")
  ) {
    throw coded("push_production_unsafe_flags_blocked");
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
  return true;
}

export function assertPushSubscribeMode() {
  if (process.env.PUSH_ENABLED !== "true") throw coded("push_disabled");
  if (process.env.PUSH_MODE !== "single_device_test") {
    throw coded("push_mode_not_single_device_test");
  }
  if (process.env.PUSH_TEST_ONLY !== "true") throw coded("push_test_only_required");
  return true;
}

export function assertAllowedTestSubscription({ subscriptionId } = {}) {
  const allowedIds = getAllowedTestSubscriptionIds();
  const maxDevices = Math.max(1, Number(process.env.PUSH_TEST_MAX_DEVICES || 2) || 2);
  if (allowedIds.length > maxDevices) {
    throw coded("push_test_too_many_subscriptions_configured");
  }
  if (!allowedIds.length) throw coded("push_test_subscription_id_missing");
  if (!allowedIds.includes(String(subscriptionId || "").trim())) {
    throw coded("push_subscription_not_allowed_in_test_mode");
  }
  return true;
}
