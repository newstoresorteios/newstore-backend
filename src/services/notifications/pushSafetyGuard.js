export function getAllowedTestUserIds() {
  return String(process.env.PUSH_ALLOWED_TEST_USER_IDS || "")
    .split(",")
    .map((v) => Number(String(v).trim()))
    .filter(Boolean);
}

export function isPushTestMode() {
  return String(process.env.PUSH_TEST_MODE || "true").toLowerCase() === "true";
}

export function isPushProductionEnabled() {
  return String(process.env.PUSH_PRODUCTION_SEND_ENABLED || "false").toLowerCase() === "true";
}

export function assertPushSendAllowed({
  userId,
  source,
  isAudience,
  isAdminTest,
  isSelfTest,
  category,
}) {
  void source;
  void category;

  if (process.env.PUSH_ENABLED !== "true") {
    const err = new Error("push_disabled");
    err.code = "push_disabled";
    throw err;
  }

  const testMode = isPushTestMode();
  const productionEnabled = isPushProductionEnabled();
  const allowedIds = getAllowedTestUserIds();

  if (testMode) {
    if (isAudience) {
      const err = new Error("push_audience_blocked_in_test_mode");
      err.code = "push_audience_blocked_in_test_mode";
      throw err;
    }

    if (!isAdminTest && !isSelfTest) {
      const err = new Error("push_only_test_allowed");
      err.code = "push_only_test_allowed";
      throw err;
    }

    if (userId && allowedIds.length > 0 && !allowedIds.includes(Number(userId))) {
      const err = new Error("push_user_not_allowed_for_test");
      err.code = "push_user_not_allowed_for_test";
      throw err;
    }

    return true;
  }

  if (!productionEnabled) {
    const err = new Error("push_production_send_disabled");
    err.code = "push_production_send_disabled";
    throw err;
  }

  return true;
}
