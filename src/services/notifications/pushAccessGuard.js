function coded(code) {
  const err = new Error(code);
  err.code = code;
  return err;
}

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

export function getPushAccessDecision({ user } = {}) {
  const allowedUserId = String(process.env.PUSH_TEST_ALLOWED_USER_ID || "").trim();
  const allowedEmail = normalizeEmail(process.env.PUSH_TEST_ALLOWED_EMAIL || "");
  const userEmail = normalizeEmail(user?.email || "");
  const pushEnabled = process.env.PUSH_ENABLED === "true";
  const mode = String(process.env.PUSH_MODE || "");
  const testOnly = process.env.PUSH_TEST_ONLY === "true";
  const matchesUserId = Boolean(allowedUserId && user?.id && String(user.id) === allowedUserId);
  const matchesEmail = Boolean(allowedEmail && userEmail && userEmail === allowedEmail);

  let reason = null;
  if (!pushEnabled) reason = "push_disabled";
  else if (mode !== "single_device_test") reason = "push_mode_not_single_device_test";
  else if (!testOnly) reason = "push_test_only_required";
  else if (!allowedUserId && !allowedEmail) reason = "push_test_allowed_user_missing";
  else if (!user || !user.id) reason = "push_user_not_authenticated";
  else if (!matchesUserId && !matchesEmail) reason = "push_hidden_for_user";

  return {
    visible: !reason,
    reason,
    userId: user?.id || null,
    hasEmail: Boolean(userEmail),
    pushEnabled,
    mode,
    testOnly,
    allowedUserIdConfigured: Boolean(allowedUserId),
    allowedEmailConfigured: Boolean(allowedEmail),
    matchesUserId,
    matchesEmail,
  };
}

export function assertPushTestAccountAllowed({ user } = {}) {
  const decision = getPushAccessDecision({ user });
  if (!decision.visible) throw coded(decision.reason || "push_hidden_for_user");
  return true;
}

export function isPushTestAccountAllowed({ user } = {}) {
  return getPushAccessDecision({ user }).visible;
}

