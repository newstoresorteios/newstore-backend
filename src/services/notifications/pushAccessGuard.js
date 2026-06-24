function coded(code) {
  const err = new Error(code);
  err.code = code;
  return err;
}

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

export function getAuthenticatedUserId({ user, auth } = {}) {
  return (
    user?.id ??
    user?.user_id ??
    user?.userId ??
    user?.sub ??
    auth?.user_id ??
    auth?.sub ??
    null
  );
}

export function getAuthenticatedUserEmail({ user, auth } = {}) {
  return normalizeEmail(user?.email ?? auth?.email ?? "");
}

export function getPushAccessDecision({ user, auth } = {}) {
  const allowedUserId = String(process.env.PUSH_TEST_ALLOWED_USER_ID || "").trim();
  const allowedEmail = normalizeEmail(process.env.PUSH_TEST_ALLOWED_EMAIL || "");
  const userId = getAuthenticatedUserId({ user, auth });
  const userEmail = getAuthenticatedUserEmail({ user, auth });
  const pushEnabled = process.env.PUSH_ENABLED === "true";
  const mode = String(process.env.PUSH_MODE || "");
  const testOnly = process.env.PUSH_TEST_ONLY === "true";
  const matchesUserId = Boolean(allowedUserId && userId != null && String(userId) === allowedUserId);
  const matchesEmail = Boolean(allowedEmail && userEmail && userEmail === allowedEmail);

  let reason = null;
  if (!pushEnabled) reason = "push_disabled";
  else if (mode !== "single_device_test") reason = "push_mode_not_single_device_test";
  else if (!testOnly) reason = "push_test_only_required";
  else if (!allowedUserId && !allowedEmail) reason = "push_test_allowed_user_missing";
  else if (userId == null && !userEmail) reason = "push_user_not_authenticated";
  else if (!matchesUserId && !matchesEmail) reason = "push_hidden_for_user";

  return {
    visible: !reason,
    reason,
    userId,
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

export function assertPushTestAccountAllowed({ user, auth } = {}) {
  const decision = getPushAccessDecision({ user, auth });
  if (!decision.visible) throw coded(decision.reason || "push_hidden_for_user");
  return true;
}

export function isPushTestAccountAllowed({ user, auth } = {}) {
  return getPushAccessDecision({ user, auth }).visible;
}

