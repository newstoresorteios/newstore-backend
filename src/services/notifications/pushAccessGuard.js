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
  const publicSubscribe = process.env.PUSH_ALLOW_PUBLIC_SUBSCRIBE === "true";
  const matchesUserId = Boolean(allowedUserId && userId != null && String(userId) === allowedUserId);
  const matchesEmail = Boolean(allowedEmail && userEmail && userEmail === allowedEmail);
  const authenticated = userId != null || Boolean(userEmail);
  const isTestAccount = matchesUserId || matchesEmail;

  let reason = null;
  if (!pushEnabled) reason = "push_disabled";
  else if (mode !== "single_device_test") reason = "push_mode_not_single_device_test";
  else if (!testOnly) reason = "push_test_only_required";
  else if (!authenticated) reason = "push_user_not_authenticated";
  else if (!isTestAccount && !publicSubscribe) reason = "push_hidden_for_user";

  const visible = !reason;
  const canSubscribe = visible;
  const canSendTest = !reason && isTestAccount;

  return {
    visible,
    allowed: visible,
    canSubscribe,
    canSendTest,
    reason,
    userId,
    hasEmail: Boolean(userEmail),
    pushEnabled,
    mode,
    testOnly,
    publicSubscribe,
    allowedUserIdConfigured: Boolean(allowedUserId),
    allowedEmailConfigured: Boolean(allowedEmail),
    matchesUserId,
    matchesEmail,
    isTestAccount,
  };
}

export function assertPushTestAccountAllowed({ user, auth } = {}) {
  const decision = getPushAccessDecision({ user, auth });
  if (!decision.canSendTest) throw coded(decision.reason || "push_hidden_for_user");
  return true;
}

export function assertPushSubscribeAllowed({ user, auth } = {}) {
  const decision = getPushAccessDecision({ user, auth });
  if (!decision.canSubscribe) throw coded(decision.reason || "push_hidden_for_user");
  return true;
}

export function isPushTestAccountAllowed({ user, auth } = {}) {
  return getPushAccessDecision({ user, auth }).canSendTest;
}

