function coded(code) {
  const err = new Error(code);
  err.code = code;
  return err;
}

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function parseCsvEnv(value) {
  return String(value || "")
    .split(",")
    .map((v) => v.trim())
    .filter(Boolean);
}

function getAllowedTestUserIds() {
  return [
    process.env.PUSH_TEST_ALLOWED_USER_ID,
    ...parseCsvEnv(process.env.PUSH_TEST_ALLOWED_USER_IDS),
  ]
    .map((v) => String(v || "").trim())
    .filter(Boolean);
}

function getAllowedTestEmails() {
  return [
    process.env.PUSH_TEST_ALLOWED_EMAIL,
    ...parseCsvEnv(process.env.PUSH_TEST_ALLOWED_EMAILS),
  ]
    .map((v) => normalizeEmail(v))
    .filter(Boolean);
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
  const allowedUserIds = getAllowedTestUserIds();
  const allowedEmails = getAllowedTestEmails();
  const userId = getAuthenticatedUserId({ user, auth });
  const userEmail = getAuthenticatedUserEmail({ user, auth });
  const pushEnabled = process.env.PUSH_ENABLED === "true";
  const mode = String(process.env.PUSH_MODE || "");
  const testOnly = process.env.PUSH_TEST_ONLY === "true";
  const publicSubscribe = process.env.PUSH_ALLOW_PUBLIC_SUBSCRIBE === "true";
  const testMode = testOnly && mode === "single_device_test";
  const productionMode = !testOnly && mode === "production";
  const matchesUserId = Boolean(userId != null && allowedUserIds.includes(String(userId)));
  const matchesEmail = Boolean(userEmail && allowedEmails.includes(userEmail));
  const authenticated = userId != null || Boolean(userEmail);
  const isTestAccount = matchesUserId || matchesEmail;

  let reason = null;
  if (!pushEnabled) reason = "push_disabled";
  else if (!authenticated) reason = "push_user_not_authenticated";
  else if (testOnly && !testMode) reason = "push_mode_not_single_device_test";
  else if (!testOnly && !productionMode) reason = "push_mode_not_production";
  else if (!isTestAccount && !publicSubscribe) reason = "push_hidden_for_user";

  const visible = !reason;
  const canSubscribe = visible;
  const canSendTest = !reason && isTestAccount && testOnly;

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
    allowedUserIdConfigured: allowedUserIds.length > 0,
    allowedEmailConfigured: allowedEmails.length > 0,
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

