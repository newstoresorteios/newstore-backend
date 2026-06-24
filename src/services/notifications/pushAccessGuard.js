function coded(code) {
  const err = new Error(code);
  err.code = code;
  return err;
}

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

export function assertPushTestAccountAllowed({ user } = {}) {
  const allowedUserId = String(process.env.PUSH_TEST_ALLOWED_USER_ID || "").trim();
  const allowedEmail = normalizeEmail(process.env.PUSH_TEST_ALLOWED_EMAIL || "");

  if (process.env.PUSH_ENABLED !== "true") throw coded("push_disabled");
  if (process.env.PUSH_MODE !== "single_device_test") {
    throw coded("push_mode_not_single_device_test");
  }
  if (process.env.PUSH_TEST_ONLY !== "true") throw coded("push_test_only_required");
  if (!allowedUserId && !allowedEmail) throw coded("push_test_allowed_user_missing");
  if (!user || !user.id) throw coded("push_user_not_authenticated");

  const idAllowed = Boolean(allowedUserId && String(user.id) === allowedUserId);
  const emailAllowed = Boolean(allowedEmail && normalizeEmail(user.email) === allowedEmail);
  if (!idAllowed && !emailAllowed) throw coded("push_hidden_for_user");
  return true;
}

export function isPushTestAccountAllowed({ user } = {}) {
  try {
    assertPushTestAccountAllowed({ user });
    return true;
  } catch {
    return false;
  }
}

