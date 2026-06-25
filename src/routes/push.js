import crypto from "crypto";
import express from "express";
import { requireAuth } from "../middleware/auth.js";
import { query } from "../db.js";
import {
  assertPushSubscribeAllowed,
  assertPushTestAccountAllowed,
  getAuthenticatedUserId,
  getPushAccessDecision,
} from "../services/notifications/pushAccessGuard.js";
import {
  deactivatePushSubscription,
  getPushPreferences,
  getPushVapidConfigStatus,
  getVapidPublicKey,
  savePushSubscription,
  sendPushToSubscriptionRow,
  sendTestPushToConfiguredSubscription,
  updatePushPreferences,
} from "../services/notifications/pushNotifications.js";
import {
  assertPushSubscribeMode,
  getAllowedTestSubscriptionIds,
} from "../services/notifications/pushSingleDeviceGuard.js";

const router = express.Router();
const TEST_LABEL = "43998640480";
const TEST_FIELDS = new Set(["title", "body", "url"]);

function requiresSetupCode() {
  return process.env.PUSH_REQUIRE_SETUP_CODE === "true";
}

function getRequestUserId(req) {
  return getAuthenticatedUserId({ user: req.user, auth: req.auth });
}

function requireRequestUserId(req) {
  const userId = getRequestUserId(req);
  if (userId == null) {
    const error = new Error("push_user_not_authenticated");
    error.code = "push_user_not_authenticated";
    throw error;
  }
  return userId;
}

function sameSecret(value, expected) {
  const a = Buffer.from(String(value || ""));
  const b = Buffer.from(String(expected || ""));
  return a.length > 0 && a.length === b.length && crypto.timingSafeEqual(a, b);
}

function statusFor(code) {
  if (code === "push_hidden_for_user") return 404;
  if (code === "push_user_not_authenticated") return 401;
  if (code === "push_test_subscription_not_found_or_inactive") return 404;
  if (code === "push_current_device_subscription_not_found") return 404;
  if (code === "push_tables_missing") return 500;
  if (code === "push_test_setup_code_required") return 403;
  if (code === "push_test_setup_code_not_configured") return 500;
  if (code?.includes("required") || code?.includes("invalid") || code?.includes("too_long")) return 400;
  if (code?.startsWith("push_")) return 403;
  return 500;
}

function sendError(res, error) {
  const code = error?.code || "push_internal_error";
  return res.status(statusFor(code)).json({
    ok: false,
    error: code,
    ...(error?.provider_status ? { provider_status: error.provider_status } : {}),
    ...(code === "push_provider_forbidden_or_vapid_mismatch"
      ? { hint: "Recreate the browser subscription after confirming VAPID keys." }
      : {}),
  });
}

function isMissingTableError(error) {
  return (
    error?.code === "42P01" ||
    error?.parent?.code === "42P01" ||
    error?.cause?.code === "42P01" ||
    String(error?.message || "").includes("42P01")
  );
}

function assertOnlyTestFields(body) {
  if (Object.keys(body || {}).some((key) => !TEST_FIELDS.has(key))) {
    const error = new Error("push_test_payload_not_allowed");
    error.code = "push_test_payload_not_allowed";
    throw error;
  }
}

function logAccessCheck(decision) {
  console.log("[push.access] decision", {
    resolved_user_id: decision.userId,
    has_email: decision.hasEmail,
    allowed_user_id_configured: decision.allowedUserIdConfigured,
    allowed_email_configured: decision.allowedEmailConfigured,
    matches_user_id: decision.matchesUserId,
    matches_email: decision.matchesEmail,
    push_enabled: decision.pushEnabled,
    mode: decision.mode || null,
    test_only: decision.testOnly,
    visible: decision.visible,
    can_subscribe: decision.canSubscribe,
    can_send_test: decision.canSendTest,
    reason: decision.reason || null,
  });
}

function logConfigStatus() {
  if (process.env.NODE_ENV === "production") return;
  const status = getPushVapidConfigStatus();
  if (!status.enabled) {
    console.warn("[push.config] disabled", {
      hasPushEnabled: status.hasPushEnabled,
      mode: status.mode,
      hasPublicKey: status.hasPublicKey,
        hasPrivateKey: status.hasPrivateKey,
        publicKeyLength: status.publicKeyLength,
        privateKeyLength: status.privateKeyLength,
        hasSubject: status.hasSubject,
        subjectValueSafe: status.subjectValueSafe,
        publicKeyFingerprint: status.publicKeyFingerprint,
      });
  } else {
    console.log("[push.config] status", {
      hasPushEnabled: status.hasPushEnabled,
      mode: status.mode,
      hasPublicKey: status.hasPublicKey,
      hasPrivateKey: status.hasPrivateKey,
      publicKeyLength: status.publicKeyLength,
      privateKeyLength: status.privateKeyLength,
      hasSubject: status.hasSubject,
      subjectValueSafe: status.subjectValueSafe,
      publicKeyFingerprint: status.publicKeyFingerprint,
    });
  }
}

router.use(requireAuth);

router.get("/access", (req, res) => {
  const decision = getPushAccessDecision({ user: req.user, auth: req.auth });
  logAccessCheck(decision);
  if (!decision.visible) {
    return res.status(404).json({
      ok: false,
      visible: false,
      allowed: false,
      can_subscribe: false,
      can_send_test: false,
      error: "push_hidden_for_user",
    });
  }
  return res.json({
    ok: true,
    visible: decision.visible,
    allowed: decision.allowed,
    can_subscribe: decision.canSubscribe,
    can_send_test: decision.canSendTest,
    mode: "single_device_test",
    test_only: true,
    test_label: process.env.PUSH_TEST_PHONE_LABEL || TEST_LABEL,
    production_send_enabled: process.env.PUSH_ALLOW_PRODUCTION_SEND === "true",
    configured_subscription_count: getAllowedTestSubscriptionIds().length,
    allowed_user_id_loaded: Boolean(String(process.env.PUSH_TEST_ALLOWED_USER_ID || "").trim()),
    requires_setup_code: requiresSetupCode(),
  });
});

router.use((req, res, next) => {
  try {
    assertPushSubscribeAllowed({ user: req.user, auth: req.auth });
    return next();
  } catch (error) {
    return res.status(404).json({ ok: false, visible: false, allowed: false, error: "push_hidden_for_user" });
  }
});

router.get("/vapid-public-key", (_req, res) => {
  const status = getPushVapidConfigStatus();
  logConfigStatus();
  return res.json({
    enabled: status.enabled,
    mode: "single_device_test",
    test_only: true,
    publicKey: status.enabled ? getVapidPublicKey() : null,
    test_label: process.env.PUSH_TEST_PHONE_LABEL || TEST_LABEL,
    publicKeyFingerprint: status.publicKeyFingerprint,
    ...(status.enabled ? {} : { error: status.error || "push_vapid_not_configured" }),
  });
});

router.get("/debug-config", (req, res) => {
  if (process.env.NODE_ENV === "production") {
    return res.status(404).json({ ok: false, error: "push_debug_not_available" });
  }
  try {
    assertPushTestAccountAllowed({ user: req.user, auth: req.auth });
  } catch (error) {
    return res.status(404).json({ ok: false, visible: false, allowed: false, error: "push_hidden_for_user" });
  }
  const subscriptionIds = getAllowedTestSubscriptionIds();
  return res.json({
    ok: true,
    vapid: getPushVapidConfigStatus(),
    allowedSubscriptionIdsCount: subscriptionIds.length,
    maxDevices: Math.max(1, Number(process.env.PUSH_TEST_MAX_DEVICES || 2) || 2),
    allowProductionSend: process.env.PUSH_ALLOW_PRODUCTION_SEND === "true",
    allowAudience: process.env.PUSH_ALLOW_AUDIENCE === "true",
    allowEngine: process.env.PUSH_ALLOW_ENGINE_EVENTS === "true",
    allowMassSend: process.env.PUSH_ALLOW_ADMIN_MASS_SEND === "true",
    allowCampaigns: process.env.PUSH_ALLOW_CAMPAIGNS === "true",
    allowDbRecipientLookup: process.env.PUSH_ALLOW_DB_RECIPIENT_LOOKUP === "true",
  });
});

router.get("/preferences", async (req, res) => {
  try {
    return res.json(await getPushPreferences({ userId: requireRequestUserId(req) }));
  } catch (error) {
    return sendError(res, error);
  }
});

router.put("/preferences", async (req, res) => {
  try {
    const allowed = new Set(["push_operational_opt_in", "push_marketing_opt_in"]);
    if (Object.keys(req.body || {}).some((key) => !allowed.has(key))) {
      const error = new Error("push_preferences_invalid");
      error.code = "push_preferences_invalid";
      throw error;
    }
    const preferences = await updatePushPreferences({
      userId: requireRequestUserId(req),
      operationalOptIn: req.body?.push_operational_opt_in,
      marketingOptIn: req.body?.push_marketing_opt_in,
    });
    return res.json({ ok: true, ...preferences });
  } catch (error) {
    return sendError(res, error);
  }
});

router.post("/subscribe", async (req, res) => {
  try {
    assertPushSubscribeMode();
    const userId = requireRequestUserId(req);
    if (process.env.NODE_ENV !== "production") {
      console.log("[push.subscribe] start", {
        user_id: userId,
      });
    }
    const allowed = new Set(["subscription", "deviceLabel", "setupCode"]);
    if (Object.keys(req.body || {}).some((key) => !allowed.has(key))) {
      const error = new Error("push_subscription_invalid");
      error.code = "push_subscription_invalid";
      throw error;
    }
    if (requiresSetupCode()) {
      const setupCode = String(req.body?.setupCode || "").trim();
      const expectedSetupCode = String(process.env.PUSH_TEST_SETUP_CODE || "").trim();
      if (process.env.NODE_ENV !== "production") {
        console.log("[push.subscribe] setup-code:check", {
          hasSetupCode: Boolean(setupCode),
          setupCodeLength: setupCode.length,
          hasExpectedSetupCode: Boolean(expectedSetupCode),
          expectedSetupCodeLength: expectedSetupCode.length,
        });
      }
      if (!expectedSetupCode) {
        const error = new Error("push_test_setup_code_not_configured");
        error.code = "push_test_setup_code_not_configured";
        throw error;
      }
      if (!setupCode || !sameSecret(setupCode, expectedSetupCode)) {
        const error = new Error("push_test_setup_code_required");
        error.code = "push_test_setup_code_required";
        throw error;
      }
    }

    const incomingEndpoint = String(req.body?.subscription?.endpoint || "").trim();
    const saved = await savePushSubscription({
      userId,
      subscription: req.body?.subscription,
      userAgent: req.get("user-agent") || null,
      deviceLabel: req.body?.deviceLabel,
    });
    if (
      process.env.PUSH_SINGLE_ACTIVE_DEVICE_PER_TEST_USER === "true" &&
      incomingEndpoint
    ) {
      await query(
        `UPDATE public.push_subscriptions
            SET is_active = false,
                updated_at = now()
          WHERE user_id = $1
            AND endpoint <> $2
            AND is_active = true`,
        [userId, incomingEndpoint]
      );
    }
    if (process.env.NODE_ENV !== "production") {
      console.log("[push.subscribe] saved", {
        subscription_id: saved.subscription_id || null,
      });
    }
    return res.status(201).json({
      ok: true,
      subscription_id: saved.subscription_id,
      test_mode: true,
      test_label: process.env.PUSH_TEST_PHONE_LABEL || TEST_LABEL,
      message: "Notificações ativadas neste dispositivo.",
    });
  } catch (error) {
    if (isMissingTableError(error)) {
      if (process.env.NODE_ENV !== "production") {
        console.log("[push.subscribe] tables_missing");
      }
      error.code = "push_tables_missing";
    }
    if (process.env.NODE_ENV !== "production") {
      console.log("[push.subscribe] blocked", { code: error?.code || null });
    }
    return sendError(res, error);
  }
});

router.post("/unsubscribe", async (req, res) => {
  try {
    if (Object.keys(req.body || {}).some((key) => key !== "endpoint")) {
      const error = new Error("push_endpoint_required");
      error.code = "push_endpoint_required";
      throw error;
    }
    return res.json(
      await deactivatePushSubscription({ userId: requireRequestUserId(req), endpoint: req.body?.endpoint })
    );
  } catch (error) {
    return sendError(res, error);
  }
});

router.post("/test-current-device", async (req, res) => {
  try {
    assertPushTestAccountAllowed({ user: req.user, auth: req.auth });
    const allowed = new Set(["subscription"]);
    if (Object.keys(req.body || {}).some((key) => !allowed.has(key))) {
      const error = new Error("push_test_payload_not_allowed");
      error.code = "push_test_payload_not_allowed";
      throw error;
    }

    const userId = requireRequestUserId(req);
    const endpoint = String(req.body?.subscription?.endpoint || "").trim();
    if (!endpoint) {
      const error = new Error("push_endpoint_required");
      error.code = "push_endpoint_required";
      throw error;
    }

    const result = await query(
      `SELECT *
         FROM public.push_subscriptions
        WHERE endpoint = $1
          AND user_id = $2
          AND is_active = true
        LIMIT 1`,
      [endpoint, userId]
    );

    const subscriptionRow = result.rows?.[0];
    if (!subscriptionRow) {
      const error = new Error("push_current_device_subscription_not_found");
      error.code = "push_current_device_subscription_not_found";
      throw error;
    }

    const out = await sendPushToSubscriptionRow({
      subscriptionRow,
      title: "New Store",
      body: "Teste controlado de Push enviado para este dispositivo.",
      url: "/conta",
      payload: {},
      source: "current_device_test",
      eventKey: "PUSH_TEST_CURRENT_DEVICE",
      category: "operational",
      requireConfiguredSubscription: false,
    });

    return res.json({ ok: true, ...out });
  } catch (error) {
    return sendError(res, error);
  }
});

async function handleTest(req, res) {
  try {
    assertPushTestAccountAllowed({ user: req.user, auth: req.auth });
    assertOnlyTestFields(req.body || {});
    const result = await sendTestPushToConfiguredSubscription({
      title: req.body?.title,
      body: req.body?.body,
      url: req.body?.url || "/me",
      payload: {},
      source: "authenticated_user_test",
    });
    return res.json({ ok: true, ...result });
  } catch (error) {
    return sendError(res, error);
  }
}

router.post("/test", handleTest);
router.post("/test-single-device", handleTest);

export default router;
