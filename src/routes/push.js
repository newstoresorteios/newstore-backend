import crypto from "crypto";
import express from "express";
import { requireAuth } from "../middleware/auth.js";
import { query } from "../db.js";
import {
  assertPushTestAccountAllowed,
  isPushTestAccountAllowed,
} from "../services/notifications/pushAccessGuard.js";
import {
  deactivatePushSubscription,
  getPushPreferences,
  getPushVapidConfigStatus,
  getVapidPublicKey,
  savePushSubscription,
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

function sameSecret(value, expected) {
  const a = Buffer.from(String(value || ""));
  const b = Buffer.from(String(expected || ""));
  return a.length > 0 && a.length === b.length && crypto.timingSafeEqual(a, b);
}

function statusFor(code) {
  if (code === "push_hidden_for_user") return 404;
  if (code === "push_user_not_authenticated") return 401;
  if (code === "push_test_subscription_not_found_or_inactive") return 404;
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

function logAccessCheck(req, visible) {
  if (process.env.NODE_ENV === "production") return;
  console.log("[push.access] check", {
    user_id: req.user?.id || null,
    allowed_user_id: process.env.PUSH_TEST_ALLOWED_USER_ID || null,
    has_allowed_email: Boolean(String(process.env.PUSH_TEST_ALLOWED_EMAIL || "").trim()),
    visible: Boolean(visible),
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
  const visible = isPushTestAccountAllowed({ user: req.user });
  logAccessCheck(req, visible);
  if (!visible) {
    return res.status(404).json({ ok: false, visible: false, error: "push_hidden_for_user" });
  }
  return res.json({
    ok: true,
    visible: true,
    allowed: true,
    mode: "single_device_test",
    test_label: process.env.PUSH_TEST_PHONE_LABEL || TEST_LABEL,
  });
});

router.use((req, res, next) => {
  try {
    assertPushTestAccountAllowed({ user: req.user });
    return next();
  } catch (error) {
    return res.status(404).json({ ok: false, visible: false, error: "push_hidden_for_user" });
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

router.get("/debug-config", (_req, res) => {
  if (process.env.NODE_ENV === "production") {
    return res.status(404).json({ ok: false, error: "push_debug_not_available" });
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
    return res.json(await getPushPreferences({ userId: req.user.id }));
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
      userId: req.user.id,
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
    if (process.env.NODE_ENV !== "production") {
      console.log("[push.subscribe] start", {
        user_id: req.user?.id || null,
      });
    }
    const allowed = new Set(["subscription", "deviceLabel", "setupCode"]);
    if (Object.keys(req.body || {}).some((key) => !allowed.has(key))) {
      const error = new Error("push_subscription_invalid");
      error.code = "push_subscription_invalid";
      throw error;
    }
    if (process.env.PUSH_ALLOW_PUBLIC_SUBSCRIBE !== "true") {
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
    const existing = incomingEndpoint
      ? await query(
          `SELECT id, is_active
             FROM public.push_subscriptions
            WHERE user_id = $1 AND endpoint = $2
            LIMIT 1`,
          [req.user.id, incomingEndpoint]
        )
      : null;
    const activeCountResult = await query(
      `SELECT COUNT(*)::int AS count
         FROM public.push_subscriptions
        WHERE user_id = $1 AND is_active = true`,
      [req.user.id]
    );
    const activeCount = Number(activeCountResult?.rows?.[0]?.count || 0);
    const maxDevices = Math.max(1, Number(process.env.PUSH_TEST_MAX_DEVICES || 2) || 2);
    if (!existing?.rows?.[0] && activeCount >= maxDevices) {
      const error = new Error("push_test_device_limit_reached");
      error.code = "push_test_device_limit_reached";
      throw error;
    }
    if (existing?.rows?.[0] && existing.rows[0].is_active !== true && activeCount >= maxDevices) {
      const error = new Error("push_test_device_limit_reached");
      error.code = "push_test_device_limit_reached";
      throw error;
    }

    const saved = await savePushSubscription({
      userId: req.user.id,
      subscription: req.body?.subscription,
      userAgent: req.get("user-agent") || null,
      deviceLabel: req.body?.deviceLabel,
    });
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
      message: "Copie este subscription_id para PUSH_TEST_SUBSCRIPTION_ID para liberar envio de teste.",
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
      await deactivatePushSubscription({ userId: req.user.id, endpoint: req.body?.endpoint })
    );
  } catch (error) {
    return sendError(res, error);
  }
});

async function handleTest(req, res) {
  try {
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
