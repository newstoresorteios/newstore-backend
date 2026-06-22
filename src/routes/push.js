import crypto from "crypto";
import express from "express";
import { requireAuth } from "../middleware/auth.js";
import {
  assertPushTestAccountAllowed,
  isPushTestAccountAllowed,
} from "../services/notifications/pushAccessGuard.js";
import {
  deactivatePushSubscription,
  getPushPreferences,
  getVapidPublicKey,
  savePushSubscription,
  sendTestPushToConfiguredSubscription,
  updatePushPreferences,
} from "../services/notifications/pushNotifications.js";

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
  if (code === "push_test_setup_code_required") return 403;
  if (code?.includes("required") || code?.includes("invalid") || code?.includes("too_long")) return 400;
  if (code?.startsWith("push_")) return 403;
  return 500;
}

function sendError(res, error) {
  const code = error?.code || "push_internal_error";
  return res.status(statusFor(code)).json({ ok: false, error: code });
}

function assertOnlyTestFields(body) {
  if (Object.keys(body || {}).some((key) => !TEST_FIELDS.has(key))) {
    const error = new Error("push_test_payload_not_allowed");
    error.code = "push_test_payload_not_allowed";
    throw error;
  }
}

router.use(requireAuth);

router.get("/access", (req, res) => {
  if (!isPushTestAccountAllowed({ user: req.user })) {
    return res.status(404).json({ ok: false, visible: false, error: "push_hidden_for_user" });
  }
  return res.json({
    ok: true,
    visible: true,
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
  return res.json({
    enabled: process.env.PUSH_ENABLED === "true",
    mode: "single_device_test",
    test_only: true,
    publicKey: getVapidPublicKey(),
    test_label: process.env.PUSH_TEST_PHONE_LABEL || TEST_LABEL,
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
    const allowed = new Set(["subscription", "deviceLabel", "setupCode"]);
    if (Object.keys(req.body || {}).some((key) => !allowed.has(key))) {
      const error = new Error("push_subscription_invalid");
      error.code = "push_subscription_invalid";
      throw error;
    }
    if (process.env.PUSH_ALLOW_PUBLIC_SUBSCRIBE !== "true") {
      const expected = String(process.env.PUSH_TEST_SETUP_CODE || "");
      if (!sameSecret(req.body?.setupCode, expected)) {
        const error = new Error("push_test_setup_code_required");
        error.code = "push_test_setup_code_required";
        throw error;
      }
    }
    const saved = await savePushSubscription({
      userId: req.user.id,
      subscription: req.body?.subscription,
      userAgent: req.get("user-agent") || null,
      deviceLabel: req.body?.deviceLabel,
    });
    return res.status(201).json({
      ok: true,
      subscription_id: saved.subscription_id,
      test_mode: true,
      test_label: process.env.PUSH_TEST_PHONE_LABEL || TEST_LABEL,
      message: "Copie este subscription_id para PUSH_TEST_SUBSCRIPTION_ID para liberar envio de teste.",
    });
  } catch (error) {
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
