import express from "express";
import { requireAuth } from "../middleware/auth.js";
import {
  isPushTestMode,
  isPushProductionEnabled,
} from "../services/notifications/pushSafetyGuard.js";
import {
  configureWebPush,
  getVapidPublicKey,
  savePushSubscription,
  deactivatePushSubscription,
  getPushPreferences,
  updatePushPreferences,
  sendPushToUser,
} from "../services/notifications/pushNotifications.js";

const router = express.Router();

configureWebPush();

router.get("/config", (_req, res) => {
  return res.json({
    enabled: process.env.PUSH_ENABLED === "true",
    test_mode: isPushTestMode(),
    production_send_enabled: isPushProductionEnabled(),
    publicKey: getVapidPublicKey(),
  });
});

router.get("/preferences", requireAuth, async (req, res) => {
  try {
    const prefs = await getPushPreferences({ userId: req.user.id });
    return res.json(prefs);
  } catch (e) {
    if (e?.code === "user_not_found") {
      return res.status(404).json({ error: "user_not_found" });
    }
    console.error("[push] preferences get error:", e?.message || e);
    return res.status(500).json({ error: "internal_error" });
  }
});

router.put("/preferences", requireAuth, async (req, res) => {
  try {
    const { push_operational_opt_in, push_marketing_opt_in } = req.body || {};
    const prefs = await updatePushPreferences({
      userId: req.user.id,
      operationalOptIn: push_operational_opt_in === true,
      marketingOptIn: push_marketing_opt_in === true,
      source: "preferences_update",
      ip: req.ip,
      userAgent: req.headers["user-agent"],
    });
    return res.json(prefs);
  } catch (e) {
    console.error("[push] preferences update error:", e?.message || e);
    return res.status(500).json({ error: "internal_error" });
  }
});

router.post("/subscribe", requireAuth, async (req, res) => {
  try {
    const { subscription, deviceLabel } = req.body || {};
    if (!subscription?.endpoint) {
      return res.status(400).json({ error: "subscription_required" });
    }

    await savePushSubscription({
      userId: req.user.id,
      subscription,
      userAgent: req.headers["user-agent"],
      deviceLabel,
    });

    const prefs = await getPushPreferences({ userId: req.user.id });
    await updatePushPreferences({
      userId: req.user.id,
      operationalOptIn: true,
      marketingOptIn: prefs.push_marketing_opt_in === true,
      source: "push_subscribe",
      ip: req.ip,
      userAgent: req.headers["user-agent"],
    });

    return res.json({ ok: true });
  } catch (e) {
    if (e?.code === "invalid_subscription") {
      return res.status(400).json({ error: "invalid_subscription" });
    }
    console.error("[push] subscribe error:", e?.message || e);
    return res.status(500).json({ error: "internal_error" });
  }
});

router.post("/unsubscribe", requireAuth, async (req, res) => {
  try {
    const { endpoint } = req.body || {};
    if (!endpoint) {
      return res.status(400).json({ error: "endpoint_required" });
    }

    await deactivatePushSubscription({
      userId: req.user.id,
      endpoint,
    });

    return res.json({ ok: true });
  } catch (e) {
    console.error("[push] unsubscribe error:", e?.message || e);
    return res.status(500).json({ error: "internal_error" });
  }
});

router.post("/test", requireAuth, async (req, res) => {
  try {
    const result = await sendPushToUser({
      userId: req.user.id,
      title: "New Store",
      body: "Push de teste funcionando neste dispositivo.",
      url: "/me",
      eventKey: "PUSH_SELF_TEST",
      category: "operational",
      source: "self_test",
      isSelfTest: true,
    });

    return res.json({
      ok: true,
      mode: "test",
      ...result,
    });
  } catch (e) {
    if (e?.code === "push_disabled") {
      return res.status(503).json({ error: e.code });
    }
    if (e?.code === "push_user_not_allowed_for_test") {
      return res.status(403).json({ error: e.code });
    }
    console.error("[push] test error:", e?.message || e);
    return res.status(500).json({ error: "internal_error" });
  }
});

export default router;
