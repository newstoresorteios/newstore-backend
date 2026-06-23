import express from "express";
import { query } from "../db.js";
import {
  isPushTestMode,
  isPushProductionEnabled,
  getAllowedTestUserIds,
  assertPushSendAllowed,
} from "../services/notifications/pushSafetyGuard.js";
import {
  sendPushToUser,
  sendPushToAudience,
} from "../services/notifications/pushNotifications.js";

const router = express.Router();

function requireInternalEngineToken(req, res, next) {
  const expected = String(process.env.INTERNAL_ENGINE_TOKEN || "").trim();
  if (!expected) {
    return res.status(503).json({ error: "internal_engine_disabled" });
  }

  const auth = req.headers?.authorization || "";
  const token = auth.replace(/^Bearer\s+/i, "").trim();
  if (!token || token !== expected) {
    return res.status(401).json({ error: "unauthorized" });
  }

  return next();
}

router.use(requireInternalEngineToken);

router.post("/events", async (req, res) => {
  try {
    const body = req.body || {};
    const {
      event_key,
      category,
      title,
      body: messageBody,
      url,
      dedupe_key,
      entity_type,
      entity_id,
      user_id,
      audience,
      payload,
      dry_run,
      channel,
      channels,
    } = body;

    if (channel === "whatsapp") {
      return res.status(400).json({ error: "whatsapp_channel_not_supported" });
    }

    const channelList = Array.isArray(channels) ? channels : [];
    if (channelList.some((c) => String(c).toLowerCase() === "whatsapp")) {
      return res.status(400).json({ error: "whatsapp_channel_not_supported" });
    }

    if (!event_key) {
      return res.status(400).json({ error: "event_key_required" });
    }

    if (!dedupe_key) {
      return res.status(400).json({ error: "dedupe_key_required" });
    }

    const mode = isPushTestMode() ? "test" : "production";

    const ledgerInsert = await query(
      `INSERT INTO public.notification_event_ledger
         (event_key, dedupe_key, channel, category, entity_type, entity_id, user_id, status, mode, meta)
       VALUES ($1, $2, 'push', $3, $4, $5, $6, 'created', $7, $8::jsonb)
       ON CONFLICT (dedupe_key) DO NOTHING
       RETURNING id`,
      [
        event_key,
        dedupe_key,
        category || null,
        entity_type || null,
        entity_id != null ? String(entity_id) : null,
        user_id != null ? Number(user_id) : null,
        mode,
        JSON.stringify({
          dry_run: dry_run === true,
          audience: audience || null,
          source: "engine",
        }),
      ]
    );

    if (!ledgerInsert.rows.length) {
      return res.json({ ok: true, deduped: true });
    }

    if (dry_run === true) {
      console.log("[push] engine:dry_run", {
        event_key,
        user_id,
        audience: audience || null,
        dedupe_key,
      });
      return res.json({ ok: true, dry_run: true, mode });
    }

    if (audience) {
      if (isPushTestMode()) {
        return res.status(403).json({ error: "push_audience_blocked_in_test_mode" });
      }

      if (!isPushProductionEnabled()) {
        return res.status(403).json({ error: "push_production_send_disabled" });
      }

      try {
        assertPushSendAllowed({
          userId: null,
          source: "engine",
          isAudience: true,
          isAdminTest: false,
          isSelfTest: false,
          category,
        });
      } catch (e) {
        return res.status(403).json({ error: e.code || e.message });
      }

      const safeTitle = title || "New Store";
      const safeBody = messageBody || "";
      const result = await sendPushToAudience({
        audience,
        title: safeTitle,
        body: safeBody,
        url: url || "/me",
        eventKey: event_key,
        category,
        payload: payload || {},
        source: "engine",
        entityId: entity_id,
      });

      await query(
        `UPDATE public.notification_event_ledger
            SET status = 'sent', meta = meta || $2::jsonb
          WHERE dedupe_key = $1`,
        [
          dedupe_key,
          JSON.stringify({ result }),
        ]
      );

      return res.json({ ok: true, mode, audience, ...result });
    }

    if (user_id != null) {
      const uid = Number(user_id);

      try {
        assertPushSendAllowed({
          userId: uid,
          source: "engine",
          isAudience: false,
          isAdminTest: false,
          isSelfTest: false,
          category,
        });
      } catch (e) {
        return res.status(403).json({ error: e.code || e.message });
      }

      const safeTitle = title || "New Store";
      const safeBody = messageBody || "";
      const result = await sendPushToUser({
        userId: uid,
        title: safeTitle,
        body: safeBody,
        url: url || "/me",
        eventKey: event_key,
        category,
        payload: payload || {},
        source: "engine",
        isAdminTest: false,
        isSelfTest: false,
      });

      await query(
        `UPDATE public.notification_event_ledger
            SET status = 'sent', meta = meta || $2::jsonb
          WHERE dedupe_key = $1`,
        [
          dedupe_key,
          JSON.stringify({ result }),
        ]
      );

      return res.json({ ok: true, mode, user_id: uid, ...result });
    }

    return res.status(400).json({ error: "user_id_or_audience_required" });
  } catch (e) {
    console.error("[internal/notifications] events error:", e?.message || e);
    return res.status(500).json({ error: "internal_error" });
  }
});

export default router;
