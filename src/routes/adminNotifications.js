// src/routes/adminNotifications.js
import express from "express";
import { query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import {
  getNotificationHealth,
  sendTestWhatsApp,
  estimateAudience,
  manualSendNotification,
  getTestModeWarning,
  BREVO_IP_BLOCKED_MESSAGE,
} from "../services/notifications/notificationCenter.js";

const router = express.Router();

router.use(requireAuth, requireAdmin);

function toInt(v, def) {
  const n = Number(v);
  return Number.isFinite(n) && n >= 0 ? Math.trunc(n) : def;
}

function attachBrevoHint(payload, dispatch) {
  if (dispatch?.error_message === "brevo_ip_not_authorized") {
    return { ...payload, brevo_message: BREVO_IP_BLOCKED_MESSAGE };
  }
  return payload;
}

router.get("/health", async (_req, res) => {
  try {
    return res.json(getNotificationHealth());
  } catch (e) {
    console.error("[admin/notifications] health error:", e?.message || e);
    return res.status(500).json({ ok: false, error: "internal_error" });
  }
});

router.get("/dispatches", async (req, res) => {
  try {
    const { status, channel, provider } = req.query || {};
    const limit = Math.min(toInt(req.query?.limit, 50), 50);
    const offset = toInt(req.query?.offset, 0);

    const conditions = [];
    const params = [];
    let idx = 1;

    if (status) {
      conditions.push(`status = $${idx++}`);
      params.push(status);
    }
    if (channel) {
      conditions.push(`channel = $${idx++}`);
      params.push(channel);
    }
    if (provider) {
      conditions.push(`provider = $${idx++}`);
      params.push(provider);
    }

    const where = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";

    params.push(limit, offset);
    const r = await query(
      `SELECT *
         FROM public.notification_dispatches
        ${where}
        ORDER BY created_at DESC
        LIMIT $${idx++} OFFSET $${idx}`,
      params
    );

    return res.json({ rows: r.rows, limit, offset });
  } catch (e) {
    console.error("[admin/notifications] dispatches error:", e?.message || e);
    return res.status(500).json({ ok: false, error: "internal_error" });
  }
});

router.get("/inbound", async (req, res) => {
  try {
    const limit = Math.min(toInt(req.query?.limit, 50), 50);
    const offset = toInt(req.query?.offset, 0);

    const r = await query(
      `SELECT *
         FROM public.notification_inbound_messages
        ORDER BY created_at DESC
        LIMIT $1 OFFSET $2`,
      [limit, offset]
    );

    return res.json({ rows: r.rows, limit, offset });
  } catch (e) {
    console.error("[admin/notifications] inbound error:", e?.message || e);
    return res.status(500).json({ ok: false, error: "internal_error" });
  }
});

router.get("/templates", async (_req, res) => {
  try {
    const r = await query(
      `SELECT *
         FROM public.notification_templates
        ORDER BY template_key, channel, provider`
    );
    return res.json({ rows: r.rows });
  } catch (e) {
    console.error("[admin/notifications] templates error:", e?.message || e);
    return res.status(500).json({ ok: false, error: "internal_error" });
  }
});

router.post("/test-whatsapp", async (req, res) => {
  try {
    const {
      phone,
      user_id: userId,
      template_key: templateKey,
      template_id: templateId,
      params,
    } = req.body || {};

    const out = await sendTestWhatsApp({
      userId: userId != null ? Number(userId) : null,
      phone,
      templateKey: templateKey || "GENERIC_TEST",
      templateId,
      params: params || {},
      adminUserId: req.user?.id ?? null,
    });

    const warning = getTestModeWarning();
    return res.json(
      attachBrevoHint(
        {
          ok: out.ok,
          dispatch: out.dispatch,
          result: out.result,
          ...(warning && { warning }),
          ...(out.brevo_message && { brevo_message: out.brevo_message }),
        },
        out.dispatch
      )
    );
  } catch (e) {
    console.error("[admin/notifications] test-whatsapp error:", e?.message || e);
    return res.status(500).json({ ok: false, error: "internal_error" });
  }
});

router.post("/audience/estimate", async (req, res) => {
  try {
    const { filter, user_id: userId, phone } = req.body || {};
    if (!filter) {
      return res.status(400).json({ ok: false, error: "filter_required" });
    }

    const out = await estimateAudience({
      filter,
      userId: userId != null ? Number(userId) : null,
      phone,
    });

    return res.json(out);
  } catch (e) {
    console.error("[admin/notifications] audience estimate error:", e?.message || e);
    return res.status(500).json({ ok: false, error: "internal_error" });
  }
});

router.post("/manual-send", async (req, res) => {
  try {
    const {
      channel,
      template_key: templateKey,
      template_id: templateId,
      filter,
      phone,
      user_id: userId,
      params,
    } = req.body || {};

    const out = await manualSendNotification({
      channel: channel || "whatsapp",
      templateKey: templateKey || "GENERIC_TEST",
      templateId,
      filter,
      userId: userId != null ? Number(userId) : null,
      phone,
      params: params || {},
      adminUserId: req.user?.id ?? null,
    });

    return res.json(
      attachBrevoHint(
        {
          ok: out.ok,
          dispatch: out.dispatch,
          result: out.result,
          warning: out.warning || getTestModeWarning(),
          message: out.message,
          ...(out.brevo_message && { brevo_message: out.brevo_message }),
          ...(out.error && { error: out.error }),
        },
        out.dispatch
      )
    );
  } catch (e) {
    console.error("[admin/notifications] manual-send error:", e?.message || e);
    return res.status(500).json({ ok: false, error: "internal_error" });
  }
});

export default router;
