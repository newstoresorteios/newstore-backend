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
import { syncBrevoWhatsAppTemplates } from "../services/notifications/brevoWhatsAppTemplates.js";
import {
  fetchBrevoWhatsAppEvents,
  syncDispatchDeliveryStatus,
} from "../services/notifications/brevoWhatsAppEvents.js";
import { getTestRecipient } from "../services/notifications/brevoWhatsApp.js";

const router = express.Router();

router.use(requireAuth, requireAdmin);

function toInt(v, def) {
  const n = Number(v);
  return Number.isFinite(n) && n >= 0 ? Math.trunc(n) : def;
}

function asRows(rows) {
  return Array.isArray(rows) ? rows : [];
}

function attachBrevoHint(payload, dispatch) {
  if (dispatch?.error_message === "brevo_ip_not_authorized") {
    return { ...payload, brevo_message: BREVO_IP_BLOCKED_MESSAGE };
  }
  return payload;
}

router.get("/health", async (_req, res) => {
  try {
    const health = getNotificationHealth();
    console.log("[admin/notifications] health", {
      testMode: health?.testMode,
      allowRealRecipients: health?.allowRealRecipients,
      brevoWhatsappEnabled: health?.brevoWhatsappEnabled,
      hasBrevoApiKey: health?.hasBrevoApiKey,
      senderNumberConfigured: health?.senderNumberConfigured,
      testRecipientConfigured: health?.testRecipientConfigured,
      genericTestTemplateEnvConfigured: health?.genericTestTemplateEnvConfigured,
      captiveTemplateEnvConfigured: health?.captiveTemplateEnvConfigured,
    });
    return res.json(health);
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

    console.log("[admin/notifications] list dispatches:start", {
      status: req.query.status || null,
      channel: req.query.channel || null,
      provider: req.query.provider || null,
      limit,
      offset,
    });

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

    const rows = asRows(r.rows);

    console.log("[admin/notifications] list dispatches:done", {
      count: rows.length,
    });

    if (rows.length === 0) {
      console.warn(
        "[admin/notifications] dispatches empty - check notification_dispatches table or filters"
      );
    }

    return res.json({
      ok: true,
      rows,
      dispatches: rows,
      items: rows,
      count: rows.length,
      limit,
      offset,
    });
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

    const rows = asRows(r.rows);

    console.log("[admin/notifications] list inbound:done", {
      count: rows.length,
    });

    return res.json({
      ok: true,
      rows,
      inbound: rows,
      messages: rows,
      items: rows,
      count: rows.length,
      limit,
      offset,
    });
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
        ORDER BY channel, provider, template_key`
    );

    const rows = asRows(r.rows);

    console.log("[admin/notifications] list templates:done", {
      count: rows.length,
    });

    if (rows.length === 0) {
      console.warn(
        "[admin/notifications] templates empty - check notification_templates table"
      );
    }

    return res.json({
      ok: true,
      rows,
      templates: rows,
      items: rows,
      count: rows.length,
    });
  } catch (e) {
    console.error("[admin/notifications] templates error:", e?.message || e);
    return res.status(500).json({ ok: false, error: "internal_error" });
  }
});

router.post("/templates/sync-brevo-whatsapp", async (req, res) => {
  try {
    console.log("[admin/notifications] sync brevo templates:start", {
      admin_user_id: req.user?.id || null,
    });

    const result = await syncBrevoWhatsAppTemplates();

    if (!result.ok) {
      console.error("[admin/notifications] sync brevo templates:error", {
        error: result.error || "sync_failed",
      });
      const status =
        result.error === "missing_brevo_api_key" ? 503 : 502;
      return res.status(status).json({
        ok: false,
        error: result.error,
        fetched_count: 0,
        synced_count: 0,
        templates: [],
      });
    }

    console.log("[admin/notifications] sync brevo templates:done", {
      fetched_count: result.fetched_count,
      synced_count: result.synced_count,
    });

    return res.json({
      ok: true,
      fetched_count: result.fetched_count,
      synced_count: result.synced_count,
      templates: result.templates,
    });
  } catch (e) {
    console.error("[admin/notifications] sync brevo templates:error", {
      error: e?.message,
    });
    return res.status(500).json({ ok: false, error: "internal_error" });
  }
});

router.get("/brevo-whatsapp-events", async (req, res) => {
  try {
    const contactNumber =
      req.query?.contactNumber ||
      req.query?.contact_number ||
      getTestRecipient() ||
      null;
    const days = toInt(req.query?.days, 1) || 1;
    const limit = Math.min(toInt(req.query?.limit, 50), 50);
    const offset = toInt(req.query?.offset, 0);
    const event = req.query?.event || null;

    console.log("[admin/notifications] brevo whatsapp events:start", {
      admin_user_id: req.user?.id || null,
      contactNumber,
      days,
      limit,
      event: event || null,
    });

    const result = await fetchBrevoWhatsAppEvents({
      contactNumber,
      days,
      limit,
      offset,
      event,
    });

    console.log("[admin/notifications] brevo whatsapp events:done", {
      ok: result.ok,
      count: result.events?.length || 0,
    });

    if (!result.ok) {
      const status =
        result.error === "missing_brevo_api_key" ? 503 : 502;
      return res.status(status).json({
        ok: false,
        error: result.error,
        contactNumber,
        events: [],
        raw: result.raw,
        count: 0,
      });
    }

    return res.json({
      ok: true,
      contactNumber: result.contactNumber || contactNumber || getTestRecipient(),
      events: result.events,
      raw: result.raw,
      count: result.events.length,
    });
  } catch (e) {
    console.error("[admin/notifications] brevo whatsapp events error:", e?.message || e);
    return res.status(500).json({ ok: false, error: "internal_error" });
  }
});

router.post("/dispatches/:id/sync-delivery-status", async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isFinite(id) || id <= 0) {
      return res.status(400).json({ ok: false, error: "invalid_dispatch_id" });
    }

    console.log("[admin/notifications] sync delivery:start", {
      dispatch_id: id,
    });

    const days = toInt(req.body?.days ?? req.query?.days, 7) || 7;
    const result = await syncDispatchDeliveryStatus(id, { days });

    if (!result.ok && result.error === "not_found") {
      return res.status(404).json({ ok: false, error: "not_found" });
    }

    if (!result.ok) {
      console.error("[admin/notifications] sync delivery:error", {
        dispatch_id: id,
        error: result.error,
      });
      return res.status(502).json({
        ok: false,
        error: result.error,
        dispatch: result.dispatch || null,
        message: result.message || null,
      });
    }

    console.log("[admin/notifications] sync delivery:done", {
      dispatch_id: id,
      provider_message_id: result.dispatch?.provider_message_id,
      matched: Boolean(result.matched_event),
      status_updated_to: result.status_updated_to ?? result.dispatch?.status,
    });

    return res.json({
      ok: true,
      dispatch: result.dispatch,
      matched_event: result.matched_event,
      events_checked: result.events_checked,
      status_updated_to: result.status_updated_to,
      message: result.message,
    });
  } catch (e) {
    console.error("[admin/notifications] sync delivery:error", {
      error: e?.message,
    });
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

    console.log("[admin/notifications] test-whatsapp:start", {
      admin_user_id: req.user?.id || null,
      template_key: req.body?.template_key || null,
      has_template_id: Boolean(req.body?.template_id),
      has_phone: Boolean(req.body?.phone),
      has_user_id: Boolean(req.body?.user_id),
    });

    const out = await sendTestWhatsApp({
      userId: userId != null ? Number(userId) : null,
      phone,
      templateKey: templateKey || "GENERIC_TEST",
      templateId,
      params: params || {},
      adminUserId: req.user?.id ?? null,
    });

    console.log("[admin/notifications] test-whatsapp:result", {
      ok: out?.ok,
      dispatch_id: out?.dispatch?.id,
      dispatch_status: out?.dispatch?.status,
      result_ok: out?.result?.ok,
      statusCode: out?.result?.statusCode,
      messageId: out?.result?.messageId,
      recipient_forced: out?.result?.recipient_forced,
      reason: out?.result?.reason || null,
      error: out?.result?.error || null,
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

    const result = await estimateAudience({
      filter,
      userId: userId != null ? Number(userId) : null,
      phone,
    });

    console.log("[admin/notifications] audience estimate", {
      filter: req.body?.filter || null,
      estimated_count: result?.estimated_count,
      test_mode: result?.test_mode,
      allow_real_recipients: result?.allow_real_recipients,
    });

    return res.json(result);
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

    console.log("[admin/notifications] manual-send:result", {
      ok: out?.ok,
      dispatch_id: out?.dispatch?.id,
      dispatch_status: out?.dispatch?.status,
      warning: out?.warning || null,
      result_ok: out?.result?.ok,
      statusCode: out?.result?.statusCode,
      messageId: out?.result?.messageId,
      recipient_forced: out?.result?.recipient_forced,
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
