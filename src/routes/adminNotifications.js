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
  runTestWhatsAppDeliveryCheck,
  maskPhone,
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

const DISPATCH_DELIVERY_COLUMNS = [
  "provider_status",
  "delivery_status",
  "delivery_event",
  "delivery_events_raw",
  "delivery_checked_at",
  "delivery_confirmed_at",
  "last_provider_event_at",
];

router.get("/diagnostics/schema", async (_req, res) => {
  try {
    const r = await query(
      `SELECT column_name
         FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'notification_dispatches'
        ORDER BY column_name`
    );
    const existing_columns = (r.rows || []).map((row) => row.column_name);
    const missing_columns = DISPATCH_DELIVERY_COLUMNS.filter(
      (c) => !existing_columns.includes(c)
    );
    const schema_ok = missing_columns.length === 0;

    console.log("[admin/notifications] diagnostics schema", {
      schema_ok,
      missing_columns,
    });

    return res.json({
      ok: true,
      schema_ok,
      notification_dispatches: {
        required_columns: DISPATCH_DELIVERY_COLUMNS,
        existing_columns,
        missing_columns,
      },
    });
  } catch (e) {
    console.error("[admin/notifications] diagnostics schema error", {
      message: e?.message || null,
      stack: e?.stack || null,
      code: e?.code || null,
      detail: e?.detail || null,
    });
    return res.status(500).json({ ok: false, error: "internal_error" });
  }
});

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

    console.log("[admin/notifications] brevo events:start", {
      admin_user_id: req.user?.id || null,
      contactNumber: contactNumber ? maskPhone(contactNumber) : "TEST_DEFAULT",
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

    console.log("[admin/notifications] brevo events:done", {
      ok: result.ok,
      count: result.events?.length || 0,
      statusCode: result.statusCode || null,
      error: result.error || null,
      reason: result.reason || null,
    });

    if (!result.ok) {
      const status =
        result.error === "missing_brevo_api_key" ? 503 : 502;
      return res.status(status).json({
        ok: false,
        error: result.error,
        reason: result.reason || null,
        contactNumber: result.contactNumber || contactNumber,
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
      error: null,
      reason: null,
    });
  } catch (e) {
    console.error("[admin/notifications] brevo events error:", e?.message || e);
    return res.status(500).json({ ok: false, error: "internal_error" });
  }
});

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

router.post("/dispatches/:id/sync-delivery-status", async (req, res) => {
  try {
    const id = String(req.params.id || "").trim();
    if (!UUID_RE.test(id)) {
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
        reason: result.reason || null,
      });
      return res.status(502).json({
        ok: false,
        error: result.error,
        reason: result.reason || null,
        dispatch: result.dispatch || null,
        message: result.message || null,
      });
    }

    const dispatch = result.dispatch;
    console.log("[admin/notifications] sync delivery:dispatch", {
      dispatch_id: id,
      provider_message_id: dispatch?.provider_message_id,
      current_status: dispatch?.status,
      current_delivery_status: dispatch?.delivery_status || null,
      recipient_masked: maskPhone(dispatch?.recipient),
    });

    if (result.matched && result.matched_event) {
      const ev = result.matched_event;
      if (String(ev.event || "").toLowerCase() === "error" || ev.errorCode === "131049") {
        console.warn("[admin/notifications] sync delivery:provider-error", {
          dispatch_id: id,
          provider_message_id: dispatch?.provider_message_id,
          event: ev.event,
          errorCode: ev.errorCode || null,
          errorType: ev.errorType || null,
          reason: ev.reason || null,
        });
      }
    }

    console.log("[admin/notifications] sync delivery:done", {
      dispatch_id: id,
      matched: Boolean(result.matched),
      events_checked: result.events_checked,
      status_updated_to: result.status_updated_to || null,
      matched_event: result.matched_event?.event || null,
      matched_reason: result.matched_event?.reason || null,
      matched_error_code: result.matched_event?.errorCode || null,
    });

    return res.json({
      ok: true,
      matched: result.matched,
      dispatch: result.dispatch,
      matched_event: result.matched_event,
      events_checked: result.events_checked,
      events: result.events,
      status_updated_to: result.status_updated_to,
      delivery_status_updated_to: result.delivery_status_updated_to,
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
      provider_status: out?.dispatch?.provider_status || null,
      delivery_status: out?.dispatch?.delivery_status || null,
      result_ok: out?.result?.ok,
      statusCode: out?.result?.statusCode,
      messageId: out?.result?.messageId,
      recipient_forced: out?.result?.recipient_forced,
      reason: out?.result?.reason || null,
      error: out?.result?.error || null,
    });

    let delivery_check = null;
    if (
      out?.ok &&
      out?.result?.provider_status === "accepted" &&
      out?.dispatch?.id &&
      out?.result?.messageId
    ) {
      try {
        console.log("[admin/notifications] test-whatsapp:delivery-check:start", {
          dispatch_id: out.dispatch.id,
          messageId: out.result.messageId,
        });

        delivery_check = await runTestWhatsAppDeliveryCheck({
          dispatchId: out.dispatch.id,
          messageId: out.result.messageId,
          contactNumber: getTestRecipient(),
        });

        const refreshed = await query(
          `SELECT * FROM public.notification_dispatches WHERE id = $1 LIMIT 1`,
          [out.dispatch.id]
        );
        if (refreshed.rows[0]) {
          out.dispatch = refreshed.rows[0];
        }

        if (
          delivery_check?.matched_event &&
          (String(delivery_check.matched_event.event || "").toLowerCase() ===
            "error" ||
            delivery_check.matched_event.errorCode === "131049")
        ) {
          console.warn("[admin/notifications] test-whatsapp:delivery-check:provider-error", {
            dispatch_id: out.dispatch.id,
            messageId: out.result.messageId,
            event: delivery_check.matched_event.event,
            errorCode: delivery_check.matched_event.errorCode || null,
            errorType: delivery_check.matched_event.errorType || null,
            reason: delivery_check.matched_event.reason || null,
          });
        }

        if (delivery_check?.recent_errors?.length) {
          console.warn("[admin/notifications] test-whatsapp:delivery-check:recent-errors", {
            dispatch_id: out.dispatch.id,
            messageId: out.result.messageId,
            recent_errors_count: delivery_check.recent_errors.length,
            first_error_code: delivery_check.recent_errors[0]?.errorCode || null,
            first_error_reason: delivery_check.recent_errors[0]?.reason || null,
          });
        }

        console.log("[admin/notifications] test-whatsapp:delivery-check:done", {
          dispatch_id: out.dispatch.id,
          messageId: out.result.messageId,
          events_checked: delivery_check?.events_checked ?? 0,
          matched: Boolean(delivery_check?.matched),
          matched_event: delivery_check?.matched_event?.event || null,
          matched_reason: delivery_check?.matched_event?.reason || null,
          matched_error_code: delivery_check?.matched_event?.errorCode || null,
          error: delivery_check?.error || null,
          reason: delivery_check?.reason || null,
        });
      } catch (deliveryErr) {
        console.error("[admin/notifications] test-whatsapp:delivery-check:error", {
          dispatch_id: out.dispatch.id,
          messageId: out.result.messageId,
          message: deliveryErr?.message || null,
          stack: deliveryErr?.stack || null,
          code: deliveryErr?.code || null,
          detail: deliveryErr?.detail || null,
          hint: deliveryErr?.hint || null,
          table: deliveryErr?.table || null,
          column: deliveryErr?.column || null,
          constraint: deliveryErr?.constraint || null,
        });

        delivery_check = {
          checked: true,
          matched: false,
          events_checked: 0,
          error: "delivery_check_failed",
          reason: deliveryErr?.message || null,
          message:
            "Envio aceito pela Brevo, mas a consulta de eventos falhou. Verifique logs do Render.",
        };
      }
    }

    const warning = getTestModeWarning();
    return res.json(
      attachBrevoHint(
        {
          ok: out.ok,
          dispatch: out.dispatch,
          result: out.result,
          ...(delivery_check && { delivery_check }),
          ...(warning && { warning }),
          ...(out.delivery_note && { delivery_note: out.delivery_note }),
          ...(out.brevo_message && { brevo_message: out.brevo_message }),
        },
        out.dispatch
      )
    );
  } catch (e) {
    console.error("[admin/notifications] test-whatsapp error", {
      message: e?.message || null,
      stack: e?.stack || null,
      code: e?.code || null,
      detail: e?.detail || null,
      hint: e?.hint || null,
      table: e?.table || null,
      column: e?.column || null,
      constraint: e?.constraint || null,
    });
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
