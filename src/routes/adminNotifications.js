// src/routes/adminNotifications.js
import express from "express";
import { query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import {
  getNotificationHealth,
  sendTestWhatsApp,
  estimateAudience,
  manualSendNotification,
  manualSendSelected,
  searchNotificationRecipients,
  updateNotificationTemplate,
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
import {
  getTestRecipient,
  isAdminTestCustomRecipientsEnabled,
} from "../services/notifications/brevoWhatsApp.js";
import { sendTestPushToConfiguredSubscription } from "../services/notifications/pushNotifications.js";
import { assertPushTestAccountAllowed } from "../services/notifications/pushAccessGuard.js";
import { getManualNotificationCatalog } from "../services/notifications/manualNotificationCatalog.js";
import {
  buildManualNotificationPreview,
  sanitizePreviewForResponse,
} from "../services/notifications/manualNotificationPreview.js";
import { sendManualPushNotification } from "../services/notifications/manualPushNotifications.js";
import { sendManualEmailNotification } from "../services/notifications/manualEmailNotifications.js";

const router = express.Router();

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

router.use(requireAuth, requireAdmin);

function manualErrorStatus(code) {
  if (code === "manual_template_not_found") return 404;
  if (code === "manual_template_not_allowed") return 400;
  if (
    code === "manual_email_smtp_not_configured" ||
    code === "manual_push_no_eligible_recipients" ||
    code === "manual_email_no_valid_recipients" ||
    code === "email_consent_not_available"
  ) return 400;
  if (
    code === "unsupported_manual_channel" ||
    code === "manual_recipients_required" ||
    code === "manual_too_many_recipients" ||
    code === "manual_audience_too_large" ||
    code === "manual_bulk_confirmation_required" ||
    String(code || "").startsWith("manual_push_")
  ) return 400;
  return 500;
}

router.get("/catalog", async (_req, res) => {
  try {
    const catalog = await getManualNotificationCatalog();
    return res.json(catalog);
  } catch (error) {
    console.error("[admin/notifications] catalog error", {
      message: error?.message || null,
      code: error?.code || null,
    });
    return res.status(500).json({ ok: false, error: "manual_preview_failed" });
  }
});

router.post("/manual/preview", async (req, res) => {
  try {
    const preview = await buildManualNotificationPreview({ payload: req.body || {} });
    return res.json(sanitizePreviewForResponse(preview));
  } catch (error) {
    const code = error?.code || "manual_preview_failed";
    console.warn("[admin/notifications] manual preview failed", {
      admin_user_id: req.user?.id || null,
      code,
    });
    return res.status(manualErrorStatus(code)).json({
      ok: false,
      error: code,
      ...(error?.max && { max: error.max }),
    });
  }
});

router.post("/manual/send", async (req, res) => {
  try {
    const channel = String(req.body?.channel || "").trim().toLowerCase();
    if (channel === "push") {
      const result = await sendManualPushNotification({
        payload: req.body || {},
        adminUserId: req.user?.id ?? null,
      });
      const status = result.error ? manualErrorStatus(result.error) : 200;
      return res.status(status).json(result);
    }

    if (channel === "email") {
      const result = await sendManualEmailNotification({
        payload: req.body || {},
        adminUserId: req.user?.id ?? null,
      });
      const status = result.error ? manualErrorStatus(result.error) : 200;
      return res.status(status).json(result);
    }

    if (channel === "whatsapp") {
      const preview = await buildManualNotificationPreview({ payload: req.body || {} });
      const cleanPreview = sanitizePreviewForResponse(preview);
      if (preview.requires_bulk_confirmation && req.body?.confirm_bulk_send !== true) {
        return res.status(400).json({
          ok: false,
          error: "manual_bulk_confirmation_required",
          eligible_users: preview.eligible_users,
          eligible_devices: preview.eligible_devices,
          valid_phones: preview.valid_phones,
          valid_emails: preview.valid_emails,
        });
      }
      if (!preview.valid_phones) {
        return res.status(400).json({
          ok: false,
          error: "manual_recipients_required",
          ...cleanPreview,
        });
      }

      const out = await manualSendSelected({
        channel: "whatsapp",
        provider: "brevo",
        templateKey: req.body?.template_key || "GENERIC_TEST",
        templateId: req.body?.template_id,
        message: req.body?.message,
        params: req.body?.params || {},
        recipients: (
          preview.normalized.audience === "all_consented"
            ? preview.normalized.eligibleUserIds
            : preview.normalized.userIds
        ).map((userId) => ({ user_id: userId })),
        useCustomRecipient: false,
        dryRun: false,
        adminUserId: req.user?.id ?? null,
        audience: preview.normalized.audience,
        consentCategory:
          preview.normalized.audience === "all_consented" ? "operational" : "manual",
        audienceStats: {
          requested_users: preview.requested_users,
          eligible_users: preview.eligible_users,
          blocked_by_consent: preview.blocked_by_consent,
          missing_contact: preview.missing_contact,
          estimated_batches: preview.estimated_batches,
        },
      });

      if (out.error) {
        const code =
          out.error === "too_many_recipients" ? "manual_too_many_recipients" :
          out.error === "recipients_required" ? "manual_recipients_required" :
          out.error;
        return res.status(manualErrorStatus(code)).json({ ok: false, error: code, ...out });
      }

      return res.json({
        ok: out.ok,
        channel: "whatsapp",
        provider: "brevo",
        campaign_id: out.campaign?.id || null,
        campaign: out.campaign || null,
        dispatches: out.dispatches || [],
        summary: out.summary || null,
        warning: out.warning || null,
        requested_users: preview.requested_users,
        eligible_users: preview.eligible_users,
        valid_phones: preview.valid_phones,
        eligible_devices: 0,
        batches_processed: out.batches_processed || 0,
        sent: out.sent || 0,
        accepted: out.accepted || 0,
        failed: out.failed || 0,
        skipped: out.skipped || 0,
        blocked_by_consent: preview.blocked_by_consent,
        missing_contact: preview.missing_contact,
      });
    }

    return res.status(400).json({ ok: false, error: "unsupported_manual_channel" });
  } catch (error) {
    const code = error?.code || "manual_send_failed";
    console.error("[admin/notifications] manual send failed", {
      admin_user_id: req.user?.id || null,
      code,
      message: error?.message || null,
    });
    return res.status(manualErrorStatus(code)).json({
      ok: false,
      error: code,
      ...(error?.max && { max: error.max }),
    });
  }
});

router.post("/push/test-single-device", async (req, res) => {
  try {
    assertPushTestAccountAllowed({ user: req.user });
    const allowedFields = new Set(["title", "body", "url"]);
    if (Object.keys(req.body || {}).some((key) => !allowedFields.has(key))) {
      return res.status(400).json({ ok: false, error: "push_test_payload_not_allowed" });
    }
    const result = await sendTestPushToConfiguredSubscription({
      title: req.body?.title,
      body: req.body?.body,
      url: req.body?.url || "/me",
      payload: {},
      source: "admin_single_device_test",
    });
    return res.json({ ok: true, ...result });
  } catch (error) {
    const code = error?.code || "push_internal_error";
    const status = code === "push_hidden_for_user"
      ? 404
      : code === "push_test_subscription_not_found_or_inactive"
      ? 404
      : code.includes("required") || code.includes("invalid") || code.includes("too_long")
        ? 400
        : code.startsWith("push_")
          ? 403
          : 500;
    return res.status(status).json({
      ok: false,
      ...(code === "push_hidden_for_user" && { visible: false }),
      error: code,
    });
  }
});

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
    const health = await getNotificationHealth();
    console.log("[admin/notifications] health", {
      testMode: health?.testMode,
      allowRealRecipients: health?.allowRealRecipients,
      brevoWhatsappEnabled: health?.brevoWhatsappEnabled,
      hasBrevoApiKey: health?.hasBrevoApiKey,
      senderNumberConfigured: health?.senderNumberConfigured,
      testRecipientConfigured: health?.testRecipientConfigured,
      genericTestTemplateEnvConfigured: health?.genericTestTemplateEnvConfigured,
      captiveTemplateEnvConfigured: health?.captiveTemplateEnvConfigured,
      captivePreauthTemplateMode: health?.captive_preauth_template_mode || null,
      captiveConfirmationPublicUrlConfigured:
        health?.captive_confirmation_public_url_configured === true,
      adminTestCustomRecipientsEnabled: health?.adminTestCustomRecipientsEnabled,
      adminTestAllowedRecipientsConfigured:
        health?.adminTestAllowedRecipientsConfigured,
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

router.get("/recipients/search", async (req, res) => {
  try {
    const q = String(req.query?.q || "").trim();
    const limit = Math.min(toInt(req.query?.limit, 20), 50);

    const rows = await searchNotificationRecipients({ q, limit });

    console.log("[admin/notifications] recipients search", {
      admin_user_id: req.user?.id || null,
      has_query: Boolean(q),
      count: rows.length,
    });

    return res.json({
      ok: true,
      rows,
      recipients: rows,
      items: rows,
      count: rows.length,
    });
  } catch (e) {
    console.error("[admin/notifications] recipients search error:", e?.message || e);
    return res.status(500).json({ ok: false, error: "internal_error" });
  }
});

router.patch("/templates/:id", async (req, res) => {
  try {
    const id = String(req.params.id || "").trim();
    if (!UUID_RE.test(id)) {
      return res.status(400).json({ ok: false, error: "invalid_template_id" });
    }

    const body = req.body || {};
    const templateKey = body.template_key;
    if (templateKey !== undefined && !String(templateKey || "").trim()) {
      return res.status(400).json({ ok: false, error: "template_key_required" });
    }

    if (
      body.default_params !== undefined &&
      body.default_params !== null &&
      (typeof body.default_params !== "object" || Array.isArray(body.default_params))
    ) {
      return res.status(400).json({ ok: false, error: "invalid_default_params" });
    }

    if (body.is_active !== undefined && typeof body.is_active !== "boolean") {
      return res.status(400).json({ ok: false, error: "invalid_is_active" });
    }

    const allowed = {};
    for (const key of [
      "template_key",
      "provider_template_id",
      "name",
      "description",
      "body_preview",
      "default_message",
      "default_params",
      "template_language",
      "template_category",
      "is_active",
    ]) {
      if (body[key] !== undefined) allowed[key] = body[key];
    }

    const out = await updateNotificationTemplate({
      templateId: id,
      patch: allowed,
    });

    if (!out.ok) {
      const status = out.error === "not_found" ? 404 : 400;
      return res.status(status).json({ ok: false, error: out.error });
    }

    console.log("[admin/notifications] template updated", {
      admin_user_id: req.user?.id || null,
      template_id: id,
      template_key: out.template?.template_key,
    });

    return res.json({ ok: true, template: out.template });
  } catch (e) {
    console.error("[admin/notifications] template update error:", e?.message || e);
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

    const catalog = await getManualNotificationCatalog();

    return res.json({
      ok: true,
      fetched_count: result.fetched_count,
      synced_count: result.synced_count,
      templates: result.templates,
      catalog,
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
      const status = [
        "invalid_dispatch_provider",
        "missing_provider_message_id",
      ].includes(result.error)
        ? 400
        : 502;
      return res.status(status).json({
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
      dispatch_id: id,
      provider_message_id:
        result.provider_message_id || result.dispatch?.provider_message_id || null,
      delivery_status:
        result.delivery_status || result.dispatch?.delivery_status || "unknown",
      events_found: result.events_found ?? (result.matched ? 1 : 0),
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

    const useCustomRecipient = req.body?.use_custom_recipient === true;

    console.log("[admin/notifications] test-whatsapp:start", {
      admin_user_id: req.user?.id || null,
      template_key: req.body?.template_key || null,
      has_template_id: Boolean(req.body?.template_id),
      has_phone: Boolean(req.body?.phone),
      has_user_id: Boolean(req.body?.user_id),
      use_custom_recipient: useCustomRecipient,
      admin_test_custom_enabled: isAdminTestCustomRecipientsEnabled(),
    });

    const out = await sendTestWhatsApp({
      userId: userId != null ? Number(userId) : null,
      phone,
      templateKey: templateKey || "GENERIC_TEST",
      templateId,
      params: params || {},
      adminUserId: req.user?.id ?? null,
      useCustomRecipient,
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
      recipient_mode: out?.result?.recipient_mode || null,
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
          contactNumber: out.result?.recipient || getTestRecipient(),
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

    const warning =
      out?.result?.recipient_mode === "admin_test_custom" ? null : getTestModeWarning();

    return res.json(
      attachBrevoHint(
        {
          ok: out.ok,
          dispatch: out.dispatch,
          result: out.result,
          custom_recipient_requested: useCustomRecipient,
          custom_recipient_enabled: isAdminTestCustomRecipientsEnabled(),
          recipient_mode: out?.result?.recipient_mode || null,
          ...(delivery_check && { delivery_check }),
          ...(warning && { warning }),
          ...(out.delivery_note && { delivery_note: out.delivery_note }),
          ...(out.brevo_message && { brevo_message: out.brevo_message }),
        },
        out.dispatch
      )
    );
  } catch (e) {
    if (e?.code === "manual_template_not_found" || e?.code === "manual_template_not_allowed") {
      return res.status(manualErrorStatus(e.code)).json({ ok: false, error: e.code });
    }
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

router.post("/manual-send-selected", async (req, res) => {
  try {
    const {
      channel,
      provider,
      template_key: templateKey,
      template_id: templateId,
      message,
      params,
      recipients,
      use_custom_recipient: useCustomRecipient,
      dry_run: dryRun,
    } = req.body || {};

    console.log("[admin/notifications] manual-send-selected:start", {
      admin_user_id: req.user?.id || null,
      template_key: templateKey || null,
      recipient_count: Array.isArray(recipients) ? recipients.length : 0,
      use_custom_recipient: useCustomRecipient === true,
      dry_run: dryRun === true,
    });

    const out = await manualSendSelected({
      channel: channel || "whatsapp",
      provider: provider || "brevo",
      templateKey: templateKey || "GENERIC_TEST",
      templateId,
      message,
      params: params || {},
      recipients: recipients || [],
      useCustomRecipient: useCustomRecipient === true,
      dryRun: dryRun === true,
      adminUserId: req.user?.id ?? null,
    });

    if (out.error === "too_many_recipients") {
      return res.status(400).json({
        ok: false,
        error: out.error,
        max: out.max,
      });
    }
    if (
      out.error === "recipients_required" ||
      out.error === "unsupported_channel_or_provider"
    ) {
      return res.status(400).json({ ok: false, error: out.error, message: out.message });
    }
    if (out.error === "missing_test_recipient") {
      return res.status(503).json({ ok: false, error: out.error, message: out.message });
    }

    console.log("[admin/notifications] manual-send-selected:result", {
      ok: out?.ok,
      campaign_id: out?.campaign?.id || null,
      summary: out?.summary || null,
      warning: out?.warning || null,
    });

    return res.json({
      ok: out.ok,
      campaign: out.campaign || null,
      dispatches: out.dispatches || [],
      summary: out.summary,
      warning: out.warning || getTestModeWarning(),
      dry_run: out.dry_run === true,
      ...(out.error && { error: out.error }),
      ...(out.message && { message: out.message }),
    });
  } catch (e) {
    if (e?.code === "manual_template_not_found" || e?.code === "manual_template_not_allowed") {
      return res.status(manualErrorStatus(e.code)).json({ ok: false, error: e.code });
    }
    console.error("[admin/notifications] manual-send-selected error:", e?.message || e);
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
    if (e?.code === "manual_template_not_found" || e?.code === "manual_template_not_allowed") {
      return res.status(manualErrorStatus(e.code)).json({ ok: false, error: e.code });
    }
    console.error("[admin/notifications] manual-send error:", e?.message || e);
    return res.status(500).json({ ok: false, error: "internal_error" });
  }
});

export default router;
