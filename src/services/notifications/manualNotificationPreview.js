import { query } from "../../db.js";
import { normalizePhoneBR } from "./brevoWhatsApp.js";
import { getWhatsappConsentStatusForUser } from "./communicationConsent.js";
import { getBuiltinEmailTemplates } from "./manualNotificationCatalog.js";

export const MANUAL_MAX_UNIQUE_USERS = 50;
const CHANNELS = new Set(["whatsapp", "push", "email"]);
const AUDIENCES = new Set(["selected", "all_active_push"]);

function runQuery(pgClient, text, params) {
  if (pgClient) return pgClient.query(text, params);
  return query(text, params);
}

function uniquePositiveIds(values) {
  if (!Array.isArray(values)) return [];
  return Array.from(new Set(
    values
      .map((value) => Number(value))
      .filter((value) => Number.isInteger(value) && value > 0)
  ));
}

function isValidEmail(value) {
  const email = String(value || "").trim();
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

export function renderTemplate(template, params = {}) {
  return String(template || "").replace(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g, (_match, key) => {
    const value = params?.[key];
    return value == null ? "" : String(value);
  });
}

function normalizeUrl(url) {
  const clean = String(url || "/").trim() || "/";
  if (!clean.startsWith("/") || clean.startsWith("//")) {
    const error = new Error("manual_push_url_invalid");
    error.code = "manual_push_url_invalid";
    throw error;
  }
  return clean.slice(0, 500);
}

function normalizeManualInput(payload = {}) {
  const channel = String(payload.channel || "").trim().toLowerCase();
  const audience = String(payload.audience || "selected").trim().toLowerCase();
  if (!CHANNELS.has(channel)) {
    const error = new Error("unsupported_manual_channel");
    error.code = "unsupported_manual_channel";
    throw error;
  }
  if (!AUDIENCES.has(audience)) {
    const error = new Error("manual_recipients_required");
    error.code = "manual_recipients_required";
    throw error;
  }
  if (channel === "email" && audience !== "selected") {
    const error = new Error("manual_recipients_required");
    error.code = "manual_recipients_required";
    throw error;
  }
  if (channel === "whatsapp" && audience !== "selected") {
    const error = new Error("manual_recipients_required");
    error.code = "manual_recipients_required";
    throw error;
  }

  const userIds = uniquePositiveIds(payload.user_ids || []);
  if (audience === "selected" && !userIds.length) {
    const error = new Error("manual_recipients_required");
    error.code = "manual_recipients_required";
    throw error;
  }
  if (userIds.length > MANUAL_MAX_UNIQUE_USERS) {
    const error = new Error("manual_too_many_recipients");
    error.code = "manual_too_many_recipients";
    error.max = MANUAL_MAX_UNIQUE_USERS;
    throw error;
  }

  return {
    channel,
    audience,
    userIds,
    templateKey: String(payload.template_key || "").trim(),
    templateId: payload.template_id == null ? null : String(payload.template_id).trim(),
    title: payload.title == null ? null : String(payload.title).trim(),
    message: payload.message == null ? null : String(payload.message).trim(),
    subject: payload.subject == null ? null : String(payload.subject).trim(),
    html: payload.html == null ? null : String(payload.html),
    text: payload.text == null ? null : String(payload.text),
    url: normalizeUrl(payload.url || "/"),
    params: payload.params && typeof payload.params === "object" && !Array.isArray(payload.params)
      ? payload.params
      : {},
  };
}

async function loadSelectedUsers(pgClient, userIds) {
  if (!userIds.length) return [];
  const result = await runQuery(
    pgClient,
    `SELECT id, name, email, phone
       FROM public.users
      WHERE id = ANY($1::int[])
      ORDER BY id`,
    [userIds]
  );
  return result.rows || [];
}

async function loadPushSubscriptions(pgClient, { audience, userIds }) {
  const params = [];
  let where = `
    WHERE is_active = true
      AND operational_opt_in = true
      AND user_id IS NOT NULL
  `;
  if (audience === "selected") {
    params.push(userIds);
    where += ` AND user_id = ANY($1::int[])`;
  }

  const result = await runQuery(
    pgClient,
    `SELECT id, user_id, endpoint, p256dh, auth
       FROM public.push_subscriptions
       ${where}
      ORDER BY user_id, updated_at DESC NULLS LAST, created_at DESC NULLS LAST`,
    params
  );
  return result.rows || [];
}

async function loadPushInactiveCount(pgClient, userIds) {
  if (!userIds.length) return 0;
  const result = await runQuery(
    pgClient,
    `SELECT COUNT(*)::int AS count
       FROM public.push_subscriptions
      WHERE user_id = ANY($1::int[])
        AND (is_active IS DISTINCT FROM true OR operational_opt_in IS DISTINCT FROM true)`,
    [userIds]
  );
  return Number(result.rows?.[0]?.count || 0);
}

async function resolveTemplate(pgClient, normalized) {
  if (!normalized.templateKey) return null;
  if (normalized.channel === "push") {
    const result = await runQuery(
      pgClient,
      `SELECT event_key AS template_key, name, description, title_template,
              body_template, url_template, category, is_active
         FROM public.notification_push_rules
        WHERE event_key = $1
        LIMIT 1`,
      [normalized.templateKey]
    );
    return result.rows?.[0] || null;
  }
  if (normalized.channel === "email") {
    const result = await runQuery(
      pgClient,
      `SELECT *
         FROM public.notification_templates
        WHERE channel = 'email'
          AND template_key = $1
        LIMIT 1`,
      [normalized.templateKey]
    ).catch((error) => {
      if (error?.code === "42P01" || error?.code === "42703") return { rows: [] };
      throw error;
    });
    return result.rows?.[0] || getBuiltinEmailTemplates().find((item) => item.template_key === normalized.templateKey) || null;
  }
  const result = await runQuery(
    pgClient,
    `SELECT *
       FROM public.notification_templates
      WHERE channel = 'whatsapp'
        AND provider = 'brevo'
        AND template_key = $1
      LIMIT 1`,
    [normalized.templateKey]
  );
  return result.rows?.[0] || null;
}

function buildPreviewText(normalized, template) {
  const params = normalized.params || {};
  if (normalized.channel === "push") {
    const title = normalized.title || renderTemplate(template?.title_template || template?.name || "", params);
    const message = normalized.message || renderTemplate(template?.body_template || template?.description || "", params);
    return {
      title_preview: title,
      message_preview: message,
      subject_preview: null,
      html_preview: null,
      text_preview: null,
    };
  }
  if (normalized.channel === "email") {
    const subject = normalized.subject || renderTemplate(template?.subject_template || template?.name || "Mensagem da New Store", params);
    const text = normalized.text || renderTemplate(template?.text_template || template?.default_message || normalized.message || "", params);
    const html = normalized.html || renderTemplate(template?.html_template || template?.default_message || normalized.message || "", params);
    return {
      title_preview: null,
      message_preview: normalized.message || null,
      subject_preview: subject,
      html_preview: html,
      text_preview: text,
    };
  }
  return {
    title_preview: null,
    message_preview: normalized.message || template?.default_message || template?.description || null,
    subject_preview: null,
    html_preview: null,
    text_preview: null,
  };
}

export async function buildManualNotificationPreview({ pgClient, payload = {} } = {}) {
  const normalized = normalizeManualInput(payload);
  const warnings = [];
  let users = [];
  let subscriptions = [];
  let inactiveSubscriptions = 0;

  if (normalized.audience === "selected") {
    users = await loadSelectedUsers(pgClient, normalized.userIds);
  }

  if (normalized.channel === "push") {
    subscriptions = await loadPushSubscriptions(pgClient, {
      audience: normalized.audience,
      userIds: normalized.userIds,
    });
    const uniqueUsers = Array.from(new Set(subscriptions.map((row) => Number(row.user_id)).filter(Boolean)));
    if (normalized.audience === "all_active_push") {
      users = uniqueUsers.map((id) => ({ id }));
    }
    inactiveSubscriptions = await loadPushInactiveCount(pgClient, normalized.userIds);
    if (uniqueUsers.length > MANUAL_MAX_UNIQUE_USERS) {
      const error = new Error("manual_too_many_recipients");
      error.code = "manual_too_many_recipients";
      error.max = MANUAL_MAX_UNIQUE_USERS;
      throw error;
    }
  }

  let validPhones = 0;
  let blockedByConsent = 0;
  let missingContact = 0;
  if (normalized.channel === "whatsapp") {
    for (const user of users) {
      const phone = normalizePhoneBR(user.phone);
      if (!phone) {
        missingContact += 1;
        continue;
      }
      const consent = await getWhatsappConsentStatusForUser({ pgClient, userId: user.id });
      if (consent.whatsapp_can_send) validPhones += 1;
      else blockedByConsent += 1;
    }
  }

  let validEmails = 0;
  if (normalized.channel === "email") {
    const seen = new Set();
    for (const user of users) {
      const email = String(user.email || "").trim().toLowerCase();
      if (!email || !isValidEmail(email)) {
        missingContact += 1;
        continue;
      }
      if (seen.has(email)) continue;
      seen.add(email);
      validEmails += 1;
    }
  }

  const template = await resolveTemplate(pgClient, normalized);
  if (normalized.templateKey && !template) warnings.push("manual_template_not_found");
  const text = buildPreviewText(normalized, template);
  const eligibleUsers = normalized.channel === "push"
    ? new Set(subscriptions.map((row) => Number(row.user_id))).size
    : normalized.channel === "email"
      ? validEmails
      : validPhones;
  const requiresBulkConfirmation =
    normalized.audience === "all_active_push" || eligibleUsers > 1;

  return {
    ok: true,
    can_send: eligibleUsers > 0 && (normalized.channel !== "push" || Boolean(text.title_preview && text.message_preview)),
    channel: normalized.channel,
    provider: normalized.channel === "push" ? "web_push" : normalized.channel === "email" ? "brevo_smtp" : "brevo",
    template,
    ...text,
    requested_users: normalized.audience === "selected" ? normalized.userIds.length : null,
    eligible_users: eligibleUsers,
    eligible_devices: subscriptions.length,
    valid_emails: validEmails,
    valid_phones: validPhones,
    blocked_by_consent: blockedByConsent,
    missing_contact: missingContact,
    inactive_subscriptions: inactiveSubscriptions,
    warnings,
    requires_bulk_confirmation: requiresBulkConfirmation,
    normalized,
  };
}

export function sanitizePreviewForResponse(preview) {
  const { normalized, ...rest } = preview || {};
  return rest;
}
