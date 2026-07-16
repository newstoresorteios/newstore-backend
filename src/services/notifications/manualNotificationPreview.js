import { query } from "../../db.js";
import { normalizePhoneBR } from "./brevoWhatsApp.js";
import { getWhatsappConsentStatusForUser } from "./communicationConsent.js";
import { getBuiltinEmailTemplates } from "./manualNotificationCatalog.js";
import { resolveManualBrevoWhatsAppTemplate } from "./manualWhatsAppTemplates.js";
import {
  MANUAL_MAX_CAMPAIGN_USERS,
  assertManualCampaignAudienceSize,
  estimatedManualBatches,
} from "./manualAudience.js";

export const MANUAL_MAX_UNIQUE_USERS = 50;
const CHANNELS = new Set(["whatsapp", "push", "email"]);
const AUDIENCES = new Set(["selected", "all_active_push", "all_consented", "all_with_email"]);
const FIXED_REMAINING_NUMBERS = new Map([
  ["EMAIL_DRAW_REMAINING_75", 75],
  ["EMAIL_DRAW_REMAINING_50", 50],
  ["EMAIL_DRAW_REMAINING_30", 30],
  ["EMAIL_DRAW_REMAINING_15", 15],
]);
const REMAINING_EMAIL_PARAM_KEYS = new Set([
  "name",
  "draw_name",
  "draw_url",
  "remaining_numbers",
]);

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

function normalizeEmailDrawUrl(url) {
  const clean = String(url || "/").trim() || "/";
  const relative = clean.startsWith("/") && !clean.startsWith("//");
  const secureAbsolute = clean.startsWith("https://");
  if (!relative && !secureAbsolute) {
    const error = new Error("manual_email_url_invalid");
    error.code = "manual_email_url_invalid";
    throw error;
  }
  return clean.slice(0, 1000);
}

function normalizeParams(channel, templateKey, value) {
  const raw = value && typeof value === "object" && !Array.isArray(value) ? value : {};
  const fixedRemaining = FIXED_REMAINING_NUMBERS.get(templateKey);
  if (channel !== "email" || fixedRemaining == null) return raw;

  const params = {};
  for (const key of REMAINING_EMAIL_PARAM_KEYS) {
    if (raw[key] != null) params[key] = raw[key];
  }
  params.remaining_numbers = fixedRemaining;
  params.draw_url = normalizeEmailDrawUrl(params.draw_url || "/");
  return params;
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
  if (channel === "email" && !["selected", "all_with_email"].includes(audience)) {
    const error = new Error("manual_recipients_required");
    error.code = "manual_recipients_required";
    throw error;
  }
  if (channel === "whatsapp" && !["selected", "all_consented"].includes(audience)) {
    const error = new Error("manual_recipients_required");
    error.code = "manual_recipients_required";
    throw error;
  }

  let userIds = uniquePositiveIds(payload.user_ids || []);
  if (channel === "email" && audience === "all_with_email") userIds = [];
  if (audience === "selected" && !userIds.length) {
    const error = new Error("manual_recipients_required");
    error.code = "manual_recipients_required";
    throw error;
  }
  if (audience === "selected" && userIds.length > MANUAL_MAX_UNIQUE_USERS) {
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
    params: normalizeParams(channel, String(payload.template_key || "").trim(), payload.params),
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

async function loadAllUsers(pgClient) {
  const result = await runQuery(
    pgClient,
    `SELECT id, name, email, phone
       FROM public.users
      ORDER BY id`,
    []
  );
  return result.rows || [];
}

export function resolveManualEmailRecipients(users) {
  const seen = new Set();
  const recipients = [];
  let missingContact = 0;
  let invalidEmails = 0;
  let duplicateEmailsRemoved = 0;

  for (const user of Array.isArray(users) ? users : []) {
    const email = String(user?.email || "").trim().toLowerCase();
    if (!email) {
      missingContact += 1;
      continue;
    }
    if (!isValidEmail(email)) {
      invalidEmails += 1;
      continue;
    }
    if (seen.has(email)) {
      duplicateEmailsRemoved += 1;
      continue;
    }
    seen.add(email);
    recipients.push({ ...user, email });
  }

  return {
    recipients,
    missingContact,
    invalidEmails,
    duplicateEmailsRemoved,
  };
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

async function loadPushExclusionStats(pgClient, { audience, userIds }) {
  if (audience === "selected" && !userIds.length) {
    return { inactiveSubscriptions: 0, blockedByConsent: 0 };
  }
  const params = [];
  const selectedClause = audience === "selected"
    ? "AND user_id = ANY($1::int[])"
    : "AND user_id IS NOT NULL";
  if (audience === "selected") params.push(userIds);
  const result = await runQuery(
    pgClient,
    `SELECT COUNT(*) FILTER (
              WHERE is_active IS DISTINCT FROM true
            )::int AS inactive_count,
            COUNT(DISTINCT user_id) FILTER (
              WHERE is_active = true
                AND operational_opt_in IS DISTINCT FROM true
            )::int AS blocked_by_consent
       FROM public.push_subscriptions
      WHERE 1 = 1
        ${selectedClause}`,
    params
  );
  const row = result.rows?.[0] || {};
  return {
    inactiveSubscriptions: Number(row.inactive_count ?? row.count ?? 0),
    blockedByConsent: Number(row.blocked_by_consent || 0),
  };
}

async function resolveTemplate(pgClient, normalized) {
  if (!normalized.templateKey) {
    if (normalized.channel === "whatsapp") {
      const error = new Error("manual_template_not_found");
      error.code = "manual_template_not_found";
      throw error;
    }
    return null;
  }
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
    const builtin = getBuiltinEmailTemplates().find(
      (item) => item.template_key === normalized.templateKey
    );
    if (FIXED_REMAINING_NUMBERS.has(normalized.templateKey)) return builtin || null;
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
    return result.rows?.[0] || builtin || null;
  }
  return resolveManualBrevoWhatsAppTemplate({
    pgClient,
    templateKey: normalized.templateKey,
  });
}

export function renderManualEmailContent(normalized, template, extraParams = {}) {
  const params = {
    ...(template?.default_params || {}),
    ...(normalized.params || {}),
    ...extraParams,
  };
  return {
    subject: renderTemplate(
      normalized.subject || template?.subject_template || template?.name || "Mensagem da New Store",
      params
    ),
    text: renderTemplate(
      normalized.text || template?.text_template || template?.default_message || normalized.message || "",
      params
    ),
    html: renderTemplate(
      normalized.html || template?.html_template || template?.default_message || normalized.message || "",
      params
    ),
  };
}

function buildPreviewText(normalized, template) {
  const params = { ...(template?.default_params || {}), ...(normalized.params || {}) };
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
    const { subject, text, html } = renderManualEmailContent(normalized, template);
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
  let blockedByConsent = 0;
  let eligibleUserIds = [];

  if (normalized.audience === "selected") {
    users = await loadSelectedUsers(pgClient, normalized.userIds);
  } else if (normalized.channel === "whatsapp" && normalized.audience === "all_consented") {
    users = await loadAllUsers(pgClient);
  } else if (normalized.channel === "email" && normalized.audience === "all_with_email") {
    users = await loadAllUsers(pgClient);
  }

  if (normalized.channel === "push") {
    subscriptions = await loadPushSubscriptions(pgClient, {
      audience: normalized.audience,
      userIds: normalized.userIds,
    });
    const uniqueUsers = Array.from(new Set(subscriptions.map((row) => Number(row.user_id)).filter(Boolean)));
    if (normalized.audience !== "selected") {
      users = uniqueUsers.map((id) => ({ id }));
    }
    const exclusionStats = await loadPushExclusionStats(pgClient, {
      audience: normalized.audience,
      userIds: normalized.userIds,
    });
    inactiveSubscriptions = exclusionStats.inactiveSubscriptions;
    blockedByConsent = exclusionStats.blockedByConsent;
    eligibleUserIds = uniqueUsers;
    if (normalized.audience === "selected" && uniqueUsers.length > MANUAL_MAX_UNIQUE_USERS) {
      const error = new Error("manual_too_many_recipients");
      error.code = "manual_too_many_recipients";
      error.max = MANUAL_MAX_UNIQUE_USERS;
      throw error;
    }
    if (normalized.audience !== "selected") {
      assertManualCampaignAudienceSize(uniqueUsers.length);
    }
  }

  let validPhones = 0;
  let missingContact = 0;
  if (normalized.channel === "whatsapp") {
    for (const user of users) {
      const phone = normalizePhoneBR(user.phone);
      if (!phone) {
        missingContact += 1;
        continue;
      }
      const consent = await getWhatsappConsentStatusForUser({
        pgClient,
        userId: user.id,
        ...(normalized.audience === "all_consented" && { category: "operational" }),
      });
      if (consent.whatsapp_can_send) {
        validPhones += 1;
        eligibleUserIds.push(Number(user.id));
      } else blockedByConsent += 1;
    }
    if (normalized.audience === "all_consented") {
      assertManualCampaignAudienceSize(eligibleUserIds.length);
    }
  }

  let validEmails = 0;
  let invalidEmails = 0;
  let duplicateEmailsRemoved = 0;
  if (normalized.channel === "email") {
    const emailAudience = resolveManualEmailRecipients(users);
    validEmails = emailAudience.recipients.length;
    missingContact = emailAudience.missingContact;
    invalidEmails = emailAudience.invalidEmails;
    duplicateEmailsRemoved = emailAudience.duplicateEmailsRemoved;
    eligibleUserIds = emailAudience.recipients.map((user) => Number(user.id));
    if (normalized.audience === "all_with_email") {
      assertManualCampaignAudienceSize(validEmails);
    }
  }

  const template = await resolveTemplate(pgClient, normalized);
  if (normalized.templateKey && !template) {
    const error = new Error("manual_template_not_found");
    error.code = "manual_template_not_found";
    throw error;
  }
  const text = buildPreviewText(normalized, template);
  const eligibleUsers = normalized.channel === "push"
    ? new Set(subscriptions.map((row) => Number(row.user_id))).size
    : normalized.channel === "email"
      ? validEmails
      : validPhones;
  const requiresBulkConfirmation =
    normalized.audience === "all_active_push" ||
    normalized.audience === "all_consented" ||
    normalized.audience === "all_with_email" ||
    eligibleUsers > 1;
  const requestedUsers = normalized.audience === "selected"
    ? normalized.userIds.length
    : normalized.channel === "whatsapp" || normalized.channel === "email"
      ? users.length
      : eligibleUsers;

  normalized.eligibleUserIds = eligibleUserIds;

  return {
    ok: true,
    can_send: eligibleUsers > 0 &&
      (normalized.channel !== "push" || Boolean(text.title_preview && text.message_preview)),
    channel: normalized.channel,
    provider: normalized.channel === "push" ? "web_push" : normalized.channel === "email" ? "brevo_smtp" : "brevo",
    template,
    ...text,
    requested_users: requestedUsers,
    eligible_users: eligibleUsers,
    eligible_devices: subscriptions.length,
    valid_emails: validEmails,
    invalid_emails: invalidEmails,
    duplicate_emails_removed: duplicateEmailsRemoved,
    valid_phones: validPhones,
    blocked_by_consent: blockedByConsent,
    missing_contact: missingContact,
    inactive_subscriptions: inactiveSubscriptions,
    estimated_batches: estimatedManualBatches(eligibleUsers),
    warnings,
    requires_bulk_confirmation: requiresBulkConfirmation,
    manual_audience_max_users: MANUAL_MAX_CAMPAIGN_USERS,
    normalized,
  };
}

export function sanitizePreviewForResponse(preview) {
  const { normalized, ...rest } = preview || {};
  return rest;
}
