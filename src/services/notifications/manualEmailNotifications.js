import nodemailer from "nodemailer";
import { query } from "../../db.js";
import { createCampaign, createDispatch, markDispatchAccepted, markDispatchFailed, updateCampaignAudienceCounts } from "./notificationLog.js";
import { MANUAL_MAX_UNIQUE_USERS, buildManualNotificationPreview } from "./manualNotificationPreview.js";

function runQuery(pgClient, text, params) {
  if (pgClient) return pgClient.query(text, params);
  return query(text, params);
}

function coded(code, extra = {}) {
  const error = new Error(code);
  error.code = code;
  Object.assign(error, extra);
  return error;
}

export function getSmtpConfigStatus() {
  return {
    configured: Boolean(
      String(process.env.SMTP_HOST || "").trim() &&
      String(process.env.SMTP_USER || "").trim() &&
      String(process.env.SMTP_PASS || "").trim()
    ),
  };
}

function smtpConfig() {
  const host = String(process.env.SMTP_HOST || "").trim();
  const port = Number(process.env.SMTP_PORT || 587);
  const user = String(process.env.SMTP_USER || "").trim();
  const pass = String(process.env.SMTP_PASS || "").trim();
  const fromEmail = String(process.env.SMTP_FROM || "contato@newstorerj.com.br").trim();
  const fromName = String(process.env.SMTP_FROM_NAME || "New Store Sorteios").trim();
  const replyTo = String(process.env.SMTP_REPLY_TO || fromEmail).trim();
  if (!host || !user || !pass) throw coded("manual_email_smtp_not_configured");
  return { host, port, user, pass, fromEmail, fromName, replyTo };
}

function isValidEmail(value) {
  const email = String(value || "").trim();
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

async function loadUsers(pgClient, userIds) {
  const result = await runQuery(
    pgClient,
    `SELECT id, name, email
       FROM public.users
      WHERE id = ANY($1::int[])
      ORDER BY id`,
    [userIds]
  );
  return result.rows || [];
}

function resolveValidRecipients(users) {
  const seen = new Set();
  const recipients = [];
  let missing = 0;
  for (const user of users) {
    const email = String(user.email || "").trim().toLowerCase();
    if (!isValidEmail(email)) {
      missing += 1;
      continue;
    }
    if (seen.has(email)) continue;
    seen.add(email);
    recipients.push({ ...user, email });
  }
  return { recipients, missing };
}

function createTransporter(config) {
  return nodemailer.createTransport({
    host: config.host,
    port: config.port,
    secure: config.port === 465,
    auth: {
      user: config.user,
      pass: config.pass,
    },
  });
}

export async function sendManualEmailNotification({
  pgClient,
  payload = {},
  adminUserId = null,
  transporter = null,
} = {}) {
  const preview = await buildManualNotificationPreview({ pgClient, payload: { ...payload, channel: "email" } });
  const normalized = preview.normalized;
  if (normalized.userIds.length > MANUAL_MAX_UNIQUE_USERS) {
    throw coded("manual_too_many_recipients", { max: MANUAL_MAX_UNIQUE_USERS });
  }
  if (preview.eligible_users > 1 && payload.confirm_bulk_send !== true) {
    return {
      ok: false,
      error: "manual_bulk_confirmation_required",
      requested_users: preview.requested_users,
      eligible_users: preview.eligible_users,
      eligible_devices: 0,
      valid_emails: preview.valid_emails,
      valid_phones: 0,
    };
  }

  const users = await loadUsers(pgClient, normalized.userIds);
  const { recipients, missing } = resolveValidRecipients(users);
  if (!recipients.length) {
    return {
      ok: false,
      error: "manual_email_no_valid_recipients",
      requested_users: normalized.userIds.length,
      eligible_users: 0,
      valid_emails: 0,
      missing_contact: missing,
      sent: 0,
      failed: 0,
      skipped: missing,
    };
  }

  const config = transporter
    ? {
        fromEmail: String(process.env.SMTP_FROM || "contato@newstorerj.com.br").trim(),
        fromName: String(process.env.SMTP_FROM_NAME || "New Store Sorteios").trim(),
        replyTo: String(process.env.SMTP_REPLY_TO || process.env.SMTP_FROM || "contato@newstorerj.com.br").trim(),
      }
    : smtpConfig();
  const mailer = transporter || createTransporter(config);
  const subject = preview.subject_preview || payload.subject || "Mensagem da New Store";
  const html = preview.html_preview || payload.html || `<p>${preview.text_preview || ""}</p>`;
  const text = preview.text_preview || payload.text || "";

  let campaign = await createCampaign({
    pgClient,
    name: `Manual email - ${subject}`.slice(0, 255),
    channel: "email",
    provider: "brevo_smtp",
    templateKey: normalized.templateKey || null,
    audienceFilter: "selected",
    audienceParams: { user_ids: normalized.userIds },
    status: "created",
    createdBy: adminUserId,
    payload: {
      source: "admin_manual",
      manual: true,
      admin_user_id: adminUserId || null,
      manual_channel: "email",
    },
    messageSnapshot: {
      source: "admin_manual",
      manual: true,
      admin_user_id: adminUserId || null,
      template_key: normalized.templateKey || null,
      subject,
      has_html: Boolean(html),
      has_text: Boolean(text),
    },
    audienceSnapshot: {
      source: "admin_manual",
      manual: true,
      requested_users: normalized.userIds.length,
      valid_emails: recipients.length,
      missing_contact: missing,
    },
    campaignType: "manual_admin",
    audienceCountExpected: recipients.length,
  });

  let sent = 0;
  let failed = 0;
  const dispatches = [];

  for (const user of recipients) {
    const dispatch = await createDispatch({
      pgClient,
      eventKey: "MANUAL_ADMIN_EMAIL",
      channel: "email",
      provider: "brevo_smtp",
      userId: user.id,
      recipient: user.email,
      recipientOriginal: user.email,
      templateKey: normalized.templateKey || null,
      campaignId: campaign.id,
      payload: {
        source: "admin_manual",
        manual: true,
        admin_user_id: adminUserId || null,
        template_key: normalized.templateKey || null,
      },
      messageSnapshot: {
        source: "admin_manual",
        manual: true,
        subject,
        has_html: Boolean(html),
        has_text: Boolean(text),
      },
      recipientSnapshot: {
        source: "admin_manual",
        manual: true,
        user_id: user.id,
        email: user.email,
      },
    });

    try {
      const info = await mailer.sendMail({
        from: `"${config.fromName}" <${config.fromEmail}>`,
        to: user.email,
        replyTo: config.replyTo,
        subject,
        html,
        text,
      });
      const updated = await markDispatchAccepted({
        pgClient,
        dispatchId: dispatch.id,
        result: {
          ok: true,
          provider_status: "accepted",
          delivery_status: "unknown",
          messageId: info?.messageId || null,
          response: { accepted: info?.accepted?.length || 0 },
        },
      });
      sent += 1;
      dispatches.push(updated);
    } catch (error) {
      const updated = await markDispatchFailed({
        pgClient,
        dispatchId: dispatch.id,
        result: {
          ok: false,
          error: "manual_email_send_failed",
          reason: error?.code || error?.message || null,
        },
      });
      failed += 1;
      dispatches.push(updated);
    }
  }

  campaign = await updateCampaignAudienceCounts(pgClient, campaign.id, {
    created: dispatches.length,
    sent,
    failed,
    skipped: missing,
  });

  return {
    ok: sent > 0 || failed > 0,
    requested_users: normalized.userIds.length,
    eligible_users: recipients.length,
    valid_emails: recipients.length,
    missing_contact: missing,
    sent,
    failed,
    skipped: missing,
    campaign_id: campaign?.id || null,
    campaign,
    dispatches,
  };
}
