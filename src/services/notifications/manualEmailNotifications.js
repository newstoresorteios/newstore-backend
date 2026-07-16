import nodemailer from "nodemailer";
import { query } from "../../db.js";
import { createCampaign, createDispatch, markDispatchAccepted, markDispatchFailed, updateCampaignAudienceCounts } from "./notificationLog.js";
import {
  MANUAL_MAX_UNIQUE_USERS,
  buildManualNotificationPreview,
  renderManualEmailContent,
  resolveManualEmailRecipients,
} from "./manualNotificationPreview.js";
import {
  assertManualCampaignAudienceSize,
  chunkManualAudience,
} from "./manualAudience.js";

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

async function loadSelectedUsers(pgClient, userIds) {
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

async function loadAllUsers(pgClient) {
  const result = await runQuery(
    pgClient,
    `SELECT id, name, email
       FROM public.users
      ORDER BY id`,
    []
  );
  return result.rows || [];
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
  if (normalized.audience === "selected" && normalized.userIds.length > MANUAL_MAX_UNIQUE_USERS) {
    throw coded("manual_too_many_recipients", { max: MANUAL_MAX_UNIQUE_USERS });
  }
  if (preview.requires_bulk_confirmation && payload.confirm_bulk_send !== true) {
    return {
      ok: false,
      error: "manual_bulk_confirmation_required",
      requested_users: preview.requested_users,
      eligible_users: preview.eligible_users,
      eligible_devices: 0,
      valid_emails: preview.valid_emails,
      valid_phones: 0,
      invalid_emails: preview.invalid_emails,
      missing_contact: preview.missing_contact,
      duplicate_emails_removed: preview.duplicate_emails_removed,
      estimated_batches: preview.estimated_batches,
      requires_bulk_confirmation: true,
    };
  }

  const users = normalized.audience === "all_with_email"
    ? await loadAllUsers(pgClient)
    : await loadSelectedUsers(pgClient, normalized.userIds);
  const {
    recipients,
    missingContact,
    invalidEmails,
    duplicateEmailsRemoved,
  } = resolveManualEmailRecipients(users);
  if (normalized.audience === "all_with_email") {
    assertManualCampaignAudienceSize(recipients.length);
  }
  if (!recipients.length) {
    return {
      ok: false,
      error: "manual_email_no_valid_recipients",
      requested_users: users.length,
      eligible_users: 0,
      valid_emails: 0,
      invalid_emails: invalidEmails,
      missing_contact: missingContact,
      duplicate_emails_removed: duplicateEmailsRemoved,
      sent: 0,
      failed: 0,
      skipped: missingContact + invalidEmails + duplicateEmailsRemoved,
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
  const excluded = missingContact + invalidEmails + duplicateEmailsRemoved;
  const batches = chunkManualAudience(recipients);

  let campaign = await createCampaign({
    pgClient,
    name: `Manual email - ${subject}`.slice(0, 255),
    channel: "email",
    provider: "brevo_smtp",
    templateKey: normalized.templateKey || null,
    audienceFilter: normalized.audience,
    audienceParams: normalized.audience === "selected"
      ? { user_ids: normalized.userIds }
      : { audience: "all_with_email" },
    status: "created",
    createdBy: adminUserId,
    payload: {
      source: "admin_manual",
      manual: true,
      channel: "email",
      provider: "brevo_smtp",
      event_key: "MANUAL_ADMIN_EMAIL",
      audience: normalized.audience,
      admin_user_id: adminUserId || null,
      manual_channel: "email",
    },
    messageSnapshot: {
      source: "admin_manual",
      manual: true,
      audience: normalized.audience,
      admin_user_id: adminUserId || null,
      template_key: normalized.templateKey || null,
      subject,
      has_html: Boolean(html),
      has_text: Boolean(text),
    },
    audienceSnapshot: {
      source: "admin_manual",
      manual: true,
      audience: normalized.audience,
      requested_users: users.length,
      valid_emails: recipients.length,
      invalid_emails: invalidEmails,
      missing_contact: missingContact,
      duplicate_emails_removed: duplicateEmailsRemoved,
      estimated_batches: batches.length,
    },
    campaignType: "manual_admin",
    audienceCountExpected: recipients.length,
  });

  let sent = 0;
  let failed = 0;
  const dispatches = [];

  let batchesProcessed = 0;
  for (const [batchIndex, batch] of batches.entries()) {
    const batchNumber = batchIndex + 1;
    batchesProcessed += 1;
    for (const user of batch) {
      const rendered = renderManualEmailContent(normalized, preview.template, {
        name: user.name || "",
      });
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
          channel: "email",
          provider: "brevo_smtp",
          event_key: "MANUAL_ADMIN_EMAIL",
          audience: normalized.audience,
          admin_user_id: adminUserId || null,
          template_key: normalized.templateKey || null,
          batch_number: batchNumber,
          total_batches: batches.length,
        },
        messageSnapshot: {
          source: "admin_manual",
          manual: true,
          subject: rendered.subject,
          has_html: Boolean(rendered.html),
          has_text: Boolean(rendered.text),
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
          subject: rendered.subject,
          html: rendered.html,
          text: rendered.text,
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
  }

  campaign = await updateCampaignAudienceCounts(pgClient, campaign.id, {
    created: dispatches.length,
    sent,
    failed,
    skipped: excluded,
  });

  return {
    ok: sent > 0 || failed > 0,
    requested_users: users.length,
    eligible_users: recipients.length,
    valid_emails: recipients.length,
    invalid_emails: invalidEmails,
    missing_contact: missingContact,
    duplicate_emails_removed: duplicateEmailsRemoved,
    estimated_batches: batches.length,
    batches_processed: batchesProcessed,
    sent,
    accepted: sent,
    failed,
    skipped: excluded,
    campaign_id: campaign?.id || null,
    campaign,
    dispatches,
  };
}
