import { query } from "../../db.js";
import { createCampaign, createDispatch, markDispatchAccepted, markDispatchFailed, updateCampaignAudienceCounts } from "./notificationLog.js";
import { sendPushToSubscriptionRow } from "./pushNotifications.js";
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

function validatePushMessage({ title, message, url }) {
  const cleanTitle = String(title || "").trim();
  const cleanBody = String(message || "").trim();
  const cleanUrl = String(url || "/").trim() || "/";
  if (!cleanTitle || !cleanBody) throw coded("manual_push_message_required");
  if (cleanTitle.length > 80) throw coded("manual_push_title_too_long");
  if (cleanBody.length > 180) throw coded("manual_push_message_too_long");
  if (!cleanUrl.startsWith("/") || cleanUrl.startsWith("//")) throw coded("manual_push_url_invalid");
  return { title: cleanTitle, body: cleanBody, url: cleanUrl };
}

async function loadSubscriptions(pgClient, normalized) {
  const params = [];
  let where = `
    WHERE is_active = true
      AND operational_opt_in = true
      AND user_id IS NOT NULL
  `;
  if (normalized.audience === "selected") {
    params.push(normalized.userIds);
    where += ` AND user_id = ANY($1::int[])`;
  }
  const result = await runQuery(
    pgClient,
    `SELECT *
       FROM public.push_subscriptions
       ${where}
      ORDER BY user_id, updated_at DESC NULLS LAST, created_at DESC NULLS LAST`,
    params
  );
  return result.rows || [];
}

function summarizeSubscriptions(rows) {
  const users = new Set(rows.map((row) => Number(row.user_id)).filter(Boolean));
  return { eligible_users: users.size, eligible_devices: rows.length };
}

export async function sendManualPushNotification({
  pgClient,
  payload = {},
  adminUserId = null,
  sendPush = sendPushToSubscriptionRow,
} = {}) {
  const preview = await buildManualNotificationPreview({ pgClient, payload: { ...payload, channel: "push" } });
  const normalized = preview.normalized;
  const message = validatePushMessage({
    title: payload.title || preview.title_preview,
    message: payload.message || preview.message_preview,
    url: normalized.url,
  });
  const subscriptions = await loadSubscriptions(pgClient, normalized);
  const audience = summarizeSubscriptions(subscriptions);

  if (!audience.eligible_devices) {
    return {
      ok: false,
      error: "manual_push_no_eligible_recipients",
      requested_users: preview.requested_users,
      ...audience,
      sent: 0,
      failed: 0,
      skipped: 0,
    };
  }
  if (audience.eligible_users > MANUAL_MAX_UNIQUE_USERS) {
    throw coded("manual_too_many_recipients", { max: MANUAL_MAX_UNIQUE_USERS });
  }
  if ((audience.eligible_users > 1 || normalized.audience === "all_active_push") && payload.confirm_bulk_send !== true) {
    return {
      ok: false,
      error: "manual_bulk_confirmation_required",
      requested_users: preview.requested_users,
      ...audience,
      valid_phones: 0,
      valid_emails: 0,
    };
  }

  let campaign = await createCampaign({
    pgClient,
    name: `Manual push - ${message.title}`,
    channel: "push",
    provider: "web_push",
    templateKey: normalized.templateKey || null,
    audienceFilter: normalized.audience,
    audienceParams: {
      user_ids: normalized.audience === "selected" ? normalized.userIds : [],
    },
    status: "created",
    createdBy: adminUserId,
    payload: {
      source: "admin_manual",
      manual: true,
      admin_user_id: adminUserId || null,
      manual_channel: "push",
    },
    messageSnapshot: {
      source: "admin_manual",
      manual: true,
      admin_user_id: adminUserId || null,
      template_key: normalized.templateKey || null,
      title: message.title,
      body: message.body,
      url: message.url,
    },
    audienceSnapshot: {
      source: "admin_manual",
      manual: true,
      audience: normalized.audience,
      eligible_users: audience.eligible_users,
      eligible_devices: audience.eligible_devices,
    },
    campaignType: "manual_admin",
    audienceCountExpected: audience.eligible_users,
  });

  let sent = 0;
  let failed = 0;
  let skipped = 0;
  const dispatches = [];

  for (const subscription of subscriptions) {
    const dispatch = await createDispatch({
      pgClient,
      eventKey: "MANUAL_ADMIN_PUSH",
      channel: "push",
      provider: "web_push",
      userId: subscription.user_id || null,
      recipient: `push_subscription:${subscription.id}`,
      recipientOriginal: `push_subscription:${subscription.id}`,
      templateKey: normalized.templateKey || null,
      campaignId: campaign.id,
      payload: {
        source: "admin_manual",
        manual: true,
        admin_user_id: adminUserId || null,
        template_key: normalized.templateKey || null,
        url: message.url,
      },
      messageSnapshot: {
        source: "admin_manual",
        manual: true,
        title: message.title,
        body: message.body,
        url: message.url,
      },
      recipientSnapshot: {
        source: "admin_manual",
        manual: true,
        user_id: subscription.user_id || null,
        subscription_id: subscription.id,
      },
    });

    try {
      const result = await sendPush({
        subscriptionRow: subscription,
        title: message.title,
        body: message.body,
        url: message.url,
        payload: {
          source: "admin_manual",
          manual: true,
          admin_user_id: adminUserId || null,
          template_key: normalized.templateKey || null,
        },
        source: "admin_manual",
        eventKey: "MANUAL_ADMIN_PUSH",
        category: "manual_admin",
        requireConfiguredSubscription: false,
        skipModeAssert: true,
      });
      const updated = await markDispatchAccepted({
        pgClient,
        dispatchId: dispatch.id,
        result: {
          ok: true,
          provider_status: "accepted",
          delivery_status: "unknown",
          response: { push_dispatch_id: result?.dispatch?.id || null },
        },
      });
      sent += 1;
      dispatches.push(updated);
    } catch (error) {
      const statusCode = Number(error?.provider_status || error?.statusCode || error?.status || 0) || null;
      const updated = await markDispatchFailed({
        pgClient,
        dispatchId: dispatch.id,
        result: {
          ok: false,
          error: error?.code || "manual_push_failed",
          reason: error?.message || null,
          response: { statusCode },
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
    skipped,
  });

  return {
    ok: sent > 0 || failed > 0,
    requested_users: preview.requested_users,
    ...audience,
    sent,
    failed,
    skipped,
    campaign_id: campaign?.id || null,
    campaign,
    dispatches,
  };
}
