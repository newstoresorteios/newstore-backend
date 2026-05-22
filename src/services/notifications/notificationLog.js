// src/services/notifications/notificationLog.js
import { query } from "../../db.js";

async function runQuery(pgClient, text, params) {
  if (pgClient) return pgClient.query(text, params);
  return query(text, params);
}

function sanitizePayload(payload) {
  if (!payload || typeof payload !== "object") return payload ?? null;
  const clone = { ...payload };
  for (const key of Object.keys(clone)) {
    const lower = key.toLowerCase();
    if (
      lower.includes("secret") ||
      lower.includes("api_key") ||
      lower.includes("api-key") ||
      lower === "authorization"
    ) {
      delete clone[key];
    }
  }
  return clone;
}

function toJsonb(value) {
  return JSON.stringify(sanitizePayload(value) ?? {});
}

export function extractDispatchErrorMessage(result) {
  const body = result?.response;
  if (
    body &&
    body.code === "unauthorized" &&
    String(body.message || "").toLowerCase().includes("unrecognised ip address")
  ) {
    return "brevo_ip_not_authorized";
  }
  return result?.reason || result?.error || "notification_failed";
}

export async function createCampaign({
  pgClient,
  name,
  channel,
  provider,
  templateKey,
  providerTemplateId = null,
  audienceFilter = null,
  audienceParams = {},
  status = "created",
  createdBy = null,
  payload = {},
  messageSnapshot = {},
  audienceSnapshot = {},
  campaignType = "manual_admin",
  audienceCountExpected = null,
}) {
  const r = await runQuery(
    pgClient,
    `INSERT INTO public.notification_campaigns (
        name,
        channel,
        provider,
        template_key,
        provider_template_id,
        audience_filter,
        audience_params,
        status,
        created_by,
        payload,
        message_snapshot,
        audience_snapshot,
        campaign_type,
        audience_count_expected
      ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10::jsonb, $11::jsonb, $12::jsonb, $13, $14)
      RETURNING *`,
    [
      name,
      channel,
      provider,
      templateKey,
      providerTemplateId,
      audienceFilter,
      toJsonb(audienceParams),
      status,
      createdBy,
      toJsonb(payload),
      toJsonb(messageSnapshot),
      toJsonb(audienceSnapshot),
      campaignType,
      audienceCountExpected,
    ]
  );
  return r.rows[0];
}

export async function updateCampaignAudienceCounts(
  pgClient,
  campaignId,
  { created = 0, sent = 0, failed = 0, skipped = 0 } = {}
) {
  const r = await runQuery(
    pgClient,
    `UPDATE public.notification_campaigns
        SET audience_count_created = COALESCE(audience_count_created, 0) + $2,
            audience_count_sent = COALESCE(audience_count_sent, 0) + $3,
            audience_count_failed = COALESCE(audience_count_failed, 0) + $4,
            audience_count_skipped = COALESCE(audience_count_skipped, 0) + $5,
            updated_at = NOW()
      WHERE id = $1
      RETURNING *`,
    [campaignId, created, sent, failed, skipped]
  );
  return r.rows[0];
}

export async function createDispatch({
  pgClient,
  eventId = null,
  eventKey,
  channel,
  provider,
  userId = null,
  drawId = null,
  recipient,
  recipientOriginal = null,
  recipientForced = false,
  templateKey = null,
  providerTemplateId = null,
  payload = null,
  campaignId = null,
  messageSnapshot = {},
  recipientSnapshot = {},
}) {
  const safeMessageSnapshot = sanitizePayload(messageSnapshot) ?? {};
  const safeRecipientSnapshot = sanitizePayload(recipientSnapshot) ?? {};

  console.log("[notifications.audit] create dispatch", {
    campaign_id: campaignId || null,
    event_key: eventKey,
    template_key: templateKey,
    user_id: userId || null,
    draw_id: drawId || null,
    recipient_forced: recipientForced,
    has_message_snapshot: Boolean(
      safeMessageSnapshot && Object.keys(safeMessageSnapshot).length
    ),
    has_recipient_snapshot: Boolean(
      safeRecipientSnapshot && Object.keys(safeRecipientSnapshot).length
    ),
  });

  const r = await runQuery(
    pgClient,
    `INSERT INTO public.notification_dispatches (
        event_id,
        event_key,
        channel,
        provider,
        user_id,
        draw_id,
        recipient,
        recipient_original,
        recipient_forced,
        template_key,
        provider_template_id,
        status,
        payload,
        attempts,
        campaign_id,
        message_snapshot,
        recipient_snapshot
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'pending', $12::jsonb, 0, $13, $14::jsonb, $15::jsonb)
      RETURNING *`,
    [
      eventId,
      eventKey,
      channel,
      provider,
      userId,
      drawId,
      recipient,
      recipientOriginal,
      recipientForced,
      templateKey,
      providerTemplateId,
      toJsonb(payload),
      campaignId,
      toJsonb(safeMessageSnapshot),
      toJsonb(safeRecipientSnapshot),
    ]
  );
  return r.rows[0];
}

export async function markDispatchSent({ pgClient, dispatchId, result }) {
  const r = await runQuery(
    pgClient,
    `UPDATE public.notification_dispatches
        SET status = 'sent',
            sent_at = NOW(),
            attempts = attempts + 1,
            provider_message_id = $2,
            response = $3::jsonb,
            error_message = NULL
      WHERE id = $1
      RETURNING *`,
    [
      dispatchId,
      result?.messageId ?? null,
      JSON.stringify(result?.response ?? null),
    ]
  );
  return r.rows[0];
}

export async function markDispatchFailed({
  pgClient,
  dispatchId,
  result,
  status: explicitStatus,
}) {
  const status =
    explicitStatus ||
    (result?.skipped ? "skipped" : "failed");
  const errorMessage = extractDispatchErrorMessage(result);

  const r = await runQuery(
    pgClient,
    `UPDATE public.notification_dispatches
        SET status = $2,
            attempts = attempts + 1,
            response = $3::jsonb,
            error_message = $4
      WHERE id = $1
      RETURNING *`,
    [
      dispatchId,
      status,
      JSON.stringify(result?.response ?? null),
      errorMessage,
    ]
  );
  return r.rows[0];
}

export async function recordInboundMessage({
  pgClient,
  provider,
  channel,
  rawPayload,
  extracted = {},
}) {
  const safeRaw = sanitizePayload(rawPayload) ?? rawPayload ?? {};

  const r = await runQuery(
    pgClient,
    `INSERT INTO public.notification_inbound_messages (
        provider,
        channel,
        conversation_id,
        message_id,
        from_phone,
        to_phone,
        user_id,
        text,
        event_name,
        raw_payload,
        status
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, 'received')
      RETURNING *`,
    [
      provider,
      channel,
      extracted.conversationId ?? null,
      extracted.messageId ?? null,
      extracted.fromPhone ?? null,
      extracted.toPhone ?? null,
      extracted.userId ?? null,
      extracted.text ?? null,
      extracted.eventName ?? null,
      JSON.stringify(safeRaw),
    ]
  );
  return r.rows[0];
}
