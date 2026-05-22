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
}) {
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
        attempts
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'pending', $12::jsonb, 0)
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
      JSON.stringify(sanitizePayload(payload) ?? {}),
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
