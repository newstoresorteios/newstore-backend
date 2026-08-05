import express from "express";
import { AUTOMATIC_EMAIL_EVENT_KEYS, handleAutomaticEmailEvent } from "../services/notifications/automaticEmailNotifications.js";

const router = express.Router();

const CLIENT_ERROR_CODES = new Set([
  "email_event_not_allowed",
  "email_draw_id_invalid",
  "email_reference_key_invalid",
  "email_draw_type_not_allowed",
]);

export function statusForEmailEventError(error) {
  if (error?.code === "email_draw_not_found") return 404;
  if (CLIENT_ERROR_CODES.has(error?.code)) return 400;
  return 500;
}

export function internalTokenAllowed(req) {
  const expected = String(process.env.PUSH_INTERNAL_EVENTS_TOKEN || "").trim();
  const received = String(req.get("x-internal-token") || "").trim();
  return Boolean(expected && received && expected === received);
}

function eventMetadata(body = {}) {
  const metadata = body.metadata && typeof body.metadata === "object" && !Array.isArray(body.metadata)
    ? { ...body.metadata }
    : {};
  for (const key of ["draw_id", "draw_type", "draw_name"]) {
    if (metadata[key] == null && body[key] != null) metadata[key] = body[key];
  }
  return metadata;
}

export async function handleInternalEmailEventRequest(
  req,
  res,
  eventHandler = handleAutomaticEmailEvent
) {
  if (!internalTokenAllowed(req)) {
    return res.status(401).json({ ok: false, error: "internal_email_event_unauthorized" });
  }
  const body = req.body || {};
  if (!AUTOMATIC_EMAIL_EVENT_KEYS.includes(String(body.event_key || "").trim())) {
    return res.status(400).json({ ok: false, error: "email_event_not_allowed" });
  }
  const metadata = eventMetadata(body);
  try {
    const result = await eventHandler({
      eventKey: body.event_key,
      referenceType: body.reference_type,
      referenceKey: body.reference_key,
      scanId: body.scan_id,
      occurredAt: body.occurred_at,
      metadata,
    });
    return res.json(result);
  } catch (error) {
    console.error("[internal/email/events] error", {
      code: error?.code || "email_event_failed",
      message: error?.message || null,
      event_key: body.event_key ?? null,
      reference_key: body.reference_key ?? null,
      draw_id: metadata.draw_id ?? null,
    });
    return res.status(statusForEmailEventError(error)).json({
      ok: false,
      error: error?.code || "email_event_failed",
    });
  }
}

router.post("/events", async (req, res) => {
  return handleInternalEmailEventRequest(req, res);
});

export default router;
