import express from "express";
import { AUTOMATIC_EMAIL_EVENT_KEYS, handleAutomaticEmailEvent } from "../services/notifications/automaticEmailNotifications.js";

const router = express.Router();

function internalTokenAllowed(req) {
  const expected = String(process.env.PUSH_INTERNAL_EVENTS_TOKEN || "").trim();
  const received = String(req.get("x-internal-token") || "").trim();
  return Boolean(expected && received && expected === received);
}

router.post("/events", async (req, res) => {
  if (!internalTokenAllowed(req)) {
    return res.status(401).json({ ok: false, error: "internal_email_event_unauthorized" });
  }
  const body = req.body || {};
  if (!AUTOMATIC_EMAIL_EVENT_KEYS.includes(String(body.event_key || "").trim())) {
    return res.status(400).json({ ok: false, error: "email_event_not_allowed" });
  }
  try {
    const result = await handleAutomaticEmailEvent({
      eventKey: body.event_key,
      referenceType: body.reference_type,
      referenceKey: body.reference_key,
      scanId: body.scan_id,
      occurredAt: body.occurred_at,
      metadata: body.metadata,
    });
    return res.json(result);
  } catch (error) {
    console.error("[internal/email/events] error", {
      code: error?.code || "email_event_failed",
      message: error?.message || null,
    });
    return res.status(error?.code === "email_draw_not_found" ? 404 : 400).json({
      ok: false,
      error: error?.code || "email_event_failed",
    });
  }
});

export default router;
