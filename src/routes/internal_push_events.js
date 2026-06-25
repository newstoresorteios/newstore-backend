import express from "express";
import { handlePushAutomationEvent } from "../services/notifications/pushAutomationEvents.js";

const router = express.Router();

function isTrue(value) {
  return String(value || "").trim() === "true";
}

function internalTokenAllowed(req) {
  const expected = String(process.env.PUSH_INTERNAL_EVENTS_TOKEN || "").trim();
  const received = String(req.get("x-internal-token") || "").trim();
  return Boolean(expected && received && expected === received);
}

function statusFor(code) {
  if (code === "internal_push_event_unauthorized") return 401;
  if (code === "push_engine_events_disabled") return 403;
  if (code === "push_engine_dry_run_required") return 403;
  if (code === "push_engine_real_send_not_supported_in_this_phase") return 403;
  if (String(code || "").startsWith("push_event_")) return 400;
  if (String(code || "").startsWith("push_rule_")) return 400;
  return 500;
}

function sendError(res, code) {
  return res.status(statusFor(code)).json({ ok: false, error: code });
}

router.post("/events", async (req, res) => {
  try {
    if (!internalTokenAllowed(req)) {
      return sendError(res, "internal_push_event_unauthorized");
    }
    if (!isTrue(process.env.PUSH_ALLOW_ENGINE_EVENTS)) {
      return sendError(res, "push_engine_events_disabled");
    }
    if (!isTrue(process.env.PUSH_ENGINE_DRY_RUN)) {
      return sendError(res, "push_engine_dry_run_required");
    }
    if (isTrue(process.env.PUSH_ENGINE_ALLOW_REAL_SEND)) {
      return sendError(res, "push_engine_real_send_not_supported_in_this_phase");
    }

    const body = req.body || {};
    const result = await handlePushAutomationEvent({
      eventKey: body.event_key,
      source: body.source || "engine",
      metadata: body.metadata || {},
      actor: { type: "internal_push_events" },
    });

    return res.json({
      ok: true,
      event_key: result.event_key,
      status: result.status,
    });
  } catch (error) {
    const code = error?.code || "push_engine_event_failed";
    console.error("[internal/push/events] error", {
      code,
      message: error?.message || null,
    });
    return sendError(res, code);
  }
});

export default router;
