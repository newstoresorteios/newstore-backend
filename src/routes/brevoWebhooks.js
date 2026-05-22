// src/routes/brevoWebhooks.js
import express from "express";
import { recordInboundMessage } from "../services/notifications/notificationLog.js";

const router = express.Router();

function getWebhookSecret(req) {
  const header = req.headers["x-webhook-secret"];
  if (header) return String(header).trim();
  if (req.query?.secret) return String(req.query.secret).trim();
  return null;
}

function pickFirst(obj, paths) {
  for (const path of paths) {
    const parts = path.split(".");
    let cur = obj;
    let found = true;
    for (const p of parts) {
      if (cur == null || typeof cur !== "object" || !(p in cur)) {
        found = false;
        break;
      }
      cur = cur[p];
    }
    if (found && cur != null && cur !== "") return cur;
  }
  return null;
}

function extractInboundFields(payload) {
  const eventName = pickFirst(payload, [
    "eventName",
    "event_name",
    "type",
  ]);

  const conversationId = pickFirst(payload, [
    "conversationId",
    "conversation_id",
  ]);

  const messageId = pickFirst(payload, [
    "messageId",
    "message.id",
    "id",
  ]);

  const text = pickFirst(payload, [
    "message.text",
    "text",
    "message.content",
    "content",
  ]);

  const fromPhone = pickFirst(payload, [
    "visitor.phone",
    "from",
    "contact.phone",
    "sender",
  ]);

  const toPhone = pickFirst(payload, ["to", "receiver", "agent.phone"]);

  return {
    eventName: eventName != null ? String(eventName) : null,
    conversationId: conversationId != null ? String(conversationId) : null,
    messageId: messageId != null ? String(messageId) : null,
    text: text != null ? String(text) : null,
    fromPhone: fromPhone != null ? String(fromPhone) : null,
    toPhone: toPhone != null ? String(toPhone) : null,
  };
}

router.post("/whatsapp", async (req, res) => {
  const configured = String(process.env.NOTIFICATION_WEBHOOK_SECRET || "").trim();
  if (!configured) {
    return res.status(503).json({ ok: false, error: "webhook_not_configured" });
  }

  const provided = getWebhookSecret(req);
  if (!provided || provided !== configured) {
    return res.status(401).json({ ok: false, error: "invalid_webhook_secret" });
  }

  try {
    const payload = req.body || {};
    const extracted = extractInboundFields(payload);

    await recordInboundMessage({
      provider: "brevo",
      channel: "whatsapp",
      rawPayload: payload,
      extracted: {
        eventName: extracted.eventName,
        conversationId: extracted.conversationId,
        messageId: extracted.messageId,
        fromPhone: extracted.fromPhone,
        toPhone: extracted.toPhone,
        text: extracted.text,
      },
    });

    return res.json({ ok: true });
  } catch (e) {
    console.error("[brevo/webhook] whatsapp inbound error:", e?.message || e);
    return res.status(500).json({ ok: false, error: "internal_error" });
  }
});

export default router;
