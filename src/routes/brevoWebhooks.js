// src/routes/brevoWebhooks.js
import express from "express";
import { recordInboundMessage } from "../services/notifications/notificationLog.js";
import {
  classifyBrevoWebhookPayload,
  extractBrevoEvents,
  extractBrevoInboundFields,
  maskPhone,
  updateDispatchDeliveryStatusByProviderMessageId,
} from "../services/notifications/brevoWhatsAppEvents.js";

const router = express.Router();

function getWebhookSecret(req) {
  const header = req.headers["x-webhook-secret"];
  if (header) return String(header).trim();
  if (req.query?.secret) return String(req.query.secret).trim();
  return null;
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
    const payloadItems = extractBrevoEvents(payload);
    const items = payloadItems.length ? payloadItems : [payload];
    const results = [];

    for (const item of items) {
      const classified = classifyBrevoWebhookPayload(item);
      const event = classified.event;

      console.log("[brevo-webhook] received", {
        kind: classified.kind,
        event: event?.event || null,
        has_message_id: Boolean(event?.messageId),
        contact_masked: event?.contactNumber ? maskPhone(event.contactNumber) : null,
      });

      if (classified.kind === "delivery") {
        if (!event?.messageId) {
          console.warn("[brevo-webhook] missing_message_id", {
            event: event?.event || null,
          });
          results.push({
            kind: "delivery",
            correlated: false,
            reason: "delivery_event_missing_message_id",
          });
          continue;
        }

        console.log("[brevo-webhook] delivery_event", {
          event: event.event || null,
          message_id_present: true,
        });

        const result = await updateDispatchDeliveryStatusByProviderMessageId({
          providerMessageId: event.messageId,
          matchedEvent: event,
          rawPayload: item,
        });

        if (result.matched) {
          console.log("[brevo-webhook] dispatch_matched", {
            dispatch_id: result.dispatch_id || result.dispatch?.id || null,
            delivery_status: result.delivery_status || null,
          });
        } else {
          console.warn("[brevo-webhook] dispatch_not_found", {
            message_id_present: true,
            event: event.event || null,
          });
        }

        results.push({
          kind: "delivery",
          correlated: result.matched === true,
          dispatch_id: result.dispatch_id || result.dispatch?.id || null,
          delivery_status: result.delivery_status || null,
        });
        continue;
      }

      if (classified.kind !== "inbound") {
        if (!event?.messageId) {
          console.warn("[brevo-webhook] missing_message_id", {
            event: event?.event || null,
            kind: classified.kind,
          });
        }
        results.push({
          kind: classified.kind,
          ignored: true,
          reason: "unknown_webhook_payload",
        });
        continue;
      }

      const extracted = extractBrevoInboundFields(item);

      console.log("[brevo-webhook] inbound_message", {
        event: extracted.eventName || null,
        from_masked: extracted.fromPhone ? maskPhone(extracted.fromPhone) : null,
        to_masked: extracted.toPhone ? maskPhone(extracted.toPhone) : null,
        has_text: Boolean(extracted.text),
      });

      await recordInboundMessage({
        provider: "brevo",
        channel: "whatsapp",
        rawPayload: item,
        extracted: {
          eventName: extracted.eventName,
          conversationId: extracted.conversationId,
          messageId: extracted.messageId,
          fromPhone: extracted.fromPhone,
          toPhone: extracted.toPhone,
          text: extracted.text,
        },
      });
      results.push({ kind: "inbound", stored: true });
    }

    if (!results.length) {
      console.warn("[brevo-webhook] missing_message_id", {
        kind: "empty_payload",
      });
      return res.json({
        ok: true,
        correlated: false,
        message: "webhook_payload_empty",
      });
    }

    return res.json({
      ok: true,
      processed: results.length,
      results,
    });
  } catch (e) {
    console.error("[brevo/webhook] whatsapp inbound error:", e?.message || e);
    return res.status(500).json({ ok: false, error: "internal_error" });
  }
});

export default router;
