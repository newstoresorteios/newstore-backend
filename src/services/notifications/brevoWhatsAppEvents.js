// src/services/notifications/brevoWhatsAppEvents.js
import { query } from "../../db.js";
import { getTestRecipient } from "./brevoWhatsApp.js";
import {
  recordDeliveryCheckNoMatch,
  updateDispatchDeliveryStatus,
} from "./notificationLog.js";

export function maskPhone(phone) {
  if (phone == null) return null;
  const d = String(phone).replace(/\D/g, "");
  if (d.length < 4) return "****";
  return `${d.slice(0, 2)}****${d.slice(-4)}`;
}

export function normalizePhoneBR(phone) {
  if (phone == null) return null;
  const digits = String(phone).replace(/\D/g, "");
  if (!digits) return null;
  if (digits.startsWith("55") && (digits.length === 12 || digits.length === 13)) {
    return digits;
  }
  if (digits.length === 10 || digits.length === 11) {
    return `55${digits}`;
  }
  return null;
}

function getBaseUrl() {
  return (process.env.BREVO_WHATSAPP_BASE_URL || "https://api.brevo.com/v3").replace(
    /\/+$/,
    ""
  );
}

function mapBrevoApiError(body, statusCode) {
  const message = String(body?.message || "");
  if (
    body?.code === "unauthorized" &&
    message.toLowerCase().includes("unrecognised ip address")
  ) {
    return "brevo_ip_not_authorized";
  }
  return body?.code || body?.message || `brevo_http_${statusCode}`;
}

function resolveContactNumber(contactNumber) {
  const raw =
    contactNumber ||
    process.env.NOTIFICATION_TEST_WHATSAPP_TO ||
    getTestRecipient();
  return normalizePhoneBR(raw) || String(raw || "").replace(/\D/g, "") || null;
}

export function extractBrevoEvents(raw) {
  if (Array.isArray(raw)) return raw;
  if (Array.isArray(raw?.events)) return raw.events;
  if (Array.isArray(raw?.items)) return raw.items;
  if (Array.isArray(raw?.whatsappEvents)) return raw.whatsappEvents;
  if (Array.isArray(raw?.logs)) return raw.logs;
  if (Array.isArray(raw?.data)) return raw.data;
  return [];
}

export function classifyBrevoReason(reason) {
  const text = String(reason || "");
  if (text.includes("131049")) {
    return {
      code: "131049",
      type: "healthy_ecosystem_engagement",
      description:
        "WhatsApp/Meta did not deliver the message to maintain healthy ecosystem engagement.",
    };
  }
  return {
    code: null,
    type: null,
    description: null,
  };
}

export function normalizeBrevoEvent(e) {
  const event = e?.event || e?.type || e?.status || e?.eventType || null;
  let messageId = e?.messageId || e?.message_id || e?.id || null;
  if (messageId == null && e?.message?.id != null) {
    messageId = e.message.id;
  }

  const contactRaw =
    e?.contactNumber ||
    e?.contact_number ||
    e?.recipient ||
    e?.to ||
    e?.contact_number_to ||
    null;

  const reason = e?.reason || e?.error || e?.message || e?.description || null;
  const classification = classifyBrevoReason(reason);

  return {
    event: event != null ? String(event) : null,
    messageId: messageId != null ? String(messageId) : null,
    contactNumber: contactRaw != null ? normalizePhoneBR(contactRaw) : null,
    date:
      e?.date ||
      e?.createdAt ||
      e?.created_at ||
      e?.timestamp ||
      e?.eventDate ||
      null,
    reason: reason != null ? String(reason) : null,
    errorCode: classification.code,
    errorType: classification.type,
    errorDescription: classification.description,
    delivery_error_code: classification.code,
    delivery_error_type: classification.type,
    raw: e,
  };
}

export function mapBrevoEventToDeliveryStatus(eventName) {
  const e = String(eventName || "").toLowerCase();
  if (e === "delivered" || e === "delivery") return "delivered";
  if (e === "read") return "read";
  if (e === "sent") return "sent_to_provider";
  if (e === "failed" || e === "failure" || e === "undelivered") return "failed";
  if (e === "rejected" || e === "reject") return "rejected";
  if (e === "error") return "failed";
  if (e === "blocked") return "rejected";
  return "unknown";
}

function isProviderFailureEvent(matchedEvent) {
  const deliveryStatus = mapBrevoEventToDeliveryStatus(matchedEvent?.event);
  return deliveryStatus === "failed" || deliveryStatus === "rejected";
}

function buildDeliveryFailureErrorMessage(matchedEvent) {
  if (!matchedEvent) return "whatsapp_delivery_failed";
  if (matchedEvent.reason) return String(matchedEvent.reason);
  if (matchedEvent.errorDescription) return String(matchedEvent.errorDescription);
  return "whatsapp_delivery_failed";
}

function logProviderDeliveryError(matchedEvent, context = {}) {
  const eventLower = String(matchedEvent?.event || "").toLowerCase();
  if (eventLower !== "error" && matchedEvent?.errorCode !== "131049") return;

  console.warn("[brevo.whatsapp.events] provider-error", {
    ...context,
    event: matchedEvent?.event || null,
    errorCode: matchedEvent?.errorCode || null,
    errorType: matchedEvent?.errorType || null,
    reason: matchedEvent?.reason || null,
    messageId: matchedEvent?.messageId || null,
  });

  if (matchedEvent?.errorCode === "131049") {
    console.warn("[brevo.whatsapp.events] meta-error-131049", {
      ...context,
      code: "131049",
      type: "healthy_ecosystem_engagement",
      reason: matchedEvent?.reason || null,
      messageId: matchedEvent?.messageId || null,
    });
  }
}

function mapRecentErrorEvents(events) {
  return (events || [])
    .filter((ev) => String(ev.event || "").toLowerCase() === "error")
    .slice(0, 5)
    .map((ev) => ({
      messageId: ev.messageId,
      date: ev.date,
      reason: ev.reason,
      errorCode: ev.errorCode,
      errorType: ev.errorType,
    }));
}

export function findEventForMessageId(events, messageId) {
  if (!messageId || !Array.isArray(events)) return null;
  const target = String(messageId).trim();
  return (
    events.find((ev) => ev.messageId && String(ev.messageId) === target) ||
    events.find(
      (ev) =>
        ev.raw?.messageId && String(ev.raw.messageId) === target
    ) ||
    null
  );
}

export async function fetchBrevoWhatsAppEvents({
  contactNumber,
  days = 1,
  limit = 50,
  offset = 0,
  event = null,
} = {}) {
  const apiKey = String(process.env.BREVO_API_KEY || "").trim();
  const normalizedContactNumber = resolveContactNumber(contactNumber);

  if (!apiKey) {
    return {
      ok: false,
      error: "missing_brevo_api_key",
      reason: null,
      statusCode: null,
      contactNumber: normalizedContactNumber,
      events: [],
      raw: null,
    };
  }

  if (!normalizedContactNumber) {
    return {
      ok: false,
      error: "missing_contact_number",
      reason: null,
      statusCode: null,
      contactNumber: null,
      events: [],
      raw: null,
    };
  }

  console.log("[brevo.whatsapp.events] fetch:start", {
    contactNumber: maskPhone(normalizedContactNumber),
    days,
    limit,
    offset,
    event: event || null,
  });

  const baseUrl = getBaseUrl();
  const timeoutMs = Number(process.env.BREVO_WHATSAPP_TIMEOUT_MS) || 15000;
  const params = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
    days: String(days),
    contactNumber: normalizedContactNumber,
    sort: "desc",
  });
  if (event) params.set("event", String(event));

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(
      `${baseUrl}/whatsapp/statistics/events?${params.toString()}`,
      {
        method: "GET",
        headers: {
          accept: "application/json",
          "api-key": apiKey,
        },
        signal: controller.signal,
      }
    );

    clearTimeout(timer);
    const text = await res.text();
    let body = {};
    try {
      body = text ? JSON.parse(text) : {};
    } catch {
      body = { raw: text };
    }

    const statusCode = res.status;

    if (!res.ok) {
      const err = mapBrevoApiError(body, statusCode);
      console.warn("[brevo.whatsapp.events] fetch:failed", {
        statusCode,
        contactNumber: maskPhone(normalizedContactNumber),
        error: body?.message || body?.code || "brevo_events_request_failed",
        reason: body?.reason || null,
        response: body,
      });
      return {
        ok: false,
        error: err,
        reason: body?.reason || null,
        statusCode,
        contactNumber: normalizedContactNumber,
        events: [],
        raw: body,
      };
    }

    const list = extractBrevoEvents(body);
    const events = list.map(normalizeBrevoEvent);

    console.log("[brevo.whatsapp.events] fetch:done", {
      statusCode,
      contactNumber: maskPhone(normalizedContactNumber),
      count: events.length,
      raw_keys: body && typeof body === "object" ? Object.keys(body) : [],
      events_sample: events.slice(0, 3).map((ev) => ({
        event: ev.event,
        messageId: ev.messageId,
        date: ev.date,
        reason: ev.reason || null,
        errorCode: ev.errorCode || null,
      })),
      error_131049_count: events.filter((ev) => ev.errorCode === "131049").length,
    });

    return {
      ok: true,
      statusCode,
      contactNumber: normalizedContactNumber,
      events,
      raw: body,
      error: null,
      reason: null,
    };
  } catch (err) {
    clearTimeout(timer);
    console.error("[brevo.whatsapp.events] fetch:error", {
      contactNumber: maskPhone(normalizedContactNumber),
      error: err?.message || null,
      reason: null,
    });
    return {
      ok: false,
      error: err?.message || "brevo_fetch_failed",
      reason: null,
      statusCode: null,
      contactNumber: normalizedContactNumber,
      events: [],
      raw: null,
    };
  }
}

export async function syncDispatchDeliveryStatus(dispatchId, { days = 7, pgClient } = {}) {
  const d = await (pgClient
    ? pgClient.query(
        `SELECT * FROM public.notification_dispatches WHERE id = $1::uuid LIMIT 1`,
        [dispatchId]
      )
    : query(
        `SELECT * FROM public.notification_dispatches WHERE id = $1::uuid LIMIT 1`,
        [dispatchId]
      ));

  const dispatch = d.rows[0];
  if (!dispatch) {
    return { ok: false, error: "not_found" };
  }

  if (dispatch.channel !== "whatsapp" || dispatch.provider !== "brevo") {
    return {
      ok: false,
      error: "invalid_dispatch_provider",
      dispatch,
      matched: false,
      message: "Dispatch não é WhatsApp/Brevo.",
    };
  }

  if (!dispatch.provider_message_id) {
    return {
      ok: false,
      error: "missing_provider_message_id",
      dispatch,
      matched: false,
      message: "Dispatch sem provider_message_id.",
    };
  }

  const contactNumber =
    normalizePhoneBR(dispatch.recipient) ||
    String(dispatch.recipient || "").replace(/\D/g, "");

  const eventsResult = await fetchBrevoWhatsAppEvents({
    contactNumber,
    days,
    limit: 50,
    offset: 0,
  });

  if (!eventsResult.ok) {
    return {
      ok: false,
      error: eventsResult.error,
      reason: eventsResult.reason,
      dispatch,
      matched: false,
      events_checked: 0,
    };
  }

  const events = eventsResult.events || [];
  const matched = findEventForMessageId(events, dispatch.provider_message_id);
  const rawPayload = {
    brevo_raw: eventsResult.raw,
    events_normalized: events,
    events_checked: events.length,
    synced_at: new Date().toISOString(),
  };

  if (!matched) {
    const updated = await recordDeliveryCheckNoMatch({
      pgClient,
      dispatchId,
      rawEvents: rawPayload,
      eventsChecked: events.length,
    });
    return {
      ok: true,
      matched: false,
      dispatch: updated,
      matched_event: null,
      events_checked: events.length,
      events,
      status_updated_to: updated?.status,
      message: "Nenhum evento correspondente encontrado na Brevo.",
    };
  }

  const deliveryStatus = mapBrevoEventToDeliveryStatus(matched.event);
  logProviderDeliveryError(matched, {
    dispatch_id: dispatchId,
    provider_message_id: dispatch.provider_message_id,
  });

  const errorMessage = isProviderFailureEvent(matched)
    ? buildDeliveryFailureErrorMessage(matched)
    : null;

  const updated = await updateDispatchDeliveryStatus({
    pgClient,
    dispatchId,
    deliveryStatus,
    matchedEvent: matched,
    rawEvents: rawPayload,
    errorMessage,
  });

  const userMessage =
    matched.errorCode === "131049"
      ? "Envio aceito pela Brevo, mas o WhatsApp/Meta bloqueou a entrega. Código 131049: healthy ecosystem engagement."
      : `Evento encontrado: ${matched.event} → delivery_status=${deliveryStatus}`;

  return {
    ok: true,
    matched: true,
    dispatch: updated,
    matched_event: matched,
    events_checked: events.length,
    events,
    status_updated_to: updated?.status,
    delivery_status_updated_to: updated?.delivery_status,
    message: userMessage,
  };
}

export async function runTestWhatsAppDeliveryCheck({
  dispatchId,
  messageId,
  contactNumber,
  pgClient,
  delayMs = 1500,
}) {
  if (delayMs > 0) {
    await new Promise((r) => setTimeout(r, delayMs));
  }

  const eventsResult = await fetchBrevoWhatsAppEvents({
    contactNumber: contactNumber || getTestRecipient(),
    days: 1,
    limit: 50,
    offset: 0,
  });

  if (!eventsResult.ok) {
    return {
      checked: true,
      matched: false,
      events_checked: 0,
      error: eventsResult.error,
      reason: eventsResult.reason || null,
      message: `Falha ao consultar eventos Brevo: ${eventsResult.error}`,
    };
  }

  const events = eventsResult.events || [];
  const matched = findEventForMessageId(events, messageId);
  const rawPayload = {
    brevo_raw: eventsResult.raw,
    events_normalized: events,
    events_checked: events.length,
    checked_at: new Date().toISOString(),
    message_id: messageId,
  };

  if (!matched) {
    const recent_errors = mapRecentErrorEvents(events);

    if (recent_errors.length > 0) {
      console.warn("[admin/notifications] test-whatsapp:delivery-check:recent-errors", {
        dispatch_id: dispatchId,
        messageId,
        recent_errors_count: recent_errors.length,
        first_error_code: recent_errors[0]?.errorCode || null,
        first_error_reason: recent_errors[0]?.reason || null,
      });
    }

    await recordDeliveryCheckNoMatch({
      pgClient,
      dispatchId,
      rawEvents: {
        ...rawPayload,
        recent_errors,
      },
      eventsChecked: events.length,
    });

    const has131049 = recent_errors.some((e) => e.errorCode === "131049");
    const message = has131049
      ? "Envio aceito pela Brevo, mas o WhatsApp/Meta bloqueou a entrega. Código 131049: healthy ecosystem engagement. Nenhum evento vinculado a este messageId ainda."
      : "Brevo accepted the message, but no delivery event for this messageId was found yet. Recent errors exist for this contact.";

    return {
      checked: true,
      matched: false,
      events_checked: events.length,
      matched_event: null,
      recent_errors,
      message,
    };
  }

  const deliveryStatus = mapBrevoEventToDeliveryStatus(matched.event);
  logProviderDeliveryError(matched, {
    dispatch_id: dispatchId,
    messageId,
  });

  const updated = await updateDispatchDeliveryStatus({
    pgClient,
    dispatchId,
    deliveryStatus,
    matchedEvent: matched,
    rawEvents: rawPayload,
    errorMessage: isProviderFailureEvent(matched)
      ? buildDeliveryFailureErrorMessage(matched)
      : null,
  });

  const message =
    matched.errorCode === "131049"
      ? "Envio aceito pela Brevo, mas o WhatsApp/Meta bloqueou a entrega. Código 131049: healthy ecosystem engagement."
      : `Evento encontrado na Brevo: ${matched.event}`;

  return {
    checked: true,
    matched: true,
    events_checked: events.length,
    matched_event: matched,
    delivery_status: updated?.delivery_status || deliveryStatus,
    dispatch_status: updated?.status,
    message,
  };
}
