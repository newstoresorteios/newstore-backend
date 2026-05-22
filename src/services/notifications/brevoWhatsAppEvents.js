// src/services/notifications/brevoWhatsAppEvents.js
import { query } from "../../db.js";
import { getTestRecipient, normalizePhoneBR } from "./brevoWhatsApp.js";

export { normalizePhoneBR };

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

function digitsOnly(phone) {
  if (phone == null) return null;
  const d = String(phone).replace(/\D/g, "");
  return d || null;
}

function resolveContactNumber(contactNumber) {
  const raw =
    contactNumber ||
    process.env.NOTIFICATION_TEST_WHATSAPP_TO ||
    getTestRecipient();
  return digitsOnly(raw) || normalizePhoneBR(raw);
}

function pickFirst(obj, keys) {
  for (const k of keys) {
    if (obj?.[k] != null && obj[k] !== "") return obj[k];
  }
  return null;
}

function normalizeEventRow(ev) {
  let messageId = pickFirst(ev, ["messageId", "message_id", "id"]);
  if (messageId == null && ev?.message?.id != null) {
    messageId = String(ev.message.id);
  }

  const event = pickFirst(ev, ["event", "type", "status"]);

  const contactRaw = pickFirst(ev, [
    "contactNumber",
    "contact_number",
    "recipient",
    "to",
    "phone",
  ]);

  return {
    event: event != null ? String(event) : null,
    messageId: messageId != null ? String(messageId) : null,
    contactNumber: digitsOnly(contactRaw) || null,
    date: pickFirst(ev, ["date", "createdAt", "created_at", "timestamp"]) || null,
    reason: pickFirst(ev, ["reason", "error", "message"]) || null,
    raw: ev,
  };
}

function extractEventsList(body) {
  if (Array.isArray(body)) return body;
  if (Array.isArray(body?.events)) return body.events;
  if (Array.isArray(body?.items)) return body.items;
  if (Array.isArray(body?.whatsappEvents)) return body.whatsappEvents;
  if (Array.isArray(body?.logs)) return body.logs;
  return [];
}

export async function fetchBrevoWhatsAppEvents({
  contactNumber,
  days = 1,
  limit = 50,
  offset = 0,
  event = null,
} = {}) {
  const apiKey = String(process.env.BREVO_API_KEY || "").trim();
  if (!apiKey) {
    return {
      ok: false,
      error: "missing_brevo_api_key",
      statusCode: null,
      contactNumber: null,
      events: [],
      raw: null,
    };
  }

  const contact = resolveContactNumber(contactNumber);
  if (!contact) {
    return {
      ok: false,
      error: "missing_contact_number",
      statusCode: null,
      contactNumber: null,
      events: [],
      raw: null,
    };
  }

  console.log("[brevo.whatsapp.events] fetch:start", {
    contactNumber: contact,
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
    contactNumber: contact,
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
      const error = mapBrevoApiError(body, statusCode);
      console.warn("[brevo.whatsapp.events] fetch:failed", {
        statusCode,
        error,
      });
      return {
        ok: false,
        error,
        statusCode,
        contactNumber: contact,
        events: [],
        raw: body,
      };
    }

    const list = extractEventsList(body);
    const events = list.map(normalizeEventRow);

    console.log("[brevo.whatsapp.events] fetch:done", {
      statusCode,
      count: events.length,
    });

    return {
      ok: true,
      statusCode,
      contactNumber: contact,
      events,
      raw: body,
    };
  } catch (err) {
    clearTimeout(timer);
    const error = err?.message || "brevo_fetch_failed";
    console.warn("[brevo.whatsapp.events] fetch:failed", {
      statusCode: null,
      error,
    });
    return {
      ok: false,
      error,
      statusCode: null,
      contactNumber: contact,
      events: [],
      raw: null,
    };
  }
}

function mapEventToDispatchStatus(eventName) {
  const e = String(eventName || "").toLowerCase();
  if (e === "delivered" || e === "delivery") return "delivered";
  if (e === "read") return "read";
  if (e === "failed" || e === "failure" || e === "undelivered") return "failed";
  if (e === "rejected" || e === "reject") return "rejected";
  if (e === "blocked") return "blocked";
  if (e === "error") return "error";
  return null;
}

function findMatchingEvent(events, providerMessageId) {
  if (!providerMessageId) return null;
  const target = String(providerMessageId).trim();
  return (
    events.find((ev) => ev.messageId && String(ev.messageId) === target) ||
    events.find(
      (ev) =>
        ev.raw?.messageId &&
        String(ev.raw.messageId) === target
    ) ||
    null
  );
}

export async function syncDispatchDeliveryStatus(dispatchId, { days = 7 } = {}) {
  const d = await query(
    `SELECT * FROM public.notification_dispatches WHERE id = $1 LIMIT 1`,
    [dispatchId]
  );
  const dispatch = d.rows[0];
  if (!dispatch) {
    return { ok: false, error: "not_found" };
  }

  if (dispatch.channel !== "whatsapp" || dispatch.provider !== "brevo") {
    return {
      ok: false,
      error: "invalid_dispatch_provider",
      dispatch,
      matched_event: null,
      events_checked: 0,
      message: "Dispatch não é WhatsApp/Brevo.",
    };
  }

  if (!dispatch.provider_message_id) {
    return {
      ok: false,
      error: "missing_provider_message_id",
      dispatch,
      matched_event: null,
      events_checked: 0,
      message: "Dispatch sem provider_message_id.",
    };
  }

  const contactNumber =
    digitsOnly(dispatch.recipient) ||
    normalizePhoneBR(dispatch.recipient);

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
      dispatch,
      matched_event: null,
      events_checked: 0,
    };
  }

  const events = eventsResult.events || [];
  const matched = findMatchingEvent(events, dispatch.provider_message_id);
  const prevResponse =
    dispatch.response && typeof dispatch.response === "object"
      ? dispatch.response
      : {};

  const responsePayload = {
    previous_response: prevResponse,
    delivery_event: matched,
    delivery_checked_at: new Date().toISOString(),
    events_checked: events.length,
  };

  if (!matched) {
    const updated = await query(
      `UPDATE public.notification_dispatches
          SET response = $2::jsonb
        WHERE id = $1
        RETURNING *`,
      [dispatchId, JSON.stringify(responsePayload)]
    );
    return {
      ok: true,
      dispatch: updated.rows[0],
      matched_event: null,
      events_checked: events.length,
      status_updated_to: dispatch.status,
      message: "Nenhum evento correspondente encontrado na Brevo.",
    };
  }

  const newStatus = mapEventToDispatchStatus(matched.event);
  const failureStatuses = new Set(["failed", "rejected", "blocked", "error"]);
  const errorMessage = failureStatuses.has(newStatus)
    ? matched.reason || matched.event
    : null;

  let statusUpdatedTo = dispatch.status;

  if (newStatus) {
    statusUpdatedTo = newStatus;
    const updated = await query(
      `UPDATE public.notification_dispatches
          SET status = $2,
              response = $3::jsonb,
              error_message = $4
        WHERE id = $1
        RETURNING *`,
      [
        dispatchId,
        newStatus,
        JSON.stringify(responsePayload),
        errorMessage,
      ]
    );
    return {
      ok: true,
      dispatch: updated.rows[0],
      matched_event: matched,
      events_checked: events.length,
      status_updated_to: statusUpdatedTo,
      message: `Status atualizado para ${statusUpdatedTo}.`,
    };
  }

  const updated = await query(
    `UPDATE public.notification_dispatches
        SET response = $2::jsonb
      WHERE id = $1
      RETURNING *`,
    [dispatchId, JSON.stringify(responsePayload)]
  );

  return {
    ok: true,
    dispatch: updated.rows[0],
    matched_event: matched,
    events_checked: events.length,
    status_updated_to: statusUpdatedTo,
    message: "Evento encontrado, mas status não alterado.",
  };
}
