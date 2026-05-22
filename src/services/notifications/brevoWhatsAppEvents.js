// src/services/notifications/brevoWhatsAppEvents.js
import { query } from "../../db.js";
import { getTestRecipient, normalizePhoneBR } from "./brevoWhatsApp.js";

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

  return {
    event: event != null ? String(event) : null,
    messageId: messageId != null ? String(messageId) : null,
    contactNumber:
      digitsOnly(
        pickFirst(ev, ["contactNumber", "contact_number", "phone", "to"])
      ) || null,
    date: pickFirst(ev, ["date", "createdAt", "created_at", "timestamp"]) || null,
    reason: pickFirst(ev, ["reason", "error", "description"]) || null,
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
      return {
        ok: false,
        error: mapBrevoApiError(body, statusCode),
        statusCode,
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
      events,
      raw: body,
    };
  } catch (err) {
    clearTimeout(timer);
    return {
      ok: false,
      error: err?.message || "brevo_fetch_failed",
      statusCode: null,
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
  if (e === "accepted" || e === "sent") return "accepted";
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

  const syncMeta = {
    synced_at: new Date().toISOString(),
    matched_event: matched,
    events_checked_count: events.length,
    brevo_status_code: eventsResult.statusCode,
  };

  if (!matched) {
    const updated = await query(
      `UPDATE public.notification_dispatches
          SET response = $2::jsonb
        WHERE id = $1
        RETURNING *`,
      [
        dispatchId,
        JSON.stringify({
          ...prevResponse,
          brevo_delivery_sync: syncMeta,
        }),
      ]
    );
    return {
      ok: true,
      dispatch: updated.rows[0],
      matched_event: null,
      events_checked: events.length,
      message: "Nenhum evento correspondente encontrado na Brevo.",
    };
  }

  const newStatus = mapEventToDispatchStatus(matched.event);
  const errorMessage =
    newStatus === "failed" || newStatus === "rejected"
      ? matched.reason || matched.event
      : null;

  let updated;
  if (newStatus) {
    updated = await query(
      `UPDATE public.notification_dispatches
          SET status = $2,
              response = $3::jsonb,
              error_message = COALESCE($4, error_message)
        WHERE id = $1
        RETURNING *`,
      [
        dispatchId,
        newStatus,
        JSON.stringify({
          ...prevResponse,
          brevo_delivery_sync: syncMeta,
          brevo_matched_event: matched,
        }),
        errorMessage,
      ]
    );
  } else {
    updated = await query(
      `UPDATE public.notification_dispatches
          SET response = $2::jsonb
        WHERE id = $1
        RETURNING *`,
      [
        dispatchId,
        JSON.stringify({
          ...prevResponse,
          brevo_delivery_sync: syncMeta,
          brevo_matched_event: matched,
        }),
      ]
    );
  }

  return {
    ok: true,
    dispatch: updated.rows[0],
    matched_event: matched,
    events_checked: events.length,
  };
}
