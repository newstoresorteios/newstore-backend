import { query } from "../../db.js";
import {
  getActivePushRuleByEventKey,
  getAllowedPushRuleEvents,
} from "./pushRules.js";
import { sendPushToSubscriptionRow } from "./pushNotifications.js";

const MAX_METADATA_BYTES = 4096;
const SENSITIVE_KEY_RE = /(password|secret|token|authorization|cookie|endpoint|p256dh|auth|vapid|private|cpf|document|phone|telefone|whatsapp)/i;

function coded(code) {
  const err = new Error(code);
  err.code = code;
  return err;
}

function cleanText(value, fallback = "") {
  return String(value ?? fallback).trim();
}

function sanitizeMetadataValue(value, depth = 0) {
  if (depth > 4) return "[truncated]";
  if (value == null) return value;
  if (typeof value === "string") return value.slice(0, 500);
  if (typeof value === "number" || typeof value === "boolean") return value;
  if (Array.isArray(value)) {
    return value.slice(0, 25).map((item) => sanitizeMetadataValue(item, depth + 1));
  }
  if (typeof value === "object") {
    const out = {};
    for (const [key, child] of Object.entries(value).slice(0, 50)) {
      const cleanKey = String(key || "").slice(0, 80);
      if (!cleanKey || SENSITIVE_KEY_RE.test(cleanKey)) continue;
      out[cleanKey] = sanitizeMetadataValue(child, depth + 1);
    }
    return out;
  }
  return String(value).slice(0, 200);
}

function sanitizeMetadata(metadata) {
  if (metadata == null) return {};
  if (typeof metadata !== "object" || Array.isArray(metadata)) {
    throw coded("push_event_metadata_invalid");
  }
  const sanitized = sanitizeMetadataValue(metadata);
  const json = JSON.stringify(sanitized);
  if (json.length > MAX_METADATA_BYTES) {
    throw coded("push_event_metadata_too_large");
  }
  return sanitized;
}

function validateEventKey(eventKey) {
  const key = cleanText(eventKey);
  if (!key) throw coded("push_event_key_required");
  if (!getAllowedPushRuleEvents().includes(key)) throw coded("push_event_key_invalid");
  return key;
}

function parseCsvEnv(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function isTrue(value) {
  return String(value || "").trim() === "true";
}

function validateReference({ referenceKey, requireReferenceKey }) {
  const key = cleanText(referenceKey).slice(0, 200);
  if (requireReferenceKey && !key) throw coded("push_reference_key_required");
  if (key && !/^[a-zA-Z0-9:_./-]+$/.test(key)) throw coded("push_reference_key_invalid");
  return key || null;
}

function validateReferenceType(referenceType) {
  const type = cleanText(referenceType).slice(0, 80);
  if (type && !/^[a-zA-Z0-9:_-]+$/.test(type)) throw coded("push_reference_type_invalid");
  return type || null;
}

function assertAutomationRealSendAllowed(eventKey) {
  if (process.env.PUSH_ENABLED !== "true") throw coded("push_disabled");
  if (process.env.PUSH_MODE !== "single_device_test") {
    throw coded("push_mode_not_single_device_test");
  }
  if (process.env.PUSH_TEST_ONLY !== "true") throw coded("push_test_only_required");
  if (
    process.env.NODE_ENV === "production" &&
    process.env.PUSH_ALLOW_PRODUCTION_SEND !== "true"
  ) {
    throw coded("push_production_send_blocked");
  }
  if (!isTrue(process.env.PUSH_ENGINE_ALLOW_REAL_SEND)) {
    throw coded("push_engine_real_send_disabled");
  }
  if (!isTrue(process.env.PUSH_ALLOW_ENGINE_EVENTS)) {
    throw coded("push_engine_events_disabled");
  }
  const allowed = parseCsvEnv(process.env.PUSH_ENGINE_REAL_SEND_EVENT_KEYS);
  if (!allowed.includes(eventKey)) throw coded("push_engine_event_not_allowed_for_real_send");
  if (process.env.PUSH_ALLOW_DB_RECIPIENT_LOOKUP === "true") {
    throw coded("push_db_recipient_lookup_must_remain_disabled");
  }
  if (process.env.PUSH_ALLOW_AUDIENCE === "true") throw coded("push_audience_blocked");
  if (process.env.PUSH_ALLOW_ADMIN_MASS_SEND === "true") throw coded("push_mass_send_blocked");
  if (process.env.PUSH_ALLOW_CAMPAIGNS === "true") throw coded("push_campaign_blocked");
}

async function insertAutomationDispatch({
  eventKey,
  status,
  category,
  title,
  body,
  url,
  payload,
}) {
  const result = await query(
    `INSERT INTO public.notification_push_dispatches (
       event_key, category, title, body, url, payload, status
     ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
     RETURNING id, event_key, status, created_at`,
    [
      eventKey,
      category || "operational",
      title,
      body,
      url || null,
      JSON.stringify(payload || {}),
      status,
    ]
  );
  return result.rows?.[0] || null;
}

export async function handlePushAutomationEvent({
  eventKey,
  source = "engine",
  referenceType = null,
  referenceKey = null,
  metadata = {},
  actor = null,
  dryRun = true,
} = {}) {
  const key = validateEventKey(eventKey);
  const safeSource = cleanText(source, "engine").slice(0, 80) || "engine";
  const safeMetadata = sanitizeMetadata(metadata);
  const safeReferenceType = validateReferenceType(referenceType);
  const requireReferenceKey = !dryRun && isTrue(process.env.PUSH_AUTOMATION_REQUIRE_REFERENCE_KEY);
  const safeReferenceKey = validateReference({ referenceKey, requireReferenceKey });
  const safeActor = actor && typeof actor === "object"
    ? sanitizeMetadataValue(actor)
    : null;

  if (!dryRun) {
    assertAutomationRealSendAllowed(key);
    const dedupe = await query(
      `SELECT id
         FROM public.notification_push_dispatches
        WHERE event_key = $1
          AND payload->>'reference_key' = $2
          AND status = 'sent'
        LIMIT 1`,
      [key, safeReferenceKey]
    );
    if (dedupe.rows?.[0]) {
      return {
        ok: true,
        event_key: key,
        status: "deduped",
        deduped: true,
        reference_key: safeReferenceKey,
      };
    }
  }

  const rule = await getActivePushRuleByEventKey(key);

  if (!rule) {
    const dispatch = await insertAutomationDispatch({
      eventKey: key,
      status: "skipped",
      category: "operational",
      title: "Push automático ignorado",
      body: "Regra inativa ou não encontrada.",
      url: null,
      payload: {
        dry_run: dryRun,
        source: safeSource,
        reference_type: safeReferenceType,
        reference_key: safeReferenceKey,
        reason: "rule_inactive_or_not_found",
        metadata: safeMetadata,
        actor: safeActor,
      },
    });
    return { ok: true, event_key: key, status: "skipped", dispatch };
  }

  if (!dryRun) {
    const recipients = await query(
      `SELECT *
         FROM public.push_subscriptions
        WHERE is_active = true
          AND operational_opt_in = true
        ORDER BY updated_at DESC, created_at DESC`
    );

    let sent = 0;
    let failed = 0;
    for (const row of recipients.rows || []) {
      try {
        await sendPushToSubscriptionRow({
          subscriptionRow: row,
          title: rule.title_template,
          body: rule.body_template,
          url: rule.url_template || "/",
          payload: {
            source: safeSource,
            reference_type: safeReferenceType,
            reference_key: safeReferenceKey,
            metadata: safeMetadata,
            automation: true,
            real_send: true,
          },
          source: "push_automation",
          eventKey: key,
          category: rule.category || "operational",
          requireConfiguredSubscription: false,
          skipModeAssert: true,
        });
        sent += 1;
      } catch {
        failed += 1;
      }
    }
    return {
      ok: true,
      event_key: key,
      status: sent > 0 ? "sent" : "failed",
      attempted: sent + failed,
      sent,
      failed,
      reference_key: safeReferenceKey,
    };
  }

  const dispatch = await insertAutomationDispatch({
    eventKey: key,
    status: "dry_run",
    category: rule.category || "operational",
    title: rule.title_template,
    body: rule.body_template,
    url: rule.url_template || null,
    payload: {
      dry_run: true,
      source: safeSource,
      reference_type: safeReferenceType,
      reference_key: safeReferenceKey,
      rule_id: rule.id,
      metadata: safeMetadata,
      actor: safeActor,
    },
  });

  return { ok: true, event_key: key, status: "dry_run", dispatch };
}
