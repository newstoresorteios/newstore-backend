import { query } from "../../db.js";
import {
  getActivePushRuleByEventKey,
  getAllowedPushRuleEvents,
} from "./pushRules.js";

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
  metadata = {},
  actor = null,
} = {}) {
  const key = validateEventKey(eventKey);
  const safeSource = cleanText(source, "engine").slice(0, 80) || "engine";
  const safeMetadata = sanitizeMetadata(metadata);
  const safeActor = actor && typeof actor === "object"
    ? sanitizeMetadataValue(actor)
    : null;

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
        dry_run: true,
        source: safeSource,
        reason: "rule_inactive_or_not_found",
        metadata: safeMetadata,
        actor: safeActor,
      },
    });
    return { ok: true, event_key: key, status: "skipped", dispatch };
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
      rule_id: rule.id,
      metadata: safeMetadata,
      actor: safeActor,
    },
  });

  return { ok: true, event_key: key, status: "dry_run", dispatch };
}
