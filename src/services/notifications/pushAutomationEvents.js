import { query } from "../../db.js";
import {
  getActivePushRuleByEventKey,
  getAllowedPushRuleEvents,
} from "./pushRules.js";
import { sendPushToSubscriptionRow } from "./pushNotifications.js";
import { handleWhatsAppAutomationEvent } from "./whatsappAutomationEvents.js";

const MAX_METADATA_BYTES = 4096;
const MAX_RECIPIENT_USER_IDS = 500;
const SENSITIVE_KEY_RE = /(password|secret|token|authorization|cookie|endpoint|p256dh|auth|vapid|private|cpf|document|phone|telefone|whatsapp)/i;

const PUBLIC_EVENT_KEYS = new Set([
  "NEW_DRAW_PUBLISHED",
  "DRAW_REMAINING_NUMBERS_75",
  "DRAW_REMAINING_NUMBERS_50",
  "DRAW_REMAINING_NUMBERS_20",
  "DRAW_REMAINING_NUMBERS_10",
  "WINNER_DEFINED",
]);

const USER_EVENT_KEYS = new Set([
  "BALANCE_EXPIRING_30_DAYS",
  "BALANCE_EXPIRING_15_DAYS",
  "BALANCE_EXPIRING_10_DAYS",
  "BALANCE_EXPIRING_7_DAYS",
  "BALANCE_EXPIRED",
]);

function coded(code) {
  const err = new Error(code);
  err.code = code;
  return err;
}

function cleanText(value, fallback = "") {
  return String(value ?? fallback).trim();
}

function sanitizeRecipientUserIds(value) {
  if (value == null) return [];
  if (!Array.isArray(value)) throw coded("push_recipient_user_ids_invalid");
  const seen = new Set();
  const out = [];
  for (const item of value) {
    const n = Number(item);
    if (!Number.isInteger(n) || n <= 0) continue;
    if (seen.has(n)) continue;
    seen.add(n);
    out.push(n);
    if (out.length >= MAX_RECIPIENT_USER_IDS) break;
  }
  return out;
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

function escapeTemplateValue(value) {
  if (value == null) return "";
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value).replace(/[<>]/g, "").slice(0, 500);
  }
  return "";
}

function renderTemplate(template, metadata = {}) {
  return String(template || "").replace(/{{\s*([a-zA-Z0-9_]+)\s*}}/g, (_match, key) => {
    if (!Object.prototype.hasOwnProperty.call(metadata, key)) return "";
    return escapeTemplateValue(metadata[key]);
  });
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

function envBool(name, defaultValue) {
  const raw = process.env[name];
  if (raw == null || raw === "") return defaultValue;
  return String(raw).trim() === "true";
}

function envPositiveInt(name, defaultValue) {
  const n = Number(process.env[name]);
  if (!Number.isFinite(n) || n <= 0) return defaultValue;
  return Math.trunc(n);
}

function validateReference({ referenceKey, requireReferenceKey }) {
  const key = cleanText(referenceKey).slice(0, 200);
  if (requireReferenceKey && !key) throw coded("push_reference_key_required");
  if (key && !/^[a-zA-Z0-9:_./-]+$/.test(key)) throw coded("push_reference_key_invalid");
  return key || null;
}

function validateScanId(scanId) {
  const clean = cleanText(scanId).slice(0, 120);
  if (clean && !/^[a-zA-Z0-9:_./-]+$/.test(clean)) throw coded("push_scan_id_invalid");
  return clean || null;
}

function validateOccurredAt(value) {
  const clean = cleanText(value).slice(0, 80);
  if (!clean) return { value: null, date: null, valid: false };
  const date = new Date(clean);
  if (Number.isNaN(date.getTime())) return { value: clean, date: null, valid: false };
  return { value: date.toISOString(), date, valid: true };
}

function validateReferenceType(referenceType) {
  const type = cleanText(referenceType).slice(0, 80);
  if (type && !/^[a-zA-Z0-9:_-]+$/.test(type)) throw coded("push_reference_type_invalid");
  return type || null;
}

function assertAutomationRealSendAllowed(eventKey) {
  if (process.env.PUSH_ENABLED !== "true") throw coded("push_disabled");
  if (process.env.PUSH_MODE !== "production") throw coded("push_mode_not_production");
  if (process.env.PUSH_TEST_ONLY === "true") throw coded("push_test_only_required");
  if (process.env.PUSH_ALLOW_PRODUCTION_SEND !== "true") {
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
  if (process.env.PUSH_ALLOW_AUDIENCE === "true") throw coded("push_audience_blocked");
  if (process.env.PUSH_ALLOW_ADMIN_MASS_SEND === "true") throw coded("push_mass_send_blocked");
  if (process.env.PUSH_ALLOW_CAMPAIGNS === "true") throw coded("push_campaign_blocked");
}

function isEngineSource(source) {
  const normalized = String(source || "").trim().toLowerCase();
  return normalized === "engine" || normalized.startsWith("engine-");
}

function logSafetyBlock({ eventKey, referenceKey, reason, source, scanId }) {
  console.warn("[push-internal] safety:block", {
    event_key: eventKey || null,
    reference_key: referenceKey || null,
    reason,
    source: source || null,
    scan_id: scanId || null,
  });
}

async function insertAutomationDispatch({
  eventKey,
  status,
  category,
  title,
  body,
  url,
  payload,
  errorMessage = null,
}) {
  const result = await query(
    `INSERT INTO public.notification_push_dispatches (
       event_key, category, title, body, url, payload, status, error_message
     ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
     RETURNING id, event_key, status, created_at`,
    [
      eventKey,
      category || "operational",
      title,
      body,
      url || null,
      JSON.stringify(payload || {}),
      status,
      errorMessage,
    ]
  );
  return result.rows?.[0] || null;
}

async function findSentDispatch({ eventKey, referenceKey }) {
  const result = await query(
    `SELECT id
       FROM public.notification_push_dispatches
      WHERE event_key = $1
        AND payload->>'reference_key' = $2
        AND status = 'sent'
      LIMIT 1`,
    [eventKey, referenceKey]
  );
  return result.rows?.[0] || null;
}

async function countReferencesForScan({ eventKey, scanId, referenceKey }) {
  if (!scanId) return 0;
  const result = await query(
    `SELECT COUNT(DISTINCT payload->>'reference_key')::int AS count
       FROM public.notification_push_dispatches
      WHERE event_key = $1
        AND payload->>'scan_id' = $2
        AND payload->>'reference_key' IS NOT NULL
        AND ($3::text IS NULL OR payload->>'reference_key' <> $3)
        AND status IN ('sent', 'dry_run')`,
    [eventKey, scanId, referenceKey || null]
  );
  return Number(result.rows?.[0]?.count || 0);
}

function buildAutomationDedupeKey({ eventKey, referenceKey }) {
  return `push:${eventKey}:${referenceKey}`;
}

function isRetryableLedgerStatus(status) {
  return String(status || "").toLowerCase() === "failed";
}

async function claimAutomationEventLedger({
  eventKey,
  referenceType,
  referenceKey,
  source,
  scanId,
  occurredAt,
  metadata,
  recipientUserIds,
  dryRun,
}) {
  if (!referenceKey) return { claimed: false, deduped: false, ledger: null };

  const dedupeKey = buildAutomationDedupeKey({ eventKey, referenceKey });
  const mode = dryRun ? "test" : "production";
  const meta = {
    source,
    scan_id: scanId,
    occurred_at: occurredAt,
    reference_type: referenceType,
    reference_key: referenceKey,
    metadata,
    recipient_user_ids: recipientUserIds,
    automation: true,
  };

  const inserted = await query(
    `INSERT INTO public.notification_event_ledger (
       event_key, dedupe_key, channel, category, entity_type, entity_id, status, mode, meta
     ) VALUES ($1, $2, 'push', 'operational', $3, $4, 'processing', $5, $6::jsonb)
     ON CONFLICT (dedupe_key) DO NOTHING
     RETURNING id, event_key, dedupe_key, status`,
    [eventKey, dedupeKey, referenceType || null, referenceKey, mode, JSON.stringify(meta)]
  );
  if (inserted.rowCount) {
    return { claimed: true, deduped: false, ledger: inserted.rows[0] };
  }

  const existing = await query(
    `SELECT id, event_key, dedupe_key, status
       FROM public.notification_event_ledger
      WHERE dedupe_key = $1
      LIMIT 1`,
    [dedupeKey]
  );
  const ledger = existing.rows?.[0] || null;
  if (ledger && isRetryableLedgerStatus(ledger.status)) {
    const retry = await query(
      `UPDATE public.notification_event_ledger
          SET status = 'processing',
              mode = $2,
              meta = $3::jsonb
        WHERE id = $1
          AND status = 'failed'
        RETURNING id, event_key, dedupe_key, status`,
      [ledger.id, mode, JSON.stringify(meta)]
    );
    if (retry.rowCount) return { claimed: true, deduped: false, ledger: retry.rows[0] };
  }

  return { claimed: false, deduped: true, ledger };
}

async function finishAutomationEventLedger(ledgerClaim, result, { category = "operational", dryRun = true } = {}) {
  if (!ledgerClaim?.claimed || !ledgerClaim?.ledger?.id) return;
  const status = String(result?.status || "processed").slice(0, 80);
  const meta = {
    final_status: status,
    deduped: Boolean(result?.deduped),
    attempted: Number(result?.attempted || 0),
    sent: Number(result?.sent || 0),
    failed: Number(result?.failed || 0),
    reason: result?.reason || null,
  };
  try {
    await query(
      `UPDATE public.notification_event_ledger
          SET status = $2,
              category = $3,
              mode = $4,
              meta = COALESCE(meta, '{}'::jsonb) || $5::jsonb
        WHERE id = $1`,
      [ledgerClaim.ledger.id, status, category || "operational", dryRun ? "test" : "production", JSON.stringify(meta)]
    );
  } catch (error) {
    console.warn("[push-internal] ledger:finish_failed", {
      event_key: ledgerClaim.ledger.event_key || null,
      dedupe_key: ledgerClaim.ledger.dedupe_key || null,
      status,
      error: error?.code || error?.message || "ledger_finish_failed",
    });
  }
}

function buildDedupedAutomationResult({ eventKey, referenceKey, ledger }) {
  return {
    ok: true,
    event_key: eventKey,
    status: "deduped",
    deduped: true,
    reference_key: referenceKey,
    reason: "already_processed",
    ledger_status: ledger?.status || null,
  };
}

async function insertSafetySkipped({
  eventKey,
  source,
  referenceType,
  referenceKey,
  scanId,
  occurredAt,
  metadata,
  recipientUserIds,
  actor,
  reason,
}) {
  logSafetyBlock({ eventKey, referenceKey, reason, source, scanId });
  const dispatch = await insertAutomationDispatch({
    eventKey,
    status: "skipped",
    category: "operational",
    title: "Push automatico bloqueado por seguranca",
    body: "Evento automatico bloqueado por regra de seguranca.",
    url: null,
    errorMessage: reason,
    payload: {
      dry_run: true,
      safety_block: true,
      source,
      scan_id: scanId,
      occurred_at: occurredAt,
      reference_type: referenceType,
      reference_key: referenceKey,
      reason,
      metadata,
      recipient_user_ids: recipientUserIds,
      actor,
    },
  });
  return { ok: true, event_key: eventKey, status: "skipped", reason, dispatch };
}

async function getRecipients({ eventKey, recipientUserIds }) {
  if (PUBLIC_EVENT_KEYS.has(eventKey)) {
    return query(
      `SELECT *
         FROM public.push_subscriptions
        WHERE is_active = true
          AND operational_opt_in = true
        ORDER BY updated_at DESC, created_at DESC`
    );
  }

  if (USER_EVENT_KEYS.has(eventKey) && recipientUserIds.length) {
    return query(
      `SELECT *
         FROM public.push_subscriptions
        WHERE is_active = true
          AND operational_opt_in = true
          AND user_id = ANY($1::bigint[])
        ORDER BY updated_at DESC, created_at DESC`,
      [recipientUserIds]
    );
  }

  return { rows: [] };
}

export async function handlePushAutomationEvent({
  eventKey,
  source = "engine",
  referenceType = null,
  referenceKey = null,
  scanId = null,
  occurredAt = null,
  metadata = {},
  recipientUserIds = [],
  actor = null,
  dryRun = true,
} = {}) {
  const key = validateEventKey(eventKey);
  const safeSource = cleanText(source, "engine").slice(0, 80) || "engine";
  const safeMetadata = sanitizeMetadata(metadata);
  const safeRecipientUserIds = sanitizeRecipientUserIds(recipientUserIds);
  const safeReferenceType = validateReferenceType(referenceType);
  const requireReferenceKey = !dryRun && isTrue(process.env.PUSH_AUTOMATION_REQUIRE_REFERENCE_KEY);
  const safeReferenceKey = validateReference({ referenceKey, requireReferenceKey });
  const safeScanId = validateScanId(scanId || safeMetadata.scan_id);
  const occurred = validateOccurredAt(occurredAt || safeMetadata.occurred_at);
  const safeActor = actor && typeof actor === "object"
    ? sanitizeMetadataValue(actor)
    : null;
  const engineSource = isEngineSource(safeSource);
  let ledgerClaim = null;
  let whatsappResult = null;

  async function ensureLedgerClaim() {
    if (ledgerClaim) return ledgerClaim;
    ledgerClaim = await claimAutomationEventLedger({
      eventKey: key,
      referenceType: safeReferenceType,
      referenceKey: safeReferenceKey,
      source: safeSource,
      scanId: safeScanId,
      occurredAt: occurred.value,
      metadata: safeMetadata,
      recipientUserIds: safeRecipientUserIds,
      dryRun,
    });
    return ledgerClaim;
  }

  async function getDedupedResultIfNeeded() {
    const claim = await ensureLedgerClaim();
    if (!claim?.deduped) return null;
    return buildDedupedAutomationResult({
      eventKey: key,
      referenceKey: safeReferenceKey,
      ledger: claim.ledger,
    });
  }

  async function finishWithLedger(result, category = "operational") {
    const finalResult = whatsappResult ? { ...result, whatsapp: whatsappResult } : result;
    await finishAutomationEventLedger(ledgerClaim, finalResult, { category, dryRun });
    return finalResult;
  }

  if (engineSource) {
    const skippedArgs = {
      eventKey: key,
      source: safeSource,
      referenceType: safeReferenceType,
      referenceKey: safeReferenceKey,
      scanId: safeScanId,
      occurredAt: occurred.value,
      metadata: safeMetadata,
      recipientUserIds: safeRecipientUserIds,
      actor: safeActor,
    };

    if (envBool("PUSH_ENGINE_REQUIRE_OCCURRED_AT", true) && !occurred.valid) {
      const deduped = await getDedupedResultIfNeeded();
      if (deduped) return deduped;
      return finishWithLedger(await insertSafetySkipped({ ...skippedArgs, reason: "safety_missing_occurred_at" }));
    }

    if (occurred.valid && envBool("PUSH_ENGINE_SAFETY_NO_BACKFILL", true)) {
      const maxAgeHours = envPositiveInt("PUSH_ENGINE_MAX_EVENT_AGE_HOURS", 24);
      if (Date.now() - occurred.date.getTime() > maxAgeHours * 60 * 60 * 1000) {
        const deduped = await getDedupedResultIfNeeded();
        if (deduped) return deduped;
        return finishWithLedger(await insertSafetySkipped({ ...skippedArgs, reason: "safety_event_too_old" }));
      }
    }

    if (envBool("PUSH_ENGINE_REQUIRE_SCAN_ID", true) && !safeScanId) {
      const deduped = await getDedupedResultIfNeeded();
      if (deduped) return deduped;
      return finishWithLedger(await insertSafetySkipped({ ...skippedArgs, reason: "safety_missing_scan_id" }));
    }

    if (!dryRun && safeScanId && !envBool("PUSH_ENGINE_ALLOW_LARGE_BATCH", false)) {
      const existingSent = safeReferenceKey
        ? await findSentDispatch({ eventKey: key, referenceKey: safeReferenceKey })
        : null;
      const maxReferences = envPositiveInt("PUSH_ENGINE_MAX_REFERENCES_PER_SCAN_PER_EVENT", 2);
      const acceptedReferences = await countReferencesForScan({
        eventKey: key,
        scanId: safeScanId,
        referenceKey: safeReferenceKey,
      });
      if (!existingSent && acceptedReferences >= maxReferences) {
        const deduped = await getDedupedResultIfNeeded();
        if (deduped) return deduped;
        return finishWithLedger(await insertSafetySkipped({ ...skippedArgs, reason: "safety_scan_event_limit_exceeded" }));
      }
    }
  }

  if (!dryRun) {
    assertAutomationRealSendAllowed(key);
  }

  const deduped = await getDedupedResultIfNeeded();
  if (deduped) return deduped;

  try {
    whatsappResult = await handleWhatsAppAutomationEvent({
      eventKey: key,
      source: safeSource,
      referenceType: safeReferenceType,
      referenceKey: safeReferenceKey,
      metadata: safeMetadata,
      recipientUserIds: safeRecipientUserIds,
      dryRun,
    });
  } catch (error) {
    console.warn("[push-internal] whatsapp automation failed", {
      event_key: key,
      reference_key: safeReferenceKey,
      code: error?.code || null,
      message: error?.message || null,
    });
    whatsappResult = {
      ok: false,
      event_key: key,
      status: "failed",
      reason: error?.code || error?.message || "whatsapp_automation_failed",
    };
  }

  const rule = await getActivePushRuleByEventKey(key);

  if (!rule) {
    const dispatch = await insertAutomationDispatch({
      eventKey: key,
      status: "skipped",
      category: "operational",
      title: "Push automatico ignorado",
      body: "Regra inativa ou nao encontrada.",
      url: null,
      payload: {
        dry_run: dryRun,
        source: safeSource,
        scan_id: safeScanId,
        occurred_at: occurred.value,
        reference_type: safeReferenceType,
        reference_key: safeReferenceKey,
        reason: "rule_inactive_or_not_found",
        metadata: safeMetadata,
        recipient_user_ids: safeRecipientUserIds,
        actor: safeActor,
      },
    });
    return finishWithLedger({ ok: true, event_key: key, status: "skipped", dispatch });
  }

  const renderedTitle = renderTemplate(rule.title_template, safeMetadata);
  const renderedBody = renderTemplate(rule.body_template, safeMetadata);
  const renderedUrl = renderTemplate(rule.url_template || "", safeMetadata) || "/";

  if (!dryRun) {
    const recipients = await getRecipients({
      eventKey: key,
      recipientUserIds: safeRecipientUserIds,
    });

    if (!recipients.rows?.length) {
      const dispatch = await insertAutomationDispatch({
        eventKey: key,
        status: "skipped",
        category: rule.category || "operational",
        title: renderedTitle || rule.title_template,
        body: renderedBody || "Nenhuma subscription ativa com opt-in operacional.",
        url: renderedUrl,
        errorMessage: "no_active_push_recipients",
        payload: {
          source: safeSource,
          scan_id: safeScanId,
          occurred_at: occurred.value,
          reference_type: safeReferenceType,
          reference_key: safeReferenceKey,
          metadata: safeMetadata,
          recipient_user_ids: safeRecipientUserIds,
          automation: true,
          real_send: true,
          no_recipients: true,
        },
      });
      return finishWithLedger({
        ok: true,
        event_key: key,
        status: "skipped",
        attempted: 0,
        sent: 0,
        failed: 0,
        reference_key: safeReferenceKey,
        dispatch,
      }, rule.category || "operational");
    }

    let sent = 0;
    let failed = 0;
    for (const row of recipients.rows || []) {
      try {
        await sendPushToSubscriptionRow({
          subscriptionRow: row,
          title: renderedTitle,
          body: renderedBody,
          url: renderedUrl,
          payload: {
            source: safeSource,
            scan_id: safeScanId,
            occurred_at: occurred.value,
            reference_type: safeReferenceType,
            reference_key: safeReferenceKey,
            metadata: safeMetadata,
            recipient_user_ids: safeRecipientUserIds,
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
    return finishWithLedger({
      ok: true,
      event_key: key,
      status: sent > 0 ? "sent" : "failed",
      attempted: sent + failed,
      sent,
      failed,
      reference_key: safeReferenceKey,
    }, rule.category || "operational");
  }

  const dispatch = await insertAutomationDispatch({
    eventKey: key,
    status: "dry_run",
    category: rule.category || "operational",
    title: renderedTitle,
    body: renderedBody,
    url: renderedUrl || null,
    payload: {
      dry_run: true,
      source: safeSource,
      scan_id: safeScanId,
      occurred_at: occurred.value,
      reference_type: safeReferenceType,
      reference_key: safeReferenceKey,
      rule_id: rule.id,
      metadata: safeMetadata,
      recipient_user_ids: safeRecipientUserIds,
      actor: safeActor,
    },
  });

  return finishWithLedger({ ok: true, event_key: key, status: "dry_run", dispatch }, rule.category || "operational");
}
