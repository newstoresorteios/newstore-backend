import { query } from "../../db.js";
import { sendBrevoWhatsAppTemplate, normalizePhoneBR } from "./brevoWhatsApp.js";
import {
  assertWhatsAppConsent,
} from "./communicationConsent.js";
import {
  createDispatch,
  markDispatchAccepted,
  markDispatchFailed,
} from "./notificationLog.js";

const AUTOMATION_WHATSAPP_EVENTS = Object.freeze({
  DRAW_REMAINING_NUMBERS_50: {
    templateKey: "DRAW_REMAINING_NUMBERS_50",
    envName: "BREVO_WHATSAPP_DRAW_REMAINING_50_TEMPLATE_ID",
    fallbackTemplateId: "25",
    audience: "public",
  },
  DRAW_REMAINING_NUMBERS_10: {
    templateKey: "DRAW_REMAINING_NUMBERS_10",
    envName: "BREVO_WHATSAPP_DRAW_REMAINING_10_TEMPLATE_ID",
    fallbackTemplateId: "26",
    audience: "public",
  },
  BALANCE_EXPIRING_15_DAYS: {
    templateKey: "BALANCE_EXPIRING_15_DAYS",
    envName: "BREVO_WHATSAPP_BALANCE_EXPIRING_15_TEMPLATE_ID",
    fallbackTemplateId: "27",
    audience: "user",
  },
});

const GRANTED_CONSENT_STATUSES = [
  "granted",
  "active",
  "opt_in",
  "allowed",
  "subscribed",
  "accepted",
];

function isTrue(value) {
  return String(value || "").trim() === "true";
}

function cleanText(value, fallback = "") {
  return String(value ?? fallback).trim();
}

function firstText(...values) {
  for (const value of values) {
    const text = cleanText(value);
    if (text) return text;
  }
  return "";
}

function publicBaseUrl() {
  return firstText(
    process.env.PUBLIC_APP_URL,
    process.env.APP_PUBLIC_URL,
    process.env.FRONTEND_URL,
    process.env.SITE_URL,
    "https://sorteiosxnamai.com.br"
  ).replace(/\/+$/, "");
}

function absoluteUrl(pathOrUrl, fallbackPath = "/") {
  const raw = firstText(pathOrUrl);
  if (/^https?:\/\//i.test(raw)) return raw;
  const path = raw || fallbackPath;
  return `${publicBaseUrl()}${path.startsWith("/") ? path : `/${path}`}`;
}

function centsToBRL(value) {
  const cents = Number(value);
  if (!Number.isFinite(cents)) return "";
  return (cents / 100).toLocaleString("pt-BR", {
    style: "currency",
    currency: "BRL",
  });
}

function formatBalanceValue(metadata = {}) {
  return firstText(
    metadata.valor_saldo,
    metadata.balance_label,
    metadata.balance_amount_label,
    metadata.amount_label,
    centsToBRL(
      metadata.valor_saldo_cents ??
        metadata.balance_cents ??
        metadata.balance_amount_cents ??
        metadata.amount_cents
    )
  );
}

function getDrawName(metadata = {}) {
  return firstText(
    metadata.nome_sorteio,
    metadata.draw_name,
    metadata.draw_title,
    metadata.title,
    metadata.name,
    metadata.product_name,
    metadata.prize_name,
    "Sorteio New Store"
  );
}

function getDrawLink(metadata = {}) {
  return absoluteUrl(
    firstText(
      metadata.link_sorteio,
      metadata.draw_url,
      metadata.url,
      metadata.link,
      metadata.path
    ),
    "/"
  );
}

function getAccountLink(metadata = {}) {
  return absoluteUrl(
    firstText(metadata.link_conta, metadata.account_url, metadata.url, metadata.link),
    "/conta"
  );
}

function buildParams({ eventKey, user, metadata }) {
  const nome = firstText(user?.name, metadata?.nome, metadata?.name, "Cliente");
  if (eventKey.startsWith("DRAW_REMAINING_NUMBERS_")) {
    return {
      nome,
      nome_sorteio: getDrawName(metadata),
      link_sorteio: getDrawLink(metadata),
    };
  }
  if (eventKey === "BALANCE_EXPIRING_15_DAYS") {
    return {
      nome,
      valor_saldo: formatBalanceValue(metadata),
      link_conta: getAccountLink(metadata),
    };
  }
  return { nome };
}

async function resolveTemplateId(config) {
  const envTemplateId = cleanText(process.env[config.envName]);
  if (envTemplateId) return { id: envTemplateId, source: "env" };

  try {
    const row = await query(
      `SELECT provider_template_id
         FROM public.notification_templates
        WHERE template_key = $1
          AND channel = 'whatsapp'
          AND provider = 'brevo'
          AND is_active = true
          AND provider_template_id IS NOT NULL
          AND TRIM(provider_template_id) <> ''
        LIMIT 1`,
      [config.templateKey]
    );
    const providerTemplateId = cleanText(row.rows?.[0]?.provider_template_id);
    if (providerTemplateId) return { id: providerTemplateId, source: "database" };
  } catch (error) {
    if (error?.code !== "42P01" && error?.code !== "42703") throw error;
  }

  return { id: config.fallbackTemplateId, source: "approved_default" };
}

async function alreadyDispatched({ eventKey, referenceKey, userId }) {
  if (!referenceKey || !userId) return false;
  const result = await query(
    `SELECT id
       FROM public.notification_dispatches
      WHERE event_key = $1
        AND channel = 'whatsapp'
        AND provider = 'brevo'
        AND user_id = $2
        AND payload->>'reference_key' = $3
      LIMIT 1`,
    [eventKey, userId, referenceKey]
  );
  return Boolean(result.rows?.[0]);
}

async function getPublicRecipients() {
  try {
    const result = await query(
      `WITH latest_consent AS (
         SELECT DISTINCT ON (user_id)
                user_id,
                LOWER(status) AS status
           FROM public.communication_consents
          WHERE LOWER(channel) = 'whatsapp'
            AND LOWER(category) IN ('operational', 'all', 'manual')
          ORDER BY user_id, created_at DESC
       )
       SELECT u.id, u.name, u.phone
         FROM public.users u
         JOIN latest_consent lc ON lc.user_id = u.id
        WHERE NULLIF(TRIM(COALESCE(u.phone, '')), '') IS NOT NULL
          AND lc.status = ANY($1::text[])
        ORDER BY u.id ASC`,
      [GRANTED_CONSENT_STATUSES]
    );
    return result.rows || [];
  } catch (error) {
    if (error?.code === "42P01" || error?.code === "42703") return [];
    throw error;
  }
}

async function getUserRecipients({ recipientUserIds, metadata }) {
  const ids = new Set();
  for (const id of recipientUserIds || []) {
    const n = Number(id);
    if (Number.isInteger(n) && n > 0) ids.add(n);
  }
  const metadataUserId = Number(metadata?.user_id ?? metadata?.userId);
  if (Number.isInteger(metadataUserId) && metadataUserId > 0) ids.add(metadataUserId);
  if (!ids.size) return [];

  const result = await query(
    `SELECT id, name, phone
       FROM public.users
      WHERE id = ANY($1::bigint[])
      ORDER BY id ASC`,
    [[...ids]]
  );
  return result.rows || [];
}

async function getRecipients({ config, recipientUserIds, metadata }) {
  if (config.audience === "public") return getPublicRecipients();
  return getUserRecipients({ recipientUserIds, metadata });
}

async function markSkipped({ dispatch, reason }) {
  return markDispatchFailed({
    dispatchId: dispatch.id,
    status: "skipped",
    result: {
      ok: false,
      skipped: true,
      reason,
      provider: "brevo",
      channel: "whatsapp",
    },
  });
}

async function sendToRecipient({
  eventKey,
  config,
  template,
  user,
  metadata,
  referenceKey,
  referenceType,
  source,
  dryRun,
}) {
  const userId = Number(user?.id);
  if (!Number.isInteger(userId) || userId <= 0) {
    return { status: "skipped", reason: "invalid_user_id" };
  }

  if (await alreadyDispatched({ eventKey, referenceKey, userId })) {
    return { status: "deduped", reason: "already_dispatched" };
  }

  const normalizedPhone = normalizePhoneBR(user.phone);
  const params = buildParams({ eventKey, user, metadata });
  if (dryRun) {
    return { status: "dry_run", reason: "dry_run" };
  }

  const dispatch = await createDispatch({
    eventKey,
    channel: "whatsapp",
    provider: "brevo",
    userId,
    drawId: Number(metadata?.draw_id ?? metadata?.drawId) || null,
    recipient: normalizedPhone,
    recipientOriginal: user.phone || null,
    recipientForced: false,
    templateKey: config.templateKey,
    providerTemplateId: template.id,
    payload: {
      params,
      source,
      reference_key: referenceKey || null,
      reference_type: referenceType || null,
      automation: true,
      dry_run: false,
    },
    messageSnapshot: {
      template_key: config.templateKey,
      provider_template_id: template.id,
      provider_template_source: template.source,
      params,
    },
    recipientSnapshot: {
      user_id: userId,
      phone: user.phone || null,
      recipient: normalizedPhone,
      source: "whatsapp_automation",
    },
  });

  if (!normalizedPhone) {
    await markSkipped({ dispatch, reason: "invalid_recipient" });
    return { status: "skipped", reason: "invalid_recipient" };
  }

  const consent = await assertWhatsAppConsent({
    userId,
    phone: user.phone || null,
    category: "operational",
    source: "whatsapp_automation",
  });

  if (!consent.ok) {
    await markSkipped({ dispatch, reason: consent.reason || "whatsapp_consent_missing" });
    return { status: "skipped", reason: consent.reason || "whatsapp_consent_missing" };
  }

  const result = await sendBrevoWhatsAppTemplate({
    to: user.phone,
    templateId: template.id,
    params,
    templateKey: config.templateKey,
    correlationId: String(dispatch.id),
    context: "whatsapp_automation",
    source: "whatsapp_automation",
    isAutomation: true,
    isCampaign: false,
    consentChecked: true,
  });

  if (result?.ok) {
    await markDispatchAccepted({ dispatchId: dispatch.id, result });
    return { status: "accepted" };
  }

  const failureStatus = result?.skipped ? "skipped" : "failed";
  await markDispatchFailed({ dispatchId: dispatch.id, result, status: failureStatus });
  return { status: failureStatus, reason: result?.reason || result?.error || "whatsapp_send_failed" };
}

export function getAllowedWhatsAppAutomationEvents() {
  return Object.keys(AUTOMATION_WHATSAPP_EVENTS);
}

export async function handleWhatsAppAutomationEvent({
  eventKey,
  source = "engine",
  referenceType = null,
  referenceKey = null,
  metadata = {},
  recipientUserIds = [],
  dryRun = true,
} = {}) {
  const key = cleanText(eventKey);
  const config = AUTOMATION_WHATSAPP_EVENTS[key];
  if (!config) {
    return { ok: true, event_key: key, status: "skipped", reason: "whatsapp_event_not_configured" };
  }

  if (!dryRun && !isTrue(process.env.NOTIFICATION_WHATSAPP_AUTOMATION_ENABLED)) {
    return { ok: true, event_key: key, status: "skipped", reason: "whatsapp_automation_disabled" };
  }

  const template = await resolveTemplateId(config);
  const recipients = await getRecipients({ config, recipientUserIds, metadata });
  if (!recipients.length) {
    return { ok: true, event_key: key, status: "skipped", reason: "no_whatsapp_recipients", attempted: 0, accepted: 0, failed: 0, skipped: 0, deduped: 0 };
  }

  let accepted = 0;
  let failed = 0;
  let skipped = 0;
  let deduped = 0;
  let dryRunCount = 0;

  for (const user of recipients) {
    const result = await sendToRecipient({
      eventKey: key,
      config,
      template,
      user,
      metadata,
      referenceKey,
      referenceType,
      source,
      dryRun,
    });
    if (result.status === "accepted") accepted += 1;
    else if (result.status === "failed") failed += 1;
    else if (result.status === "deduped") deduped += 1;
    else if (result.status === "dry_run") dryRunCount += 1;
    else skipped += 1;
  }

  return {
    ok: true,
    event_key: key,
    status: accepted > 0 ? "accepted" : failed > 0 ? "failed" : dryRun ? "dry_run" : "skipped",
    attempted: recipients.length,
    accepted,
    failed,
    skipped,
    deduped,
    dry_run: dryRunCount,
    template_id: template.id,
    template_source: template.source,
  };
}

export default {
  getAllowedWhatsAppAutomationEvents,
  handleWhatsAppAutomationEvent,
};
