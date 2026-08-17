import { query } from "../../db.js";
import {
  createCampaign,
  createDispatch,
  markDispatchAccepted,
  markDispatchFailed,
  updateCampaignAudienceCounts,
} from "./notificationLog.js";
import { createSmtpTransporter, getSmtpConfig } from "./manualEmailNotifications.js";

export const BALANCE_EMAIL_EVENT_KEYS = Object.freeze([
  "EMAIL_BALANCE_EXPIRING_30_DAYS",
  "EMAIL_BALANCE_EXPIRING_20_DAYS",
  "EMAIL_BALANCE_EXPIRING_10_DAYS",
  "EMAIL_BALANCE_EXPIRING_7_DAYS",
  "EMAIL_BALANCE_EXPIRING_3_DAYS",
  "EMAIL_BALANCE_EXPIRED",
]);

const BALANCE_EVENT_STAGES = new Map([
  ["EMAIL_BALANCE_EXPIRING_30_DAYS", { days: 30, referenceStage: "30_days" }],
  ["EMAIL_BALANCE_EXPIRING_20_DAYS", { days: 20, referenceStage: "20_days" }],
  ["EMAIL_BALANCE_EXPIRING_10_DAYS", { days: 10, referenceStage: "10_days" }],
  ["EMAIL_BALANCE_EXPIRING_7_DAYS", { days: 7, referenceStage: "7_days" }],
  ["EMAIL_BALANCE_EXPIRING_3_DAYS", { days: 3, referenceStage: "3_days" }],
  ["EMAIL_BALANCE_EXPIRED", { expired: true, referenceStage: "expired" }],
]);

const SAO_PAULO_TIME_ZONE = "America/Sao_Paulo";
const FALLBACK_SITE_URL = "https://sorteiosxnamai.com.br";

function cleanText(value) {
  return String(value ?? "").trim();
}

function validEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(cleanText(value));
}

function automationEnabled() {
  return cleanText(process.env.NOTIFICATION_EMAIL_AUTOMATION_ENABLED).toLowerCase() === "true";
}

function siteUrl() {
  return cleanText(
    process.env.PUBLIC_APP_URL ||
      process.env.FRONTEND_URL ||
      process.env.SITE_URL ||
      FALLBACK_SITE_URL
  ).replace(/\/+$/, "");
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function booleanEnv(name, fallback = false) {
  const value = cleanText(process.env[name]).toLowerCase();
  if (!value) return fallback;
  return ["1", "true", "yes", "on"].includes(value);
}

export function formatBalanceValue(balanceCents) {
  const cents = Number(balanceCents);
  const amount = Number.isFinite(cents) ? cents / 100 : 0;
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(amount).replace(/[\u00a0\u202f]/gu, " ");
}

function saoPauloDateKey(value) {
  const directDate = cleanText(value).match(/^(\d{4}-\d{2}-\d{2})/u)?.[1];
  if (directDate) return directDate;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: SAO_PAULO_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(parsed);
  const byType = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${byType.year}-${byType.month}-${byType.day}`;
}

export function formatBalanceExpiryDate(value) {
  const dateKey = saoPauloDateKey(value);
  if (!dateKey) return "";
  const [year, month, day] = dateKey.split("-");
  return `${day}/${month}/${year}`;
}

export function balanceStageForEvent(eventKey) {
  return BALANCE_EVENT_STAGES.get(cleanText(eventKey)) || null;
}

export function balanceStageMatches(eventKey, daysToExpire) {
  const stage = balanceStageForEvent(eventKey);
  const days = Number(daysToExpire);
  if (!stage || !Number.isInteger(days)) return false;
  return stage.expired === true ? days < 0 : days === stage.days;
}

export function buildBalanceReferenceKey({ userId, expiresOn, eventKey }) {
  const stage = balanceStageForEvent(eventKey);
  const dateKey = saoPauloDateKey(expiresOn);
  if (!stage || !dateKey) return null;
  return `user_balance:${Number(userId)}:expires:${dateKey}:email:${stage.referenceStage}`;
}

export async function loadBalanceExpiryContext(userId, runQuery = query) {
  const result = await runQuery(
    // expires_on é cast para texto no próprio SQL: a coluna é DATE (sem
    // fuso), e o driver pg parseia DATE como objeto Date ancorado no fuso
    // local do processo Node. Em produção isso vira meia-noite UTC, que a
    // conversão via Intl "America/Sao_Paulo" mais adiante recua um dia.
    // Trazer como texto evita que a data civil passe por qualquer
    // conversão de fuso.
    `SELECT user_id, name, email, balance_cents, balance_reference_at,
            expires_at, expires_on::text AS expires_on, days_to_expire, expiry_source
       FROM public.user_coupon_balance_expiry
      WHERE user_id = $1
      LIMIT 1`,
    [userId]
  );
  return result.rows?.[0] || null;
}

// Aritmética de calendário pura em UTC, usada só para deduplicar contra a
// chave legada (um dia a menos) de envios já aceitos antes desta correção.
// Nunca interpreta expires_on como instante/fuso — apenas desloca o rótulo
// de data em um dia dentro do calendário proléptico.
export function previousCalendarDateKey(dateKey) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(cleanText(dateKey));
  if (!match) return null;
  const [, yearStr, monthStr, dayStr] = match;
  const previous = new Date(Date.UTC(Number(yearStr), Number(monthStr) - 1, Number(dayStr) - 1));
  const yyyy = previous.getUTCFullYear();
  const mm = String(previous.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(previous.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

async function alreadyBalanceDispatched(
  { eventKey, referenceKey, legacyReferenceKey, userId },
  runQuery = query
) {
  const candidateKeys = legacyReferenceKey && legacyReferenceKey !== referenceKey
    ? [referenceKey, legacyReferenceKey]
    : [referenceKey];
  const result = await runQuery(
    `SELECT 1
       FROM public.notification_dispatches
      WHERE channel = 'email'
        AND event_key = $1
        AND user_id = $2
        AND draw_id IS NULL
        AND payload->>'source' = 'automation'
        AND payload->>'automation' = 'true'
        AND payload->>'reference_key' = ANY($3::text[])
        AND status NOT IN ('failed', 'skipped')
      LIMIT 1`,
    [eventKey, userId, candidateKeys]
  );
  return Boolean(result.rowCount);
}

function effectiveFromDateKey() {
  return saoPauloDateKey(process.env.EMAIL_BALANCE_AUTOMATION_EFFECTIVE_FROM);
}

export function expiredBalanceIsEligible(expiresOn) {
  if (booleanEnv("EMAIL_BALANCE_EXPIRED_BACKFILL_ENABLED", false)) return true;
  const effectiveFrom = effectiveFromDateKey();
  const expiryDate = saoPauloDateKey(expiresOn);
  if (!effectiveFrom || !expiryDate) return false;
  return expiryDate >= effectiveFrom;
}

function renderBalanceTemplate(eventKey, user, context) {
  const name = cleanText(user.name) || "Cliente";
  const balanceValue = formatBalanceValue(context.balance_cents);
  const expiresDate = formatBalanceExpiryDate(context.expires_on || context.expires_at);
  const accountUrl = `${siteUrl()}/conta`;
  const templates = {
    EMAIL_BALANCE_EXPIRING_30_DAYS: {
      subject: `Seu saldo de ${balanceValue} vence em 30 dias`,
      paragraphs: [
        `Você possui ${balanceValue} de saldo disponível na New Store.`,
        `Esse saldo vence em ${expiresDate}, daqui a 30 dias.`,
        "Acesse sua conta e confira as possibilidades de uso antes do vencimento.",
      ],
      button: "CONFERIR MEU SALDO",
    },
    EMAIL_BALANCE_EXPIRING_20_DAYS: {
      subject: `Faltam 20 dias para usar seu saldo de ${balanceValue}`,
      paragraphs: [
        `Seu saldo de ${balanceValue} continua disponível, mas vence em ${expiresDate}.`,
        "Faltam 20 dias para utilizá-lo.",
      ],
      button: "ACESSAR MINHA CONTA",
    },
    EMAIL_BALANCE_EXPIRING_10_DAYS: {
      subject: "Atenção: seu saldo vence em 10 dias",
      paragraphs: [
        `Faltam apenas 10 dias para o vencimento do seu saldo de ${balanceValue}.`,
        `A data de vencimento é ${expiresDate}.`,
      ],
      button: "USAR MEU SALDO",
    },
    EMAIL_BALANCE_EXPIRING_7_DAYS: {
      subject: "Seu saldo vence em 7 dias",
      paragraphs: [
        `Seu saldo de ${balanceValue} vence em uma semana, no dia ${expiresDate}.`,
        "Confira sua conta para não perder o prazo de utilização.",
      ],
      button: "CONFERIR SALDO",
    },
    EMAIL_BALANCE_EXPIRING_3_DAYS: {
      subject: `Últimos 3 dias para usar seu saldo de ${balanceValue}`,
      paragraphs: [
        `Restam apenas 3 dias para utilizar seu saldo de ${balanceValue}.`,
        `O prazo termina em ${expiresDate}.`,
      ],
      button: "ACESSAR AGORA",
    },
    EMAIL_BALANCE_EXPIRED: {
      subject: `O prazo do seu saldo de ${balanceValue} terminou`,
      paragraphs: [
        `O prazo de utilização do saldo de ${balanceValue} terminou em ${expiresDate}.`,
        "Acesse sua conta para consultar os detalhes.",
      ],
      button: "VER MINHA CONTA",
    },
  };
  const template = templates[eventKey];
  const htmlParagraphs = template.paragraphs
    .map((paragraph) => `<p>${escapeHtml(paragraph)}</p>`)
    .join("");
  return {
    subject: template.subject,
    html: `<p>Olá, ${escapeHtml(name)}!</p>${htmlParagraphs}<p><a href="${escapeHtml(accountUrl)}">${template.button}</a></p>`,
    text: `Olá, ${name}!\n\n${template.paragraphs.join("\n\n")}\n\n${template.button}: ${accountUrl}`,
    templateKey: eventKey,
    balanceValue,
    expiresDate,
    accountUrl,
  };
}

function skippedResult({ eventKey, referenceKey, userId, reason, elapsedMs }) {
  console.log("[email-balance-automation] skipped", {
    event_key: eventKey,
    reference_key: referenceKey || null,
    user_id: userId || null,
    reason,
    status: "skipped",
    elapsed_ms: elapsedMs,
  });
  return {
    ok: true,
    status: "skipped",
    reason,
    event_key: eventKey,
    reference_key: referenceKey || null,
    user_id: userId || null,
    sent: 0,
    failed: 0,
    skipped: 1,
    deduped: 0,
  };
}

export async function handleAutomaticBalanceEmailEvent({
  eventKey,
  referenceKey: receivedReferenceKey,
  metadata = {},
  occurredAt = null,
} = {}, dependencies = {}) {
  const startedAt = Date.now();
  const key = cleanText(eventKey);
  const userId = Number(metadata?.user_id);
  if (!BALANCE_EMAIL_EVENT_KEYS.includes(key)) {
    const error = new Error("email_event_not_allowed");
    error.code = "email_event_not_allowed";
    throw error;
  }
  if (!Number.isSafeInteger(userId) || userId <= 0) {
    const error = new Error("email_user_id_invalid");
    error.code = "email_user_id_invalid";
    throw error;
  }
  if (!automationEnabled()) {
    return {
      ok: true,
      status: "disabled",
      reason: "disabled",
      event_key: key,
      reference_key: cleanText(receivedReferenceKey) || null,
      user_id: userId,
      sent: 0,
      failed: 0,
      skipped: 0,
      deduped: 0,
    };
  }

  const loadContext = dependencies.loadBalanceContext || loadBalanceExpiryContext;
  const wasAlreadyDispatched = dependencies.alreadyDispatched || alreadyBalanceDispatched;
  const resolveSmtpConfig = dependencies.getSmtpConfig || getSmtpConfig;
  const createMailer = dependencies.createSmtpTransporter || createSmtpTransporter;
  const createCampaignRecord = dependencies.createCampaign || createCampaign;
  const createDispatchRecord = dependencies.createDispatch || createDispatch;
  const acceptDispatch = dependencies.markDispatchAccepted || markDispatchAccepted;
  const failDispatch = dependencies.markDispatchFailed || markDispatchFailed;
  const updateCampaign = dependencies.updateCampaignAudienceCounts || updateCampaignAudienceCounts;
  const context = await loadContext(userId);
  if (!context || Number(context.balance_cents) <= 0) {
    return skippedResult({
      eventKey: key,
      referenceKey: receivedReferenceKey,
      userId,
      reason: "balance_not_positive",
      elapsedMs: Date.now() - startedAt,
    });
  }
  if (!validEmail(context.email)) {
    return skippedResult({
      eventKey: key,
      referenceKey: receivedReferenceKey,
      userId,
      reason: "balance_email_invalid",
      elapsedMs: Date.now() - startedAt,
    });
  }
  if (!context.balance_reference_at || !context.expires_at || !context.expires_on || !context.expiry_source) {
    return skippedResult({
      eventKey: key,
      referenceKey: receivedReferenceKey,
      userId,
      reason: "balance_expiry_source_missing",
      elapsedMs: Date.now() - startedAt,
    });
  }

  const referenceKey = buildBalanceReferenceKey({ userId, expiresOn: context.expires_on, eventKey: key });
  // Compatibilidade temporária: dispatches aceitos antes da correção do
  // parsing de expires_on foram gravados com a chave um dia anterior. Não
  // reenviar esses três e-mails só porque a chave histórica ficou errada.
  const legacyReferenceKey = buildBalanceReferenceKey({
    userId,
    expiresOn: previousCalendarDateKey(context.expires_on),
    eventKey: key,
  });
  if (!referenceKey) {
    return skippedResult({
      eventKey: key,
      referenceKey: receivedReferenceKey,
      userId,
      reason: "balance_expiry_source_missing",
      elapsedMs: Date.now() - startedAt,
    });
  }
  if (!balanceStageMatches(key, context.days_to_expire)) {
    return skippedResult({
      eventKey: key,
      referenceKey,
      userId,
      reason: "balance_stage_mismatch",
      elapsedMs: Date.now() - startedAt,
    });
  }
  if (key === "EMAIL_BALANCE_EXPIRED" && !expiredBalanceIsEligible(context.expires_on)) {
    return skippedResult({
      eventKey: key,
      referenceKey,
      userId,
      reason: "balance_expired_before_effective_from",
      elapsedMs: Date.now() - startedAt,
    });
  }

  const stage = balanceStageForEvent(key)?.referenceStage || null;
  const logContext = {
    event_key: key,
    reference_key: referenceKey,
    user_id: userId,
    balance_cents: Number(context.balance_cents),
    expires_at: context.expires_at,
    days_to_expire: Number(context.days_to_expire),
    stage,
  };
  console.log("[email-balance-automation] validated", logContext);
  if (await wasAlreadyDispatched({ eventKey: key, referenceKey, legacyReferenceKey, drawId: null, userId })) {
    console.log("[email-balance-automation] deduped", {
      ...logContext,
      status: "deduped",
      elapsed_ms: Date.now() - startedAt,
    });
    return {
      ok: true,
      status: "deduped",
      event_key: key,
      reference_key: referenceKey,
      user_id: userId,
      sent: 0,
      failed: 0,
      skipped: 1,
      deduped: 1,
    };
  }

  let smtp;
  try {
    smtp = resolveSmtpConfig();
  } catch (error) {
    if (error?.code !== "manual_email_smtp_not_configured") throw error;
    return {
      ok: false,
      status: "configuration_error",
      reason: error.code,
      event_key: key,
      reference_key: referenceKey,
      user_id: userId,
      sent: 0,
      failed: 0,
      skipped: 1,
      deduped: 0,
    };
  }

  const user = { id: userId, name: context.name, email: cleanText(context.email).toLowerCase() };
  const rendered = renderBalanceTemplate(key, user, context);
  const balanceSnapshot = {
    source: "automation",
    automation: true,
    event_key: key,
    reference_type: "user_balance",
    reference_key: referenceKey,
    user_id: userId,
    balance_cents: Number(context.balance_cents),
    expires_at: context.expires_at,
    expires_date: rendered.expiresDate,
    days_to_expire: Number(context.days_to_expire),
    expiry_source: context.expiry_source,
    stage,
  };
  const campaign = await createCampaignRecord({
    name: `Automatic email - ${rendered.subject}`.slice(0, 255),
    channel: "email",
    provider: "brevo_smtp",
    templateKey: rendered.templateKey,
    audienceFilter: "specific_user",
    audienceParams: { user_id: userId, event_key: key, reference_key: referenceKey },
    payload: { ...balanceSnapshot, occurred_at: occurredAt },
    messageSnapshot: { ...balanceSnapshot, subject: rendered.subject },
    audienceSnapshot: { ...balanceSnapshot, resolved_recipients: 1 },
    campaignType: "automation",
    audienceCountExpected: 1,
  });
  const dispatch = await createDispatchRecord({
    eventKey: key,
    channel: "email",
    provider: "brevo_smtp",
    userId,
    drawId: null,
    recipient: user.email,
    recipientOriginal: user.email,
    templateKey: rendered.templateKey,
    campaignId: campaign.id,
    payload: balanceSnapshot,
    messageSnapshot: { ...balanceSnapshot, subject: rendered.subject, html: rendered.html, text: rendered.text },
    recipientSnapshot: { ...balanceSnapshot, name: user.name || null },
  });

  let sent = 0;
  let failed = 0;
  try {
    const info = await createMailer(smtp).sendMail({
      from: `"${smtp.fromName}" <${smtp.fromEmail}>`,
      to: user.email,
      replyTo: smtp.replyTo,
      subject: rendered.subject,
      html: rendered.html,
      text: rendered.text,
    });
    await acceptDispatch({
      dispatchId: dispatch.id,
      result: {
        ok: true,
        provider_status: "accepted",
        delivery_status: "unknown",
        messageId: info?.messageId || null,
        response: { accepted: info?.accepted?.length || 0 },
      },
    });
    sent = 1;
  } catch (error) {
    failed = 1;
    await failDispatch({
      dispatchId: dispatch.id,
      result: { ok: false, error: "automatic_email_send_failed", reason: error?.code || error?.message || null },
    });
  }
  await updateCampaign(null, campaign.id, { created: 1, sent, failed, skipped: 0 });
  const status = failed ? "failed" : "processed";
  console.log("[email-balance-automation] completed", {
    ...logContext,
    status,
    elapsed_ms: Date.now() - startedAt,
  });
  return {
    ok: true,
    status,
    event_key: key,
    reference_key: referenceKey,
    user_id: userId,
    sent,
    failed,
    skipped: 0,
    deduped: 0,
  };
}
