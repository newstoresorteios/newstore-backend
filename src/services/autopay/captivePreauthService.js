import crypto from "node:crypto";
import { query } from "../../db.js";
import { getTicketPriceCents as getGlobalTicketPriceCents } from "../config.js";
import { chargeAuthorizedCaptivePreauth as chargeAuthorizedCaptivePreauthWithAutopay } from "../autopayRunner.js";
import {
  getWhatsAppProviderReadiness,
  resolveRecipientForCurrentMode,
  sendBrevoWhatsAppTemplate,
} from "../notifications/brevoWhatsApp.js";
import { assertWhatsAppConsent, WHATSAPP_CONSENT_CATEGORY_DEFAULT } from "../notifications/communicationConsent.js";
import { createDispatch, markDispatchAccepted, markDispatchFailed } from "../notifications/notificationLog.js";

const LOG_PREFIX = "[captive-preauth]";
const PUBLIC_URL_FALLBACK = "https://sorteiosxnamai.com.br";
const AUTHORIZATION_STATUSES = new Set(["pending", "authorized", "declined", "expired", "charged", "failed"]);
const PREAUTH_TEMPLATE_KEY = "CAPTIVE_PREAUTH_REQUEST";
const CONFIRMATION_CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const AUTOPAY_BASE_AMOUNT_COLUMNS = [
  "authorized_amount_cents",
  "max_authorized_amount_cents",
  "default_amount_cents",
  "amount_cents",
];

function log(event, extra = {}) {
  console.log(`${LOG_PREFIX} ${event}`, extra);
}

function warn(event, extra = {}) {
  console.warn(`${LOG_PREFIX} ${event}`, extra);
}

function error(event, extra = {}) {
  console.error(`${LOG_PREFIX} ${event}`, extra);
}

function normalizeBaseUrl() {
  const raw =
    process.env.PUBLIC_APP_URL ||
    process.env.APP_PUBLIC_URL ||
    process.env.FRONTEND_URL ||
    process.env.SITE_URL ||
    PUBLIC_URL_FALLBACK;
  return String(raw || PUBLIC_URL_FALLBACK).trim().replace(/\/+$/, "");
}

function normalizeUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  try {
    const url = new URL(raw);
    if (!/^https?:$/i.test(url.protocol)) return null;
    return url.toString().replace(/\/+$/, "");
  } catch {
    return null;
  }
}

function appendConfirmPath(baseUrl) {
  const base = normalizeUrl(baseUrl);
  if (!base) return null;
  return `${base.replace(/\/+$/, "")}/cativo/confirmar`;
}

export function getCaptivePreauthTemplateMode() {
  const mode = String(process.env.CAPTIVE_PREAUTH_TEMPLATE_MODE || "params").trim().toLowerCase();
  return mode === "static_link" ? "static_link" : "params";
}

export function resolveCaptiveConfirmationPublicUrl() {
  const explicit = normalizeUrl(process.env.CAPTIVE_CONFIRMATION_PUBLIC_URL);
  if (explicit) return explicit;

  return (
    appendConfirmPath(process.env.PUBLIC_APP_URL) ||
    appendConfirmPath(process.env.FRONTEND_URL) ||
    appendConfirmPath(process.env.SITE_URL) ||
    null
  );
}

function toPositiveInt(value) {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : null;
}

function envBool(name, defaultValue = false) {
  const raw = process.env[name];
  if (raw == null || String(raw).trim() === "") return Boolean(defaultValue);
  return String(raw).trim().toLowerCase() === "true";
}

function getCaptivePreauthExpiresHours() {
  const fromEnv = toPositiveInt(process.env.CAPTIVE_PREAUTH_EXPIRES_HOURS);
  return fromEnv || 12;
}

function getDefaultAuthorizedBaseAmountCents() {
  return toPositiveInt(process.env.CAPTIVE_AUTOPAY_DEFAULT_AMOUNT_CENTS) || 5500;
}

export function shouldRequireCaptivePreauth({ currentAmountCents, authorizedBaseAmountCents }) {
  const current = Number(currentAmountCents);
  const base = Number(authorizedBaseAmountCents);
  return Number.isFinite(current) && Number.isFinite(base) && current > base;
}

function safeStatus(status) {
  const s = String(status || "").trim().toLowerCase();
  return AUTHORIZATION_STATUSES.has(s) ? s : "unknown";
}

function safeError(value, fallback = "notification_failed") {
  const text = String(value || fallback).trim();
  return text.replace(/[\r\n\t]+/g, " ").slice(0, 180);
}

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizePhoneDigits(value) {
  return String(value || "").replace(/\D/g, "");
}

function phoneLookupVariants(value) {
  const digits = normalizePhoneDigits(value);
  if (!digits) return [];
  const variants = new Set([digits]);
  if (digits.startsWith("55") && digits.length > 11) variants.add(digits.slice(2));
  if (!digits.startsWith("55") && (digits.length === 10 || digits.length === 11)) variants.add(`55${digits}`);
  return [...variants];
}

function isCaptivePreauthWhatsAppEnabled() {
  return envBool("CAPTIVE_PREAUTH_WHATSAPP_ENABLED", false);
}

function buildWhatsAppSkipDiagnostics({ reason, templateId }) {
  const readiness = getWhatsAppProviderReadiness();
  return {
    reason,
    captive_preauth_whatsapp_enabled: isCaptivePreauthWhatsAppEnabled(),
    brevo_whatsapp_enabled: readiness.brevo_whatsapp_enabled,
    notification_whatsapp_campaign_enabled: readiness.notification_whatsapp_campaign_enabled,
    test_mode: readiness.test_mode,
    has_test_to: readiness.has_test_to,
    has_brevo_api_key: readiness.has_brevo_api_key,
    has_template_id: Boolean(templateId),
  };
}

function logWhatsAppSkipped({ drawId, userId, captiveNumber, reason, templateId }) {
  warn("whatsapp_skipped", {
    draw_id: Number(drawId),
    user_id: Number(userId),
    captive_number: Number(captiveNumber),
    ...buildWhatsAppSkipDiagnostics({ reason, templateId }),
  });
}

function getDrawTitle(draw) {
  return (
    draw?.title ||
    draw?.name ||
    draw?.prize_name ||
    draw?.product_name ||
    draw?.winner_name ||
    `Sorteio #${draw?.id}`
  );
}

function formatAmountBRL(amountCents) {
  const value = Number(amountCents || 0) / 100;
  return value.toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
}

function buildCaptivePreauthMessage({ customerName, confirmationCode }) {
  return [
    `Olá, ${customerName}.`,
    "",
    "A New Store possui uma confirmação pendente para sua conta.",
    "",
    `Código de confirmação: ${confirmationCode}.`,
    "",
    "Use o botão abaixo para acessar a página de confirmação.",
    "",
    "Caso tenha dúvidas, responda esta mensagem.",
  ].join("\n");
}

function buildLegacyCaptivePreauthMessage({ customerName, drawTitle, amount, number, authorizeUrl, declineUrl }) {
  return [
    `Olá, ${customerName}! Um novo sorteio especial da New Store foi lançado: ${drawTitle}.`,
    "",
    `O valor da cota para esta rodada é de ${amount}.`,
    "",
    `Como você possui o número cativo ${number}, gostaria de autorizar sua participação nesta rodada?`,
    "",
    "SIM, AUTORIZAR:",
    authorizeUrl,
    "",
    "NÃO PARTICIPAR DESTA RODADA:",
    declineUrl,
  ].join("\n");
}

async function getDraw(drawId) {
  const id = Number(drawId);
  if (!Number.isInteger(id) || id <= 0) {
    const err = new Error("invalid_draw_id");
    err.status = 400;
    throw err;
  }

  const result = await query(
    `SELECT *
       FROM public.draws
      WHERE id = $1
      LIMIT 1`,
    [id]
  );

  if (!result.rowCount) {
    const err = new Error("draw_not_found");
    err.status = 404;
    throw err;
  }

  return result.rows[0];
}

async function getDrawPriceCents(draw) {
  const drawId = Number(draw?.id);
  const columns = await query(
    `SELECT column_name
      FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name = 'draws'
        AND column_name IN ('ticket_price_cents', 'price_cents', 'quota_price_cents', 'amount_cents')`
  );
  const drawColumns = new Set((columns.rows || []).map((row) => row.column_name));

  for (const columnName of ["ticket_price_cents", "price_cents", "quota_price_cents", "amount_cents"]) {
    if (drawColumns.has(columnName)) {
      const value = toPositiveInt(draw?.[columnName]);
      if (value) return value;
    }
  }

  try {
    const configValue = toPositiveInt(await getGlobalTicketPriceCents());
    if (configValue) return configValue;
  } catch {}

  const legacy = await query(
    `SELECT value
       FROM public.app_config
      WHERE key IN ('ticket_price_cents', 'price_cents')
      ORDER BY updated_at DESC NULLS LAST
      LIMIT 1`
  );
  const legacyValue = toPositiveInt(legacy.rows?.[0]?.value);
  if (legacyValue) return legacyValue;

  const err = new Error("ticket_price_unavailable");
  err.status = 422;
  err.draw_id = drawId;
  throw err;
}

async function listActiveCaptives() {
  const profileColumns = await query(
    `SELECT 1
       FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'autopay_profiles'
        AND column_name = 'authorization_mode'
      LIMIT 1`
  );
  const hasAuthorizationMode = Boolean(profileColumns.rows?.length);
  if (!hasAuthorizationMode) {
    warn("profile_requires_preauth", {
      selected: 0,
      reason: "authorization_mode_column_missing",
    });
    return { rows: [], skippedNotificationsDisabled: 0 };
  }

  const numberColumns = await query(
    `SELECT column_name
       FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'autopay_numbers'
        AND column_name = 'preauth_notifications_enabled'`
  );
  const hasPreauthNotificationsEnabled = Boolean(numberColumns.rows?.length);
  const amountColumns = await query(
    `SELECT column_name
       FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'autopay_profiles'
        AND column_name = ANY($1::text[])`,
    [AUTOPAY_BASE_AMOUNT_COLUMNS]
  );
  const existingAmountColumns = new Set((amountColumns.rows || []).map((row) => row.column_name));
  const amountExpressions = AUTOPAY_BASE_AMOUNT_COLUMNS
    .filter((columnName) => existingAmountColumns.has(columnName))
    .map((columnName) => `ap.${columnName}`);
  const authorizedBaseAmountExpr = amountExpressions.length
    ? `COALESCE(${amountExpressions.join(", ")})`
    : "NULL";
  const preauthNotificationsFilter = hasPreauthNotificationsEnabled
    ? "AND COALESCE(an.preauth_notifications_enabled, true) = true"
    : "";
  let skippedNotificationsDisabled = 0;

  if (hasPreauthNotificationsEnabled) {
    const skippedResult = await query(
      `SELECT COUNT(*)::int AS count
         FROM public.autopay_numbers an
         JOIN public.autopay_profiles ap ON ap.id = an.autopay_id
        WHERE ap.active = true
          AND an.active = true
          AND COALESCE(ap.authorization_mode, false) = true
          AND COALESCE(an.preauth_notifications_enabled, true) = false`
    );
    skippedNotificationsDisabled = Number(skippedResult.rows?.[0]?.count || 0);
  }

  const result = await query(
    `SELECT
        an.id AS autopay_number_id,
        an.autopay_id AS autopay_profile_id,
        an.n AS captive_number,
        ap.user_id,
        ${authorizedBaseAmountExpr} AS authorized_base_amount_cents,
        u.name AS user_name,
        u.phone AS user_phone
       FROM public.autopay_numbers an
       JOIN public.autopay_profiles ap ON ap.id = an.autopay_id
       JOIN public.users u ON u.id = ap.user_id
      WHERE ap.active = true
        AND an.active = true
        AND COALESCE(ap.authorization_mode, false) = true
        ${preauthNotificationsFilter}
      ORDER BY an.n ASC, ap.user_id ASC`
  );
  log("profile_requires_preauth", {
    selected: result.rows?.length || 0,
    skipped_notifications_disabled: skippedNotificationsDisabled,
  });
  return { rows: result.rows || [], skippedNotificationsDisabled };
}

export function createToken() {
  return crypto.randomBytes(32).toString("base64url");
}

export function createConfirmationCode() {
  let code = "";
  for (let i = 0; i < 6; i += 1) {
    code += CONFIRMATION_CODE_ALPHABET[crypto.randomInt(CONFIRMATION_CODE_ALPHABET.length)];
  }
  return code;
}

export function normalizeConfirmationCode(code) {
  return String(code || "").trim().replace(/\s+/g, "").toUpperCase();
}

export function hashToken(token) {
  const value = String(token || "").trim();
  if (!value) return null;
  return crypto.createHash("sha256").update(value, "utf8").digest("hex");
}

export function hashConfirmationCode(code) {
  const value = normalizeConfirmationCode(code);
  if (!value) return null;
  return crypto.createHash("sha256").update(value, "utf8").digest("hex");
}

export function buildAuthorizeUrl(token) {
  return `${normalizeBaseUrl()}/cativo/autorizar?token=${encodeURIComponent(token)}`;
}

export function buildDeclineUrl(token) {
  return `${normalizeBaseUrl()}/cativo/recusar?token=${encodeURIComponent(token)}`;
}

export function buildManageUrl() {
  return resolveCaptiveConfirmationPublicUrl();
}

async function resolveCaptivePreauthTemplateConfig() {
  const envTemplateId = String(process.env.CAPTIVE_PREAUTH_BREVO_TEMPLATE_ID || "").trim();
  if (envTemplateId) {
    return {
      templateId: envTemplateId,
      source: "env",
      templateKey: PREAUTH_TEMPLATE_KEY,
    };
  }

  try {
    const result = await query(
      `SELECT provider_template_id
         FROM public.notification_templates
        WHERE LOWER(channel) = 'whatsapp'
          AND LOWER(provider) = 'brevo'
          AND template_key = $1
          AND COALESCE(is_active, true) = true
          AND provider_template_id IS NOT NULL
          AND TRIM(provider_template_id) <> ''
        ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
        LIMIT 1`,
      [PREAUTH_TEMPLATE_KEY]
    );
    const templateId = result.rows?.[0]?.provider_template_id;
    if (templateId != null && String(templateId).trim()) {
      return {
        templateId: String(templateId).trim(),
        source: "database",
        templateKey: PREAUTH_TEMPLATE_KEY,
      };
    }
  } catch (err) {
    if (err?.code !== "42P01" && err?.code !== "42703") throw err;
  }

  return {
    templateId: null,
    source: "missing",
    templateKey: PREAUTH_TEMPLATE_KEY,
  };
}

async function updateAuthorizationNotification({ authorizationId, dispatchId = null, status, errorMessage = null }) {
  const result = await query(
    `UPDATE public.autopay_draw_authorizations
        SET notification_dispatch_id = COALESCE($2::uuid, notification_dispatch_id),
            notification_status = $3,
            notification_error = $4,
            updated_at = now()
      WHERE id = $1
      RETURNING id, notification_dispatch_id, notification_status, notification_error`,
    [authorizationId, dispatchId, status, errorMessage ? safeError(errorMessage) : null]
  );
  return result.rows?.[0] || null;
}

async function getAuthorizationTableSchema() {
  const result = await query(
    `SELECT column_name
       FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'autopay_draw_authorizations'
        AND column_name IN ('confirmation_code_hash', 'confirmation_code_created_at')`
  );
  const columns = new Set((result.rows || []).map((row) => row.column_name));
  return {
    confirmation_code_hash: columns.has("confirmation_code_hash"),
    confirmation_code_created_at: columns.has("confirmation_code_created_at"),
  };
}

async function sendCaptivePreauthWhatsApp({
  authorization,
  captive,
  draw,
  authorizeUrl,
  declineUrl,
  manageUrl,
  confirmationCode,
  templateId,
  templateMode = "params",
}) {
  const authorizationId = String(authorization.id);
  const userId = Number(authorization.user_id);
  const captiveNumber = Number(authorization.captive_number);
  const customerName = String(captive.user_name || "").trim() || "Cliente";
  const drawTitle = String(getDrawTitle(draw) || `Sorteio #${draw.id}`);
  const amount = formatAmountBRL(authorization.amount_cents);
  const number = String(captiveNumber);
  const staticLinkMode = templateMode === "static_link";
  const params = staticLinkMode
    ? undefined
    : {
        customer_name: customerName,
        confirmation_code: confirmationCode || null,
        draw_title: drawTitle,
        amount,
        number,
        authorize_url: manageUrl,
        decline_url: manageUrl,
        manage_url: manageUrl,
      };
  const message = staticLinkMode
    ? null
    : buildCaptivePreauthMessage({
        customerName,
        confirmationCode,
      });

  if (staticLinkMode) {
    log("whatsapp_static_link_mode", {
      draw_id: Number(draw.id),
      user_id: userId,
      captive_number: captiveNumber,
      template_id: String(templateId || ""),
      static_link_mode: true,
      params_sent: false,
      confirmation_url_configured: Boolean(manageUrl),
    });
  }

  const consent = await assertWhatsAppConsent({
    userId,
    phone: captive.user_phone || null,
    category: WHATSAPP_CONSENT_CATEGORY_DEFAULT,
    source: "captive_preauth",
  });

  if (!consent.ok) {
    const reason = consent.reason || "whatsapp_consent_missing";
    await updateAuthorizationNotification({
      authorizationId,
      status: "skipped",
      errorMessage: reason,
    });
    logWhatsAppSkipped({
      drawId: draw.id,
      userId,
      captiveNumber,
      reason,
      templateId,
    });
    return { notification_status: "skipped", notification_error: reason, notification_dispatch_id: null };
  }

  const preResolvedRecipient = resolveRecipientForCurrentMode(captive.user_phone, {
    context: "captive_preauth",
  });
  const dispatch = await createDispatch({
    eventKey: "captive_preauth_request",
    channel: "whatsapp",
    provider: "brevo",
    userId,
    drawId: Number(draw.id),
    recipient: preResolvedRecipient.recipient || null,
    recipientOriginal: preResolvedRecipient.recipient_original || captive.user_phone || null,
    recipientForced: preResolvedRecipient.recipient_forced === true,
    templateKey: PREAUTH_TEMPLATE_KEY,
    providerTemplateId: templateId,
    payload: {
      authorization_id: authorizationId,
      captive_number: captiveNumber,
    },
    messageSnapshot: {
      template_key: PREAUTH_TEMPLATE_KEY,
      provider_template_id: templateId,
      template_mode: templateMode,
      ...(staticLinkMode ? {} : { params, message }),
    },
    recipientSnapshot: {
      user_id: userId,
      has_phone: Boolean(captive.user_phone),
      recipient_forced: preResolvedRecipient.recipient_forced === true,
      recipient_mode: preResolvedRecipient.recipient_mode || null,
      whatsapp_consent_status: consent.whatsapp_consent_status || null,
      whatsapp_can_send: consent.whatsapp_can_send === true,
    },
  });

  const result = await sendBrevoWhatsAppTemplate({
    to: captive.user_phone,
    templateId,
    params,
    templateKey: PREAUTH_TEMPLATE_KEY,
    correlationId: String(dispatch.id),
    context: "captive_preauth",
    source: "captive_preauth",
    isAutomation: false,
    isCampaign: true,
    consentChecked: true,
  });

  if (result?.ok) {
    await markDispatchAccepted({ dispatchId: dispatch.id, result });
    await updateAuthorizationNotification({
      authorizationId,
      dispatchId: dispatch.id,
      status: result.provider_status || "accepted",
      errorMessage: null,
    });
    log("whatsapp_sent", {
      authorization_id: authorizationId,
      draw_id: Number(draw.id),
      user_id: userId,
      captive_number: captiveNumber,
      dispatch_id: dispatch.id,
      provider_status: result.provider_status || "accepted",
    });
    return {
      notification_status: result.provider_status || "accepted",
      notification_error: null,
      notification_dispatch_id: String(dispatch.id),
    };
  }

  const failureStatus = result?.skipped ? "skipped" : "failed";
  const failureReason = safeError(result?.reason || result?.error || "whatsapp_send_failed");
  await markDispatchFailed({ dispatchId: dispatch.id, result, status: failureStatus });
  await updateAuthorizationNotification({
    authorizationId,
    dispatchId: dispatch.id,
    status: failureStatus,
    errorMessage: failureReason,
  });
  if (result?.skipped) {
    logWhatsAppSkipped({
      drawId: draw.id,
      userId,
      captiveNumber,
      reason: failureReason,
      templateId,
    });
  } else {
    warn("whatsapp_failed", {
      authorization_id: authorizationId,
      draw_id: Number(draw.id),
      user_id: userId,
      captive_number: captiveNumber,
      dispatch_id: dispatch.id,
      status: failureStatus,
      reason: failureReason,
    });
  }
  return {
    notification_status: failureStatus,
    notification_error: failureReason,
    notification_dispatch_id: String(dispatch.id),
  };
}

export async function createCaptivePreAuthorizationsForDraw(drawId, options = {}) {
  const adminUserId = options.adminUserId != null ? Number(options.adminUserId) : null;
  const draw = await getDraw(drawId);
  const amountCents = await getDrawPriceCents(draw);
  const activeCaptives = await listActiveCaptives();
  const captives = activeCaptives.rows || [];
  const skippedNotificationsDisabled = Number(activeCaptives.skippedNotificationsDisabled || 0);
  const whatsappEnabled = isCaptivePreauthWhatsAppEnabled();
  const templateMode = getCaptivePreauthTemplateMode();
  const confirmationPublicUrl = resolveCaptiveConfirmationPublicUrl();
  const authorizationSchema = await getAuthorizationTableSchema();
  const templateConfig = whatsappEnabled
    ? await resolveCaptivePreauthTemplateConfig()
    : { templateId: null, source: "missing", templateKey: PREAUTH_TEMPLATE_KEY };
  const templateId = templateConfig.templateId;

  log("create_started", {
    draw_id: Number(draw.id),
    admin_user_id: adminUserId,
    active_captives: captives.length,
    skipped_notifications_disabled: skippedNotificationsDisabled,
    whatsapp_enabled: whatsappEnabled,
    captive_preauth_template_mode: templateMode,
    confirmation_url_configured: Boolean(confirmationPublicUrl),
    captive_preauth_template_id: templateId || null,
    captive_preauth_template_source: templateConfig.source,
  });

  if (templateMode === "static_link" && !confirmationPublicUrl) {
    warn("config_missing", {
      draw_id: Number(draw.id),
      admin_user_id: adminUserId,
      config: "CAPTIVE_CONFIRMATION_PUBLIC_URL",
    });
  }

  if (!whatsappEnabled) {
    log("whatsapp_disabled", { draw_id: Number(draw.id), admin_user_id: adminUserId });
  } else if (!templateId) {
    warn("whatsapp_template_missing", { draw_id: Number(draw.id), admin_user_id: adminUserId });
  }

  if (!captives.length) {
    log("no_captives_found", { draw_id: Number(draw.id), admin_user_id: adminUserId });
    return {
      ok: true,
      draw_id: Number(draw.id),
      created: 0,
      already_exists: 0,
      skipped: 0,
      skipped_notifications_disabled: skippedNotificationsDisabled,
      items: [],
    };
  }

  let created = 0;
  let alreadyExists = 0;
  let skipped = 0;
  let skippedAmountNotIncreased = 0;
  const items = [];
  const expiresAt = new Date(Date.now() + getCaptivePreauthExpiresHours() * 60 * 60 * 1000);

  for (const captive of captives) {
    const captiveNumber = Number(captive.captive_number);
    const userId = Number(captive.user_id);
    const authorizedBaseAmountCents =
      toPositiveInt(captive.authorized_base_amount_cents) || getDefaultAuthorizedBaseAmountCents();

    if (!Number.isInteger(captiveNumber) || !Number.isInteger(userId)) {
      skipped++;
      continue;
    }

    if (!shouldRequireCaptivePreauth({
      currentAmountCents: amountCents,
      authorizedBaseAmountCents,
    })) {
      skipped++;
      skippedAmountNotIncreased++;
      log("skipped_amount_not_increased", {
        draw_id: Number(draw.id),
        user_id: userId,
        captive_number: captiveNumber,
        current_amount_cents: amountCents,
        authorized_base_amount_cents: authorizedBaseAmountCents,
      });
      continue;
    }

    log("required_amount_increased", {
      draw_id: Number(draw.id),
      user_id: userId,
      captive_number: captiveNumber,
      current_amount_cents: amountCents,
      authorized_base_amount_cents: authorizedBaseAmountCents,
    });

    const token = createToken();
    const tokenHash = hashToken(token);
    const canStoreConfirmationCode =
      authorizationSchema.confirmation_code_hash && authorizationSchema.confirmation_code_created_at;
    const confirmationCode = canStoreConfirmationCode ? createConfirmationCode() : null;
    const confirmationCodeHash = canStoreConfirmationCode ? hashConfirmationCode(confirmationCode) : null;
    const confirmationColumns = canStoreConfirmationCode
      ? ", confirmation_code_hash, confirmation_code_created_at"
      : "";
    const confirmationValues = canStoreConfirmationCode ? ", $8, now()" : "";
    const expiresAtParam = canStoreConfirmationCode ? "$9" : "$8";
    const createdByParam = canStoreConfirmationCode ? "$10" : "$9";
    const insertParams = [
      Number(draw.id),
      userId,
      captive.autopay_profile_id,
      captive.autopay_number_id,
      captiveNumber,
      amountCents,
      tokenHash,
    ];
    if (canStoreConfirmationCode) insertParams.push(confirmationCodeHash);
    insertParams.push(expiresAt, adminUserId);
    const insert = await query(
      `INSERT INTO public.autopay_draw_authorizations (
          draw_id,
          user_id,
          autopay_profile_id,
          autopay_number_id,
          captive_number,
          amount_cents,
          status,
          token_hash${confirmationColumns},
          expires_at,
          created_by
        )
       VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7${confirmationValues}, ${expiresAtParam}, ${createdByParam})
       ON CONFLICT (draw_id, user_id, captive_number) DO NOTHING
       RETURNING id, user_id, captive_number, amount_cents, status, expires_at,
                 notification_dispatch_id, notification_status, notification_error`,
      insertParams
    );

    if (insert.rowCount) {
      created++;
      const row = insert.rows[0];
      log("pending_created", {
        draw_id: Number(draw.id),
        authorization_id: row.id,
        user_id: Number(row.user_id),
        captive_number: Number(row.captive_number),
      });
      const authorizeUrl = buildAuthorizeUrl(token);
      const declineUrl = buildDeclineUrl(token);
      const manageUrl = confirmationPublicUrl;
      if (confirmationCode) {
        log("confirmation_code_created", {
          draw_id: Number(draw.id),
          user_id: Number(row.user_id),
          captive_number: Number(row.captive_number),
          code_present: true,
        });
      }
      let notification = {
        notification_dispatch_id: row.notification_dispatch_id || null,
        notification_status: row.notification_status || null,
        notification_error: row.notification_error || null,
      };

      if (!whatsappEnabled) {
        await updateAuthorizationNotification({
          authorizationId: row.id,
          status: "not_sent",
          errorMessage: null,
        });
        notification = { ...notification, notification_status: "not_sent", notification_error: null };
      } else if (!templateId) {
        await updateAuthorizationNotification({
          authorizationId: row.id,
          status: "skipped",
          errorMessage: "whatsapp_template_missing",
        });
        logWhatsAppSkipped({
          drawId: draw.id,
          userId: Number(row.user_id),
          captiveNumber: Number(row.captive_number),
          reason: "whatsapp_template_missing",
          templateId,
        });
        notification = { ...notification, notification_status: "skipped", notification_error: "whatsapp_template_missing" };
      } else {
        try {
          notification = await sendCaptivePreauthWhatsApp({
            authorization: row,
            captive,
            draw,
            authorizeUrl,
            declineUrl,
            manageUrl,
            confirmationCode,
            templateId,
            templateMode,
          });
        } catch (err) {
          const failureReason = safeError(err?.message || "whatsapp_send_failed");
          await updateAuthorizationNotification({
            authorizationId: row.id,
            status: "failed",
            errorMessage: failureReason,
          });
          error("whatsapp_failed", {
            authorization_id: row.id,
            draw_id: Number(draw.id),
            user_id: Number(row.user_id),
            captive_number: Number(row.captive_number),
            reason: failureReason,
          });
          notification = {
            notification_dispatch_id: null,
            notification_status: "failed",
            notification_error: failureReason,
          };
        }
      }

      items.push({
        authorization_id: String(row.id),
        user_id: Number(row.user_id),
        captive_number: Number(row.captive_number),
        status: safeStatus(row.status),
        amount_cents: Number(row.amount_cents),
        expires_at: row.expires_at,
        authorize_url: authorizeUrl,
        decline_url: declineUrl,
        ...notification,
      });
      continue;
    }

    alreadyExists++;
    const existing = await query(
      `SELECT id, user_id, captive_number, amount_cents, status, expires_at,
              notification_dispatch_id, notification_status, notification_error
         FROM public.autopay_draw_authorizations
        WHERE draw_id = $1
          AND user_id = $2
          AND captive_number = $3
        LIMIT 1`,
      [Number(draw.id), userId, captiveNumber]
    );
    const row = existing.rows?.[0];
    log("pending_already_exists", {
      draw_id: Number(draw.id),
      authorization_id: row?.id || null,
      user_id: userId,
      captive_number: captiveNumber,
      status: row?.status || null,
    });
    if (row) {
      items.push({
        authorization_id: String(row.id),
        user_id: Number(row.user_id),
        captive_number: Number(row.captive_number),
        status: safeStatus(row.status),
        amount_cents: Number(row.amount_cents),
        expires_at: row.expires_at,
        authorize_url: null,
        decline_url: null,
        notification_dispatch_id: row.notification_dispatch_id || null,
        notification_status: row.notification_status || null,
        notification_error: row.notification_error || null,
      });
    }
  }

  return {
    ok: true,
    draw_id: Number(draw.id),
    created,
    already_exists: alreadyExists,
    skipped,
    skipped_notifications_disabled: skippedNotificationsDisabled,
    skipped_amount_not_increased: skippedAmountNotIncreased,
    whatsapp_enabled: whatsappEnabled,
    captive_preauth_template_mode: templateMode,
    captive_confirmation_public_url_configured: Boolean(confirmationPublicUrl),
    captive_preauth_template_id: templateId || null,
    captive_preauth_template_source: templateConfig.source,
    captive_preauth_template_key: PREAUTH_TEMPLATE_KEY,
    items,
  };
}

async function getAuthorizationByToken(token) {
  const tokenHash = hashToken(token);
  if (!tokenHash) return null;
  const result = await query(
    `SELECT *
       FROM public.autopay_draw_authorizations
      WHERE token_hash = $1
      LIMIT 1`,
    [tokenHash]
  );
  return result.rows?.[0] || null;
}

async function getAuthorizationByConfirmationCode(code) {
  const codeHash = hashConfirmationCode(code);
  if (!codeHash) return { error: "invalid_confirmation_code", row: null };
  const result = await query(
    `SELECT *
       FROM public.autopay_draw_authorizations
      WHERE confirmation_code_hash = $1`,
    [codeHash]
  );
  if ((result.rows || []).length > 1) {
    return { error: "duplicate_confirmation_code", row: null };
  }
  return { error: null, row: result.rows?.[0] || null };
}

async function getAuthorizationById(id) {
  const result = await query(
    `SELECT *
       FROM public.autopay_draw_authorizations
      WHERE id = $1
      LIMIT 1`,
    [id]
  );
  return result.rows?.[0] || null;
}

function alreadyDecidedResponse(row) {
  const status = safeStatus(row?.status);
  return {
    ok: true,
    code: "already_decided",
    already_decided: true,
    status,
    message: "Esta decisão já foi registrada anteriormente.",
    authorization: row,
  };
}

async function expireAuthorization(row) {
  const updated = await query(
    `UPDATE public.autopay_draw_authorizations
        SET status = 'expired',
            expired_at = COALESCE(expired_at, now()),
            updated_at = now()
      WHERE id = $1
        AND status = 'pending'
      RETURNING *`,
    [row.id]
  );
  return updated.rows?.[0] || { ...row, status: "expired" };
}

export async function expirePendingCaptivePreauths() {
  const result = await query(
    `UPDATE public.autopay_draw_authorizations
        SET status = 'expired',
            expired_at = COALESCE(expired_at, now()),
            updated_at = now()
      WHERE status = 'pending'
        AND expires_at IS NOT NULL
        AND expires_at <= now()
      RETURNING id, draw_id, user_id, captive_number`
  );
  const rows = result.rows || [];
  log("expired_pending_scan", {
    expired_count: rows.length,
    draw_ids: [...new Set(rows.map((row) => Number(row.draw_id)).filter(Number.isFinite))],
  });
  return { expired_count: rows.length, rows };
}

async function publicAuthorization(row) {
  if (!row) return null;
  let drawTitle = `Sorteio #${row.draw_id}`;
  try {
    const draw = await getDraw(row.draw_id);
    drawTitle = String(getDrawTitle(draw) || drawTitle);
  } catch {}
  return {
    status: safeStatus(row.status),
    draw_title: drawTitle,
    captive_number: Number(row.captive_number),
    amount: formatAmountBRL(row.amount_cents),
  };
}

async function lookupPublicUser({ email, phone }) {
  const normalizedEmail = normalizeEmail(email);
  const phoneVariants = phoneLookupVariants(phone);
  if (!normalizedEmail || !phoneVariants.length) return null;

  const result = await query(
    `SELECT id
       FROM public.users
      WHERE LOWER(TRIM(email)) = $1
        AND regexp_replace(COALESCE(phone, ''), '\\D', '', 'g') = ANY($2::text[])
      LIMIT 1`,
    [normalizedEmail, phoneVariants]
  );
  return result.rows?.[0] || null;
}

function logPublicEvent(event, { email, phone, userId = null, found = false, authorizationId = null, status = null, expired = false } = {}) {
  log(event, {
    email_present: Boolean(normalizeEmail(email)),
    phone_present: Boolean(normalizePhoneDigits(phone)),
    found: Boolean(found),
    user_id: userId != null ? Number(userId) : null,
    authorization_id: authorizationId || null,
    status: status || null,
    expired: Boolean(expired),
  });
}

export async function lookupCaptivePreauthPublic({ email, phone }) {
  await expirePendingCaptivePreauths();
  const user = await lookupPublicUser({ email, phone });
  if (!user) {
    logPublicEvent("public_lookup", { email, phone, found: false });
    return { ok: true, items: [] };
  }

  const result = await query(
    `SELECT id, draw_id, user_id, captive_number, amount_cents, status, expires_at
       FROM public.autopay_draw_authorizations
      WHERE user_id = $1
        AND status = 'pending'
        AND expires_at > now()
      ORDER BY expires_at ASC, created_at ASC`,
    [user.id]
  );
  const rows = result.rows || [];
  const items = await Promise.all(
    rows.map(async (row) => ({
      id: String(row.id),
      draw_id: Number(row.draw_id),
      captive_number: Number(row.captive_number),
      amount: formatAmountBRL(row.amount_cents),
      status: safeStatus(row.status),
      expires_at: row.expires_at,
      draw_title: (await publicAuthorization(row))?.draw_title || `Sorteio #${row.draw_id}`,
    }))
  );

  logPublicEvent("public_lookup", {
    email,
    phone,
    userId: user.id,
    found: items.length > 0,
    status: items.length ? "pending" : "empty",
  });
  return { ok: true, items };
}

async function getPublicAuthorizationForDecision({ email, phone, authorizationId }) {
  await expirePendingCaptivePreauths();
  const user = await lookupPublicUser({ email, phone });
  if (!user) return { user: null, row: null };
  const id = String(authorizationId || "").trim();
  if (!UUID_RE.test(id)) return { user, row: null };
  const result = await query(
    `SELECT *
       FROM public.autopay_draw_authorizations
      WHERE id = $1
        AND user_id = $2
      LIMIT 1`,
    [id, user.id]
  );
  return { user, row: result.rows?.[0] || null };
}

export async function authorizeCaptivePreauthPublic({ email, phone, authorizationId }) {
  const { user, row } = await getPublicAuthorizationForDecision({ email, phone, authorizationId });
  if (!user || !row) {
    logPublicEvent("public_authorize", { email, phone, found: false, authorizationId });
    return { ok: false, code: "authorization_not_found", status: "not_found" };
  }
  const result = await applyAuthorizationDecision(row, "authorize", "public");
  if (
    result.ok &&
    result.code === "authorized_success" &&
    isCaptivePreauthChargeOnAuthorizeEnabled()
  ) {
    const chargeResult = await chargeAuthorizedCaptivePreauth(result.authorization.id);
    const status = chargeResult.status || "failed";
    logPublicEvent("public_authorize", {
      email,
      phone,
      userId: user.id,
      found: true,
      authorizationId,
      status,
      expired: status === "expired",
    });
    if (!chargeResult.ok) {
      return { ok: false, code: chargeResult.code || "payment_failed", status, charged: false };
    }
    return { ...result, status: "charged", charged: true, charge: chargeResult };
  }

  logPublicEvent("public_authorize", {
    email,
    phone,
    userId: user.id,
    found: true,
    authorizationId,
    status: result.status,
    expired: result.status === "expired",
  });
  return { ...result, charged: false };
}

export async function declineCaptivePreauthPublic({ email, phone, authorizationId }) {
  const { user, row } = await getPublicAuthorizationForDecision({ email, phone, authorizationId });
  if (!user || !row) {
    logPublicEvent("public_decline", { email, phone, found: false, authorizationId });
    return { ok: false, code: "authorization_not_found", status: "not_found" };
  }
  const result = await applyAuthorizationDecision(row, "decline", "public");
  logPublicEvent("public_decline", {
    email,
    phone,
    userId: user.id,
    found: true,
    authorizationId,
    status: result.status,
    expired: result.status === "expired",
  });
  return result;
}

async function applyAuthorizationDecision(row, decision, source = "token") {
  if (!row) {
    const code = source === "confirmation_code" ? "invalid_confirmation_code" : "token_invalid";
    warn(source === "confirmation_code" ? "confirmation_code_invalid" : "token_invalid", {
      action: decision,
      code_present: source === "confirmation_code" ? false : undefined,
    });
    return { ok: false, code, status: "invalid" };
  }

  const currentStatus = safeStatus(row.status);
  if (currentStatus !== "pending") {
    return alreadyDecidedResponse(row);
  }

  if (new Date(row.expires_at).getTime() <= Date.now()) {
    const expired = await expireAuthorization(row);
    warn(source === "confirmation_code" ? "confirmation_code_expired" : "token_expired", {
      authorization_id: row.id,
      draw_id: Number(row.draw_id),
      user_id: Number(row.user_id),
      action: decision,
    });
    return {
      ok: false,
      code: source === "confirmation_code" ? "confirmation_code_expired" : "token_expired",
      status: "expired",
      authorization: expired,
    };
  }

  const isAuthorize = decision === "authorize";
  const nextStatus = isAuthorize ? "authorized" : "declined";
  const atColumn = isAuthorize ? "authorized_at" : "declined_at";
  const updated = await query(
    `UPDATE public.autopay_draw_authorizations
        SET status = $2,
            ${atColumn} = now(),
            updated_at = now()
      WHERE id = $1
        AND status = 'pending'
      RETURNING *`,
    [row.id, nextStatus]
  );
  const out = updated.rows?.[0] || null;
  if (!out) {
    const current = await getAuthorizationById(row.id);
    return alreadyDecidedResponse(current || row);
  }

  log(isAuthorize ? "authorize_success" : "decline_success", {
    authorization_id: row.id,
    draw_id: Number(row.draw_id),
    user_id: Number(row.user_id),
    captive_number: Number(row.captive_number),
  });
  return { ok: true, code: `${nextStatus}_success`, status: nextStatus, authorization: out };
}

async function applyTokenDecision(token, decision) {
  const row = await getAuthorizationByToken(token);
  return applyAuthorizationDecision(row, decision, "token");
}

export async function lookupCaptivePreauthByCode(code) {
  const normalized = normalizeConfirmationCode(code);
  const lookup = await getAuthorizationByConfirmationCode(normalized);
  if (lookup.error || !lookup.row) {
    warn("confirmation_code_lookup", {
      found: false,
      status: "invalid",
      expired: false,
      code_present: Boolean(normalized),
    });
    return { ok: false, error: lookup.error || "invalid_confirmation_code" };
  }

  let row = lookup.row;
  let expired = safeStatus(row.status) === "expired";
  if (safeStatus(row.status) === "pending" && new Date(row.expires_at).getTime() < Date.now()) {
    row = await expireAuthorization(row);
    expired = true;
  }

  warn("confirmation_code_lookup", {
    found: true,
    status: safeStatus(row.status),
    expired,
    code_present: true,
  });
  return { ok: true, authorization: await publicAuthorization(row) };
}

export async function authorizeCaptivePreauthByCode(code) {
  const normalized = normalizeConfirmationCode(code);
  const lookup = await getAuthorizationByConfirmationCode(normalized);
  if (lookup.error || !lookup.row) {
    return { ok: false, code: lookup.error || "invalid_confirmation_code", status: "invalid" };
  }
  return applyAuthorizationDecision(lookup.row, "authorize", "confirmation_code");
}

export async function declineCaptivePreauthByCode(code) {
  const normalized = normalizeConfirmationCode(code);
  const lookup = await getAuthorizationByConfirmationCode(normalized);
  if (lookup.error || !lookup.row) {
    return { ok: false, code: lookup.error || "invalid_confirmation_code", status: "invalid" };
  }
  return applyAuthorizationDecision(lookup.row, "decline", "confirmation_code");
}

export function isCaptivePreauthEnabled() {
  return envBool("CAPTIVE_PREAUTH_ENABLED", false);
}

export function isCaptivePreauthChargeOnAuthorizeEnabled() {
  return envBool("CAPTIVE_PREAUTH_CHARGE_ON_AUTHORIZE_ENABLED", false);
}

export async function chargeAuthorizedCaptivePreauth(authorizationId, options = {}) {
  return chargeAuthorizedCaptivePreauthWithAutopay(authorizationId, options);
}

export function isCaptivePreauthExpiryScanEnabled() {
  return envBool("CAPTIVE_PREAUTH_EXPIRY_SCAN_ENABLED", true);
}

export function getCaptivePreauthExpiryScanIntervalMs() {
  return toPositiveInt(process.env.CAPTIVE_PREAUTH_EXPIRY_SCAN_INTERVAL_MS) || 300000;
}

export { isCaptivePreauthWhatsAppEnabled };

export async function authorizeCaptivePreauthByToken(token) {
  return applyTokenDecision(token, "authorize");
}

export async function declineCaptivePreauthByToken(token) {
  return applyTokenDecision(token, "decline");
}

export default {
  createCaptivePreAuthorizationsForDraw,
  createToken,
  createConfirmationCode,
  hashToken,
  hashConfirmationCode,
  buildAuthorizeUrl,
  buildDeclineUrl,
  buildManageUrl,
  shouldRequireCaptivePreauth,
  getCaptivePreauthTemplateMode,
  resolveCaptiveConfirmationPublicUrl,
  lookupCaptivePreauthByCode,
  authorizeCaptivePreauthByToken,
  declineCaptivePreauthByToken,
  authorizeCaptivePreauthByCode,
  declineCaptivePreauthByCode,
  lookupCaptivePreauthPublic,
  authorizeCaptivePreauthPublic,
  declineCaptivePreauthPublic,
  chargeAuthorizedCaptivePreauth,
  expirePendingCaptivePreauths,
  isCaptivePreauthEnabled,
  isCaptivePreauthChargeOnAuthorizeEnabled,
  isCaptivePreauthWhatsAppEnabled,
  isCaptivePreauthExpiryScanEnabled,
  getCaptivePreauthExpiryScanIntervalMs,
};
