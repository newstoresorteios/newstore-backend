import crypto from "node:crypto";
import { getPool, query } from "../../db.js";
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

function makeReservationId() {
  return crypto.randomUUID();
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

async function resolveDrawAmountCents(draw, options = {}) {
  const hasExplicitAmount = options.amountCents !== undefined && options.amountCents !== null;
  const amountCents = hasExplicitAmount ? toPositiveInt(options.amountCents) : await getDrawPriceCents(draw);
  if (!amountCents) {
    const err = new Error("invalid_ticket_price");
    err.status = 400;
    err.draw_id = Number(draw?.id);
    throw err;
  }
  return amountCents;
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
  const authorizationModeExpr = hasAuthorizationMode ? "COALESCE(ap.authorization_mode, false)" : "false";
  const preauthNotificationsExpr = hasPreauthNotificationsEnabled
    ? "COALESCE(an.preauth_notifications_enabled, true)"
    : "true";
  let skippedNotificationsDisabled = 0;

  if (hasPreauthNotificationsEnabled) {
    const skippedResult = await query(
      `SELECT COUNT(*)::int AS count
         FROM public.autopay_numbers an
         JOIN public.autopay_profiles ap ON ap.id = an.autopay_id
        WHERE ap.active = true
          AND an.active = true
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
        ${authorizationModeExpr} AS authorization_mode,
        ${preauthNotificationsExpr} AS preauth_notifications_enabled,
        u.name AS user_name,
        u.phone AS user_phone
       FROM public.autopay_numbers an
       JOIN public.autopay_profiles ap ON ap.id = an.autopay_id
       JOIN public.users u ON u.id = ap.user_id
      WHERE ap.active = true
        AND an.active = true
      ORDER BY an.n ASC, ap.user_id ASC`
  );
  log("active_captives_for_preauth", {
    selected: result.rows?.length || 0,
    skipped_notifications_disabled: skippedNotificationsDisabled,
    authorization_mode_column_present: hasAuthorizationMode,
    preauth_notifications_column_present: hasPreauthNotificationsEnabled,
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

async function markAuthorizationReservationFailed(authorizationId, reason) {
  const failureReason = safeError(reason || "number_not_available");
  const result = await query(
    `UPDATE public.autopay_draw_authorizations
        SET status = 'failed',
            notification_status = 'skipped',
            notification_error = $2,
            updated_at = now()
      WHERE id = $1
        AND status = 'pending'
      RETURNING *`,
    [authorizationId, failureReason]
  );
  return result.rows?.[0] || null;
}

async function createPendingCaptiveReservationForAuthorization(authorization) {
  const drawId = Number(authorization?.draw_id);
  const userId = Number(authorization?.user_id);
  const captiveNumber = Number(authorization?.captive_number);
  const expiresAt = authorization?.expires_at ? new Date(authorization.expires_at) : null;

  if (
    !Number.isInteger(drawId) ||
    !Number.isInteger(userId) ||
    !Number.isInteger(captiveNumber) ||
    !(expiresAt instanceof Date) ||
    Number.isNaN(expiresAt.getTime())
  ) {
    return { ok: false, reason: "invalid_authorization" };
  }

  const pool = await getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const locked = await client.query(
      `SELECT n, status, reservation_id
         FROM public.numbers
        WHERE draw_id = $1
          AND n = $2
        FOR UPDATE`,
      [drawId, captiveNumber]
    );
    if (!locked.rowCount) {
      await client.query("ROLLBACK");
      return { ok: false, reason: "number_not_found" };
    }

    let numberRow = locked.rows[0];
    if (String(numberRow.status || "").toLowerCase() === "sold") {
      await client.query("ROLLBACK");
      return { ok: false, reason: "number_sold" };
    }

    if (String(numberRow.status || "").toLowerCase() === "reserved" && numberRow.reservation_id) {
      const currentReservation = await client.query(
        `SELECT id, user_id, numbers, status, expires_at
           FROM public.reservations
          WHERE id = $1
          FOR UPDATE`,
        [numberRow.reservation_id]
      );
      const reservation = currentReservation.rows?.[0] || null;
      if (!reservation) {
        await client.query(
          `UPDATE public.numbers
              SET status = 'available',
                  reservation_id = NULL
            WHERE draw_id = $1
              AND n = $2
              AND reservation_id = $3
              AND status = 'reserved'`,
          [drawId, captiveNumber, numberRow.reservation_id]
        );
      } else {
        const reservationStatus = String(reservation?.status || "").toLowerCase();
        const isBlocking = ["active", "pending", "reserved", ""].includes(reservationStatus);
        const reservationExpiresAt = reservation?.expires_at ? new Date(reservation.expires_at).getTime() : null;
        const isExpired = reservationExpiresAt && reservationExpiresAt <= Date.now();
        const belongsToAuthorization =
          reservation &&
          Number(reservation.user_id) === userId &&
          (reservation.numbers || []).map(Number).includes(captiveNumber);

        if (isBlocking && !isExpired && belongsToAuthorization) {
          await client.query("COMMIT");
          return {
            ok: true,
            reservation_id: String(reservation.id),
            already_reserved: true,
          };
        }

        if (isBlocking && isExpired) {
          await client.query(
            `UPDATE public.reservations
                SET status = 'expired',
                    expires_at = now()
              WHERE id = $1`,
            [reservation.id]
          );
          await client.query(
            `UPDATE public.numbers
                SET status = 'available',
                    reservation_id = NULL
              WHERE draw_id = $1
                AND n = $2
                AND reservation_id = $3
                AND status = 'reserved'`,
            [drawId, captiveNumber, reservation.id]
          );
        } else if (isBlocking) {
          await client.query("ROLLBACK");
          return { ok: false, reason: "number_not_available" };
        } else {
          await client.query(
            `UPDATE public.numbers
                SET status = 'available',
                    reservation_id = NULL
              WHERE draw_id = $1
                AND n = $2
                AND reservation_id = $3
                AND status = 'reserved'`,
            [drawId, captiveNumber, numberRow.reservation_id]
          );
        }
      }
    }

    const after = await client.query(
      `SELECT n, status, reservation_id
         FROM public.numbers
        WHERE draw_id = $1
          AND n = $2
        FOR UPDATE`,
      [drawId, captiveNumber]
    );
    numberRow = after.rows?.[0] || null;
    if (!numberRow || String(numberRow.status || "").toLowerCase() !== "available") {
      await client.query("ROLLBACK");
      return { ok: false, reason: "number_not_available" };
    }

    const reservationId = makeReservationId();
    await client.query(
      `INSERT INTO public.reservations (id, user_id, draw_id, numbers, status, created_at, expires_at)
       VALUES ($1, $2, $3, $4::int[], 'pending', now(), $5)`,
      [reservationId, userId, drawId, [captiveNumber], expiresAt]
    );
    const updated = await client.query(
      `UPDATE public.numbers
          SET status = 'reserved',
              reservation_id = $3
        WHERE draw_id = $1
          AND n = $2
          AND status = 'available'`,
      [drawId, captiveNumber, reservationId]
    );
    if (updated.rowCount !== 1) {
      await client.query("ROLLBACK");
      return { ok: false, reason: "number_not_available" };
    }

    await client.query("COMMIT");
    return { ok: true, reservation_id: reservationId, already_reserved: false };
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    throw err;
  } finally {
    client.release();
  }
}

async function releasePendingCaptiveReservationForAuthorization(authorization) {
  const drawId = Number(authorization?.draw_id);
  const userId = Number(authorization?.user_id);
  const captiveNumber = Number(authorization?.captive_number);
  if (!Number.isInteger(drawId) || !Number.isInteger(userId) || !Number.isInteger(captiveNumber)) {
    return { ok: false, released: false, reason: "invalid_authorization" };
  }

  const pool = await getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const reservationResult = await client.query(
      `SELECT id, numbers, status, expires_at
         FROM public.reservations
        WHERE draw_id = $1
          AND user_id = $2
          AND $3 = ANY(numbers)
          AND lower(coalesce(status, '')) IN ('pending', 'active', 'reserved', '')
        ORDER BY
          CASE WHEN lower(coalesce(status, '')) = 'pending' THEN 0 ELSE 1 END,
          expires_at DESC NULLS LAST,
          created_at DESC NULLS LAST
        LIMIT 1
        FOR UPDATE`,
      [drawId, userId, captiveNumber]
    );
    const reservation = reservationResult.rows?.[0] || null;
    if (!reservation) {
      await client.query("COMMIT");
      return { ok: true, released: false, reason: "reservation_not_found" };
    }

    await client.query(
      `UPDATE public.reservations
          SET status = 'expired',
              expires_at = now()
        WHERE id = $1
          AND lower(coalesce(status, '')) IN ('pending', 'active', 'reserved', '')`,
      [reservation.id]
    );
    const numberUpdate = await client.query(
      `UPDATE public.numbers
          SET status = 'available',
              reservation_id = NULL
        WHERE draw_id = $1
          AND n = $2
          AND reservation_id = $3
          AND status = 'reserved'`,
      [drawId, captiveNumber, reservation.id]
    );

    await client.query("COMMIT");
    return {
      ok: true,
      released: numberUpdate.rowCount > 0,
      reservation_id: String(reservation.id),
    };
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    throw err;
  } finally {
    client.release();
  }
}

async function validateExistingCaptivePreauthReservation(authorization) {
  const drawId = Number(authorization?.draw_id);
  const userId = Number(authorization?.user_id);
  const captiveNumber = Number(authorization?.captive_number);
  if (!Number.isInteger(drawId) || !Number.isInteger(userId) || !Number.isInteger(captiveNumber)) {
    return { ok: false, reason: "invalid_authorization" };
  }

  const result = await query(
    `SELECT n.status AS number_status,
            n.reservation_id,
            r.user_id,
            r.numbers,
            r.status AS reservation_status,
            r.expires_at
       FROM public.numbers n
       LEFT JOIN public.reservations r ON r.id = n.reservation_id
      WHERE n.draw_id = $1
        AND n.n = $2
      LIMIT 1`,
    [drawId, captiveNumber]
  );
  const row = result.rows?.[0] || null;
  if (!row) return { ok: false, reason: "number_not_found" };
  if (String(row.number_status || "").toLowerCase() !== "reserved" || !row.reservation_id) {
    return { ok: false, reason: "preauth_reservation_not_available" };
  }

  const reservationStatus = String(row.reservation_status || "").toLowerCase();
  const reservationExpiresAt = row.expires_at ? new Date(row.expires_at).getTime() : null;
  const isBlocking = ["active", "pending", "reserved", ""].includes(reservationStatus);
  const isExpired = reservationExpiresAt && reservationExpiresAt <= Date.now();
  const belongsToAuthorization =
    Number(row.user_id) === userId &&
    (row.numbers || []).map(Number).includes(captiveNumber);

  if (!isBlocking || isExpired || !belongsToAuthorization) {
    return { ok: false, reason: isExpired ? "preauth_reservation_expired" : "preauth_reservation_not_available" };
  }
  return { ok: true, reservation_id: String(row.reservation_id) };
}

export async function createCaptivePreAuthorizationsForDraw(drawId, options = {}) {
  const adminUserId = options.adminUserId != null ? Number(options.adminUserId) : null;
  const draw = await getDraw(drawId);
  const amountCents = await resolveDrawAmountCents(draw, options);
  const defaultAmountCents = getDefaultAuthorizedBaseAmountCents();
  const preauthRequiredByDrawAmount = shouldRequireCaptivePreauth({
    currentAmountCents: amountCents,
    authorizedBaseAmountCents: defaultAmountCents,
  });

  if (!preauthRequiredByDrawAmount) {
    log("skipped_amount_not_above_default", {
      draw_id: Number(draw.id),
      current_amount_cents: amountCents,
      default_amount_cents: defaultAmountCents,
    });
    return {
      ok: true,
      draw_id: Number(draw.id),
      skipped: true,
      reason: "amount_not_above_default",
      draw_ticket_price_cents: amountCents,
      default_amount_cents: defaultAmountCents,
      required: false,
      created: 0,
      already_exists: 0,
      items: [],
    };
  }

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
  let reservationCreated = 0;
  let reservationAlreadyExists = 0;
  let reservationFailed = 0;
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

    log("required_amount_increased", {
      draw_id: Number(draw.id),
      user_id: userId,
      captive_number: captiveNumber,
      current_amount_cents: amountCents,
      authorized_base_amount_cents: authorizedBaseAmountCents,
      default_amount_cents: defaultAmountCents,
      authorization_mode: captive.authorization_mode === true,
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
      const reservation = await createPendingCaptiveReservationForAuthorization({
        ...row,
        draw_id: Number(draw.id),
      });
      if (!reservation.ok) {
        reservationFailed++;
        skipped++;
        const reason = reservation.reason || "number_not_available";
        const failedAuthorization = await markAuthorizationReservationFailed(row.id, reason);
        warn("reservation_failed", {
          draw_id: Number(draw.id),
          authorization_id: row.id,
          user_id: Number(row.user_id),
          captive_number: Number(row.captive_number),
          reason,
        });
        items.push({
          authorization_id: String(row.id),
          user_id: Number(row.user_id),
          captive_number: Number(row.captive_number),
          status: safeStatus(failedAuthorization?.status || "failed"),
          amount_cents: Number(row.amount_cents),
          expires_at: row.expires_at,
          reservation_status: "failed",
          reservation_error: reason,
          notification_status: failedAuthorization?.notification_status || "skipped",
          notification_error: failedAuthorization?.notification_error || reason,
        });
        continue;
      }
      if (reservation.already_reserved) reservationAlreadyExists++;
      else reservationCreated++;
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
      } else if (captive.preauth_notifications_enabled === false) {
        await updateAuthorizationNotification({
          authorizationId: row.id,
          status: "skipped",
          errorMessage: "preauth_notifications_disabled",
        });
        logWhatsAppSkipped({
          drawId: draw.id,
          userId: Number(row.user_id),
          captiveNumber: Number(row.captive_number),
          reason: "preauth_notifications_disabled",
          templateId,
        });
        notification = { ...notification, notification_status: "skipped", notification_error: "preauth_notifications_disabled" };
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
        reservation_id: reservation.reservation_id || null,
        reservation_status: reservation.already_reserved ? "already_reserved" : "created",
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
      let reservation = { ok: true, reservation_id: null, already_reserved: false };
      if (safeStatus(row.status) === "pending") {
        reservation = await createPendingCaptiveReservationForAuthorization({
          ...row,
          draw_id: Number(draw.id),
        });
        if (!reservation.ok) {
          reservationFailed++;
          const reason = reservation.reason || "number_not_available";
          const failedAuthorization = await markAuthorizationReservationFailed(row.id, reason);
          warn("existing_reservation_failed", {
            draw_id: Number(draw.id),
            authorization_id: row.id,
            user_id: userId,
            captive_number: captiveNumber,
            reason,
          });
          items.push({
            authorization_id: String(row.id),
            user_id: Number(row.user_id),
            captive_number: Number(row.captive_number),
            status: safeStatus(failedAuthorization?.status || "failed"),
            amount_cents: Number(row.amount_cents),
            expires_at: row.expires_at,
            reservation_status: "failed",
            reservation_error: reason,
            notification_dispatch_id: row.notification_dispatch_id || null,
            notification_status: failedAuthorization?.notification_status || row.notification_status || "skipped",
            notification_error: failedAuthorization?.notification_error || row.notification_error || reason,
          });
          continue;
        }
        if (reservation.already_reserved) reservationAlreadyExists++;
        else reservationCreated++;
      }
      items.push({
        authorization_id: String(row.id),
        user_id: Number(row.user_id),
        captive_number: Number(row.captive_number),
        status: safeStatus(row.status),
        amount_cents: Number(row.amount_cents),
        expires_at: row.expires_at,
        reservation_id: reservation.reservation_id || null,
        reservation_status: reservation.already_reserved ? "already_reserved" : (reservation.reservation_id ? "created" : null),
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
    skipped_amount_not_increased: 0,
    reservation_created: reservationCreated,
    reservation_already_exists: reservationAlreadyExists,
    reservation_failed: reservationFailed,
    whatsapp_enabled: whatsappEnabled,
    captive_preauth_template_mode: templateMode,
    captive_confirmation_public_url_configured: Boolean(confirmationPublicUrl),
    captive_preauth_template_id: templateId || null,
    captive_preauth_template_source: templateConfig.source,
    captive_preauth_template_key: PREAUTH_TEMPLATE_KEY,
    items,
  };
}

async function hasCompletedCaptivePreauthCharge(authorizationId) {
  const result = await query(
    `SELECT 1
       FROM public.autopay_runs
      WHERE status = 'charged_ok'
        AND provider_request->>'authorization_id' = $1
      LIMIT 1`,
    [String(authorizationId)]
  );
  return result.rowCount > 0;
}

async function hasApprovedPaymentForAuthorization(authorization) {
  const drawId = Number(authorization?.draw_id);
  const userId = Number(authorization?.user_id);
  const captiveNumber = Number(authorization?.captive_number);
  if (!Number.isInteger(drawId) || !Number.isInteger(userId) || !Number.isInteger(captiveNumber)) {
    return false;
  }
  const result = await query(
    `SELECT 1
       FROM public.payments
      WHERE draw_id = $1
        AND user_id = $2
        AND lower(status) IN ('approved', 'paid', 'pago')
        AND $3 = ANY(numbers)
      LIMIT 1`,
    [drawId, userId, captiveNumber]
  );
  return result.rowCount > 0;
}

async function getRecoverableFailedAuthorizationInfo(authorization, options = {}) {
  const status = safeStatus(authorization?.status);
  const base = {
    recoverable: false,
    retryable: false,
    reason: null,
    reservation_id: null,
  };
  if (status !== "failed") return { ...base, reason: "status_not_failed" };

  const expiresAt = authorization?.expires_at ? new Date(authorization.expires_at).getTime() : null;
  if (!expiresAt || expiresAt <= Date.now()) return { ...base, reason: "authorization_expired" };

  const reservation = options.reservation || await validateExistingCaptivePreauthReservation(authorization);
  if (!reservation.ok) {
    warn("failed_authorization_not_recoverable", {
      authorization_id: authorization?.id || null,
      draw_id: Number(authorization?.draw_id),
      user_id: Number(authorization?.user_id),
      captive_number: Number(authorization?.captive_number),
      reservation_id: reservation.reservation_id || null,
      reason: reservation.reason || "reservation_invalid",
    });
    return { ...base, reason: reservation.reason || "reservation_invalid" };
  }

  if (await hasCompletedCaptivePreauthCharge(authorization.id)) {
    warn("failed_authorization_not_recoverable", {
      authorization_id: authorization.id,
      draw_id: Number(authorization.draw_id),
      user_id: Number(authorization.user_id),
      captive_number: Number(authorization.captive_number),
      reservation_id: reservation.reservation_id || null,
      reason: "charged_ok_exists",
    });
    return { ...base, reason: "charged_ok_exists", reservation_id: reservation.reservation_id || null };
  }

  if (await hasApprovedPaymentForAuthorization(authorization)) {
    warn("failed_authorization_not_recoverable", {
      authorization_id: authorization.id,
      draw_id: Number(authorization.draw_id),
      user_id: Number(authorization.user_id),
      captive_number: Number(authorization.captive_number),
      reservation_id: reservation.reservation_id || null,
      reason: "approved_payment_exists",
    });
    return { ...base, reason: "approved_payment_exists", reservation_id: reservation.reservation_id || null };
  }

  log("failed_authorization_recoverable", {
    authorization_id: authorization.id,
    draw_id: Number(authorization.draw_id),
    user_id: Number(authorization.user_id),
    captive_number: Number(authorization.captive_number),
    reservation_id: reservation.reservation_id || null,
    reason: "reservation_valid",
  });
  return {
    recoverable: true,
    retryable: true,
    reason: "reservation_valid",
    reservation_id: reservation.reservation_id || null,
  };
}

async function isRecoverableFailedAuthorization(authorization) {
  const info = await getRecoverableFailedAuthorizationInfo(authorization);
  return info.recoverable === true;
}

async function listPendingCaptivePreauthsForReissue(drawId) {
  const numberColumns = await query(
    `SELECT column_name
       FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'autopay_numbers'
        AND column_name = 'preauth_notifications_enabled'`
  );
  const hasPreauthNotificationsEnabled = Boolean(numberColumns.rows?.length);
  const preauthNotificationsExpr = hasPreauthNotificationsEnabled
    ? "COALESCE(an.preauth_notifications_enabled, true)"
    : "true";

  const result = await query(
    `SELECT ada.*,
            ${preauthNotificationsExpr} AS preauth_notifications_enabled,
            u.name AS user_name,
            u.phone AS user_phone
       FROM public.autopay_draw_authorizations ada
       JOIN public.autopay_profiles ap ON ap.id = ada.autopay_profile_id
        AND ap.user_id = ada.user_id
        AND ap.active = true
       JOIN public.autopay_numbers an ON an.id = ada.autopay_number_id
        AND an.autopay_id = ap.id
        AND an.n = ada.captive_number
        AND an.active = true
       JOIN public.users u ON u.id = ada.user_id
      WHERE ada.draw_id = $1
        AND ada.status IN ('pending', 'failed')
        AND ada.expires_at > now()
      ORDER BY ada.expires_at ASC, ada.created_at ASC`,
    [Number(drawId)]
  );
  return result.rows || [];
}

async function reissuePendingCaptivePreauthCredentials({ authorizationId, amountCents, schema }) {
  const token = createToken();
  const tokenHash = hashToken(token);
  let confirmationCode = null;
  const params = [authorizationId, amountCents, tokenHash];
  const setParts = [
    "amount_cents = $2",
    "token_hash = $3",
    "status = 'pending'",
    "authorized_at = NULL",
    "charged_at = NULL",
    "updated_at = now()",
  ];

  if (schema.confirmation_code_hash && schema.confirmation_code_created_at) {
    confirmationCode = createConfirmationCode();
    params.push(hashConfirmationCode(confirmationCode));
    setParts.push(`confirmation_code_hash = $${params.length}`);
    setParts.push("confirmation_code_created_at = now()");
  }

  const result = await query(
    `UPDATE public.autopay_draw_authorizations
        SET ${setParts.join(", ")}
      WHERE id = $1
        AND status IN ('pending', 'failed')
        AND expires_at > now()
      RETURNING *`,
    params
  );

  return {
    token,
    confirmationCode,
    authorization: result.rows?.[0] || null,
  };
}

function classifyReissueNotification(result) {
  const status = String(result?.notification_status || "").toLowerCase();
  const reason = String(result?.notification_error || "").toLowerCase();
  if (status === "skipped" && reason.includes("consent")) return "skipped_consent";
  if (status === "skipped") return "skipped_other";
  if (status && status !== "failed") return "sent";
  return "failed";
}

export async function reissueAndResendPendingCaptivePreauths({ drawId, adminUserId }) {
  const id = Number(drawId);
  if (!Number.isInteger(id) || id <= 0) {
    const err = new Error("invalid_draw_id");
    err.status = 400;
    throw err;
  }

  const draw = await getDraw(id);
  if (String(draw?.draw_type || "principal").toLowerCase() !== "principal") {
    const err = new Error("draw_not_principal");
    err.status = 400;
    throw err;
  }

  const officialAmountCents = await resolveDrawAmountCents(draw);
  const defaultAmountCents = getDefaultAuthorizedBaseAmountCents();
  if (!shouldRequireCaptivePreauth({
    currentAmountCents: officialAmountCents,
    authorizedBaseAmountCents: defaultAmountCents,
  })) {
    const err = new Error("captive_preauth_not_required");
    err.status = 409;
    throw err;
  }

  await expirePendingCaptivePreauths();

  const rows = await listPendingCaptivePreauthsForReissue(id);
  const schema = await getAuthorizationTableSchema();
  const whatsappEnabled = isCaptivePreauthWhatsAppEnabled();
  const templateMode = getCaptivePreauthTemplateMode();
  const confirmationPublicUrl = resolveCaptiveConfirmationPublicUrl();
  const templateConfig = whatsappEnabled
    ? await resolveCaptivePreauthTemplateConfig()
    : { templateId: null, source: "missing", templateKey: PREAUTH_TEMPLATE_KEY };
  const templateId = templateConfig.templateId;
  const seen = new Set();
  const summary = {
    ok: true,
    draw_id: id,
    official_amount_cents: officialAmountCents,
    pending_found: rows.length,
    amount_corrected: 0,
    failed_recovered: 0,
    credentials_reissued: 0,
    sent: 0,
    skipped_consent: 0,
    skipped_notifications_disabled: 0,
    skipped_near_expiration: 0,
    skipped_reservation_unavailable: 0,
    skipped_already_charged: 0,
    skipped_whatsapp_disabled: 0,
    skipped_template_missing: 0,
    skipped_other: 0,
    failed: 0,
    untouched_non_pending: 0,
  };

  for (const row of rows) {
    const authorizationId = String(row.id);
    if (seen.has(authorizationId)) continue;
    seen.add(authorizationId);

    const reservation = await validateExistingCaptivePreauthReservation(row);
    if (!reservation.ok) {
      summary.skipped_reservation_unavailable++;
      continue;
    }

    if (safeStatus(row.status) === "failed") {
      const recoverable = await getRecoverableFailedAuthorizationInfo(row, { reservation });
      if (!recoverable.recoverable) {
        summary.skipped_other++;
        continue;
      }
    } else if (safeStatus(row.status) !== "pending") {
      summary.untouched_non_pending++;
      continue;
    }

    if (await hasCompletedCaptivePreauthCharge(authorizationId)) {
      summary.skipped_already_charged++;
      continue;
    }

    const expiresAtMs = row.expires_at ? new Date(row.expires_at).getTime() : 0;
    const nearExpiration = expiresAtMs && expiresAtMs - Date.now() < 10 * 60 * 1000;
    if (nearExpiration) {
      const reissued = await reissuePendingCaptivePreauthCredentials({
        authorizationId,
        amountCents: officialAmountCents,
        schema,
      });
      if (reissued.authorization) {
        if (Number(row.amount_cents) !== officialAmountCents) summary.amount_corrected++;
        if (safeStatus(row.status) === "failed") summary.failed_recovered++;
        summary.credentials_reissued++;
      }
      summary.skipped_near_expiration++;
      continue;
    }

    const reissued = await reissuePendingCaptivePreauthCredentials({
      authorizationId,
      amountCents: officialAmountCents,
      schema,
    });
    if (!reissued.authorization) {
      summary.untouched_non_pending++;
      continue;
    }
    if (Number(row.amount_cents) !== officialAmountCents) summary.amount_corrected++;
    if (safeStatus(row.status) === "failed") {
      summary.failed_recovered++;
      log("failed_authorization_recovered", {
        authorization_id: authorizationId,
        draw_id: id,
        user_id: Number(row.user_id),
        captive_number: Number(row.captive_number),
        reservation_id: reservation.reservation_id || null,
        reason: "admin_reissue",
      });
    }
    summary.credentials_reissued++;

    if (!whatsappEnabled) {
      await updateAuthorizationNotification({
        authorizationId,
        status: "not_sent",
        errorMessage: null,
      });
      summary.skipped_whatsapp_disabled++;
      continue;
    }

    if (row.preauth_notifications_enabled === false) {
      await updateAuthorizationNotification({
        authorizationId,
        status: "skipped",
        errorMessage: "preauth_notifications_disabled",
      });
      summary.skipped_notifications_disabled++;
      continue;
    }

    if (!templateId) {
      await updateAuthorizationNotification({
        authorizationId,
        status: "skipped",
        errorMessage: "whatsapp_template_missing",
      });
      summary.skipped_template_missing++;
      continue;
    }

    try {
      const notification = await sendCaptivePreauthWhatsApp({
        authorization: reissued.authorization,
        captive: row,
        draw,
        authorizeUrl: buildAuthorizeUrl(reissued.token),
        declineUrl: buildDeclineUrl(reissued.token),
        manageUrl: confirmationPublicUrl,
        confirmationCode: reissued.confirmationCode,
        templateId,
        templateMode,
      });
      summary[classifyReissueNotification(notification)]++;
    } catch (err) {
      const failureReason = safeError(err?.message || "whatsapp_send_failed");
      await updateAuthorizationNotification({
        authorizationId,
        status: "failed",
        errorMessage: failureReason,
      });
      error("admin_reissue_whatsapp_failed", {
        authorization_id: authorizationId,
        draw_id: id,
        admin_user_id: adminUserId || null,
        reason: failureReason,
      });
      summary.failed++;
    }
  }

  log("admin_reissue_resend_completed", {
    admin_user_id: adminUserId || null,
    draw_id: id,
    pending_found: summary.pending_found,
    amount_corrected: summary.amount_corrected,
    sent: summary.sent,
    skipped:
      summary.skipped_consent +
      summary.skipped_notifications_disabled +
      summary.skipped_near_expiration +
      summary.skipped_reservation_unavailable +
      summary.skipped_already_charged +
      summary.skipped_whatsapp_disabled +
      summary.skipped_template_missing +
      summary.skipped_other,
    failed: summary.failed,
  });

  return summary;
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
  if (status === "failed") {
    return {
      ok: false,
      code: "payment_failed",
      status: "failed",
      retryable: true,
      message: "A tentativa de cobrança não foi concluída. A participação ainda pode ser confirmada novamente enquanto a reserva estiver válida.",
      authorization: row,
    };
  }
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
  const expired = updated.rows?.[0] || { ...row, status: "expired" };
  try {
    await releasePendingCaptiveReservationForAuthorization(expired);
  } catch (err) {
    warn("reservation_release_failed", {
      authorization_id: expired?.id || row?.id || null,
      draw_id: expired?.draw_id || row?.draw_id || null,
      reason: safeError(err?.message || "release_failed"),
    });
  }
  return expired;
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
  let releasedReservations = 0;
  for (const row of rows) {
    try {
      const release = await releasePendingCaptiveReservationForAuthorization(row);
      if (release.released) releasedReservations++;
    } catch (err) {
      warn("reservation_release_failed", {
        authorization_id: row.id,
        draw_id: Number(row.draw_id),
        user_id: Number(row.user_id),
        captive_number: Number(row.captive_number),
        reason: safeError(err?.message || "release_failed"),
      });
    }
  }
  log("expired_pending_scan", {
    expired_count: rows.length,
    released_reservations: releasedReservations,
    draw_ids: [...new Set(rows.map((row) => Number(row.draw_id)).filter(Number.isFinite))],
  });
  return { expired_count: rows.length, released_reservations: releasedReservations, rows };
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
        AND status IN ('pending', 'failed')
        AND expires_at > now()
      ORDER BY expires_at ASC, created_at ASC`,
    [user.id]
  );
  const rows = result.rows || [];
  const filteredRows = [];
  for (const row of rows) {
    if (safeStatus(row.status) === "failed" && !(await isRecoverableFailedAuthorization(row))) continue;
    filteredRows.push(row);
  }
  const items = await Promise.all(
    filteredRows.map(async (row) => ({
      id: String(row.id),
      draw_id: Number(row.draw_id),
      captive_number: Number(row.captive_number),
      amount_cents: Number(row.amount_cents),
      amount: formatAmountBRL(row.amount_cents),
      status: safeStatus(row.status),
      retryable: safeStatus(row.status) === "failed",
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

function normalizeUserId(value) {
  const id = Number(value);
  return Number.isInteger(id) && id > 0 ? id : null;
}

async function userHasCaptiveNumber(userId) {
  const id = normalizeUserId(userId);
  if (!id) return false;
  const result = await query(
    `SELECT 1
      FROM public.autopay_profiles ap
       JOIN public.autopay_numbers an ON an.autopay_id = ap.id
      WHERE ap.user_id = $1
        AND ap.active = true
        AND an.active = true
      LIMIT 1`,
    [id]
  );
  return result.rowCount > 0;
}

export async function lookupCaptivePreauthForUser(userId) {
  const id = normalizeUserId(userId);
  if (!id) return { ok: true, has_captive: false, items: [] };

  await expirePendingCaptivePreauths();

  const hasCaptive = await userHasCaptiveNumber(id);
  if (!hasCaptive) {
    return { ok: true, has_captive: false, items: [] };
  }

  const result = await query(
    `SELECT id, draw_id, user_id, captive_number, amount_cents, status, expires_at
       FROM public.autopay_draw_authorizations
      WHERE user_id = $1
        AND status IN ('pending', 'failed')
        AND expires_at > now()
      ORDER BY expires_at ASC, created_at ASC`,
    [id]
  );
  const rows = result.rows || [];
  const filteredRows = [];
  for (const row of rows) {
    if (safeStatus(row.status) === "failed" && !(await isRecoverableFailedAuthorization(row))) continue;
    filteredRows.push(row);
  }
  const items = await Promise.all(
    filteredRows.map(async (row) => ({
      id: String(row.id),
      draw_id: Number(row.draw_id),
      draw_title: (await publicAuthorization(row))?.draw_title || `Sorteio #${row.draw_id}`,
      captive_number: Number(row.captive_number),
      amount_cents: Number(row.amount_cents),
      amount: formatAmountBRL(row.amount_cents),
      status: safeStatus(row.status),
      retryable: safeStatus(row.status) === "failed",
      expires_at: row.expires_at instanceof Date ? row.expires_at.toISOString() : row.expires_at,
    }))
  );

  log("account_lookup", {
    user_id: id,
    has_captive: true,
    pending_count: items.length,
  });
  return { ok: true, has_captive: true, items };
}

async function getAuthorizationForUserDecision({ userId, authorizationId }) {
  await expirePendingCaptivePreauths();
  const id = normalizeUserId(userId);
  const authId = String(authorizationId || "").trim();
  if (!id || !UUID_RE.test(authId)) return { row: null };
  const result = await query(
    `SELECT *
       FROM public.autopay_draw_authorizations
      WHERE id = $1
        AND user_id = $2
      LIMIT 1`,
    [authId, id]
  );
  return { row: result.rows?.[0] || null };
}

export async function authorizeCaptivePreauthForUser({ userId, authorizationId }) {
  const { row } = await getAuthorizationForUserDecision({ userId, authorizationId });
  if (!row) {
    log("account_authorize_not_found", {
      user_id: normalizeUserId(userId),
      authorization_id: authorizationId || null,
    });
    return { ok: false, code: "authorization_not_found", status: "not_found" };
  }
  const result = await applyAuthorizationDecision(row, "authorize", "account");
  const finalResult = await chargeAfterAuthorizationIfEnabled(result, "account");
  log("account_authorize", {
    user_id: Number(row.user_id),
    authorization_id: row.id,
    status: finalResult.status,
    already_decided: finalResult.already_decided === true,
  });
  return finalResult;
}

export async function declineCaptivePreauthForUser({ userId, authorizationId }) {
  const { row } = await getAuthorizationForUserDecision({ userId, authorizationId });
  if (!row) {
    log("account_decline_not_found", {
      user_id: normalizeUserId(userId),
      authorization_id: authorizationId || null,
    });
    return { ok: false, code: "authorization_not_found", status: "not_found" };
  }
  const result = await applyAuthorizationDecision(row, "decline", "account");
  log("account_decline", {
    user_id: Number(row.user_id),
    authorization_id: row.id,
    status: result.status,
    already_decided: result.already_decided === true,
  });
  return result;
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
  const finalResult = await chargeAfterAuthorizationIfEnabled(result, "public");

  logPublicEvent("public_authorize", {
    email,
    phone,
    userId: user.id,
    found: true,
    authorizationId,
    status: finalResult.status,
    expired: finalResult.status === "expired",
  });
  return finalResult;
}

async function chargeAfterAuthorizationIfEnabled(result, source = "public") {
  if (
    result.ok &&
    result.code === "authorized_success" &&
    isCaptivePreauthChargeOnAuthorizeEnabled()
  ) {
    const chargeResult = await chargeAuthorizedCaptivePreauth(result.authorization.id);
    const status = chargeResult.status || "failed";
    if (!chargeResult.ok) {
      return { ok: false, code: chargeResult.code || "payment_failed", status, charged: false, source };
    }
    return { ...result, status: "charged", charged: true, charge: chargeResult, source };
  }
  return { ...result, charged: false, source };
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
  if (currentStatus === "failed") {
    const recoverable = await getRecoverableFailedAuthorizationInfo(row);
    if (!recoverable.recoverable) {
      return {
        ok: false,
        code: "payment_failed",
        status: "failed",
        retryable: false,
        message: "A tentativa de cobrança não foi concluída, mas a reserva não está mais disponível para nova tentativa.",
        authorization: row,
      };
    }
    if (decision === "authorize") {
      return recoverFailedAuthorizationForRetry(row, source);
    }
    if (decision === "decline") {
      const updated = await query(
        `UPDATE public.autopay_draw_authorizations
            SET status = 'declined',
                declined_at = now(),
                updated_at = now()
          WHERE id = $1
            AND status = 'failed'
          RETURNING *`,
        [row.id]
      );
      const out = updated.rows?.[0] || null;
      if (!out) {
        const current = await getAuthorizationById(row.id);
        return alreadyDecidedResponse(current || row);
      }
      try {
        await releasePendingCaptiveReservationForAuthorization(out);
      } catch (err) {
        warn("reservation_release_failed", {
          authorization_id: row.id,
          draw_id: Number(row.draw_id),
          user_id: Number(row.user_id),
          captive_number: Number(row.captive_number),
          reason: safeError(err?.message || "release_failed"),
        });
      }
      return { ok: true, code: "declined_success", status: "declined", authorization: out };
    }
  }
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
  if (!isAuthorize) {
    try {
      await releasePendingCaptiveReservationForAuthorization(out);
    } catch (err) {
      warn("reservation_release_failed", {
        authorization_id: row.id,
        draw_id: Number(row.draw_id),
        user_id: Number(row.user_id),
        captive_number: Number(row.captive_number),
        reason: safeError(err?.message || "release_failed"),
      });
    }
  }
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

export async function lookupCaptivePreauthByToken(token) {
  const row = await getAuthorizationByToken(token);
  if (!row) {
    return { ok: false, code: "token_invalid", status: "invalid" };
  }

  const status = safeStatus(row.status);
  const expiresAt = row.expires_at ? new Date(row.expires_at).getTime() : null;
  if (status === "pending" && expiresAt && expiresAt <= Date.now()) {
    return {
      ok: false,
      code: "token_expired",
      status: "expired",
      authorization: await publicAuthorization({ ...row, status: "expired" }),
    };
  }

  return {
    ok: true,
    status,
    authorization: await publicAuthorization(row),
  };
}

async function recoverFailedAuthorizationForRetry(row, source = "account") {
  const pool = await getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const locked = await client.query(
      `SELECT *
         FROM public.autopay_draw_authorizations
        WHERE id = $1
        FOR UPDATE`,
      [row.id]
    );
    const current = locked.rows?.[0] || null;
    if (!current || safeStatus(current.status) !== "failed") {
      await client.query("ROLLBACK");
      return alreadyDecidedResponse(current || row);
    }

    const numberResult = await client.query(
      `SELECT status AS number_status,
              reservation_id
         FROM public.numbers
        WHERE draw_id = $1
          AND n = $2
        FOR UPDATE`,
      [Number(current.draw_id), Number(current.captive_number)]
    );
    const numberRow = numberResult.rows?.[0] || null;
    let reservationRow = null;
    if (numberRow?.reservation_id) {
      const reservationResult = await client.query(
        `SELECT user_id, numbers, status AS reservation_status, expires_at
           FROM public.reservations
          WHERE id = $1
          FOR UPDATE`,
        [numberRow.reservation_id]
      );
      reservationRow = reservationResult.rows?.[0] || null;
    }
    const reservationStatus = String(reservationRow?.reservation_status || "").toLowerCase();
    const reservationExpiresAt = reservationRow?.expires_at ? new Date(reservationRow.expires_at).getTime() : null;
    const reservationValid =
      numberRow &&
      reservationRow &&
      String(numberRow.number_status || "").toLowerCase() === "reserved" &&
      numberRow.reservation_id &&
      ["pending", "active", "reserved", ""].includes(reservationStatus) &&
      (!reservationExpiresAt || reservationExpiresAt > Date.now()) &&
      Number(reservationRow.user_id) === Number(current.user_id) &&
      (reservationRow.numbers || []).map(Number).includes(Number(current.captive_number));

    if (!reservationValid) {
      await client.query("ROLLBACK");
      warn("failed_authorization_not_recoverable", {
        authorization_id: current.id,
        draw_id: Number(current.draw_id),
        user_id: Number(current.user_id),
        captive_number: Number(current.captive_number),
        reservation_id: numberRow?.reservation_id || null,
        reason: "reservation_invalid",
      });
      return {
        ok: false,
        code: "payment_failed",
        status: "failed",
        retryable: false,
        authorization: current,
      };
    }

    const charged = await client.query(
      `SELECT 1
         FROM public.autopay_runs
        WHERE status = 'charged_ok'
          AND provider_request->>'authorization_id' = $1
        LIMIT 1`,
      [String(current.id)]
    );
    const approvedPayment = await client.query(
      `SELECT 1
         FROM public.payments
        WHERE draw_id = $1
          AND user_id = $2
          AND lower(status) IN ('approved', 'paid', 'pago')
          AND $3 = ANY(numbers)
        LIMIT 1`,
      [Number(current.draw_id), Number(current.user_id), Number(current.captive_number)]
    );
    if (charged.rowCount || approvedPayment.rowCount) {
      await client.query("ROLLBACK");
      warn("failed_authorization_not_recoverable", {
        authorization_id: current.id,
        draw_id: Number(current.draw_id),
        user_id: Number(current.user_id),
        captive_number: Number(current.captive_number),
        reservation_id: numberRow.reservation_id || null,
        reason: charged.rowCount ? "charged_ok_exists" : "approved_payment_exists",
      });
      return {
        ok: false,
        code: "payment_failed",
        status: "failed",
        retryable: false,
        authorization: current,
      };
    }

    const expiresAt = current.expires_at ? new Date(current.expires_at).getTime() : null;
    if (!expiresAt || expiresAt <= Date.now()) {
      await client.query("ROLLBACK");
      return {
        ok: false,
        code: "token_expired",
        status: "expired",
        authorization: current,
      };
    }

    const updated = await client.query(
      `UPDATE public.autopay_draw_authorizations
          SET status = 'authorized',
              authorized_at = now(),
              charged_at = NULL,
              updated_at = now()
        WHERE id = $1
          AND status = 'failed'
        RETURNING *`,
      [current.id]
    );
    await client.query("COMMIT");
    const out = updated.rows?.[0] || current;
    log("failed_authorization_retry_started", {
      authorization_id: out.id,
      draw_id: Number(out.draw_id),
      user_id: Number(out.user_id),
      captive_number: Number(out.captive_number),
      reservation_id: numberRow.reservation_id || null,
      reason: source,
    });
    return { ok: true, code: "authorized_success", status: "authorized", authorization: out, retryable: true };
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    throw err;
  } finally {
    client.release();
  }
}

export async function authorizeCaptivePreauthByCode(code) {
  const normalized = normalizeConfirmationCode(code);
  const lookup = await getAuthorizationByConfirmationCode(normalized);
  if (lookup.error || !lookup.row) {
    return { ok: false, code: lookup.error || "invalid_confirmation_code", status: "invalid" };
  }
  const result = await applyAuthorizationDecision(lookup.row, "authorize", "confirmation_code");
  return chargeAfterAuthorizationIfEnabled(result, "confirmation_code");
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

export async function resolveCaptivePreauthDrawRequirement(drawId, options = {}) {
  const draw = await getDraw(drawId);
  const drawTicketPriceCents = await resolveDrawAmountCents(draw, options);
  const defaultAmountCents = getDefaultAuthorizedBaseAmountCents();
  return {
    draw_id: Number(draw.id),
    draw_ticket_price_cents: drawTicketPriceCents,
    default_amount_cents: defaultAmountCents,
    required: drawTicketPriceCents > defaultAmountCents,
  };
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
  const result = await applyTokenDecision(token, "authorize");
  return chargeAfterAuthorizationIfEnabled(result, "token");
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
  lookupCaptivePreauthByToken,
  authorizeCaptivePreauthByToken,
  declineCaptivePreauthByToken,
  authorizeCaptivePreauthByCode,
  declineCaptivePreauthByCode,
  lookupCaptivePreauthPublic,
  authorizeCaptivePreauthPublic,
  declineCaptivePreauthPublic,
  lookupCaptivePreauthForUser,
  authorizeCaptivePreauthForUser,
  declineCaptivePreauthForUser,
  reissueAndResendPendingCaptivePreauths,
  chargeAuthorizedCaptivePreauth,
  expirePendingCaptivePreauths,
  isCaptivePreauthEnabled,
  resolveCaptivePreauthDrawRequirement,
  isCaptivePreauthChargeOnAuthorizeEnabled,
  isCaptivePreauthWhatsAppEnabled,
  isCaptivePreauthExpiryScanEnabled,
  getCaptivePreauthExpiryScanIntervalMs,
};
