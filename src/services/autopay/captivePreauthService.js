import crypto from "node:crypto";
import { query } from "../../db.js";
import { getTicketPriceCents as getGlobalTicketPriceCents } from "../config.js";
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

function toPositiveInt(value) {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : null;
}

function safeStatus(status) {
  const s = String(status || "").trim().toLowerCase();
  return AUTHORIZATION_STATUSES.has(s) ? s : "unknown";
}

function safeError(value, fallback = "notification_failed") {
  const text = String(value || fallback).trim();
  return text.replace(/[\r\n\t]+/g, " ").slice(0, 180);
}

function isCaptivePreauthWhatsAppEnabled() {
  return String(process.env.CAPTIVE_PREAUTH_WHATSAPP_ENABLED || "false").trim().toLowerCase() === "true";
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

function buildCaptivePreauthMessage({ customerName, drawTitle, amount, number, authorizeUrl, declineUrl }) {
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
        AND column_name IN ('ticket_price_cents', 'price_cents', 'amount_cents')`
  );
  const drawColumns = new Set((columns.rows || []).map((row) => row.column_name));

  for (const columnName of ["ticket_price_cents", "price_cents", "amount_cents"]) {
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
  const columns = await query(
    `SELECT 1
       FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'autopay_profiles'
        AND column_name = 'authorization_mode'
      LIMIT 1`
  );
  const hasAuthorizationMode = Boolean(columns.rows?.length);
  if (!hasAuthorizationMode) {
    warn("profile_requires_preauth", {
      selected: 0,
      reason: "authorization_mode_column_missing",
    });
    return [];
  }

  const result = await query(
    `SELECT
        an.id AS autopay_number_id,
        an.autopay_id AS autopay_profile_id,
        an.n AS captive_number,
        ap.user_id,
        u.name AS user_name,
        u.phone AS user_phone
       FROM public.autopay_numbers an
       JOIN public.autopay_profiles ap ON ap.id = an.autopay_id
       JOIN public.users u ON u.id = ap.user_id
      WHERE ap.active = true
        AND an.active = true
        AND COALESCE(ap.authorization_mode, false) = true
      ORDER BY an.n ASC, ap.user_id ASC`
  );
  log("profile_requires_preauth", { selected: result.rows?.length || 0 });
  return result.rows || [];
}

export function createToken() {
  return crypto.randomBytes(32).toString("base64url");
}

export function hashToken(token) {
  const value = String(token || "").trim();
  if (!value) return null;
  return crypto.createHash("sha256").update(value, "utf8").digest("hex");
}

export function buildAuthorizeUrl(token) {
  return `${normalizeBaseUrl()}/cativo/autorizar?token=${encodeURIComponent(token)}`;
}

export function buildDeclineUrl(token) {
  return `${normalizeBaseUrl()}/cativo/recusar?token=${encodeURIComponent(token)}`;
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

async function sendCaptivePreauthWhatsApp({ authorization, captive, draw, authorizeUrl, declineUrl, templateId }) {
  const authorizationId = String(authorization.id);
  const userId = Number(authorization.user_id);
  const captiveNumber = Number(authorization.captive_number);
  const customerName = String(captive.user_name || "").trim() || "Cliente";
  const drawTitle = String(getDrawTitle(draw) || `Sorteio #${draw.id}`);
  const amount = formatAmountBRL(authorization.amount_cents);
  const number = String(captiveNumber);
  const params = {
    customer_name: customerName,
    draw_title: drawTitle,
    amount,
    number,
    authorize_url: authorizeUrl,
    decline_url: declineUrl,
  };
  const message = buildCaptivePreauthMessage({
    customerName,
    drawTitle,
    amount,
    number,
    authorizeUrl,
    declineUrl,
  });

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
      params,
      message,
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
  const captives = await listActiveCaptives();
  const whatsappEnabled = isCaptivePreauthWhatsAppEnabled();
  const templateConfig = whatsappEnabled
    ? await resolveCaptivePreauthTemplateConfig()
    : { templateId: null, source: "missing", templateKey: PREAUTH_TEMPLATE_KEY };
  const templateId = templateConfig.templateId;

  log("create_started", {
    draw_id: Number(draw.id),
    admin_user_id: adminUserId,
    active_captives: captives.length,
    whatsapp_enabled: whatsappEnabled,
    captive_preauth_template_id: templateId || null,
    captive_preauth_template_source: templateConfig.source,
  });

  if (!whatsappEnabled) {
    log("whatsapp_disabled", { draw_id: Number(draw.id), admin_user_id: adminUserId });
  } else if (!templateId) {
    warn("whatsapp_template_missing", { draw_id: Number(draw.id), admin_user_id: adminUserId });
  }

  if (!captives.length) {
    log("no_captives_found", { draw_id: Number(draw.id), admin_user_id: adminUserId });
    return { ok: true, draw_id: Number(draw.id), created: 0, already_exists: 0, skipped: 0, items: [] };
  }

  let created = 0;
  let alreadyExists = 0;
  let skipped = 0;
  const items = [];
  const expiresAt = new Date(Date.now() + 12 * 60 * 60 * 1000);

  for (const captive of captives) {
    const captiveNumber = Number(captive.captive_number);
    const userId = Number(captive.user_id);

    if (!Number.isInteger(captiveNumber) || !Number.isInteger(userId)) {
      skipped++;
      continue;
    }

    const token = createToken();
    const tokenHash = hashToken(token);
    const insert = await query(
      `INSERT INTO public.autopay_draw_authorizations (
          draw_id,
          user_id,
          autopay_profile_id,
          autopay_number_id,
          captive_number,
          amount_cents,
          status,
          token_hash,
          expires_at,
          created_by
        )
       VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, $8, $9)
       ON CONFLICT (draw_id, user_id, captive_number) DO NOTHING
       RETURNING id, user_id, captive_number, amount_cents, status, expires_at,
                 notification_dispatch_id, notification_status, notification_error`,
      [
        Number(draw.id),
        userId,
        captive.autopay_profile_id,
        captive.autopay_number_id,
        captiveNumber,
        amountCents,
        tokenHash,
        expiresAt,
        adminUserId,
      ]
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
            templateId,
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
    whatsapp_enabled: whatsappEnabled,
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

async function applyTokenDecision(token, decision) {
  const row = await getAuthorizationByToken(token);
  if (!row) {
    warn("token_invalid", { action: decision });
    return { ok: false, code: "token_invalid", status: "invalid" };
  }

  const currentStatus = safeStatus(row.status);
  if (currentStatus !== "pending") {
    return alreadyDecidedResponse(row);
  }

  if (new Date(row.expires_at).getTime() < Date.now()) {
    const expired = await expireAuthorization(row);
    warn("token_expired", {
      authorization_id: row.id,
      draw_id: Number(row.draw_id),
      user_id: Number(row.user_id),
      action: decision,
    });
    return {
      ok: false,
      code: "token_expired",
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

export function isCaptivePreauthEnabled() {
  return String(process.env.CAPTIVE_PREAUTH_ENABLED || "false").trim().toLowerCase() === "true";
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
  hashToken,
  buildAuthorizeUrl,
  buildDeclineUrl,
  authorizeCaptivePreauthByToken,
  declineCaptivePreauthByToken,
  isCaptivePreauthEnabled,
  isCaptivePreauthWhatsAppEnabled,
};
