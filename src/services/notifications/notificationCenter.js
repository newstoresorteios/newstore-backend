// src/services/notifications/notificationCenter.js
import { query } from "../../db.js";
import {
  sendBrevoWhatsAppTemplate,
  normalizePhoneBR,
  isTruthy,
  isWhatsAppEnabled,
  getTestRecipient,
  shouldForceTestRecipient,
  isTestModeActive,
  isAllowRealRecipients,
  isAdminTestCustomRecipientsEnabled,
  resolveRecipientForCurrentMode,
} from "./brevoWhatsApp.js";
import {
  createDispatch,
  createCampaign,
  updateCampaignAudienceCounts,
  markDispatchAccepted,
  markDispatchFailed,
  extractDispatchErrorMessage,
} from "./notificationLog.js";
import {
  WHATSAPP_CONSENT_CATEGORY_DEFAULT,
  assertWhatsAppConsent,
  countWhatsAppConsentForAudience,
  getWhatsappConsentStatusForUser,
  isWhatsAppConsentRequired,
  isUnlinkedWhatsAppPhoneAllowed,
} from "./communicationConsent.js";
import {
  getCaptivePreauthTemplateMode,
  resolveCaptiveConfirmationPublicUrl,
} from "../autopay/captivePreauthService.js";

export const DELIVERY_NOTE_ACCEPTED =
  "accepted_by_brevo_not_delivery_confirmed";

const CAMPAIGN_AUDIENCE_FILTERS = new Set([
  "all_users",
  "active_balance",
  "specific_user",
  "specific_phone",
]);

async function runQuery(pgClient, text, params) {
  if (pgClient) return pgClient.query(text, params);
  return query(text, params);
}

export const TEST_MODE_WARNING = "TEST_MODE_ACTIVE_REAL_RECIPIENTS_BLOCKED";

export const BREVO_IP_BLOCKED_MESSAGE =
  "A Brevo bloqueou o IP de saída. Adicione o IP nas configurações de Authorized IPs da Brevo.";

export const MISSING_TEMPLATE_ID_MESSAGE =
  "Template ID ausente. Configure BREVO_WHATSAPP_GENERIC_TEST_TEMPLATE_ID, preencha provider_template_id no banco ou informe template_id no formulário.";

export function getTestModeWarning() {
  return shouldForceTestRecipient() ? TEST_MODE_WARNING : null;
}

export async function resolveCaptivePreauthTemplateHealth({ pgClient } = {}) {
  const envTemplateId = String(process.env.CAPTIVE_PREAUTH_BREVO_TEMPLATE_ID || "").trim();
  if (envTemplateId) {
    return {
      configured: true,
      id: envTemplateId,
      source: "env",
      key: "CAPTIVE_PREAUTH_REQUEST",
    };
  }

  try {
    const row = await getTemplateByKey({
      pgClient,
      templateKey: "CAPTIVE_PREAUTH_REQUEST",
      channel: "whatsapp",
      provider: "brevo",
      activeOnly: true,
    });
    const id = row?.provider_template_id != null ? String(row.provider_template_id).trim() : "";
    if (id) {
      return {
        configured: true,
        id,
        source: "database",
        key: "CAPTIVE_PREAUTH_REQUEST",
      };
    }
  } catch (err) {
    if (err?.code !== "42P01" && err?.code !== "42703") throw err;
  }

  return {
    configured: false,
    id: null,
    source: "missing",
    key: "CAPTIVE_PREAUTH_REQUEST",
  };
}

export async function getNotificationHealth() {
  const captiveTemplate = await resolveCaptivePreauthTemplateHealth();
  const captiveConfirmationPublicUrl = resolveCaptiveConfirmationPublicUrl();
  const captivePreauthTemplateMode = getCaptivePreauthTemplateMode();
  return {
    ok: true,
    notificationCenterEnabled: isTruthy(process.env.NOTIFICATION_CENTER_ENABLED),
    testMode: isTestModeActive(),
    allowRealRecipients: isAllowRealRecipients(),
    brevoWhatsappEnabled: isWhatsAppEnabled(),
    hasBrevoApiKey: !!String(process.env.BREVO_API_KEY || "").trim(),
    senderNumberConfigured: !!String(
      process.env.BREVO_WHATSAPP_SENDER_NUMBER || ""
    ).trim(),
    testRecipientConfigured: !!getTestRecipient(),
    genericTestTemplateEnvConfigured: Boolean(
      process.env.BREVO_WHATSAPP_GENERIC_TEST_TEMPLATE_ID ||
        process.env.BREVO_WHATSAPP_TEMPLATE_ID
    ),
    captiveTemplateEnvConfigured: Boolean(
      process.env.BREVO_WHATSAPP_CAPTIVE_AUTH_TEMPLATE_ID ||
        process.env.CAPTIVE_PREAUTH_BREVO_TEMPLATE_ID
    ),
    captive_preauth_template_configured: captiveTemplate.configured,
    captive_preauth_template_id: captiveTemplate.id,
    captive_preauth_template_source: captiveTemplate.source,
    captive_preauth_template_key: captiveTemplate.key,
    captive_confirmation_public_url: captiveConfirmationPublicUrl,
    captive_confirmation_public_url_configured: Boolean(captiveConfirmationPublicUrl),
    captive_preauth_template_mode: captivePreauthTemplateMode,
    adminTestCustomRecipientsEnabled: isAdminTestCustomRecipientsEnabled(),
    adminTestAllowedRecipientsConfigured: Boolean(
      String(process.env.NOTIFICATION_ADMIN_TEST_ALLOWED_RECIPIENTS || "").trim()
    ),
    whatsappConsentRequired: isWhatsAppConsentRequired(),
    whatsappAllowUnlinkedPhone: isUnlinkedWhatsAppPhoneAllowed(),
  };
}

export function getManualSendMaxRecipients() {
  const n = Number(process.env.NOTIFICATION_MANUAL_SEND_MAX_RECIPIENTS);
  if (Number.isFinite(n) && n > 0) return Math.min(Math.trunc(n), 50);
  return 50;
}

export async function getTemplateByKey({
  pgClient,
  templateKey,
  channel,
  provider,
  activeOnly = true,
}) {
  const activeClause = activeOnly ? "AND is_active = true" : "";
  const r = await runQuery(
    pgClient,
    `SELECT *
       FROM public.notification_templates
      WHERE template_key = $1
        AND channel = $2
        AND provider = $3
        ${activeClause}
      LIMIT 1`,
    [templateKey, channel, provider]
  );
  return r.rows[0] || null;
}

export async function getTemplateById({ pgClient, templateId }) {
  const r = await runQuery(
    pgClient,
    `SELECT * FROM public.notification_templates WHERE id = $1 LIMIT 1`,
    [templateId]
  );
  return r.rows[0] || null;
}

export async function resolveTemplateId({
  pgClient,
  templateKey,
  channel,
  provider,
  explicitTemplateId,
}) {
  if (explicitTemplateId != null && String(explicitTemplateId).trim() !== "") {
    return String(explicitTemplateId).trim();
  }

  const row = await getTemplateByKey({ pgClient, templateKey, channel, provider });
  if (row?.provider_template_id != null) {
    return String(row.provider_template_id);
  }

  if (templateKey === "GENERIC_TEST") {
    const id =
      process.env.BREVO_WHATSAPP_GENERIC_TEST_TEMPLATE_ID ||
      process.env.BREVO_WHATSAPP_TEMPLATE_ID;
    return id ? String(id).trim() : null;
  }
  if (templateKey === "CAPTIVE_AUTHORIZATION_REQUESTED") {
    const id = process.env.BREVO_WHATSAPP_CAPTIVE_AUTH_TEMPLATE_ID;
    return id ? String(id).trim() : null;
  }

  return null;
}

async function lookupUserPhone(pgClient, userId) {
  const r = await runQuery(
    pgClient,
    `SELECT id, phone
       FROM public.users
      WHERE id = $1
      LIMIT 1`,
    [userId]
  );
  const row = r.rows[0];
  if (!row) return null;
  return row.phone || null;
}

async function lookupUserForRecipient(pgClient, userId) {
  const r = await runQuery(
    pgClient,
    `SELECT id, name, email, phone, coupon_code, coupon_value_cents, is_admin
       FROM public.users
      WHERE id = $1
      LIMIT 1`,
    [userId]
  );
  return r.rows[0] || null;
}

export async function searchNotificationRecipients({
  pgClient,
  q = "",
  limit = 20,
} = {}) {
  const maxLimit = Math.min(Math.max(Number(limit) || 20, 1), 50);
  const term = String(q || "").trim();

  if (!term) {
    return [];
  }

  const pattern = `%${term.replace(/[%_\\]/g, "\\$&")}%`;
  const r = await runQuery(
    pgClient,
    `SELECT id, name, email, phone,
            NULLIF(TRIM(coupon_code), '') AS coupon_code,
            COALESCE(coupon_value_cents, 0)::bigint AS coupon_value_cents,
            COALESCE(is_admin, false) AS is_admin
       FROM public.users
      WHERE name ILIKE $1 ESCAPE '\\'
         OR email ILIKE $1 ESCAPE '\\'
         OR phone ILIKE $1 ESCAPE '\\'
         OR NULLIF(TRIM(coupon_code), '') ILIKE $1 ESCAPE '\\'
      ORDER BY name NULLS LAST, id
      LIMIT $2`,
    [pattern, maxLimit]
  );
  const rows = r.rows || [];
  return Promise.all(
    rows.map(async (row) => ({
      ...row,
      ...(await getWhatsappConsentStatusForUser({
        pgClient,
        userId: row.id,
        category: WHATSAPP_CONSENT_CATEGORY_DEFAULT,
      })),
    }))
  );
}

const TEMPLATE_PATCH_FIELDS = new Set([
  "template_key",
  "provider_template_id",
  "name",
  "description",
  "body_preview",
  "default_message",
  "default_params",
  "template_language",
  "template_category",
  "is_active",
]);

export async function updateNotificationTemplate({
  pgClient,
  templateId,
  patch,
}) {
  const existing = await getTemplateById({ pgClient, templateId });
  if (!existing) {
    return { ok: false, error: "not_found" };
  }

  const updates = [];
  const params = [templateId];
  let idx = 2;

  for (const [key, value] of Object.entries(patch || {})) {
    if (!TEMPLATE_PATCH_FIELDS.has(key)) continue;

    if (key === "default_params") {
      if (value !== null && (typeof value !== "object" || Array.isArray(value))) {
        return { ok: false, error: "invalid_default_params" };
      }
      updates.push(`default_params = $${idx++}::jsonb`);
      params.push(JSON.stringify(value ?? {}));
      continue;
    }

    if (key === "is_active") {
      updates.push(`is_active = $${idx++}`);
      params.push(value === true);
      continue;
    }

    if (key === "provider_template_id") {
      updates.push(`provider_template_id = $${idx++}`);
      params.push(value == null || value === "" ? null : String(value));
      continue;
    }

    updates.push(`${key} = $${idx++}`);
    params.push(value == null ? null : String(value));
  }

  if (!updates.length) {
    return { ok: true, template: existing };
  }

  updates.push("updated_at = NOW()");

  const r = await runQuery(
    pgClient,
    `UPDATE public.notification_templates
        SET ${updates.join(", ")}
      WHERE id = $1
      RETURNING *`,
    params
  );

  return { ok: true, template: r.rows[0] };
}

function normalizeManualRecipients(recipients) {
  if (!Array.isArray(recipients)) return [];
  return recipients
    .map((r) => {
      if (!r || typeof r !== "object") return null;
      if (r.user_id != null) {
        return { user_id: Number(r.user_id), phone: null, name: null };
      }
      if (r.phone) {
        return {
          user_id: null,
          phone: String(r.phone).trim(),
          name: r.name ? String(r.name).trim() : null,
        };
      }
      return null;
    })
    .filter(Boolean);
}

function resolveManualSendSecurity({
  recipientCount,
  useCustomRecipient,
  dryRun,
}) {
  const testMode = isTestModeActive();
  const allowRealRecipients = isAllowRealRecipients();
  const forced = shouldForceTestRecipient();
  const customEnabled = isAdminTestCustomRecipientsEnabled();
  const allowCustomReal =
    useCustomRecipient === true &&
    customEnabled &&
    recipientCount <= 5 &&
    !dryRun;
  const blockBulkReal = recipientCount > 5 && forced;

  return {
    testMode,
    allowRealRecipients,
    forced,
    allowCustomReal,
    blockBulkReal,
    warning:
      blockBulkReal && forced
        ? TEST_MODE_WARNING
        : forced && !allowCustomReal
          ? TEST_MODE_WARNING
          : null,
  };
}

function buildManualSendCampaignStatus(security) {
  if (security.blockBulkReal) return "blocked_real_recipients";
  if (security.forced) return "test_mode";
  return "created";
}

async function finalizeDispatch({ pgClient, dispatch, result }) {
  let updated;

  if (result?.skipped) {
    updated = await markDispatchFailed({
      pgClient,
      dispatchId: dispatch.id,
      result,
      status: "skipped",
    });
  } else if (result?.ok && result?.provider_status === "accepted") {
    updated = await markDispatchAccepted({
      pgClient,
      dispatchId: dispatch.id,
      result,
    });
  } else if (result?.ok) {
    updated = await markDispatchAccepted({
      pgClient,
      dispatchId: dispatch.id,
      result,
    });
  } else {
    updated = await markDispatchFailed({
      pgClient,
      dispatchId: dispatch.id,
      result,
    });
  }

  console.log("[notifications.center] dispatch finalized", {
    dispatch_id: dispatch.id,
    status: updated?.status,
    provider_status: updated?.provider_status || null,
    delivery_status: updated?.delivery_status || null,
    provider_message_id: updated?.provider_message_id || null,
    result_ok: result?.ok ?? null,
    result_error: result?.error || null,
    result_reason: result?.reason || null,
  });

  return updated;
}

function buildAdminResult(dispatch, result, { includeTestModeWarning = true } = {}) {
  const errorMessage = dispatch?.error_message || extractDispatchErrorMessage(result);
  const brevoIpBlocked = errorMessage === "brevo_ip_not_authorized";
  const showWarning =
    includeTestModeWarning &&
    result?.recipient_mode !== "admin_test_custom" &&
    getTestModeWarning();

  return {
    ok: !!result?.ok,
    dispatch,
    result,
    ...(showWarning && { warning: getTestModeWarning() }),
    ...(result?.ok &&
      result?.provider_status === "accepted" && {
        delivery_note: DELIVERY_NOTE_ACCEPTED,
      }),
    ...(brevoIpBlocked && { brevo_message: BREVO_IP_BLOCKED_MESSAGE }),
  };
}

async function markDispatchSkippedForConsent({ pgClient, dispatch, consent }) {
  const skipped = {
    ok: false,
    skipped: true,
    reason: consent?.reason || "whatsapp_consent_missing",
    provider: "brevo",
    channel: "whatsapp",
    whatsapp_consent_status: consent?.whatsapp_consent_status || "missing",
    whatsapp_consent_category:
      consent?.whatsapp_consent_category || WHATSAPP_CONSENT_CATEGORY_DEFAULT,
  };
  return markDispatchFailed({
    pgClient,
    dispatchId: dispatch.id,
    result: skipped,
    status: "skipped",
  });
}

export async function sendTestWhatsApp({
  pgClient,
  userId = null,
  phone = null,
  templateKey = "GENERIC_TEST",
  templateId = null,
  params = {},
  adminUserId = null,
  useCustomRecipient = false,
}) {
  let requestedPhone = phone ? String(phone).trim() : null;

  if (!requestedPhone && userId) {
    requestedPhone = await lookupUserPhone(pgClient, userId);
  }

  const testRecipient = getTestRecipient();
  const originalRecipient = requestedPhone || testRecipient;
  const preResolved = resolveRecipientForCurrentMode(originalRecipient, {
    allowAdminTestCustomRecipient: useCustomRecipient === true,
    context: "admin_test",
  });

  if (!preResolved.ok) {
    return {
      ok: false,
      dispatch: null,
      result: {
        ok: false,
        skipped: true,
        reason: preResolved.reason,
        recipient_mode: preResolved.recipient_mode,
      },
      warning: TEST_MODE_WARNING,
    };
  }

  const resolvedTemplateId = await resolveTemplateId({
    pgClient,
    templateKey,
    channel: "whatsapp",
    provider: "brevo",
    explicitTemplateId: templateId,
  });

  const normalizedOriginal =
    normalizePhoneBR(originalRecipient) || originalRecipient;
  const dispatchRecipient = preResolved.recipient;
  const dispatchForced = preResolved.recipient_forced;
  const testMode = isTestModeActive();
  const allowRealRecipients = isAllowRealRecipients();

  const messageSnapshot = {
    template_key: templateKey,
    provider_template_id: resolvedTemplateId,
    params,
    requested_phone: phone || null,
    requested_user_id: userId || null,
    admin_user_id: adminUserId || null,
    test_mode: testMode,
    allow_real_recipients: allowRealRecipients,
    use_custom_recipient: useCustomRecipient === true,
    admin_test_custom_recipients_enabled: isAdminTestCustomRecipientsEnabled(),
  };

  const recipientSnapshot = {
    user_id: userId || null,
    phone: phone || null,
    source: userId ? "specific_user" : "specific_phone",
    test_mode: testMode,
    recipient_forced_expected: dispatchForced,
    recipient_mode: preResolved.recipient_mode,
  };

  const dispatch = await createDispatch({
    pgClient,
    eventKey: "ADMIN_TEST_WHATSAPP",
    channel: "whatsapp",
    provider: "brevo",
    userId: userId || null,
    recipient: dispatchRecipient,
    recipientOriginal: preResolved.recipient_original,
    recipientForced: dispatchForced,
    templateKey,
    providerTemplateId: resolvedTemplateId,
    payload: {
      params,
      admin_user_id: adminUserId || null,
      test_mode: testMode,
      requested_phone: phone || null,
      requested_user_id: userId || null,
    },
    messageSnapshot,
    recipientSnapshot,
  });

  if (!resolvedTemplateId) {
    const skipped = {
      ok: false,
      skipped: true,
      reason: "missing_template_id",
      message: MISSING_TEMPLATE_ID_MESSAGE,
      provider: "brevo",
      channel: "whatsapp",
    };
    const updated = await markDispatchFailed({
      pgClient,
      dispatchId: dispatch.id,
      result: skipped,
    });
    return buildAdminResult(updated, skipped);
  }

  const consent = await assertWhatsAppConsent({
    pgClient,
    userId: userId || null,
    phone: originalRecipient,
    category: WHATSAPP_CONSENT_CATEGORY_DEFAULT,
    source: "admin_test",
    recipientForced: preResolved.recipient_forced === true,
  });

  recipientSnapshot.whatsapp_consent_status = consent.whatsapp_consent_status || null;
  recipientSnapshot.whatsapp_consent_category = consent.whatsapp_consent_category || null;
  recipientSnapshot.whatsapp_can_send = consent.whatsapp_can_send === true;

  if (!consent.ok) {
    const updated = await markDispatchSkippedForConsent({ pgClient, dispatch, consent });
    return buildAdminResult(updated, {
      ok: false,
      skipped: true,
      reason: consent.reason || "whatsapp_consent_missing",
      provider: "brevo",
      channel: "whatsapp",
    });
  }

  const result = await sendBrevoWhatsAppTemplate({
    to: originalRecipient,
    templateId: resolvedTemplateId,
    params,
    templateKey,
    correlationId: String(dispatch.id),
    context: "admin_test",
    allowAdminTestCustomRecipient: useCustomRecipient === true,
    consentChecked: true,
  });

  recipientSnapshot.recipient_mode = result.recipient_mode || preResolved.recipient_mode;

  const updated = await finalizeDispatch({ pgClient, dispatch, result });
  return buildAdminResult(updated, result, { includeTestModeWarning: true });
}

export async function estimateAudience({ pgClient, filter, userId = null, phone = null }) {
  const test_mode = isTestModeActive();
  const allow_real_recipients = isAllowRealRecipients();
  let estimated_count = 0;
  let message = "";
  let consentStats = {
    total_candidates: 0,
    allowed_by_whatsapp_consent: 0,
    blocked_by_whatsapp_consent: 0,
    whatsapp_consent_category: WHATSAPP_CONSENT_CATEGORY_DEFAULT,
  };

  switch (filter) {
    case "all_users": {
      consentStats = await countWhatsAppConsentForAudience({
        pgClient,
        whereSql: `WHERE NULLIF(TRIM(u.phone), '') IS NOT NULL`,
      });
      estimated_count = consentStats.total_candidates;
      message = "Usuarios com telefone cadastrado";
      break;
    }
    case "active_balance": {
      consentStats = await countWhatsAppConsentForAudience({
        pgClient,
        whereSql: `WHERE NULLIF(TRIM(u.phone), '') IS NOT NULL
                     AND COALESCE(u.coupon_value_cents, 0) > 0`,
      });
      estimated_count = consentStats.total_candidates;
      message = "Usuarios com saldo de cupom maior que zero";
      break;
    }
    case "specific_user": {
      if (!userId) {
        estimated_count = 0;
        message = "user_id não informado";
        break;
      }
      const r = await runQuery(
        pgClient,
        `SELECT 1 FROM public.users WHERE id = $1 AND NULLIF(TRIM(phone), '') IS NOT NULL LIMIT 1`,
        [userId]
      );
      estimated_count = r.rows.length ? 1 : 0;
      const consent = estimated_count
        ? await getWhatsappConsentStatusForUser({
            pgClient,
            userId,
            category: WHATSAPP_CONSENT_CATEGORY_DEFAULT,
          })
        : { whatsapp_can_send: false };
      consentStats = {
        total_candidates: estimated_count,
        allowed_by_whatsapp_consent: consent.whatsapp_can_send ? 1 : 0,
        blocked_by_whatsapp_consent: consent.whatsapp_can_send ? 0 : estimated_count,
        whatsapp_consent_category: WHATSAPP_CONSENT_CATEGORY_DEFAULT,
      };
      message = estimated_count
        ? "Um usuario especifico"
        : "Usuario nao encontrado ou sem telefone";
      break;
    }
    case "specific_phone": {
      const normalized = normalizePhoneBR(phone);
      estimated_count = normalized ? 1 : 0;
      const unlinkedAllowed = isUnlinkedWhatsAppPhoneAllowed();
      consentStats = {
        total_candidates: estimated_count,
        allowed_by_whatsapp_consent: estimated_count && unlinkedAllowed ? 1 : 0,
        blocked_by_whatsapp_consent: estimated_count && !unlinkedAllowed ? 1 : 0,
        whatsapp_consent_category: WHATSAPP_CONSENT_CATEGORY_DEFAULT,
      };
      message = normalized
        ? "Um telefone especifico valido, sem consentimento auditavel por usuario"
        : "Telefone invalido ou ausente";
      break;
    }
    default:
      message = `Filtro desconhecido: ${filter}`;
      estimated_count = 0;
  }

  if (shouldForceTestRecipient()) {
    message = `${message}. Envio real bloqueado: apenas o número de teste receberá mensagens nesta fase.`;
  }

  return {
    filter,
    estimated_count,
    ...consentStats,
    test_mode,
    allow_real_recipients,
    message,
  };
}

export async function manualSendNotification({
  pgClient,
  channel,
  templateKey,
  templateId = null,
  filter = null,
  userId = null,
  phone = null,
  params = {},
  adminUserId = null,
}) {
  if (channel !== "whatsapp") {
    return {
      ok: false,
      error: "unsupported_channel",
      message: "Nesta fase apenas channel=whatsapp é suportado",
      warning: getTestModeWarning(),
    };
  }

  const testRecipient = getTestRecipient();
  if (!testRecipient) {
    return {
      ok: false,
      error: "missing_test_recipient",
      message:
        "NOTIFICATION_TEST_WHATSAPP_TO ausente ou inválido. Configure o número de teste.",
      warning: TEST_MODE_WARNING,
    };
  }

  const testMode = isTestModeActive();
  const allowRealRecipients = isAllowRealRecipients();
  const forced = shouldForceTestRecipient();

  const resolvedTemplateId = await resolveTemplateId({
    pgClient,
    templateKey,
    channel: "whatsapp",
    provider: "brevo",
    explicitTemplateId: templateId,
  });

  let campaign = null;
  let estimatedCount = null;

  if (filter && CAMPAIGN_AUDIENCE_FILTERS.has(filter)) {
    const estimate = await estimateAudience({ pgClient, filter, userId, phone });
    estimatedCount = estimate.estimated_count;

    const audienceParams = {
      user_id: userId || null,
      phone: phone || null,
    };

    const messageSnapshot = {
      template_key: templateKey,
      provider_template_id: resolvedTemplateId,
      params,
      filter,
      requested_phone: phone || null,
      requested_user_id: userId || null,
      admin_user_id: adminUserId || null,
      test_mode: testMode,
      allow_real_recipients: allowRealRecipients,
      warning: TEST_MODE_WARNING,
    };

    const audienceSnapshot = {
      filter,
      estimated_count: estimatedCount,
      user_id: userId || null,
      phone: phone || null,
      test_mode: testMode,
      allow_real_recipients: allowRealRecipients,
      real_send_blocked: forced,
    };

    campaign = await createCampaign({
      pgClient,
      name: `Manual admin — ${filter}`,
      channel: "whatsapp",
      provider: "brevo",
      templateKey,
      providerTemplateId: resolvedTemplateId,
      audienceFilter: filter,
      audienceParams,
      status: forced ? "test_mode" : "created",
      createdBy: adminUserId,
      payload: {
        admin_user_id: adminUserId || null,
        test_mode: testMode,
        note: TEST_MODE_WARNING,
      },
      messageSnapshot,
      audienceSnapshot,
      campaignType: "manual_admin",
      audienceCountExpected: estimatedCount,
    });
  }

  const messageSnapshot = {
    template_key: templateKey,
    provider_template_id: resolvedTemplateId,
    params,
    filter: filter || null,
    requested_phone: phone || null,
    requested_user_id: userId || null,
    admin_user_id: adminUserId || null,
    test_mode: testMode,
    allow_real_recipients: allowRealRecipients,
    warning: TEST_MODE_WARNING,
    campaign_id: campaign?.id || null,
  };

  const recipientSnapshot = {
    user_id: userId || null,
    phone: phone || null,
    source: filter || "test_recipient",
    intended_filter: filter || null,
    test_mode: testMode,
    recipient_forced_expected: forced,
    redirected_to_test: forced,
    test_recipient: testRecipient,
    actual_recipient: testRecipient,
    estimated_audience_count: estimatedCount,
  };

  const dispatch = await createDispatch({
    pgClient,
    eventKey: "MANUAL_ADMIN_TEST_SEND",
    channel: "whatsapp",
    provider: "brevo",
    userId: userId || null,
    recipient: testRecipient,
    recipientOriginal:
      normalizePhoneBR(phone) ||
      (userId ? `user:${userId}` : filter || null),
    recipientForced: true,
    templateKey,
    providerTemplateId: resolvedTemplateId,
    campaignId: campaign?.id || null,
    payload: {
      params,
      admin_user_id: adminUserId || null,
      test_mode: testMode,
      filter: filter || null,
      requested_phone: phone || null,
      requested_user_id: userId || null,
      note: TEST_MODE_WARNING,
      campaign_id: campaign?.id || null,
    },
    messageSnapshot,
    recipientSnapshot,
  });

  if (!resolvedTemplateId) {
    const skipped = {
      ok: false,
      skipped: true,
      reason: "missing_template_id",
      message: MISSING_TEMPLATE_ID_MESSAGE,
      provider: "brevo",
      channel: "whatsapp",
    };
    const updated = await markDispatchFailed({
      pgClient,
      dispatchId: dispatch.id,
      result: skipped,
    });
    if (campaign?.id) {
      await updateCampaignAudienceCounts(pgClient, campaign.id, {
        created: 1,
        skipped: 1,
      });
      campaign = await runQuery(
        pgClient,
        `SELECT * FROM public.notification_campaigns WHERE id = $1`,
        [campaign.id]
      ).then((r) => r.rows[0]);
    }
    return {
      ok: false,
      campaign,
      ...buildAdminResult(updated, skipped),
      message: TEST_MODE_WARNING,
    };
  }

  const consent = await assertWhatsAppConsent({
    pgClient,
    userId: userId || null,
    phone: phone || null,
    category: WHATSAPP_CONSENT_CATEGORY_DEFAULT,
    source: "manual_send",
    recipientForced: forced === true,
  });

  recipientSnapshot.whatsapp_consent_status = consent.whatsapp_consent_status || null;
  recipientSnapshot.whatsapp_consent_category = consent.whatsapp_consent_category || null;
  recipientSnapshot.whatsapp_can_send = consent.whatsapp_can_send === true;

  if (!consent.ok) {
    const updated = await markDispatchSkippedForConsent({ pgClient, dispatch, consent });
    if (campaign?.id) {
      await updateCampaignAudienceCounts(pgClient, campaign.id, {
        created: 1,
        skipped: 1,
      });
      campaign = await runQuery(
        pgClient,
        `SELECT * FROM public.notification_campaigns WHERE id = $1`,
        [campaign.id]
      ).then((r) => r.rows[0]);
    }
    return {
      ok: false,
      campaign,
      ...buildAdminResult(updated, {
        ok: false,
        skipped: true,
        reason: consent.reason || "whatsapp_consent_missing",
        provider: "brevo",
        channel: "whatsapp",
      }),
      message: consent.reason || TEST_MODE_WARNING,
    };
  }

  const result = await sendBrevoWhatsAppTemplate({
    to: testRecipient,
    templateId: resolvedTemplateId,
    params,
    templateKey,
    correlationId: String(dispatch.id),
    context: "manual_send",
    allowAdminTestCustomRecipient: false,
    consentChecked: true,
  });

  const updated = await finalizeDispatch({ pgClient, dispatch, result });

  if (campaign?.id) {
    await updateCampaignAudienceCounts(pgClient, campaign.id, {
      created: 1,
      sent: result.ok && !result.skipped ? 1 : 0,
      failed: !result.ok && !result.skipped ? 1 : 0,
      skipped: result.skipped ? 1 : 0,
    });
    const refreshed = await runQuery(
      pgClient,
      `SELECT * FROM public.notification_campaigns WHERE id = $1`,
      [campaign.id]
    );
    campaign = refreshed.rows[0];
  }

  return {
    ok: !!result?.ok,
    campaign,
    ...buildAdminResult(updated, result),
    message: TEST_MODE_WARNING,
  };
}

export async function manualSendSelected({
  pgClient,
  channel = "whatsapp",
  provider = "brevo",
  templateKey = "GENERIC_TEST",
  templateId = null,
  message = null,
  params = {},
  recipients = [],
  useCustomRecipient = false,
  dryRun = false,
  adminUserId = null,
}) {
  if (channel !== "whatsapp" || provider !== "brevo") {
    return {
      ok: false,
      error: "unsupported_channel_or_provider",
      message: "Nesta fase apenas channel=whatsapp e provider=brevo são suportados",
    };
  }

  const maxRecipients = getManualSendMaxRecipients();
  const normalizedRecipients = normalizeManualRecipients(recipients);

  if (!normalizedRecipients.length) {
    return { ok: false, error: "recipients_required" };
  }
  if (normalizedRecipients.length > maxRecipients) {
    return {
      ok: false,
      error: "too_many_recipients",
      max: maxRecipients,
    };
  }

  const testRecipient = getTestRecipient();
  if (!testRecipient && shouldForceTestRecipient()) {
    return {
      ok: false,
      error: "missing_test_recipient",
      message:
        "NOTIFICATION_TEST_WHATSAPP_TO ausente ou inválido. Configure o número de teste.",
    };
  }

  const security = resolveManualSendSecurity({
    recipientCount: normalizedRecipients.length,
    useCustomRecipient,
    dryRun,
  });

  const resolvedTemplateId = await resolveTemplateId({
    pgClient,
    templateKey,
    channel,
    provider,
    explicitTemplateId: templateId,
  });

  const sendParams = { ...(params || {}) };
  if (message != null && String(message).trim() !== "") {
    sendParams.message = String(message).trim();
  }

  const messageSnapshot = {
    channel,
    provider,
    template_key: templateKey,
    provider_template_id: resolvedTemplateId,
    message: message != null ? String(message) : null,
    params: sendParams,
    admin_user_id: adminUserId || null,
    test_mode: security.testMode,
    allow_real_recipients: security.allowRealRecipients,
    use_custom_recipient: useCustomRecipient === true,
  };

  let campaign = null;
  if (normalizedRecipients.length > 1) {
    const audienceSnapshot = {
      source: "manual_selected",
      recipient_count: normalizedRecipients.length,
      test_mode: security.testMode,
      allow_real_recipients: security.allowRealRecipients,
      real_send_blocked: security.blockBulkReal || security.forced,
      use_custom_recipient: useCustomRecipient === true,
    };

    campaign = await createCampaign({
      pgClient,
      name: `Manual selected — ${normalizedRecipients.length} destinatários`,
      channel,
      provider,
      templateKey,
      providerTemplateId: resolvedTemplateId,
      audienceFilter: "manual_selected",
      audienceParams: { recipient_count: normalizedRecipients.length },
      status: buildManualSendCampaignStatus(security),
      createdBy: adminUserId,
      payload: {
        admin_user_id: adminUserId || null,
        test_mode: security.testMode,
        dry_run: dryRun === true,
      },
      messageSnapshot,
      audienceSnapshot,
      campaignType: "manual_selected",
      audienceCountExpected: normalizedRecipients.length,
    });
  }

  const dispatches = [];
  const summary = {
    requested_count: normalizedRecipients.length,
    created_count: 0,
    accepted_count: 0,
    failed_count: 0,
    skipped_count: 0,
    forced_count: 0,
  };

  const brevoContext =
    security.allowCustomReal && !security.blockBulkReal
      ? "admin_test"
      : "manual_send_selected";

  const allowAdminTestCustom =
    security.allowCustomReal && !security.blockBulkReal;

  for (const item of normalizedRecipients) {
    let userRow = null;
    let source = "manual_phone";
    let requestedRecipient = null;

    if (item.user_id) {
      userRow = await lookupUserForRecipient(pgClient, item.user_id);
      source = "selected_user";
      requestedRecipient = userRow?.phone || `user:${item.user_id}`;
    } else {
      requestedRecipient = item.phone;
    }

    const originalRecipient =
      normalizePhoneBR(userRow?.phone || item.phone) ||
      userRow?.phone ||
      item.phone ||
      testRecipient;

    let preResolved;
    if (security.blockBulkReal) {
      preResolved = {
        ok: true,
        recipient: testRecipient,
        recipient_original: originalRecipient,
        recipient_forced: true,
        recipient_mode: "forced_test_recipient",
      };
    } else {
      preResolved = resolveRecipientForCurrentMode(originalRecipient, {
        allowAdminTestCustomRecipient: allowAdminTestCustom,
        context: brevoContext,
      });
    }

    if (!preResolved.ok) {
      summary.skipped_count += 1;
      dispatches.push({
        ok: false,
        skipped: true,
        reason: preResolved.reason,
        user_id: item.user_id || null,
      });
      continue;
    }

    if (preResolved.recipient_forced) {
      summary.forced_count += 1;
    }

    const recipientSnapshot = {
      user_id: userRow?.id || item.user_id || null,
      name: userRow?.name || item.name || null,
      email: userRow?.email || null,
      phone: userRow?.phone || item.phone || null,
      source,
      requested_recipient: requestedRecipient,
      resolved_recipient: preResolved.recipient,
      recipient_forced: preResolved.recipient_forced,
      recipient_mode: preResolved.recipient_mode,
    };

    if (dryRun) {
      summary.created_count += 1;
      dispatches.push({
        ok: true,
        dry_run: true,
        recipient_snapshot: recipientSnapshot,
        would_send_to: preResolved.recipient,
      });
      continue;
    }

    const dispatch = await createDispatch({
      pgClient,
      eventKey: "MANUAL_ADMIN_SELECTED_SEND",
      channel,
      provider,
      userId: userRow?.id || item.user_id || null,
      recipient: preResolved.recipient,
      recipientOriginal: preResolved.recipient_original,
      recipientForced: preResolved.recipient_forced,
      templateKey,
      providerTemplateId: resolvedTemplateId,
      campaignId: campaign?.id || null,
      payload: {
        params: sendParams,
        admin_user_id: adminUserId || null,
        test_mode: security.testMode,
        campaign_id: campaign?.id || null,
      },
      messageSnapshot,
      recipientSnapshot,
    });

    summary.created_count += 1;

    if (!resolvedTemplateId) {
      const skipped = {
        ok: false,
        skipped: true,
        reason: "missing_template_id",
        message: MISSING_TEMPLATE_ID_MESSAGE,
        provider,
        channel,
      };
      const updated = await markDispatchFailed({
        pgClient,
        dispatchId: dispatch.id,
        result: skipped,
      });
      summary.skipped_count += 1;
      dispatches.push(updated);
      continue;
    }

    const consent = await assertWhatsAppConsent({
      pgClient,
      userId: userRow?.id || item.user_id || null,
      phone: userRow?.phone || item.phone || null,
      category: WHATSAPP_CONSENT_CATEGORY_DEFAULT,
      source: "manual_send_selected",
      recipientForced: preResolved.recipient_forced === true,
    });

    recipientSnapshot.whatsapp_consent_status = consent.whatsapp_consent_status || null;
    recipientSnapshot.whatsapp_consent_category = consent.whatsapp_consent_category || null;
    recipientSnapshot.whatsapp_can_send = consent.whatsapp_can_send === true;

    if (!consent.ok) {
      const updated = await markDispatchSkippedForConsent({ pgClient, dispatch, consent });
      summary.skipped_count += 1;
      dispatches.push(updated);
      continue;
    }

    const result = await sendBrevoWhatsAppTemplate({
      to: originalRecipient,
      templateId: resolvedTemplateId,
      params: sendParams,
      templateKey,
      correlationId: String(dispatch.id),
      context: brevoContext,
      allowAdminTestCustomRecipient: allowAdminTestCustom,
      consentChecked: true,
    });

    recipientSnapshot.recipient_mode =
      result.recipient_mode || preResolved.recipient_mode;
    recipientSnapshot.recipient_forced = result.recipient_forced;

    const updated = await finalizeDispatch({ pgClient, dispatch, result });
    dispatches.push(updated);

    if (result?.skipped) summary.skipped_count += 1;
    else if (result?.ok && result?.provider_status === "accepted") {
      summary.accepted_count += 1;
    } else if (result?.ok) summary.accepted_count += 1;
    else summary.failed_count += 1;
  }

  if (campaign?.id && !dryRun) {
    await updateCampaignAudienceCounts(pgClient, campaign.id, {
      created: summary.created_count,
      sent: summary.accepted_count,
      failed: summary.failed_count,
      skipped: summary.skipped_count,
    });
    const refreshed = await runQuery(
      pgClient,
      `SELECT * FROM public.notification_campaigns WHERE id = $1`,
      [campaign.id]
    );
    campaign = refreshed.rows[0];
  }

  const anyAccepted = summary.accepted_count > 0;
  const warning = security.warning || null;

  return {
    ok: dryRun ? true : anyAccepted || summary.skipped_count > 0,
    campaign,
    dispatches,
    summary,
    warning,
    dry_run: dryRun === true,
  };
}
