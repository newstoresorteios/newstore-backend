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
} from "./brevoWhatsApp.js";
import {
  createDispatch,
  createCampaign,
  updateCampaignAudienceCounts,
  markDispatchAccepted,
  markDispatchFailed,
  extractDispatchErrorMessage,
} from "./notificationLog.js";

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

export function getNotificationHealth() {
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
      process.env.BREVO_WHATSAPP_CAPTIVE_AUTH_TEMPLATE_ID
    ),
  };
}

export async function getTemplateByKey({
  pgClient,
  templateKey,
  channel,
  provider,
}) {
  const r = await runQuery(
    pgClient,
    `SELECT *
       FROM public.notification_templates
      WHERE template_key = $1
        AND channel = $2
        AND provider = $3
        AND is_active = true
      LIMIT 1`,
    [templateKey, channel, provider]
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

async function finalizeDispatch({ pgClient, dispatch, result }) {
  if (result?.skipped) {
    return markDispatchFailed({
      pgClient,
      dispatchId: dispatch.id,
      result,
      status: "skipped",
    });
  }
  if (result?.ok && result?.provider_status === "accepted") {
    return markDispatchAccepted({ pgClient, dispatchId: dispatch.id, result });
  }
  if (result?.ok) {
    return markDispatchAccepted({ pgClient, dispatchId: dispatch.id, result });
  }
  return markDispatchFailed({ pgClient, dispatchId: dispatch.id, result });
}

function buildAdminResult(dispatch, result) {
  const errorMessage = dispatch?.error_message || extractDispatchErrorMessage(result);
  const brevoIpBlocked = errorMessage === "brevo_ip_not_authorized";

  return {
    ok: !!result?.ok,
    dispatch,
    result,
    warning: getTestModeWarning(),
    ...(result?.ok &&
      result?.provider_status === "accepted" && {
        delivery_note: DELIVERY_NOTE_ACCEPTED,
      }),
    ...(brevoIpBlocked && { brevo_message: BREVO_IP_BLOCKED_MESSAGE }),
  };
}

export async function sendTestWhatsApp({
  pgClient,
  userId = null,
  phone = null,
  templateKey = "GENERIC_TEST",
  templateId = null,
  params = {},
  adminUserId = null,
}) {
  let requestedPhone = phone ? String(phone).trim() : null;

  if (!requestedPhone && userId) {
    requestedPhone = await lookupUserPhone(pgClient, userId);
  }

  const forced = shouldForceTestRecipient();
  const testRecipient = getTestRecipient();
  if (forced && !testRecipient) {
    return {
      ok: false,
      dispatch: null,
      result: {
        ok: false,
        skipped: true,
        reason: "missing_test_recipient",
      },
      warning: TEST_MODE_WARNING,
    };
  }

  const originalRecipient = requestedPhone || testRecipient;
  const resolvedTemplateId = await resolveTemplateId({
    pgClient,
    templateKey,
    channel: "whatsapp",
    provider: "brevo",
    explicitTemplateId: templateId,
  });

  const normalizedOriginal =
    normalizePhoneBR(originalRecipient) || originalRecipient;
  const dispatchRecipient = forced ? testRecipient : normalizedOriginal;
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
  };

  const recipientSnapshot = {
    user_id: userId || null,
    phone: phone || null,
    source: userId ? "specific_user" : "specific_phone",
    test_mode: testMode,
    recipient_forced_expected: forced,
  };

  const dispatch = await createDispatch({
    pgClient,
    eventKey: "ADMIN_TEST_WHATSAPP",
    channel: "whatsapp",
    provider: "brevo",
    userId: userId || null,
    recipient: dispatchRecipient,
    recipientOriginal: normalizedOriginal,
    recipientForced: forced,
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

  const result = await sendBrevoWhatsAppTemplate({
    to: originalRecipient,
    templateId: resolvedTemplateId,
    params,
    templateKey,
    correlationId: String(dispatch.id),
  });

  const updated = await finalizeDispatch({ pgClient, dispatch, result });
  return buildAdminResult(updated, result);
}

export async function estimateAudience({ pgClient, filter, userId = null, phone = null }) {
  const test_mode = isTestModeActive();
  const allow_real_recipients = isAllowRealRecipients();
  let estimated_count = 0;
  let message = "";

  switch (filter) {
    case "all_users": {
      const r = await runQuery(
        pgClient,
        `SELECT COUNT(*)::int AS c
           FROM public.users
          WHERE NULLIF(TRIM(email), '') IS NOT NULL
             OR NULLIF(TRIM(phone), '') IS NOT NULL`
      );
      estimated_count = Number(r.rows[0]?.c || 0);
      message = "Usuários com e-mail ou telefone cadastrado";
      break;
    }
    case "active_balance": {
      const r = await runQuery(
        pgClient,
        `SELECT COUNT(*)::int AS c
           FROM public.users
          WHERE COALESCE(coupon_value_cents, 0) > 0`
      );
      estimated_count = Number(r.rows[0]?.c || 0);
      message = "Usuários com saldo de cupom maior que zero";
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
        `SELECT 1 FROM public.users WHERE id = $1 LIMIT 1`,
        [userId]
      );
      estimated_count = r.rows.length ? 1 : 0;
      message = estimated_count
        ? "Um usuário específico"
        : "Usuário não encontrado";
      break;
    }
    case "specific_phone": {
      const normalized = normalizePhoneBR(phone);
      estimated_count = normalized ? 1 : 0;
      message = normalized
        ? "Um telefone específico (válido)"
        : "Telefone inválido ou ausente";
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

  const result = await sendBrevoWhatsAppTemplate({
    to: testRecipient,
    templateId: resolvedTemplateId,
    params,
    templateKey,
    correlationId: String(dispatch.id),
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
