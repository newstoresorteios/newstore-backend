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
  markDispatchSent,
  markDispatchFailed,
  extractDispatchErrorMessage,
} from "./notificationLog.js";

async function runQuery(pgClient, text, params) {
  if (pgClient) return pgClient.query(text, params);
  return query(text, params);
}

export const TEST_MODE_WARNING = "TEST_MODE_ACTIVE_REAL_RECIPIENTS_BLOCKED";

export const BREVO_IP_BLOCKED_MESSAGE =
  "A Brevo bloqueou o IP de saída. Adicione o IP nas configurações de Authorized IPs da Brevo.";

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
  if (result.ok && !result.skipped) {
    return markDispatchSent({ pgClient, dispatchId: dispatch.id, result });
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
      test_mode: isTestModeActive(),
      requested_phone: phone || null,
      requested_user_id: userId || null,
    },
  });

  if (!resolvedTemplateId) {
    const skipped = {
      ok: false,
      skipped: true,
      reason: "missing_template_id",
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

  const resolvedTemplateId = await resolveTemplateId({
    pgClient,
    templateKey,
    channel: "whatsapp",
    provider: "brevo",
    explicitTemplateId: templateId,
  });

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
    payload: {
      params,
      admin_user_id: adminUserId || null,
      test_mode: isTestModeActive(),
      filter: filter || null,
      requested_phone: phone || null,
      requested_user_id: userId || null,
      note: TEST_MODE_WARNING,
    },
  });

  if (!resolvedTemplateId) {
    const skipped = {
      ok: false,
      skipped: true,
      reason: "missing_template_id",
      provider: "brevo",
      channel: "whatsapp",
    };
    const updated = await markDispatchFailed({
      pgClient,
      dispatchId: dispatch.id,
      result: skipped,
    });
    return {
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

  return {
    ...buildAdminResult(updated, result),
    message: TEST_MODE_WARNING,
  };
}
