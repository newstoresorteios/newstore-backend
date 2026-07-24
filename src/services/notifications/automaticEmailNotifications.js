import { query } from "../../db.js";
import {
  createCampaign,
  createDispatch,
  markDispatchAccepted,
  markDispatchFailed,
  updateCampaignAudienceCounts,
} from "./notificationLog.js";
import { createSmtpTransporter, getSmtpConfig } from "./manualEmailNotifications.js";
import { renderTemplate } from "./manualNotificationPreview.js";

export const AUTOMATIC_EMAIL_EVENT_KEYS = Object.freeze([
  "NEW_DRAW_PUBLISHED",
  "EMAIL_DRAW_REMAINING_75",
  "EMAIL_DRAW_REMAINING_50",
  "EMAIL_DRAW_REMAINING_30",
  "EMAIL_DRAW_REMAINING_15",
  "DRAW_CLOSED",
]);

const REMAINING_THRESHOLDS = new Map([
  ["EMAIL_DRAW_REMAINING_75", 75],
  ["EMAIL_DRAW_REMAINING_50", 50],
  ["EMAIL_DRAW_REMAINING_30", 30],
  ["EMAIL_DRAW_REMAINING_15", 15],
]);
const CAIXA_URL = "https://www.youtube.com/@caixa";
const FALLBACK_SITE_URL = "https://sorteiosxnamai.com.br";

function cleanText(value) {
  return String(value ?? "").trim();
}

function isEnabled() {
  return cleanText(process.env.NOTIFICATION_EMAIL_AUTOMATION_ENABLED).toLowerCase() === "true";
}

export function isDrawClosedForEmail(draw) {
  return Boolean(draw?.closed_at);
}

function validEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(cleanText(value));
}

function baseUrl() {
  return cleanText(
    process.env.PUBLIC_APP_URL ||
      process.env.FRONTEND_URL ||
      process.env.SITE_URL ||
      FALLBACK_SITE_URL
  ).replace(/\/+$/, "");
}

function absoluteDrawUrl(drawId) {
  return `${baseUrl()}/?draw_id=${encodeURIComponent(String(drawId))}`;
}

function referencePrefix(drawType) {
  return drawType === "principal" ? "draw" : "additional_draw";
}

function drawName(draw, config, principalConfig) {
  return (
    cleanText(draw?.product_name) ||
    cleanText(config?.banner_title) ||
    (draw?.draw_type === "principal" ? cleanText(principalConfig?.value) : "") ||
    `Sorteio #${draw.id}`
  );
}

function eventError(code, extra = {}) {
  const error = new Error(code);
  error.code = code;
  Object.assign(error, extra);
  return error;
}

async function loadDrawContext(drawId) {
  const drawResult = await query(
    `SELECT id, status, draw_type, product_name, opened_at, closed_at
       FROM public.draws
      WHERE id = $1`,
    [drawId]
  );
  const draw = drawResult.rows?.[0];
  if (!draw) throw eventError("email_draw_not_found", { drawId });

  const configResult = await query(
    `SELECT id, banner_title
       FROM public.app_config_new
      WHERE id = $1`,
    [String(drawId)]
  ).catch((error) => (error?.code === "42P01" ? { rows: [] } : Promise.reject(error)));
  let principalConfig = null;
  if (cleanText(draw.draw_type || "principal") === "principal" && !configResult.rows?.[0]?.banner_title) {
    principalConfig = (await query(
      `SELECT value FROM public.app_config WHERE key = 'banner_title' LIMIT 1`
    ).catch((error) => (error?.code === "42P01" ? { rows: [] } : Promise.reject(error)))).rows?.[0] || null;
  }

  const resolvedType = cleanText(draw.draw_type) || "principal";
  if (!["principal", "adicional", "secundario"].includes(resolvedType)) {
    throw eventError("email_draw_type_not_allowed", { drawId });
  }
  return {
    draw: { ...draw, draw_type: resolvedType },
    config: configResult.rows?.[0] || null,
    principalConfig,
    drawName: drawName({ ...draw, draw_type: resolvedType }, configResult.rows?.[0], principalConfig),
    drawUrl: absoluteDrawUrl(drawId),
  };
}

async function loadRecipients(drawId, eventKey) {
  const sql = eventKey === "DRAW_CLOSED"
    ? `SELECT DISTINCT u.id, u.name, u.email
         FROM public.users u
        WHERE (EXISTS (
                 SELECT 1 FROM public.reservations r
                  WHERE r.user_id = u.id
                    AND r.draw_id = $1
                    AND lower(coalesce(r.status, '')) IN ('paid', 'pago', 'approved')
               ) OR EXISTS (
                 SELECT 1 FROM public.payments p
                  WHERE p.user_id = u.id
                    AND p.draw_id = $1
                    AND lower(coalesce(p.status, '')) IN ('approved', 'paid', 'pago')
               ))
          AND u.email IS NOT NULL
        ORDER BY u.id`
    : `SELECT id, name, email
         FROM public.users
        WHERE email IS NOT NULL
        ORDER BY id`;
  const result = await query(sql, [drawId]);
  const seen = new Set();
  return (result.rows || []).filter((user) => {
    const email = cleanText(user.email).toLowerCase();
    if (!validEmail(email) || seen.has(email)) return false;
    seen.add(email);
    return true;
  });
}

async function loadRemaining(drawId) {
  const result = await query(
    `SELECT COUNT(*) FILTER (WHERE status = 'available')::int AS remaining_numbers
       FROM public.numbers
      WHERE draw_id = $1`,
    [drawId]
  );
  return Number(result.rows?.[0]?.remaining_numbers || 0);
}

function renderAutomaticTemplate(eventKey, user, context, remainingNumbers) {
  const params = {
    name: cleanText(user.name) || "Cliente",
    draw_name: context.drawName,
    draw_url: context.drawUrl,
    remaining_numbers: remainingNumbers,
  };
  if (eventKey === "NEW_DRAW_PUBLISHED") {
    return {
      subject: renderTemplate("Novo sorteio disponível — {{draw_name}}", params),
      html: `<p>Olá, {{name}}!</p><p>Um novo sorteio está disponível:</p><p><strong>{{draw_name}}</strong></p><p>Acesse para participar:</p><p><a href="{{draw_url}}">{{draw_url}}</a></p><p>Boa sorte!</p><p>Equipe NewStore</p>`.replace(/\{\{(\w+)\}\}/g, (_m, key) => params[key] ?? ""),
      text: renderTemplate("Olá, {{name}}!\n\nUm novo sorteio está disponível:\n\n{{draw_name}}\n\nAcesse para participar:\n{{draw_url}}\n\nBoa sorte!\n\nEquipe NewStore", params),
      templateKey: "NEW_DRAW_EMAIL",
    };
  }
  if (eventKey === "DRAW_CLOSED") {
    return {
      subject: renderTemplate("Sorteio {{draw_name}} encerrado — acompanhe o resultado", params),
      html: renderTemplate(`<p>Olá, {{name}}!</p><p>O sorteio <strong>{{draw_name}}</strong> foi encerrado.</p><p>O resultado será acompanhado pelo canal oficial da CAIXA no YouTube:</p><p><a href="${CAIXA_URL}">${CAIXA_URL}</a></p><p>O vencedor será o participante que possuir o <strong>último número sorteado da Lotomania</strong>.</p><p>Boa sorte!</p><p>Equipe NewStore</p>`, params),
      text: renderTemplate(`Olá, {{name}}!\n\nO sorteio {{draw_name}} foi encerrado.\n\nAcompanhe o resultado pelo canal oficial da CAIXA:\n\n${CAIXA_URL}\n\nO vencedor será o participante que possuir o último número sorteado da Lotomania.\n\nBoa sorte!\n\nEquipe NewStore`, params),
      templateKey: "DRAW_CLOSED_EMAIL",
    };
  }
  const threshold = REMAINING_THRESHOLDS.get(eventKey);
  return {
    subject: renderTemplate(`Restam ${threshold} números disponíveis no sorteio {{draw_name}}`, params),
    html: renderTemplate(`<p>Olá, {{name}}!</p><p>Restam ${threshold} números disponíveis no sorteio {{draw_name}}.</p><p><a href="{{draw_url}}">Acesse o site para escolher seus números</a></p>`, params),
    text: renderTemplate(`Olá, {{name}}!\n\nRestam ${threshold} números disponíveis no sorteio {{draw_name}}.\n\nAcesse o site para escolher seus números:\n{{draw_url}}`, params),
    templateKey: eventKey,
  };
}

async function alreadyDispatched({ eventKey, referenceKey, drawId, userId }) {
  const result = await query(
    `SELECT 1
       FROM public.notification_dispatches
      WHERE channel = 'email'
        AND event_key = $1
        AND user_id = $2
        AND draw_id = $3
        AND payload->>'source' = 'automation'
        AND payload->>'automation' = 'true'
        AND payload->>'reference_key' = $4
        AND status NOT IN ('failed', 'skipped')
      LIMIT 1`,
    [eventKey, userId, drawId, referenceKey]
  );
  return Boolean(result.rowCount);
}

export async function handleAutomaticEmailEvent({
  eventKey,
  referenceType = null,
  referenceKey,
  metadata = {},
  occurredAt = null,
} = {}, dependencies = {}) {
  const loadContext = dependencies.loadDrawContext || loadDrawContext;
  const loadEventRecipients = dependencies.loadRecipients || loadRecipients;
  const loadEventRemaining = dependencies.loadRemaining || loadRemaining;
  const wasAlreadyDispatched = dependencies.alreadyDispatched || alreadyDispatched;
  const resolveSmtpConfig = dependencies.getSmtpConfig || getSmtpConfig;
  const createMailer = dependencies.createSmtpTransporter || createSmtpTransporter;
  const createCampaignRecord = dependencies.createCampaign || createCampaign;
  const createDispatchRecord = dependencies.createDispatch || createDispatch;
  const acceptDispatch = dependencies.markDispatchAccepted || markDispatchAccepted;
  const failDispatch = dependencies.markDispatchFailed || markDispatchFailed;
  const updateCampaign = dependencies.updateCampaignAudienceCounts || updateCampaignAudienceCounts;
  const key = cleanText(eventKey);
  const drawId = Number(metadata?.draw_id);
  if (!AUTOMATIC_EMAIL_EVENT_KEYS.includes(key)) throw eventError("email_event_not_allowed");
  if (!Number.isInteger(drawId) || drawId <= 0) throw eventError("email_draw_id_invalid");
  if (!cleanText(referenceKey)) throw eventError("email_reference_key_invalid");
  console.log("[email-automation] event_received", { event_key: key, reference_key: referenceKey, draw_id: drawId });
  if (!isEnabled()) {
    console.log("[email-automation] skipped", { event_key: key, reference_key: referenceKey, draw_id: drawId, reason: "disabled" });
    return {
      ok: true,
      status: "disabled",
      reason: "disabled",
      event_key: key,
      reference_key: referenceKey,
      draw_id: drawId,
      sent: 0,
      failed: 0,
      skipped: 0,
      deduped: 0,
    };
  }

  const context = await loadContext(drawId);
  if (key === "DRAW_CLOSED" && !isDrawClosedForEmail(context.draw)) {
    return {
      ok: true,
      status: "skipped",
      reason: "draw_not_closed",
      event_key: key,
      reference_key: referenceKey,
      draw_id: drawId,
      sent: 0,
      failed: 0,
      skipped: 0,
      deduped: 0,
    };
  }
  if (REMAINING_THRESHOLDS.has(key) && context.draw.status !== "open") {
    return {
      ok: true,
      status: "skipped",
      reason: "draw_not_open",
      event_key: key,
      reference_key: referenceKey,
      draw_id: drawId,
      sent: 0,
      failed: 0,
      skipped: 0,
      deduped: 0,
    };
  }
  const remainingNumbers = REMAINING_THRESHOLDS.has(key) ? await loadEventRemaining(drawId) : null;
  const recipients = await loadEventRecipients(drawId, key);
  console.log("[email-automation] recipients_resolved", { event_key: key, reference_key: referenceKey, draw_id: drawId, count: recipients.length });
  if (!recipients.length) {
    return {
      ok: true,
      status: "no_recipients",
      event_key: key,
      reference_key: referenceKey,
      draw_id: drawId,
      sent: 0,
      failed: 0,
      skipped: 0,
      deduped: 0,
    };
  }

  const pendingRecipients = [];
  let deduped = 0;
  for (const user of recipients) {
    if (await wasAlreadyDispatched({ eventKey: key, referenceKey, drawId, userId: user.id })) {
      deduped += 1;
      console.log("[email-automation] dispatch_deduped", { event_key: key, reference_key: referenceKey, draw_id: drawId, user_id: user.id });
    } else {
      pendingRecipients.push(user);
    }
  }
  if (!pendingRecipients.length) {
    return {
      ok: true,
      status: "deduped",
      event_key: key,
      reference_key: referenceKey,
      draw_id: drawId,
      sent: 0,
      failed: 0,
      skipped: deduped,
      deduped,
    };
  }

  let smtp;
  try {
    smtp = resolveSmtpConfig();
  } catch (error) {
    if (error?.code !== "manual_email_smtp_not_configured") throw error;
    console.error("[email-automation] configuration_error", {
      event_key: key,
      reference_key: referenceKey,
      draw_id: drawId,
      code: error.code,
    });
    return {
      ok: false,
      status: "configuration_error",
      reason: error.code,
      event_key: key,
      reference_key: referenceKey,
      draw_id: drawId,
      sent: 0,
      failed: 0,
      skipped: pendingRecipients.length + deduped,
      deduped,
    };
  }
  const mailer = createMailer(smtp);
  const renderedByUser = (user) => renderAutomaticTemplate(key, user, context, remainingNumbers);
  const firstRendered = renderedByUser(pendingRecipients[0]);
  const campaign = await createCampaignRecord({
    name: `Automatic email - ${firstRendered.subject}`.slice(0, 255),
    channel: "email",
    provider: "brevo_smtp",
    templateKey: firstRendered.templateKey,
    audienceFilter: key === "DRAW_CLOSED" ? "draw_participants" : "all_with_email",
    audienceParams: { draw_id: drawId, event_key: key, reference_key: referenceKey },
    payload: { source: "automation", automation: true, event_key: key, reference_key: referenceKey, draw_id: drawId, reference_type: referenceType, occurred_at: occurredAt },
    messageSnapshot: { source: "automation", automation: true, event_key: key, subject: firstRendered.subject },
    audienceSnapshot: { source: "automation", automation: true, draw_id: drawId, resolved_recipients: recipients.length },
    campaignType: "automation",
    audienceCountExpected: recipients.length,
  });

  let sent = 0;
  let failed = 0;
  for (const user of pendingRecipients) {
    const rendered = renderedByUser(user);
    const dispatch = await createDispatchRecord({
      eventKey: key,
      channel: "email",
      provider: "brevo_smtp",
      userId: user.id,
      drawId,
      recipient: user.email,
      recipientOriginal: user.email,
      templateKey: rendered.templateKey,
      campaignId: campaign.id,
      payload: { source: "automation", automation: true, event_key: key, reference_key: referenceKey, draw_id: drawId, reference_type: referenceType },
      messageSnapshot: { source: "automation", automation: true, subject: rendered.subject, html: rendered.html, text: rendered.text },
      recipientSnapshot: { source: "automation", automation: true, user_id: user.id, email: user.email, draw_id: drawId },
    });
    try {
      const info = await mailer.sendMail({
        from: `"${smtp.fromName}" <${smtp.fromEmail}>`,
        to: user.email,
        replyTo: smtp.replyTo,
        subject: rendered.subject,
        html: rendered.html,
        text: rendered.text,
      });
      await acceptDispatch({ dispatchId: dispatch.id, result: { ok: true, provider_status: "accepted", delivery_status: "unknown", messageId: info?.messageId || null, response: { accepted: info?.accepted?.length || 0 } } });
      sent += 1;
      console.log("[email-automation] dispatch_sent", { event_key: key, reference_key: referenceKey, draw_id: drawId, user_id: user.id });
    } catch (error) {
      failed += 1;
      await failDispatch({ dispatchId: dispatch.id, result: { ok: false, error: "automatic_email_send_failed", reason: error?.code || error?.message || null } });
      console.error("[email-automation] dispatch_failed", { event_key: key, reference_key: referenceKey, draw_id: drawId, user_id: user.id, code: error?.code || null });
    }
  }
  await updateCampaign(null, campaign.id, { created: sent + failed, sent, failed, skipped: deduped });
  const status = failed === 0
    ? "processed"
    : sent === 0
      ? "failed"
      : "partial_failure";
  return {
    ok: true,
    status,
    event_key: key,
    reference_key: referenceKey,
    draw_id: drawId,
    sent,
    failed,
    skipped: deduped,
    deduped,
  };
}
