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
import {
  BALANCE_EMAIL_EVENT_KEYS,
  handleAutomaticBalanceEmailEvent,
} from "./automaticBalanceEmailNotifications.js";

export const AUTOMATIC_EMAIL_EVENT_KEYS = Object.freeze([
  "NEW_DRAW_PUBLISHED",
  "EMAIL_DRAW_REMAINING_75",
  "EMAIL_DRAW_REMAINING_50",
  "EMAIL_DRAW_REMAINING_30",
  "EMAIL_DRAW_REMAINING_15",
  "DRAW_CLOSED",
  ...BALANCE_EMAIL_EVENT_KEYS,
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

function cleanDisplayText(value) {
  return cleanText(value).replace(/\s+/gu, " ");
}

export function resolveDrawDisplayName({
  drawId,
  drawType,
  payloadDrawName,
  databaseDrawName,
} = {}) {
  const currentDatabaseName = cleanDisplayText(databaseDrawName);
  if (currentDatabaseName) return currentDatabaseName;

  const receivedName = cleanDisplayText(payloadDrawName);
  if (receivedName) return receivedName;

  const normalizedType = cleanText(drawType).toLowerCase() || "principal";
  const numericDrawId = Number(drawId);
  const idSuffix = Number.isInteger(numericDrawId) && numericDrawId > 0
    ? ` #${numericDrawId}`
    : "";
  if (normalizedType === "adicional") return `Sorteio adicional${idSuffix}`;
  if (normalizedType === "secundario") return `Sorteio secundário${idSuffix}`;
  return `Sorteio principal${idSuffix}`;
}

export function resolveDrawTypeLabel(drawType) {
  const normalizedType = cleanText(drawType).toLowerCase();
  if (normalizedType === "adicional") return "Sorteio adicional";
  if (normalizedType === "secundario") return "Sorteio secundário";
  return "Sorteio principal";
}

function distinctDisplayText(value, otherValue) {
  const text = cleanDisplayText(value);
  if (!text) return null;
  return text.localeCompare(cleanDisplayText(otherValue), "pt-BR", { sensitivity: "base" }) === 0
    ? null
    : text;
}

function drawStatusLabel(draw) {
  if (draw?.closed_at || cleanText(draw?.status).toLowerCase() === "closed") return "Encerrado";
  if (["sorteado", "realized", "realizado"].includes(cleanText(draw?.status).toLowerCase())) {
    return "Resultado disponível";
  }
  if (cleanText(draw?.status).toLowerCase() === "open") return "Aberto";
  return cleanDisplayText(draw?.status) || "Situação atualizada";
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function subjectDrawName(value) {
  const name = cleanDisplayText(value);
  if (name.length <= 180) return name;
  return `${name.slice(0, 179).trimEnd()}…`;
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

function databaseDrawName(draw, config, principalConfig) {
  return (
    cleanText(draw?.product_name) ||
    cleanText(config?.banner_title) ||
    (draw?.draw_type === "principal" ? cleanText(principalConfig?.value) : "") ||
    null
  );
}

function eventError(code, extra = {}) {
  const error = new Error(code);
  error.code = code;
  Object.assign(error, extra);
  return error;
}

export async function loadDrawContext(drawId, runQuery = query) {
  const drawResult = await runQuery(
    `SELECT id, status, draw_type, product_name, product_link, opened_at, closed_at
       FROM public.draws
      WHERE id = $1`,
    [drawId]
  );
  const draw = drawResult.rows?.[0];
  if (!draw) throw eventError("email_draw_not_found", { drawId });

  const configResult = await runQuery(
    `SELECT id, banner_title
       FROM public.app_config_new
      WHERE id = $1`,
    [String(drawId)]
  ).catch((error) => (error?.code === "42P01" ? { rows: [] } : Promise.reject(error)));
  let principalConfig = null;
  if (cleanText(draw.draw_type || "principal") === "principal" && !configResult.rows?.[0]?.banner_title) {
    principalConfig = (await runQuery(
      `SELECT value FROM public.app_config WHERE key = 'banner_title' LIMIT 1`
    ).catch((error) => (error?.code === "42P01" ? { rows: [] } : Promise.reject(error)))).rows?.[0] || null;
  }

  const resolvedType = cleanText(draw.draw_type) || "principal";
  if (!["principal", "adicional", "secundario"].includes(resolvedType)) {
    throw eventError("email_draw_type_not_allowed", { drawId });
  }
  const currentDatabaseDrawName = databaseDrawName(
    { ...draw, draw_type: resolvedType },
    configResult.rows?.[0],
    principalConfig
  );
  const configuredDescription = cleanText(configResult.rows?.[0]?.banner_title) ||
    (resolvedType === "principal" ? cleanText(principalConfig?.value) : "");
  const drawName = resolveDrawDisplayName({
    drawId,
    drawType: resolvedType,
    databaseDrawName: currentDatabaseDrawName,
  });
  const drawTypeLabel = resolveDrawTypeLabel(resolvedType);
  const drawDescription = distinctDisplayText(configuredDescription, drawName);
  return {
    draw: { ...draw, draw_type: resolvedType },
    config: configResult.rows?.[0] || null,
    principalConfig,
    databaseDrawName: currentDatabaseDrawName,
    drawName,
    drawDescription,
    drawTypeLabel,
    drawDisplayTitle: `${drawTypeLabel} — ${drawName}`,
    drawStatusLabel: drawStatusLabel(draw),
    drawUrl: absoluteDrawUrl(drawId),
  };
}

export async function loadRecipients(drawId, eventKey, runQuery = query) {
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
  const params = eventKey === "DRAW_CLOSED" ? [drawId] : [];
  const result = await runQuery(sql, params);
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
  const drawType = cleanText(context?.draw?.draw_type).toLowerCase() || "principal";
  const drawTypeLabel = context.drawTypeLabel || resolveDrawTypeLabel(drawType);
  const drawName = context.drawName;
  const drawDescription = distinctDisplayText(context.drawDescription, drawName);
  const drawDisplayTitle = context.drawDisplayTitle || `${drawTypeLabel} — ${drawName}`;
  const statusLabel = context.drawStatusLabel || drawStatusLabel(context.draw);
  const actualRemaining = Number.isInteger(Number(remainingNumbers))
    ? Number(remainingNumbers)
    : REMAINING_THRESHOLDS.get(eventKey);
  const params = {
    name: cleanText(user.name) || "Cliente",
    draw_name: drawName,
    draw_description: drawDescription || "",
    draw_type_label: drawTypeLabel,
    draw_type_subject: drawTypeLabel.toLocaleLowerCase("pt-BR"),
    draw_display_title: drawDisplayTitle,
    draw_status: statusLabel,
    draw_url: context.drawUrl,
    remaining_numbers: actualRemaining,
  };
  const subjectParams = {
    ...params,
    draw_name: subjectDrawName(params.draw_name),
  };
  const htmlParams = {
    ...params,
    name: escapeHtml(params.name),
    draw_name: escapeHtml(params.draw_name),
    draw_description: escapeHtml(params.draw_description),
    draw_type_label: escapeHtml(params.draw_type_label),
    draw_display_title: escapeHtml(params.draw_display_title),
    draw_status: escapeHtml(params.draw_status),
    draw_url: escapeHtml(params.draw_url),
  };
  const htmlDescription = params.draw_description
    ? `<p>{{draw_description}}</p>`
    : "";
  const textDescription = params.draw_description
    ? `\n\n{{draw_description}}`
    : "";
  if (eventKey === "NEW_DRAW_PUBLISHED") {
    return {
      subject: renderTemplate("Novo {{draw_type_subject}} — {{draw_name}}", subjectParams),
      html: renderTemplate(`<p>Olá, {{name}}!</p><p>Um novo <strong>{{draw_type_label}}</strong> está disponível:</p><p><strong>{{draw_name}}</strong></p>${htmlDescription}<p><strong>Situação:</strong> {{draw_status}}</p><p>Acesse para participar:</p><p><a href="{{draw_url}}">{{draw_url}}</a></p><p>Boa sorte!</p><p>Equipe NewStore</p>`, htmlParams),
      text: renderTemplate(`Olá, {{name}}!\n\nUm novo {{draw_type_subject}} está disponível:\n\n{{draw_name}}${textDescription}\n\nSituação: {{draw_status}}\n\nAcesse para participar:\n{{draw_url}}\n\nBoa sorte!\n\nEquipe NewStore`, params),
      templateKey: "NEW_DRAW_EMAIL",
    };
  }
  if (eventKey === "DRAW_CLOSED") {
    return {
      subject: renderTemplate("{{draw_type_label}} — {{draw_name}} — encerrado", subjectParams),
      html: renderTemplate(`<p>Olá, {{name}}!</p><p>O <strong>{{draw_display_title}}</strong> foi encerrado.</p>${htmlDescription}<p><strong>Situação:</strong> {{draw_status}}</p><p>O resultado será acompanhado pelo canal oficial da CAIXA no YouTube:</p><p><a href="${CAIXA_URL}">${CAIXA_URL}</a></p><p>O vencedor será o participante que possuir o <strong>último número sorteado da Lotomania</strong>.</p><p>Boa sorte!</p><p>Equipe NewStore</p>`, htmlParams),
      text: renderTemplate(`Olá, {{name}}!\n\nO {{draw_display_title}} foi encerrado.${textDescription}\n\nSituação: {{draw_status}}\n\nAcompanhe o resultado pelo canal oficial da CAIXA:\n\n${CAIXA_URL}\n\nO vencedor será o participante que possuir o último número sorteado da Lotomania.\n\nBoa sorte!\n\nEquipe NewStore`, params),
      templateKey: "DRAW_CLOSED_EMAIL",
    };
  }
  return {
    subject: renderTemplate("Restam {{remaining_numbers}} números no {{draw_type_subject}} — {{draw_name}}", subjectParams),
    html: renderTemplate(`<p>Olá, {{name}}!</p><p>Restam <strong>{{remaining_numbers}} números</strong> no {{draw_display_title}}.</p>${htmlDescription}<p><strong>Situação:</strong> {{draw_status}}</p><p><a href="{{draw_url}}">Acesse o site para escolher seus números</a></p>`, htmlParams),
    text: renderTemplate(`Olá, {{name}}!\n\nRestam {{remaining_numbers}} números no {{draw_display_title}}.${textDescription}\n\nSituação: {{draw_status}}\n\nAcesse o site para escolher seus números:\n{{draw_url}}`, params),
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
        AND draw_id IS NOT DISTINCT FROM $3
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
  if (!AUTOMATIC_EMAIL_EVENT_KEYS.includes(key)) throw eventError("email_event_not_allowed");
  if (BALANCE_EMAIL_EVENT_KEYS.includes(key)) {
    if (cleanText(referenceType) && cleanText(referenceType) !== "user_balance") {
      throw eventError("email_reference_type_invalid");
    }
    return handleAutomaticBalanceEmailEvent({
      eventKey: key,
      referenceType,
      referenceKey,
      metadata,
      occurredAt,
    }, dependencies);
  }
  if (cleanText(referenceType) && !["draw", "additional_draw"].includes(cleanText(referenceType))) {
    throw eventError("email_reference_type_invalid");
  }
  const drawId = Number(metadata?.draw_id);
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

  const loadedContext = await loadContext(drawId);
  const drawType = cleanText(loadedContext?.draw?.draw_type || metadata?.draw_type).toLowerCase() || "principal";
  const payloadDrawName = metadata?.draw_name ?? metadata?.product_name ?? null;
  const databaseNameWasProvided = Object.prototype.hasOwnProperty.call(
    loadedContext || {},
    "databaseDrawName"
  );
  const currentDatabaseDrawName = databaseNameWasProvided
    ? loadedContext.databaseDrawName
    : loadedContext?.drawName;
  const resolvedDrawName = resolveDrawDisplayName({
    drawId,
    drawType,
    payloadDrawName,
    databaseDrawName: currentDatabaseDrawName,
  });
  const drawTypeLabel = loadedContext?.drawTypeLabel || resolveDrawTypeLabel(drawType);
  const resolvedDrawDescription = distinctDisplayText(
    loadedContext?.drawDescription,
    resolvedDrawName
  );
  const context = {
    ...loadedContext,
    drawName: resolvedDrawName,
    drawDescription: resolvedDrawDescription,
    drawTypeLabel,
    drawDisplayTitle: `${drawTypeLabel} — ${resolvedDrawName}`,
    drawStatusLabel: loadedContext?.drawStatusLabel || drawStatusLabel(loadedContext?.draw),
  };
  console.log("[email-automation] draw_name_resolved", {
    draw_id: drawId,
    draw_type: drawType,
    payload_draw_name: cleanDisplayText(payloadDrawName) || null,
    database_draw_name: cleanDisplayText(currentDatabaseDrawName) || null,
    resolved_draw_name: resolvedDrawName,
    resolved_draw_description: resolvedDrawDescription,
    draw_type_label: drawTypeLabel,
    reference_key: referenceKey,
  });
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
  const drawSnapshot = {
    source: "automation",
    automation: true,
    event_key: key,
    reference_key: referenceKey,
    reference_type: referenceType,
    draw_id: drawId,
    draw_type: drawType,
    draw_type_label: context.drawTypeLabel,
    draw_name: context.drawName,
    draw_description: context.drawDescription || null,
    draw_display_title: context.drawDisplayTitle,
    draw_url: context.drawUrl,
    draw_status: context.draw?.status || null,
    draw_status_label: context.drawStatusLabel,
    remaining_numbers: remainingNumbers,
  };
  const campaign = await createCampaignRecord({
    name: `Automatic email - ${firstRendered.subject}`.slice(0, 255),
    channel: "email",
    provider: "brevo_smtp",
    templateKey: firstRendered.templateKey,
    audienceFilter: key === "DRAW_CLOSED" ? "draw_participants" : "all_with_email",
    audienceParams: { draw_id: drawId, event_key: key, reference_key: referenceKey },
    payload: { ...drawSnapshot, occurred_at: occurredAt },
    messageSnapshot: { ...drawSnapshot, subject: firstRendered.subject },
    audienceSnapshot: { ...drawSnapshot, resolved_recipients: recipients.length },
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
      payload: drawSnapshot,
      messageSnapshot: { ...drawSnapshot, subject: rendered.subject, html: rendered.html, text: rendered.text },
      recipientSnapshot: { ...drawSnapshot, user_id: user.id, email: user.email },
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
