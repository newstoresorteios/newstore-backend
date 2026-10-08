import { createHash } from "node:crypto";
import { getPool, query } from "../../db.js";
import {
  createCampaign,
  createDispatch,
  markDispatchAccepted,
  markDispatchFailed,
  updateCampaignAudienceCounts,
} from "./notificationLog.js";
import { createSmtpTransporter, getSmtpConfig } from "./manualEmailNotifications.js";

// E-mails de RESULTADO (sorteio realizado). Reutilizam notification_dispatches / notification_campaigns:
// um registro por evento + destinatario, dedupe por payload.reference_key e reenvio de falhas.
// Ficam desligados (fail-closed) ate NOTIFICATION_EMAIL_RESULT_EFFECTIVE_FROM ser definido.
export const RESULT_EMAIL_EVENT_KEYS = Object.freeze([
  "EMAIL_RESULT_WINNER",
  "EMAIL_RESULT_PARTICIPANT",
  "EMAIL_RESULT_ADMIN",
]);

const RESULT_EVENTS = new Map([
  ["EMAIL_RESULT_WINNER", { suffix: "result_winner_email", templateKey: "RESULT_WINNER_EMAIL", audience: "draw_winner" }],
  ["EMAIL_RESULT_PARTICIPANT", { suffix: "result_participant_email", templateKey: "RESULT_PARTICIPANT_EMAIL", audience: "draw_participants_not_winner" }],
  ["EMAIL_RESULT_ADMIN", { suffix: "result_admin_email", templateKey: "RESULT_ADMIN_EMAIL", audience: "admin" }],
]);

// Janela de aceitacao (recuperacao) dos e-mails de resultado. O engine republica por 168h; as 24h extras
// absorvem atrasos do agendador. Alem dela, eventos criticos pendentes geram um alerta unico.
const DEFAULT_MAX_AGE_HOURS = 192;
// Falha definitiva so e sinalizada para o vencedor e para a administracao.
const CRITICAL_EVENT_KEYS = new Set(["EMAIL_RESULT_WINNER", "EMAIL_RESULT_ADMIN"]);
const DEFAULT_PENDING_STALE_MINUTES = 20;
const DEFAULT_MAX_ATTEMPTS = 5;
const FALLBACK_SITE_URL = "https://sorteiosxnamai.com.br";

function cleanText(value) {
  return String(value ?? "").trim();
}

function validEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(cleanText(value));
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function eventError(code, extra = {}) {
  const error = new Error(code);
  error.code = code;
  Object.assign(error, extra);
  return error;
}

function positiveInt(value, fallback) {
  const parsed = Number.parseInt(cleanText(value), 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function automationEnabled() {
  return cleanText(process.env.NOTIFICATION_EMAIL_AUTOMATION_ENABLED).toLowerCase() === "true";
}

// Ponto de corte: resultados realizados ANTES dele nunca geram e-mail (protege backfills/historico).
// Sem valor valido => desligado.
export function resultEffectiveFrom() {
  const raw = cleanText(process.env.NOTIFICATION_EMAIL_RESULT_EFFECTIVE_FROM);
  if (!raw) return null;
  const parsed = new Date(raw);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

export function resultConfig() {
  return {
    effectiveFrom: resultEffectiveFrom(),
    maxAgeHours: positiveInt(process.env.NOTIFICATION_EMAIL_RESULT_MAX_AGE_HOURS, DEFAULT_MAX_AGE_HOURS),
    pendingStaleMinutes: positiveInt(
      process.env.NOTIFICATION_EMAIL_RESULT_PENDING_STALE_MINUTES,
      DEFAULT_PENDING_STALE_MINUTES
    ),
    maxAttempts: positiveInt(process.env.NOTIFICATION_EMAIL_RESULT_MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS),
    adminEmail: cleanText(process.env.NOTIFICATION_RESULT_ADMIN_EMAIL || process.env.ADMIN_EMAIL),
  };
}

function siteUrl() {
  return cleanText(
    process.env.PUBLIC_APP_URL || process.env.FRONTEND_URL || process.env.SITE_URL || FALLBACK_SITE_URL
  ).replace(/\/+$/, "");
}

function drawTypeInfo(drawType) {
  const type = cleanText(drawType).toLowerCase() || "principal";
  if (type === "adicional") return { type, referencePrefix: "additional_draw", referenceType: "additional_draw", label: "Sorteio adicional" };
  if (type === "secundario") return { type, referencePrefix: "additional_draw", referenceType: "additional_draw", label: "Sorteio secundário" };
  return { type: "principal", referencePrefix: "draw", referenceType: "draw", label: "Sorteio principal" };
}

// Chave canonica: independe do que o chamador enviar, evitando duplicidade por chaves diferentes.
export function canonicalResultReferenceKey(drawType, drawId, eventKey) {
  const event = RESULT_EVENTS.get(eventKey);
  if (!event) throw eventError("email_event_not_allowed");
  return `${drawTypeInfo(drawType).referencePrefix}:${Number(drawId)}:${event.suffix}`;
}

// Identificador estavel por (chave de referencia, destinatario). Reenvios do MESMO evento levam o
// mesmo Message-ID, o que permite a clientes de e-mail suprimir duplicatas (melhor esforco).
export function resultMessageId(referenceKey, recipientKey, fromEmail) {
  const hash = createHash("sha256").update(`${referenceKey}|${recipientKey}`).digest("hex").slice(0, 32);
  const domain = cleanText(fromEmail).split("@")[1] || "newstore.local";
  return `<result-${hash}@${domain}>`;
}

// ---- Historico de despachos -> decisao (funcao pura) ---------------------------------------------
// delivered : ja confirmado (qualquer status que nao seja pending/failed/skipped)
// in_flight : existe pending recente (outro processo pode estar enviando) -> nao enviar
// exhausted : tentativas mal sucedidas >= maxAttempts -> parar e deixar log
// retry     : enviar; abandonedIds sao pendings antigos a encerrar como falha
export function classifyDispatchHistory(rows = [], { now = new Date(), pendingStaleMinutes = DEFAULT_PENDING_STALE_MINUTES, maxAttempts = DEFAULT_MAX_ATTEMPTS } = {}) {
  const staleMs = pendingStaleMinutes * 60 * 1000;
  let unsuccessful = 0;
  const abandonedIds = [];
  let hasFreshPending = false;
  for (const row of rows) {
    const status = cleanText(row.status).toLowerCase();
    if (status === "failed" || status === "skipped") {
      unsuccessful += 1;
    } else if (status === "pending") {
      const createdAt = new Date(row.created_at);
      const age = now.getTime() - createdAt.getTime();
      if (Number.isNaN(age) || age >= staleMs) {
        abandonedIds.push(row.id);
        unsuccessful += 1;
      } else {
        hasFreshPending = true;
      }
    } else {
      return { state: "delivered", abandonedIds: [], unsuccessful };
    }
  }
  if (hasFreshPending) return { state: "in_flight", abandonedIds: [], unsuccessful };
  if (unsuccessful >= maxAttempts) return { state: "exhausted", abandonedIds, unsuccessful };
  return { state: "retry", abandonedIds, unsuccessful };
}

// Marcador persistente de falha definitiva (uma linha em notification_dispatches com payload.final_failure).
// Nao e uma tentativa de envio: nao conta para o limite e impede novo alerta e novo reenvio.
export function isFinalFailureMarker(row) {
  return cleanText(row?.final_failure).toLowerCase() === "true";
}

// ---- Acesso a dados -----------------------------------------------------------------------------
export async function loadResultDraw(drawId, runQuery = query) {
  const drawResult = await runQuery(
    `SELECT id, status, draw_type, product_name, product_link, winner_number, winner_user_id, winner_name,
            realized_at, closed_at
       FROM public.draws
      WHERE id = $1`,
    [drawId]
  );
  const draw = drawResult.rows?.[0];
  if (!draw) throw eventError("email_draw_not_found", { drawId });
  const resolvedType = cleanText(draw.draw_type) || "principal";
  if (!["principal", "adicional", "secundario"].includes(resolvedType)) {
    throw eventError("email_draw_type_not_allowed", { drawId });
  }
  const config = await runQuery(
    `SELECT banner_title FROM public.app_config_new WHERE id = $1`,
    [String(drawId)]
  ).catch((error) => (error?.code === "42P01" ? { rows: [] } : Promise.reject(error)));
  return { ...draw, draw_type: resolvedType, banner_title: config.rows?.[0]?.banner_title || null };
}

export async function loadResultWinner(userId, runQuery = query) {
  if (!Number.isInteger(Number(userId)) || Number(userId) <= 0) return null;
  const result = await runQuery(`SELECT id, name, email FROM public.users WHERE id = $1`, [userId]);
  return result.rows?.[0] || null;
}

// Mesmos participantes do e-mail de encerramento (reserva paga OU pagamento aprovado), um por usuario,
// sem o vencedor.
export async function loadResultParticipants(drawId, winnerUserId, runQuery = query) {
  const result = await runQuery(
    `SELECT DISTINCT u.id, u.name, u.email
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
        AND u.id <> $2
      ORDER BY u.id`,
    [drawId, Number.isInteger(Number(winnerUserId)) ? Number(winnerUserId) : -1]
  );
  const seen = new Set();
  return (result.rows || []).filter((user) => {
    const email = cleanText(user.email).toLowerCase();
    if (!validEmail(email) || seen.has(email)) return false;
    seen.add(email);
    return true;
  });
}

export async function loadDispatchHistory({ eventKey, drawId, referenceKey, userId, recipient }, runQuery = query) {
  const byUser = Number.isInteger(Number(userId)) && Number(userId) > 0;
  const result = await runQuery(
    `SELECT id, status, created_at, payload->>'final_failure' AS final_failure
       FROM public.notification_dispatches
      WHERE channel = 'email'
        AND event_key = $1
        AND draw_id = $2
        AND payload->>'source' = 'automation'
        AND payload->>'reference_key' = $3
        AND ${byUser ? "user_id = $4" : "user_id IS NULL AND lower(recipient) = lower($4)"}
      ORDER BY id`,
    [eventKey, drawId, referenceKey, byUser ? Number(userId) : cleanText(recipient)]
  );
  return result.rows || [];
}

// Trava por chave de referencia: um unico processador por evento. Usa trava de TRANSACAO
// (pg_try_advisory_xact_lock) em uma transacao mantida aberta durante o envio: funciona tambem atras de
// pooler em modo transacao (a conexao fica presa enquanto a transacao existe). Nada e gravado nela.
// Se o processo morrer, a conexao cai, a transacao e abortada e a trava e liberada pelo Postgres.
export async function acquireResultEventLock(referenceKey, poolProvider = getPool) {
  const pool = await poolProvider();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await client.query(
      "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 7001)) AS locked",
      [referenceKey]
    );
    if (!result.rows?.[0]?.locked) {
      await client.query("ROLLBACK");
      client.release();
      return null;
    }
  } catch (error) {
    try { await client.query("ROLLBACK"); } catch { /* conexao ja encerrada */ }
    client.release();
    throw error;
  }
  return async () => {
    try {
      await client.query("ROLLBACK");
    } finally {
      client.release();
    }
  };
}

// ---- Mensagens ----------------------------------------------------------------------------------
function formatWinnerNumber(value) {
  const number = Number(value);
  return Number.isInteger(number) && number >= 0 ? String(number).padStart(2, "0") : cleanText(value);
}

function drawNames(draw) {
  const info = drawTypeInfo(draw.draw_type);
  const idSuffix = ` #${Number(draw.id)}`;
  const name = cleanText(draw.product_name) || cleanText(draw.banner_title) || `${info.label}${idSuffix}`;
  return { typeLabel: info.label, name, displayTitle: `${info.label} — ${name}` };
}

function resultMeta(metadata = {}) {
  const contest = Number(metadata?.contest_number);
  const resultDate = cleanText(metadata?.result_date);
  return {
    contestNumber: Number.isInteger(contest) && contest > 0 ? contest : null,
    resultDate: /^\d{4}-\d{2}-\d{2}$/.test(resultDate) ? resultDate : null,
  };
}

export function buildResultEmail(eventKey, { draw, winner, recipient, meta = {} }) {
  const names = drawNames(draw);
  const number = formatWinnerNumber(draw.winner_number);
  const contestLine = meta.contestNumber
    ? `Concurso da Lotomania utilizado: ${meta.contestNumber}${meta.resultDate ? ` (${meta.resultDate})` : ""}`
    : null;
  const drawUrl = `${siteUrl()}/?draw_id=${encodeURIComponent(String(draw.id))}`;
  const name = cleanText(recipient?.name) || "Cliente";
  const lines = [];
  let subject;
  let templateKey;
  if (eventKey === "EMAIL_RESULT_WINNER") {
    templateKey = "RESULT_WINNER_EMAIL";
    subject = `Parabéns! Você venceu — ${names.name}`.slice(0, 200);
    lines.push(`Olá, ${name}!`, `Parabéns! Você é o vencedor do ${names.displayTitle} (#${draw.id}).`, `Número vencedor: ${number}`);
    if (contestLine) lines.push(contestLine);
    lines.push("Nossa equipe entrará em contato com as próximas instruções para o recebimento do prêmio.", "Se você não reconhece esta mensagem, por favor, ignore.", "Equipe NewStore");
  } else if (eventKey === "EMAIL_RESULT_PARTICIPANT") {
    templateKey = "RESULT_PARTICIPANT_EMAIL";
    subject = `Resultado disponível — ${names.name}`.slice(0, 200);
    lines.push(`Olá, ${name}!`, `O resultado do ${names.displayTitle} (#${draw.id}) já está disponível.`, `Número vencedor: ${number}`);
    if (contestLine) lines.push(contestLine);
    lines.push("Desta vez você não foi contemplado. Obrigado por participar!", `Acompanhe os próximos sorteios: ${drawUrl}`, "Equipe NewStore");
  } else {
    templateKey = "RESULT_ADMIN_EMAIL";
    const identified = Boolean(winner);
    subject = (identified
      ? `Resultado definido — ${names.displayTitle} (#${draw.id})`
      : `PENDÊNCIA: resultado sem comprador identificado — ${names.displayTitle} (#${draw.id})`).slice(0, 200);
    lines.push(`${names.displayTitle} (#${draw.id}) foi realizado e marcado como SORTEADO.`, `Número sorteado: ${number}`);
    if (contestLine) lines.push(contestLine);
    if (identified) {
      lines.push("Vencedor identificado:", `- Nome: ${cleanText(winner.name) || "-"}`, `- E-mail: ${cleanText(winner.email) || "-"}`, `- Usuário: #${winner.id}`);
    } else {
      lines.push("ATENÇÃO: nenhum comprador foi identificado para o número sorteado. Nenhum e-mail foi enviado aos participantes. Verifique o sorteio antes de qualquer comunicação.");
    }
    lines.push(`Realizado em (UTC): ${draw.realized_at ? new Date(draw.realized_at).toISOString() : "-"}`);
  }
  const text = lines.join("\n\n");
  const html = lines.map((line) => `<p>${escapeHtml(line)}</p>`).join("");
  return { subject, text, html, templateKey };
}

// ---- Tratador do evento -------------------------------------------------------------------------
function result(base, status, extra = {}) {
  return { ok: true, status, ...base, sent: 0, failed: 0, skipped: 0, deduped: 0, ...extra };
}

export async function handleAutomaticResultEmailEvent({
  eventKey,
  referenceType = null,
  referenceKey,
  metadata = {},
  occurredAt = null,
} = {}, dependencies = {}) {
  const key = cleanText(eventKey);
  const event = RESULT_EVENTS.get(key);
  if (!event) throw eventError("email_event_not_allowed");
  const drawId = Number(metadata?.draw_id);
  if (!Number.isInteger(drawId) || drawId <= 0) throw eventError("email_draw_id_invalid");
  if (!cleanText(referenceKey)) throw eventError("email_reference_key_invalid");
  if (cleanText(referenceType) && !["draw", "additional_draw"].includes(cleanText(referenceType))) {
    throw eventError("email_reference_type_invalid");
  }

  const now = (dependencies.now || (() => new Date()))();
  const config = dependencies.resultConfig ? dependencies.resultConfig() : resultConfig();
  const base = { event_key: key, reference_key: referenceKey, draw_id: drawId };

  if (!automationEnabled()) {
    return result(base, "disabled", { reason: "disabled" });
  }
  if (!config.effectiveFrom) {
    console.log("[email-result] skipped", { ...base, reason: "result_effective_from_not_configured" });
    return result(base, "disabled", { reason: "result_effective_from_not_configured" });
  }

  const loadDraw = dependencies.loadResultDraw || loadResultDraw;
  const loadWinner = dependencies.loadResultWinner || loadResultWinner;
  const loadParticipants = dependencies.loadResultParticipants || loadResultParticipants;
  const loadHistory = dependencies.loadDispatchHistory || loadDispatchHistory;
  const acquireLock = dependencies.acquireResultEventLock || acquireResultEventLock;
  const resolveSmtpConfig = dependencies.getSmtpConfig || getSmtpConfig;
  const createMailer = dependencies.createSmtpTransporter || createSmtpTransporter;
  const createCampaignRecord = dependencies.createCampaign || createCampaign;
  const createDispatchRecord = dependencies.createDispatch || createDispatch;
  const acceptDispatch = dependencies.markDispatchAccepted || markDispatchAccepted;
  const failDispatch = dependencies.markDispatchFailed || markDispatchFailed;
  const updateCampaign = dependencies.updateCampaignAudienceCounts || updateCampaignAudienceCounts;

  const draw = await loadDraw(drawId);
  const canonicalKey = canonicalResultReferenceKey(draw.draw_type, drawId, key);
  if (cleanText(referenceKey) !== canonicalKey) throw eventError("email_reference_key_invalid");
  const typeInfo = drawTypeInfo(draw.draw_type);
  if (cleanText(referenceType) && cleanText(referenceType) !== typeInfo.referenceType) {
    throw eventError("email_reference_type_invalid");
  }

  const realizedAt = draw.realized_at ? new Date(draw.realized_at) : null;
  if (
    cleanText(draw.status).toLowerCase() !== "sorteado" ||
    !realizedAt ||
    Number.isNaN(realizedAt.getTime()) ||
    draw.winner_number === null ||
    draw.winner_number === undefined
  ) {
    return result(base, "skipped", { reason: "result_not_defined" });
  }
  if (realizedAt < config.effectiveFrom) {
    return result(base, "skipped", { reason: "result_before_effective_from" });
  }
  const expired = now.getTime() - realizedAt.getTime() > config.maxAgeHours * 3600 * 1000;
  if (expired && !CRITICAL_EVENT_KEYS.has(key)) {
    return result(base, "skipped", { reason: "result_too_old" });
  }

  const winnerUserId = Number.isInteger(Number(draw.winner_user_id)) && Number(draw.winner_user_id) > 0
    ? Number(draw.winner_user_id)
    : null;
  const winner = winnerUserId ? await loadWinner(winnerUserId) : null;

  // Sem comprador identificado: nao ha parabens, e os participantes NAO sao informados de "nao contemplado".
  if (!winner && key !== "EMAIL_RESULT_ADMIN") {
    console.log("[email-result] skipped", { ...base, reason: "no_identified_winner" });
    return result(base, "skipped", { reason: "no_identified_winner" });
  }

  let recipients;
  if (key === "EMAIL_RESULT_WINNER") {
    recipients = validEmail(winner.email) ? [{ id: winner.id, name: winner.name, email: cleanText(winner.email) }] : [];
  } else if (key === "EMAIL_RESULT_PARTICIPANT") {
    recipients = await loadParticipants(drawId, winnerUserId);
  } else {
    recipients = validEmail(config.adminEmail) ? [{ id: null, name: "Administração", email: config.adminEmail }] : [];
  }
  console.log("[email-result] recipients_resolved", { ...base, count: recipients.length });
  if (!recipients.length) {
    return result(base, "no_recipients", { reason: key === "EMAIL_RESULT_ADMIN" ? "admin_email_not_configured" : "no_recipients" });
  }

  const release = await acquireLock(canonicalKey);
  if (!release) {
    console.log("[email-result] in_progress", base);
    return result(base, "in_progress", { reason: "event_locked_by_another_processor" });
  }
  let sent = 0;
  let failed = 0;
  let deduped = 0;
  let inFlight = 0;
  let exhausted = 0;
  let alerts = 0;
  const meta = resultMeta(metadata);
  const snapshot = {
    source: "automation",
    automation: true,
    event_key: key,
    reference_key: canonicalKey,
    reference_type: typeInfo.referenceType,
    draw_id: drawId,
    draw_type: typeInfo.type,
    draw_status: "sorteado",
    winner_number: draw.winner_number,
    winner_identified: Boolean(winner),
    contest_number: meta.contestNumber,
    result_date: meta.resultDate,
  };
  // Falha definitiva (tentativas esgotadas ou janela vencida): registra UMA vez, sinaliza falha e nunca reenvia.
  const recordFinalFailure = async (user, reason, attempts) => {
    const marker = await createDispatchRecord({
      eventKey: key,
      channel: "email",
      provider: "brevo_smtp",
      userId: user.id,
      drawId,
      recipient: user.email,
      recipientOriginal: user.email,
      templateKey: event.templateKey,
      payload: { ...snapshot, final_failure: true, final_failure_reason: reason, attempts },
      messageSnapshot: { ...snapshot, final_failure: true, final_failure_reason: reason },
      recipientSnapshot: { ...snapshot, user_id: user.id },
    });
    await failDispatch({ dispatchId: marker.id, status: "skipped", result: { ok: false, error: reason, reason } });
    alerts += 1;
    console.error("[email-result] critical_notification_failed", { ...base, reason, attempts });
  };
  try {
    const toSend = [];
    for (const user of recipients) {
      const fullHistory = await loadHistory({ eventKey: key, drawId, referenceKey: canonicalKey, userId: user.id, recipient: user.email });
      const alreadyAlerted = fullHistory.some(isFinalFailureMarker);
      const history = fullHistory.filter((row) => !isFinalFailureMarker(row));
      const decision = classifyDispatchHistory(history, { now, pendingStaleMinutes: config.pendingStaleMinutes, maxAttempts: config.maxAttempts });
      if (decision.state === "delivered") {
        deduped += 1;
      } else if (alreadyAlerted) {
        // falha definitiva ja registrada e sinalizada: sem novo alerta e sem reenvio automatico
        exhausted += 1;
      } else if (expired) {
        if (decision.state === "in_flight") {
          inFlight += 1;
        } else {
          await recordFinalFailure(user, "recovery_window_expired", decision.unsuccessful);
        }
      } else if (decision.state === "in_flight") {
        inFlight += 1;
        console.log("[email-result] dispatch_in_flight", { ...base, user_id: user.id });
      } else if (decision.state === "exhausted") {
        exhausted += 1;
        if (CRITICAL_EVENT_KEYS.has(key)) {
          await recordFinalFailure(user, "retry_exhausted", decision.unsuccessful);
        } else {
          console.warn("[email-result] dispatch_retry_exhausted", { ...base, user_id: user.id, attempts: decision.unsuccessful });
        }
      } else {
        for (const dispatchId of decision.abandonedIds) {
          await failDispatch({ dispatchId, result: { ok: false, error: "abandoned_pending_recovered", reason: "stale_pending" } });
          console.warn("[email-result] pending_abandoned_recovered", { ...base, user_id: user.id, dispatch_id: dispatchId });
        }
        toSend.push(user);
      }
    }
    if (!toSend.length) {
      const idleStatus = alerts ? "critical_failure" : deduped ? "deduped" : "nothing_to_send";
      return result(base, idleStatus, {
        failed: alerts,
        critical_alerts: alerts,
        skipped: deduped + inFlight + exhausted,
        deduped,
        in_flight: inFlight,
        exhausted,
      });
    }

    const smtp = resolveSmtpConfig();
    const mailer = createMailer(smtp);
    const preview = buildResultEmail(key, { draw, winner, recipient: toSend[0], meta });
    const campaign = await createCampaignRecord({
      name: `Automatic result email - ${preview.subject}`.slice(0, 255),
      channel: "email",
      provider: "brevo_smtp",
      templateKey: event.templateKey,
      audienceFilter: event.audience,
      audienceParams: { draw_id: drawId, event_key: key, reference_key: canonicalKey },
      payload: { ...snapshot, occurred_at: occurredAt },
      messageSnapshot: { ...snapshot, subject: preview.subject },
      audienceSnapshot: { ...snapshot, resolved_recipients: recipients.length },
      campaignType: "automation",
      audienceCountExpected: recipients.length,
    });

    for (const user of toSend) {
      const rendered = buildResultEmail(key, { draw, winner, recipient: user, meta });
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
        payload: snapshot,
        messageSnapshot: { ...snapshot, subject: rendered.subject, html: rendered.html, text: rendered.text },
        recipientSnapshot: { ...snapshot, user_id: user.id, email: user.email },
      });
      try {
        const info = await mailer.sendMail({
          from: `"${smtp.fromName}" <${smtp.fromEmail}>`,
          to: user.email,
          replyTo: smtp.replyTo,
          subject: rendered.subject,
          html: rendered.html,
          text: rendered.text,
          messageId: resultMessageId(canonicalKey, user.id ?? user.email, smtp.fromEmail),
        });
        try {
          await acceptDispatch({ dispatchId: dispatch.id, result: { ok: true, provider_status: "accepted", delivery_status: "unknown", messageId: info?.messageId || null, response: { accepted: info?.accepted?.length || 0 } } });
        } catch (markError) {
          // Provedor aceitou, mas o registro falhou: o pending sera recuperado (uma vez, com o mesmo Message-ID).
          console.error("[email-result] dispatch_mark_failed_after_send", { ...base, user_id: user.id, dispatch_id: dispatch.id, code: markError?.code || null });
        }
        sent += 1;
        console.log("[email-result] dispatch_sent", { ...base, user_id: user.id });
      } catch (error) {
        failed += 1;
        console.error("[email-result] dispatch_failed", { ...base, user_id: user.id, code: error?.code || null });
        try {
          await failDispatch({ dispatchId: dispatch.id, result: { ok: false, error: "automatic_result_email_send_failed", reason: error?.code || error?.message || null } });
        } catch (markError) {
          // O pending sera encerrado como abandonado na proxima execucao; os demais destinatarios seguem.
          console.error("[email-result] dispatch_fail_mark_failed", { ...base, user_id: user.id, dispatch_id: dispatch.id, code: markError?.code || null });
        }
      }
    }
    await updateCampaign(null, campaign.id, { created: sent + failed, sent, failed, skipped: deduped });
    const finalStatus = alerts
      ? "critical_failure"
      : failed === 0 ? "processed" : sent === 0 ? "failed" : "partial_failure";
    return result(base, finalStatus, {
      sent,
      failed: failed + alerts,
      critical_alerts: alerts,
      skipped: deduped + inFlight + exhausted,
      deduped,
      in_flight: inFlight,
      exhausted,
    });
  } finally {
    try {
      await release();
    } catch (releaseError) {
      console.warn("[email-result] lock_release_failed", { ...base, code: releaseError?.code || null });
    }
  }
}
