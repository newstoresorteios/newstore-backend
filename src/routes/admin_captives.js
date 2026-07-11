import { Router } from "express";
import { query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import {
  authorizeCurrentDrawCaptivePreauthForAdmin,
  getCurrentCaptiveDrawContext,
  setCurrentDrawCaptiveParticipation,
} from "../services/autopay/captivePreauthService.js";

const router = Router();
const LOG_PREFIX = "[admin-captives]";

const ALLOWED_STATUS = new Set([
  "todos",
  "ativos",
  "pausados",
  "sem_whatsapp",
  "com_whatsapp",
  "sem_cartao",
  "com_cartao",
]);

const CARD_READY_SQL = "(ap.vindi_customer_id IS NOT NULL AND ap.vindi_payment_profile_id IS NOT NULL)";
const HISTORY_STATUSES = new Set(["accepted", "sent", "delivered", "skipped", "failed"]);
const HISTORY_ATTEMPT_TYPES = new Set(["initial", "reissue", "manual_activation"]);
const PARTICIPATION_FILTERS = new Set(["all", "enabled", "disabled", "pending", "confirmed", "failed"]);
const ADMIN_AUTHORIZATION_ERROR_CODES = new Set([
  "participation_not_found",
  "invalid_draw_id",
  "invalid_admin_user",
  "current_principal_draw_not_found",
  "draw_not_current_principal",
  "current_principal_draw_changed",
  "captive_preauth_not_required",
  "captive_binding_invalid",
  "captive_reservation_invalid",
  "captive_group_row_mismatch",
  "captive_group_changed",
  "captive_payment_profile_mismatch",
  "participation_disabled_current_draw",
  "participation_declined_by_customer",
  "authorization_expired",
  "payment_failed_retry_required",
  "payment_failed",
  "payment_result_unknown",
  "payment_provider_unavailable",
  "payment_preflight_failed",
  "payment_attempt_persist_failed",
  "participation_not_pending",
  "authorization_amount_outdated",
  "authorization_amount_mismatch",
  "payment_in_progress",
  "payment_method_unavailable",
  "group_already_partially_or_fully_charged",
  "group_requires_review",
  "group_changed",
  "admin_authorization_failed",
]);

function getDefaultAmountCents() {
  const n = Number(process.env.CAPTIVE_AUTOPAY_DEFAULT_AMOUNT_CENTS);
  return Number.isInteger(n) && n > 0 ? n : 5500;
}

function formatAmountLabel(amountCents) {
  const value = Number(amountCents || 0) / 100;
  return value.toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
}

function buildPolicy() {
  const defaultAmountCents = getDefaultAmountCents();
  const defaultAmountLabel = formatAmountLabel(defaultAmountCents);
  return {
    default_amount_cents: defaultAmountCents,
    default_amount_label: defaultAmountLabel,
    variable_pricing_requires_preauth: true,
    automatic_label: `Automático até ${defaultAmountLabel}`,
    preauth_label: "Sempre pedir autorização",
    variable_price_rule_label: `Acima de ${defaultAmountLabel}, a pré-autorização é obrigatória para todos.`,
  };
}

function toPositiveInt(value, fallback, max) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.min(Math.trunc(n), max);
}

function maskPhone(phone) {
  const digits = String(phone || "").replace(/\D/g, "");
  if (!digits) return null;
  if (digits.length <= 4) return "****";
  const prefix = digits.length > 10 ? digits.slice(0, 2) : "";
  return `${prefix}${"*".repeat(Math.max(4, digits.length - prefix.length - 4))}${digits.slice(-4)}`;
}

function maskHistoryPhone(phone) {
  const digits = String(phone || "").replace(/\D/g, "");
  if (!digits) return null;
  const last4 = digits.slice(-4).padStart(4, "*");
  if (digits.startsWith("55") && digits.length >= 12) {
    return `+${digits.slice(0, 2)} ${digits.slice(2, 4)} *****-${last4}`;
  }
  return `*****-${last4}`;
}

function adminAuthorizationMessage(code) {
  return {
    participation_not_found: "Participação não encontrada.",
    current_principal_draw_not_found: "Nenhum sorteio principal está aberto.",
    draw_not_current_principal: "A participação não pertence ao sorteio principal atual.",
    captive_preauth_not_required: "O sorteio atual utiliza o fluxo automático padrão.",
    captive_binding_invalid: "O vínculo entre o cliente e um dos números cativos está inconsistente.",
    captive_reservation_invalid: "Uma das reservas do grupo não está mais válida. Atualize a lista e revise os números.",
    captive_group_row_mismatch: "A quantidade de autorizações, números ou reservas do grupo está inconsistente.",
    captive_group_changed: "A participação foi alterada depois que a lista foi carregada. Atualize a página antes de tentar novamente.",
    captive_payment_profile_mismatch: "O perfil de pagamento dos números selecionados está inconsistente.",
    participation_disabled_current_draw: "O número cativo está desativado neste sorteio.",
    participation_declined_by_customer: "O cliente recusou esta participação. É necessário reabrir a autorização antes de confirmar administrativamente.",
    authorization_expired: "O prazo desta autorização expirou.",
    payment_failed_retry_required: "A cobrança anterior falhou. É necessária uma nova confirmação antes de tentar novamente.",
    participation_not_pending: "Esta participação não está pendente de confirmação.",
    authorization_amount_outdated: "O valor da autorização está desatualizado. Reemita a confirmação antes de autorizar.",
    authorization_amount_mismatch: "O valor calculado para a cobrança está inconsistente.",
    payment_in_progress: "Já existe uma cobrança em processamento para esta participação.",
    payment_method_unavailable: "O cliente não possui um cartão válido disponível para cobrança.",
    group_already_partially_or_fully_charged: "O grupo possui uma participação já cobrada e precisa de revisão.",
    group_requires_review: "O grupo possui participações inconsistentes e precisa de revisão.",
    group_changed: "O grupo foi alterado durante a autorização. Atualize a lista e revise as participações.",
    payment_failed: "A cobrança não foi aprovada pelo cartão cadastrado.",
    payment_result_unknown: "Não foi possível confirmar o resultado da cobrança. Verifique a Vindi antes de tentar novamente.",
    payment_provider_unavailable: "A Vindi está temporariamente indisponível. Tente novamente mais tarde.",
    payment_preflight_failed: "A validação financeira falhou antes do envio da cobrança.",
    payment_attempt_persist_failed: "Não foi possível registrar a tentativa financeira.",
    authorization_charge_not_configured: "A autorização foi registrada, mas o cartão não está disponível para cobrança.",
  }[code] || "Não foi possível autorizar a cobrança.";
}

function normalizeAdminAuthorizationErrorCode(value) {
  const code = String(value || "").trim();
  return ADMIN_AUTHORIZATION_ERROR_CODES.has(code) ? code : "admin_authorization_failed";
}

function normalizeConsentStatus(status) {
  const s = String(status || "").trim().toLowerCase();
  if (!s) return "missing";
  if (["granted", "active", "opt_in", "allowed", "subscribed", "accepted"].includes(s)) return "granted";
  if (["revoked", "opt_out", "denied", "blocked", "unsubscribed"].includes(s)) return "revoked";
  return "unknown";
}

function mapLastRunStatus(status) {
  const s = String(status || "").trim().toLowerCase();
  if (!s) return null;
  if (["charged_ok", "ok", "approved", "paid"].includes(s)) return "approved";
  if (["charged_fail", "error", "failed", "rejected"].includes(s)) return "failed";
  if (["attempt", "reserved", "billed", "charged"].includes(s)) return "pending";
  if (["skipped", "ignored", "inactive", "missingvindi"].includes(s)) return "ignored";
  return s;
}

function mapRow(row, policy = buildPolicy()) {
  const profileActive = row.profile_active === true;
  const numberActive = row.number_active !== false;
  const preauthNotificationsEnabled = row.preauth_notifications_enabled !== false;
  return {
    id: String(row.id),
    autopay_number_id: String(row.id),
    user_id: Number(row.user_id),
    user_name: row.user_name || null,
    user_email: row.user_email || null,
    user_phone_masked: maskPhone(row.user_phone),
    captive_number: Number(row.captive_number),
    captive_number_label: String(Number(row.captive_number)).padStart(2, "0"),
    participation_active: profileActive && numberActive,
    profile_active: profileActive,
    number_active: numberActive,
    authorization_mode: row.authorization_mode === true,
    requires_preauth: row.authorization_mode === true,
    authorization_mode_label: row.authorization_mode === true ? policy.preauth_label : policy.automatic_label,
    variable_price_rule_label: policy.variable_price_rule_label,
    preauth_notifications_enabled: preauthNotificationsEnabled,
    preauth_notifications_label: preauthNotificationsEnabled ? "Ativas" : "Pausadas",
    autopay_profile_id: String(row.autopay_profile_id),
    card_status: row.has_card ? "configured" : "missing",
    whatsapp_consent_status: normalizeConsentStatus(row.whatsapp_consent_status),
    last_run_status: mapLastRunStatus(row.last_run_status),
    last_run_at: row.last_run_at || null,
    created_at: row.created_at || null,
    updated_at: row.updated_at || null,
  };
}

async function getAdminCaptivesSchema() {
  const result = await query(
    `SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'autopay_numbers'
        AND column_name IN ('active', 'created_at', 'updated_at', 'preauth_notifications_enabled')`
  );
  const columns = new Set((result.rows || []).map((row) => row.column_name));
  const tables = await query(
    `SELECT table_name
       FROM information_schema.tables
      WHERE table_schema = 'public'
        AND table_name IN ('communication_consents', 'autopay_runs')`
  );
  const tableNames = new Set((tables.rows || []).map((row) => row.table_name));
  const runColumnsResult = await query(
    `SELECT column_name
       FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'autopay_runs'
        AND column_name IN ('status', 'created_at', 'updated_at')`
  );
  const runColumns = new Set((runColumnsResult.rows || []).map((row) => row.column_name));
  const profileColumnsResult = await query(
    `SELECT column_name
       FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'autopay_profiles'
        AND column_name IN ('authorization_mode', 'updated_at')`
  );
  const profileColumns = new Set((profileColumnsResult.rows || []).map((row) => row.column_name));
  return {
    numberColumns: {
      active: columns.has("active"),
      created_at: columns.has("created_at"),
      updated_at: columns.has("updated_at"),
      preauth_notifications_enabled: columns.has("preauth_notifications_enabled"),
    },
    hasCommunicationConsents: tableNames.has("communication_consents"),
    hasAutopayRuns: tableNames.has("autopay_runs"),
    runColumns: {
      status: runColumns.has("status"),
      created_at: runColumns.has("created_at"),
      updated_at: runColumns.has("updated_at"),
    },
    profileColumns: {
      authorization_mode: profileColumns.has("authorization_mode"),
      updated_at: profileColumns.has("updated_at"),
    },
  };
}

function buildCaptivesBaseSql({
  numberColumns,
  profileColumns = { authorization_mode: false, updated_at: true },
  hasCommunicationConsents = true,
  hasAutopayRuns = true,
  runColumns = { status: true, created_at: true, updated_at: true },
  includeWhere = "",
} = {}) {
  const numberActiveExpr = numberColumns.active ? "an.active" : "true";
  const authorizationModeExpr = profileColumns.authorization_mode ? "COALESCE(ap.authorization_mode, false)" : "false";
  const preauthNotificationsExpr = numberColumns.preauth_notifications_enabled
    ? "COALESCE(an.preauth_notifications_enabled, true)"
    : "true";
  const createdAtExpr = numberColumns.created_at ? "COALESCE(an.created_at, ap.created_at)" : "ap.created_at";
  const updatedAtExpr = numberColumns.updated_at
    ? `COALESCE(an.updated_at, ap.updated_at, ${createdAtExpr})`
    : `COALESCE(ap.updated_at, ${createdAtExpr})`;
  const latestConsentSql = hasCommunicationConsents
    ? `SELECT DISTINCT ON (user_id)
             user_id,
             LOWER(status) AS status
        FROM public.communication_consents
       WHERE LOWER(channel) = 'whatsapp'
       ORDER BY user_id, created_at DESC`
    : `SELECT NULL::bigint AS user_id, 'unknown'::text AS status WHERE false`;
  const runStatusExpr = runColumns.status ? "status" : "NULL::text";
  const runAtExpr = runColumns.updated_at && runColumns.created_at
    ? "COALESCE(updated_at, created_at)"
    : runColumns.updated_at
      ? "updated_at"
      : runColumns.created_at
        ? "created_at"
        : "NULL::timestamptz";
  const latestRunSql = hasAutopayRuns
    ? `SELECT DISTINCT ON (autopay_id)
             autopay_id,
             ${runStatusExpr} AS status,
             ${runAtExpr} AS run_at
        FROM public.autopay_runs
       ORDER BY autopay_id, ${runAtExpr} DESC NULLS LAST`
    : `SELECT NULL::uuid AS autopay_id, NULL::text AS status, NULL::timestamptz AS run_at WHERE false`;
  return `
    WITH latest_consent AS (
      ${latestConsentSql}
    ),
    latest_run AS (
      ${latestRunSql}
    )
    SELECT
      an.id,
      an.autopay_id AS autopay_profile_id,
      an.n AS captive_number,
      ${numberActiveExpr} AS number_active,
      ${authorizationModeExpr} AS authorization_mode,
      ${preauthNotificationsExpr} AS preauth_notifications_enabled,
      ${createdAtExpr} AS created_at,
      ${updatedAtExpr} AS updated_at,
      ap.user_id,
      ap.active AS profile_active,
      ${CARD_READY_SQL} AS has_card,
      u.name AS user_name,
      u.email AS user_email,
      u.phone AS user_phone,
      COALESCE(lc.status, 'missing') AS whatsapp_consent_status,
      lr.status AS last_run_status,
      lr.run_at AS last_run_at
    FROM public.autopay_numbers an
    JOIN public.autopay_profiles ap ON ap.id = an.autopay_id
    LEFT JOIN public.users u ON u.id = ap.user_id
    LEFT JOIN latest_consent lc ON lc.user_id = ap.user_id
    LEFT JOIN latest_run lr ON lr.autopay_id = ap.id
    ${includeWhere}
  `;
}

const HISTORY_EFFECTIVE_STATUS_SQL = `CASE
  WHEN LOWER(COALESCE(nd.delivery_status, '')) IN ('delivered', 'read') THEN 'delivered'
  WHEN LOWER(COALESCE(nd.delivery_status, '')) IN ('failed', 'undelivered') THEN 'failed'
  WHEN LOWER(COALESCE(nd.status, '')) = 'failed' THEN 'failed'
  WHEN LOWER(COALESCE(nd.status, '')) = 'skipped' THEN 'skipped'
  WHEN LOWER(COALESCE(nd.status, '')) = 'sent' THEN 'sent'
  WHEN LOWER(COALESCE(nd.status, '')) = 'accepted' THEN 'accepted'
  ELSE attempt.status
END`;

router.get("/notification-history", requireAuth, requireAdmin, async (req, res) => {
  try {
    const page = toPositiveInt(req.query?.page, 1, 100000);
    const limit = toPositiveInt(req.query?.limit, 50, 100);
    const offset = (page - 1) * limit;
    const search = String(req.query?.search || "").trim();
    const status = String(req.query?.status || "").trim().toLowerCase();
    const attemptType = String(req.query?.attempt_type || "").trim().toLowerCase();
    const rawDrawId = String(req.query?.draw_id || "").trim();
    const drawId = rawDrawId ? Number(rawDrawId) : null;
    if (rawDrawId && (!Number.isInteger(drawId) || drawId <= 0)) {
      return res.status(400).json({ ok: false, error: "invalid_draw_id" });
    }
    if (status && !HISTORY_STATUSES.has(status)) {
      return res.status(400).json({ ok: false, error: "invalid_status" });
    }
    if (attemptType && !HISTORY_ATTEMPT_TYPES.has(attemptType)) {
      return res.status(400).json({ ok: false, error: "invalid_attempt_type" });
    }

    const params = [];
    const where = [];
    const addParam = (value) => {
      params.push(value);
      return `$${params.length}`;
    };
    if (drawId) where.push(`attempt.draw_id = ${addParam(drawId)}`);
    if (status) where.push(`${HISTORY_EFFECTIVE_STATUS_SQL} = ${addParam(status)}`);
    if (attemptType) where.push(`attempt.attempt_type = ${addParam(attemptType)}`);
    if (search) {
      const searchParam = addParam(`%${search}%`);
      where.push(`(
        u.name ILIKE ${searchParam}
        OR u.email ILIKE ${searchParam}
        OR CAST(attempt.captive_number AS text) ILIKE ${searchParam}
      )`);
    }
    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const fromSql = `
      FROM public.captive_preauth_notification_attempts attempt
      JOIN public.autopay_draw_authorizations auth
        ON auth.id = attempt.authorization_id
      JOIN public.users u ON u.id = attempt.user_id
      LEFT JOIN public.draws d ON d.id = attempt.draw_id
      LEFT JOIN public.notification_dispatches nd
        ON nd.id = attempt.provider_dispatch_id
      ${whereSql}`;
    const countResult = await query(`SELECT COUNT(*)::int AS total ${fromSql}`, params);
    const total = Number(countResult.rows?.[0]?.total || 0);
    const rows = await query(
      `SELECT attempt.id,
              attempt.authorization_id,
              attempt.draw_id,
              attempt.user_id,
              attempt.captive_number,
              attempt.amount_cents,
              COALESCE(attempt.template_id, nd.provider_template_id) AS template_id,
              attempt.attempt_type,
              ${HISTORY_EFFECTIVE_STATUS_SQL} AS status,
              COALESCE(
                attempt.error_code,
                CASE WHEN ${HISTORY_EFFECTIVE_STATUS_SQL} = 'failed' THEN 'provider_failed' END
              ) AS error_code,
              attempt.created_at,
              auth.status AS authorization_status,
              u.name AS user_name,
              u.email AS user_email,
              u.phone AS user_phone,
              COALESCE(d.product_name, 'Sorteio #' || attempt.draw_id::text) AS draw_title
         ${fromSql}
        ORDER BY attempt.created_at DESC, attempt.id DESC
        LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
      [...params, limit, offset]
    );

    return res.json({
      ok: true,
      page,
      limit,
      total,
      items: (rows.rows || []).map((row) => ({
        id: String(row.id),
        authorization_id: String(row.authorization_id),
        draw_id: Number(row.draw_id),
        draw_title: row.draw_title,
        user_id: Number(row.user_id),
        user_name: row.user_name || null,
        user_email: row.user_email || null,
        user_phone_masked: maskHistoryPhone(row.user_phone),
        captive_number: Number(row.captive_number),
        amount_cents: Number(row.amount_cents),
        template_id: row.template_id || null,
        attempt_type: row.attempt_type,
        status: row.status,
        error_code: row.error_code || null,
        authorization_status: row.authorization_status || null,
        created_at: row.created_at,
      })),
    });
  } catch (error) {
    console.error(`${LOG_PREFIX} notification_history_failed`, {
      admin_user_id: req.user?.id || null,
      message: error?.message || null,
      code: error?.code || null,
    });
    return res.status(500).json({ ok: false, error: "notification_history_failed" });
  }
});

const APPROVED_PARTICIPATION_SQL = `(
  LOWER(COALESCE(auth.status, '')) = 'charged'
  OR EXISTS (
    SELECT 1
      FROM public.autopay_runs run
     WHERE run.status = 'charged_ok'
       AND run.provider_request->>'authorization_id' = auth.id::text
  )
  OR EXISTS (
    SELECT 1
      FROM public.payments payment
     WHERE payment.draw_id = auth.draw_id
       AND payment.user_id = auth.user_id
       AND (
         LOWER(payment.status) IN ('approved', 'paid', 'pago')
         OR LOWER(COALESCE(payment.vindi_status, '')) IN ('approved', 'paid', 'pago', 'success', 'successful')
       )
       AND an.n::integer = ANY(payment.numbers)
  )
)`;

async function loadCurrentDrawParticipation({ search = "", status = "all", page = 1, limit = 50, autopayNumberId = null } = {}) {
  const context = await getCurrentCaptiveDrawContext();
  if (!context.draw) {
    return {
      ok: true,
      draw: null,
      page: 1,
      limit,
      total: 0,
      pending_authorizations: 0,
      disabled_count: 0,
      items: [],
    };
  }

  const safePage = toPositiveInt(page, 1, 100000);
  const safeLimit = toPositiveInt(limit, 50, 100);
  const offset = (safePage - 1) * safeLimit;
  const normalizedStatus = PARTICIPATION_FILTERS.has(status) ? status : "all";
  const params = [context.draw_id];
  const where = ["ap.active = true", "an.active = true"];
  const addParam = (value) => {
    params.push(value);
    return `$${params.length}`;
  };
  if (autopayNumberId) where.push(`an.id = ${addParam(autopayNumberId)}`);
  const whereSql = `WHERE ${where.join(" AND ")}`;
  const fromSql = `
    FROM public.autopay_numbers an
    JOIN public.autopay_profiles ap ON ap.id = an.autopay_id
    JOIN public.users u ON u.id = ap.user_id
    LEFT JOIN public.autopay_draw_captive_overrides draw_override
      ON draw_override.draw_id = $1
     AND draw_override.autopay_number_id = an.id
    LEFT JOIN public.autopay_draw_authorizations auth
      ON auth.draw_id = $1
     AND auth.user_id = ap.user_id
     AND auth.captive_number = an.n
     AND (auth.autopay_number_id = an.id OR auth.autopay_number_id IS NULL)
    LEFT JOIN LATERAL (
      SELECT authorization_source, admin_user_id, created_at
        FROM public.captive_preauth_authorization_events authorization_event
       WHERE authorization_event.authorization_id = auth.id
         AND authorization_event.new_status = 'authorized'
       ORDER BY authorization_event.created_at DESC
       LIMIT 1
    ) latest_authorization_event ON true
    LEFT JOIN public.numbers draw_number
      ON draw_number.draw_id = $1
     AND draw_number.n = an.n
    LEFT JOIN public.reservations reservation
      ON reservation.id = draw_number.reservation_id
    ${whereSql}`;

  const countResult = await query(`SELECT COUNT(*)::int AS total ${fromSql}`, params);
  const total = Number(countResult.rows?.[0]?.total || 0);
  const rows = await query(
    `SELECT an.id AS autopay_number_id,
            ap.user_id,
            u.name AS user_name,
            u.email AS user_email,
            an.n AS captive_number,
            ap.active AS profile_active,
            an.active AS number_active,
            COALESCE(draw_override.enabled, true) AS enabled_current_draw,
            draw_override.reason AS override_reason,
            auth.id AS authorization_id,
            auth.status AS authorization_status,
            auth.amount_cents AS authorization_amount_cents,
            auth.expires_at AS authorization_expires_at,
            auth.notification_status,
            auth.notification_error,
            latest_authorization_event.authorization_source,
            latest_authorization_event.admin_user_id AS authorized_by_admin_id,
            latest_authorization_event.created_at AS admin_authorized_at,
            draw_number.status AS draw_number_status,
            draw_number.reservation_id,
            reservation.status AS reservation_status,
            ${APPROVED_PARTICIPATION_SQL} AS payment_approved,
            EXISTS (
              SELECT 1
                FROM public.autopay_runs run_in_progress
               WHERE run_in_progress.draw_id = auth.draw_id
                 AND run_in_progress.user_id = auth.user_id
                 AND LOWER(COALESCE(run_in_progress.status, '')) IN ('attempt', 'reserved', 'billed', 'charged')
                 AND (
                   run_in_progress.provider_request->>'authorization_id' = auth.id::text
                   OR an.n = ANY(COALESCE(run_in_progress.tried_numbers, '{}'::smallint[]))
                 )
            ) AS payment_in_progress,
            (
              LOWER(COALESCE(draw_number.status, '')) = 'reserved'
              AND draw_number.reservation_id IS NOT NULL
              AND LOWER(COALESCE(reservation.status, '')) IN ('pending', 'active', 'reserved', '')
              AND (reservation.expires_at IS NULL OR reservation.expires_at > now())
              AND reservation.user_id = auth.user_id
              AND an.n::integer = ANY(reservation.numbers)
            ) AS reservation_valid,
            (
              LOWER(COALESCE(auth.status, '')) = 'failed'
              AND auth.expires_at > now()
              AND LOWER(COALESCE(draw_number.status, '')) = 'reserved'
              AND draw_number.reservation_id IS NOT NULL
              AND LOWER(COALESCE(reservation.status, '')) IN ('pending', 'active', 'reserved', '')
              AND reservation.user_id = auth.user_id
              AND an.n::integer = ANY(reservation.numbers)
              AND NOT ${APPROVED_PARTICIPATION_SQL}
            ) AS failed_retryable
       ${fromSql}
      ORDER BY an.n ASC, u.name ASC NULLS LAST
      LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
    [...params, autopayNumberId ? 1 : 100, autopayNumberId ? offset : 0]
  );

  const stats = await query(
    `SELECT
        COUNT(*) FILTER (WHERE COALESCE(draw_override.enabled, true) = false)::int AS disabled_count,
        COUNT(*) FILTER (
          WHERE COALESCE(draw_override.enabled, true) = true
            AND LOWER(COALESCE(auth.status, '')) = 'pending'
            AND auth.expires_at > now()
        )::int AS pending_authorizations
       FROM public.autopay_numbers an
       JOIN public.autopay_profiles ap ON ap.id = an.autopay_id
       LEFT JOIN public.autopay_draw_captive_overrides draw_override
         ON draw_override.draw_id = $1
        AND draw_override.autopay_number_id = an.id
       LEFT JOIN public.autopay_draw_authorizations auth
         ON auth.draw_id = $1
        AND auth.user_id = ap.user_id
        AND auth.captive_number = an.n
        AND (auth.autopay_number_id = an.id OR auth.autopay_number_id IS NULL)
      WHERE ap.active = true
        AND an.active = true`,
    [context.draw_id]
  );

  const mappedItems = (rows.rows || []).map((row) => {
      const enabledCurrentDraw = row.enabled_current_draw === true;
      const authorizationStatus = String(row.authorization_status || "").toLowerCase() || null;
      const authorizationSource = row.authorization_source || (
        ["authorized", "charged"].includes(authorizationStatus) || row.payment_approved === true
          ? "client"
          : null
      );
      let participationState = "no_authorization";
      if (!enabledCurrentDraw) participationState = "disabled";
      else if (row.payment_approved === true || authorizationStatus === "charged") {
        participationState = authorizationSource === "admin" ? "confirmed_admin" : "confirmed_client";
      }
      else if (row.payment_in_progress === true) participationState = "payment_processing";
      else if (authorizationStatus === "authorized") {
        participationState = authorizationSource === "admin" ? "confirmed_admin" : "confirmed_client";
      }
      else if (authorizationStatus === "pending") participationState = "pending";
      else if (authorizationStatus === "failed" && row.failed_retryable === true) participationState = "failed_retryable";
      else if (authorizationStatus === "failed") participationState = "failed";
      else if (authorizationStatus === "declined") participationState = "declined";
      else if (authorizationStatus === "expired") participationState = "expired";
      return {
        autopay_number_id: String(row.autopay_number_id),
        user_id: Number(row.user_id),
        user_name: row.user_name || null,
        user_email: row.user_email || null,
        captive_number: Number(row.captive_number),
        permanent_status: row.profile_active === true && row.number_active === true ? "active" : "inactive",
        enabled_current_draw: enabledCurrentDraw,
        override_reason: row.override_reason || null,
        participation_state: participationState,
        authorization_id: row.authorization_id ? String(row.authorization_id) : null,
        authorization_status: authorizationStatus,
        authorization_source: authorizationSource,
        authorized_by_admin_id: row.authorized_by_admin_id == null ? null : Number(row.authorized_by_admin_id),
        authorized_at: row.admin_authorized_at || null,
        reservation_id: row.reservation_id ? String(row.reservation_id) : null,
        reservation_status: row.reservation_status || null,
        draw_number_status: row.draw_number_status || null,
        notification_status: row.notification_status || null,
        notification_error: row.notification_error || null,
        authorization_amount_cents: row.authorization_amount_cents == null ? null : Number(row.authorization_amount_cents),
        authorization_expires_at: row.authorization_expires_at || null,
        payment_approved: row.payment_approved === true,
        payment_in_progress: row.payment_in_progress === true,
        reservation_valid: row.reservation_valid === true,
        payment_status: row.payment_approved === true || authorizationStatus === "charged"
          ? "paid"
          : row.payment_in_progress === true || authorizationStatus === "authorized"
            ? "processing"
            : authorizationStatus === "failed" ? "failed" : "pending",
        retryable: row.failed_retryable === true,
        can_admin_authorize:
          context.preauth_required === true &&
          enabledCurrentDraw &&
          (authorizationStatus === "pending" || (authorizationStatus === "failed" && row.failed_retryable === true)) &&
          row.payment_approved !== true &&
          row.payment_in_progress !== true &&
          row.reservation_valid === true &&
          Number(row.authorization_amount_cents) === Number(context.official_amount_cents) &&
          Boolean(row.authorization_expires_at) &&
          new Date(row.authorization_expires_at).getTime() > Date.now(),
      };
    });
  const basePayload = {
    ok: true,
    draw: {
      draw_id: context.draw_id,
      amount_cents: context.official_amount_cents,
      default_amount_cents: context.default_amount_cents,
      preauth_required: context.preauth_required,
    },
    page: safePage,
    limit: safeLimit,
    pending_authorizations: Number(stats.rows?.[0]?.pending_authorizations || 0),
    disabled_count: Number(stats.rows?.[0]?.disabled_count || 0),
  };
  if (autopayNumberId) {
    return { ...basePayload, total, items: mappedItems };
  }

  const groupsByUser = new Map();
  for (const item of mappedItems) {
    const key = `${context.draw_id}:${item.user_id}`;
    const current = groupsByUser.get(key) || {
      group_key: key,
      draw_id: context.draw_id,
      user_id: item.user_id,
      user_name: item.user_name,
      user_email: item.user_email,
      items: [],
    };
    current.items.push(item);
    groupsByUser.set(key, current);
  }
  let groups = [...groupsByUser.values()].map((groupItem) => {
    const groupItems = [...groupItem.items].sort((a, b) => a.captive_number - b.captive_number);
    const statuses = groupItems.map((item) => item.authorization_status);
    const chargedItems = groupItems.filter((item) => item.payment_approved || item.authorization_status === "charged");
    const remainingEligibleItems = groupItems.filter((item) => item.can_admin_authorize === true);
const blockedItems = groupItems.filter((item) => !chargedItems.includes(item) && !remainingEligibleItems.includes(item));
    const hardBlockedItems = blockedItems.filter((item) => !["declined", "expired"].includes(item.authorization_status));
    const financialItems = [...chargedItems, ...remainingEligibleItems].sort((a, b) => a.captive_number - b.captive_number);
    const allConfirmed = financialItems.length > 0 && chargedItems.length === financialItems.length;
    const allDisabled = groupItems.every((item) => item.enabled_current_draw !== true);
    const partiallyCharged = chargedItems.length > 0 && remainingEligibleItems.length > 0 && hardBlockedItems.length === 0;
    const canAdminAuthorize = context.preauth_required === true && chargedItems.length === 0 && remainingEligibleItems.length > 0 && hardBlockedItems.length === 0;
    const canAdminAuthorizeRemaining = context.preauth_required === true && partiallyCharged;
    const eligibleItems = remainingEligibleItems;
    const unitAmountCents = Number(context.official_amount_cents);
    const totalAmountCents = financialItems.reduce(
      (sum, item) => sum + Number(item.authorization_amount_cents || 0),
      0
    );
    let participationState = "review_required";
    if (allConfirmed) participationState = "confirmed";
    else if (allDisabled) participationState = "disabled";
    else if ((canAdminAuthorize || canAdminAuthorizeRemaining) && statuses.includes("failed")) participationState = "failed_retryable";
    else if (canAdminAuthorize) participationState = "pending";
    else if (groupItems.some((item) => item.payment_in_progress)) participationState = "payment_processing";
    return {
      ...groupItem,
      items: groupItems,
      authorization_id: eligibleItems[0]?.authorization_id || groupItems[0]?.authorization_id || null,
      authorization_ids: financialItems.map((item) => item.authorization_id).filter(Boolean),
      captive_numbers: financialItems.map((item) => item.captive_number),
      quantity: financialItems.length,
      charged_quantity: chargedItems.length,
      remaining_quantity: remainingEligibleItems.length,
      unit_amount_cents: unitAmountCents,
      total_amount_cents: canAdminAuthorizeRemaining ? remainingEligibleItems.reduce((sum, item) => sum + Number(item.authorization_amount_cents || 0), 0) : totalAmountCents,
      original_total_amount_cents: totalAmountCents,
      charged_amount_cents: chargedItems.reduce((sum, item) => sum + Number(item.authorization_amount_cents || 0), 0),
      remaining_amount_cents: remainingEligibleItems.reduce((sum, item) => sum + Number(item.authorization_amount_cents || 0), 0),
      charged_captive_numbers: chargedItems.map((item) => item.captive_number),
      remaining_captive_numbers: remainingEligibleItems.map((item) => item.captive_number),
      partially_charged: partiallyCharged,
      participation_state: participationState,
      can_admin_authorize: canAdminAuthorize,
      can_admin_authorize_remaining: canAdminAuthorizeRemaining,
      requires_review: !canAdminAuthorize && !canAdminAuthorizeRemaining && !allConfirmed && !allDisabled,
      payment_approved: allConfirmed,
      payment_in_progress: groupItems.some((item) => item.payment_in_progress),
      enabled_current_draw: groupItems.every((item) => item.enabled_current_draw === true),
    };
  });

  const normalizedSearch = String(search || "").trim().toLowerCase();
  if (normalizedSearch) {
    groups = groups.filter((groupItem) => (
      String(groupItem.user_name || "").toLowerCase().includes(normalizedSearch) ||
      String(groupItem.user_email || "").toLowerCase().includes(normalizedSearch) ||
      groupItem.captive_numbers.some((number) => String(number).includes(normalizedSearch))
    ));
  }
  if (normalizedStatus === "enabled") groups = groups.filter((item) => item.enabled_current_draw);
  if (normalizedStatus === "disabled") groups = groups.filter((item) => !item.enabled_current_draw);
  if (normalizedStatus === "pending") groups = groups.filter((item) => item.participation_state === "pending");
  if (normalizedStatus === "confirmed") groups = groups.filter((item) => item.participation_state === "confirmed");
  if (normalizedStatus === "failed") {
    groups = groups.filter((item) => ["failed_retryable", "review_required"].includes(item.participation_state));
  }
  groups.sort((a, b) => (
    String(a.user_name || "").localeCompare(String(b.user_name || ""), "pt-BR") ||
    a.user_id - b.user_id
  ));
  const groupedTotal = groups.length;
  const paginatedGroups = groups.slice(offset, offset + safeLimit);
  return { ...basePayload, total: groupedTotal, items: paginatedGroups };
}

router.get("/current-draw-participation", requireAuth, requireAdmin, async (req, res) => {
  try {
    const payload = await loadCurrentDrawParticipation({
      search: String(req.query?.search || "").trim(),
      status: String(req.query?.status || "all").trim().toLowerCase(),
      page: req.query?.page,
      limit: req.query?.limit,
    });
    return res.json(payload);
  } catch (error) {
    console.error(`${LOG_PREFIX} current_draw_participation_failed`, {
      admin_user_id: req.user?.id || null,
      message: error?.message || null,
      code: error?.code || null,
    });
    const migrationErrorHints = [
      "autopay_draw_captive_overrides",
      "captive_preauth_authorization_events",
      "draw_override",
      "authorization_event",
      "latest_authorization_event",
    ];
    const errorContext = [
      error?.message,
      error?.table,
      error?.column,
      error?.relation,
    ].filter(Boolean).join(" ").toLowerCase();
    const isRequiredMigrationError =
      ["42P01", "42703"].includes(error?.code) &&
      migrationErrorHints.some((hint) => errorContext.includes(hint));
    if (isRequiredMigrationError) {
      return res.status(503).json({
        ok: false,
        error: "database_migration_required",
        required_migrations: [
          "023_captive_admin_controls.sql",
          "024_captive_preauth_authorization_audit.sql",
        ],
      });
    }
    return res.status(500).json({ ok: false, error: "current_draw_participation_failed" });
  }
});

router.post("/current-draw-participation/:authorizationId/authorize", requireAuth, requireAdmin, async (req, res) => {
  try {
    const result = await authorizeCurrentDrawCaptivePreauthForAdmin({
      authorizationId: req.params.authorizationId,
      drawId: req.body?.draw_id,
      adminUserId: req.user?.id,
    });
    const refreshed = result.autopay_number_id
      ? await loadCurrentDrawParticipation({
          autopayNumberId: result.autopay_number_id,
          page: 1,
          limit: 1,
        })
      : null;
    if (!result.ok) {
      const status = result.code === "payment_provider_unavailable"
        ? 503
        : result.code === "payment_attempt_persist_failed"
          ? 500
          : ["captive_payment_profile_mismatch", "captive_group_changed", "group_changed", "group_requires_review", "group_already_partially_or_fully_charged", "authorization_amount_mismatch", "payment_result_unknown", "payment_in_progress", "payment_preflight_failed"].includes(result.code)
            ? 409
            : 402;
      return res.status(status).json({
        ...result,
        error: result.code || "payment_failed",
        message: adminAuthorizationMessage(result.code || "payment_failed"),
        item: refreshed?.items?.[0] || null,
      });
    }
    console.log(`${LOG_PREFIX} current_draw_participation_authorized`, {
      admin_user_id: req.user?.id || null,
      authorization_id: req.params.authorizationId,
      draw_id: Number(req.body?.draw_id),
      authorization_source: result.authorization_source,
      status: result.status,
      charged: result.charged === true,
      already_decided: result.already_decided === true,
    });
    return res.json({
      success: true,
      ...result,
      item: refreshed?.items?.[0] || null,
    });
  } catch (error) {
    const rawCode = error?.message || "admin_authorization_failed";
    const code = normalizeAdminAuthorizationErrorCode(rawCode);
    console.error(`${LOG_PREFIX} current_draw_participation_authorize_failed`, {
      admin_user_id: req.user?.id || null,
      authorization_id: req.params?.authorizationId || null,
      draw_id: req.body?.draw_id || null,
      message: rawCode,
      code: error?.code || null,
    });
    return res.status(error?.status || 500).json({
      ok: false,
      error: code,
      message: adminAuthorizationMessage(code),
      reason: error?.reason || null,
      retryable: false,
    });
  }
});

router.patch("/current-draw-participation/:autopayNumberId", requireAuth, requireAdmin, async (req, res) => {
  try {
    const result = await setCurrentDrawCaptiveParticipation({
      autopayNumberId: req.params.autopayNumberId,
      enabled: req.body?.enabled,
      reason: req.body?.reason,
      adminUserId: req.user?.id,
    });
    const refreshed = await loadCurrentDrawParticipation({
      autopayNumberId: req.params.autopayNumberId,
      page: 1,
      limit: 1,
    });
    return res.json({ ...result, item: refreshed.items?.[0] || null });
  } catch (error) {
    console.error(`${LOG_PREFIX} current_draw_participation_update_failed`, {
      admin_user_id: req.user?.id || null,
      autopay_number_id: req.params?.autopayNumberId || null,
      message: error?.message || null,
      code: error?.code || null,
    });
    return res.status(error?.status || 500).json({
      ok: false,
      error: error?.message || "current_draw_participation_update_failed",
    });
  }
});

router.get("/", requireAuth, requireAdmin, async (req, res) => {
  try {
    const page = toPositiveInt(req.query?.page, 1, 100000);
    const pageSize = toPositiveInt(req.query?.pageSize, 50, 100);
    const offset = (page - 1) * pageSize;
    const q = String(req.query?.q || "").trim();
    const status = ALLOWED_STATUS.has(String(req.query?.status || "todos"))
      ? String(req.query?.status || "todos")
      : "todos";
    const schema = await getAdminCaptivesSchema();
    const { numberColumns } = schema;
    const policy = buildPolicy();

    const where = [];
    const params = [];
    const addParam = (value) => {
      params.push(value);
      return `$${params.length}`;
    };

    if (q) {
      const qParam = addParam(`%${q}%`);
      where.push(`(
        u.name ILIKE ${qParam}
        OR u.email ILIKE ${qParam}
        OR u.phone ILIKE ${qParam}
        OR CAST(an.n AS text) ILIKE ${qParam}
      )`);
    }

    if (status === "ativos") where.push(`(ap.active = true AND ${numberColumns.active ? "an.active = true" : "true"})`);
    if (status === "pausados") where.push(`(ap.active = false OR ${numberColumns.active ? "an.active = false" : "false"})`);
    if (status === "sem_whatsapp") where.push(`COALESCE(lc.status, 'missing') NOT IN ('granted','active','opt_in','allowed','subscribed','accepted')`);
    if (status === "com_whatsapp") where.push(`COALESCE(lc.status, 'missing') IN ('granted','active','opt_in','allowed','subscribed','accepted')`);
    if (status === "sem_cartao") where.push(`NOT ${CARD_READY_SQL}`);
    if (status === "com_cartao") where.push(CARD_READY_SQL);

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const baseSql = buildCaptivesBaseSql({ ...schema, includeWhere: whereSql });

    const countResult = await query(`SELECT COUNT(*)::int AS total FROM (${baseSql}) captives_count`, params);
    const total = Number(countResult.rows?.[0]?.total || 0);

    const listParams = [...params, pageSize, offset];
    const listResult = await query(
      `${baseSql}
       ORDER BY captive_number ASC, user_name ASC NULLS LAST
       LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
      listParams
    );

    console.log(`${LOG_PREFIX} list`, {
      admin_user_id: req.user?.id || null,
      status,
      page,
      pageSize,
      total,
      has_query: Boolean(q),
    });

    return res.json({
      ok: true,
      page,
      pageSize,
      total,
      policy,
      items: (listResult.rows || []).map((row) => mapRow(row, policy)),
    });
  } catch (error) {
    console.error(`${LOG_PREFIX} list_failed`, {
      admin_user_id: req.user?.id || null,
      message: error?.message || null,
      code: error?.code || null,
    });
    return res.status(500).json({ ok: false, error: "admin_captives_list_failed" });
  }
});

router.patch("/:id/participation", requireAuth, requireAdmin, async (req, res) => {
  try {
    if (typeof req.body?.active !== "boolean") {
      return res.status(400).json({ ok: false, error: "invalid_active" });
    }

    const schema = await getAdminCaptivesSchema();
    const { numberColumns } = schema;
    if (!numberColumns.active) {
      console.warn(`${LOG_PREFIX} toggle_failed`, {
        admin_user_id: req.user?.id || null,
        reason: "missing_autopay_numbers_active_column",
      });
      return res.status(409).json({ ok: false, error: "migration_required" });
    }

    const id = String(req.params.id || "").trim();
    const updatedAtSet = numberColumns.updated_at ? ", updated_at = now()" : "";
    const updated = await query(
      `UPDATE public.autopay_numbers
          SET active = $2${updatedAtSet}
        WHERE id = $1
        RETURNING id`,
      [id, req.body.active]
    );

    if (!updated.rowCount) {
      console.warn(`${LOG_PREFIX} toggle_failed`, {
        admin_user_id: req.user?.id || null,
        reason: "not_found",
      });
      return res.status(404).json({ ok: false, error: "captive_not_found" });
    }

    const itemResult = await query(
      buildCaptivesBaseSql({
        ...schema,
        includeWhere: "WHERE an.id = $1",
      }),
      [id]
    );

    console.log(`${LOG_PREFIX} toggle_participation`, {
      admin_user_id: req.user?.id || null,
      active: req.body.active,
    });

    return res.json({ ok: true, item: mapRow(itemResult.rows[0]) });
  } catch (error) {
    console.error(`${LOG_PREFIX} toggle_failed`, {
      admin_user_id: req.user?.id || null,
      message: error?.message || null,
      code: error?.code || null,
    });
    return res.status(500).json({ ok: false, error: "admin_captives_toggle_failed" });
  }
});

router.patch("/:id/authorization-mode", requireAuth, requireAdmin, async (req, res) => {
  try {
    if (typeof req.body?.authorization_mode !== "boolean") {
      return res.status(400).json({ ok: false, error: "invalid_authorization_mode" });
    }

    const schema = await getAdminCaptivesSchema();
    const { profileColumns } = schema;
    if (!profileColumns.authorization_mode) {
      console.warn(`${LOG_PREFIX} update_authorization_mode_failed`, {
        admin_user_id: req.user?.id || null,
        reason: "missing_autopay_profiles_authorization_mode_column",
      });
      return res.status(409).json({ ok: false, error: "migration_required" });
    }

    const id = String(req.params.id || "").trim();
    const updatedAtSet = profileColumns.updated_at ? ", updated_at = now()" : "";
    const updated = await query(
      `UPDATE public.autopay_profiles ap
          SET authorization_mode = $2${updatedAtSet}
         FROM public.autopay_numbers an
        WHERE an.id = $1
          AND an.autopay_id = ap.id
        RETURNING ap.id`,
      [id, req.body.authorization_mode]
    );

    if (!updated.rowCount) {
      console.warn(`${LOG_PREFIX} update_authorization_mode_failed`, {
        admin_user_id: req.user?.id || null,
        reason: "not_found",
      });
      return res.status(404).json({ ok: false, error: "captive_not_found" });
    }

    const itemResult = await query(
      buildCaptivesBaseSql({
        ...schema,
        includeWhere: "WHERE an.id = $1",
      }),
      [id]
    );

    console.log(`${LOG_PREFIX} update_authorization_mode`, {
      admin_user_id: req.user?.id || null,
      authorization_mode: req.body.authorization_mode,
    });

    return res.json({ ok: true, item: mapRow(itemResult.rows[0]) });
  } catch (error) {
    console.error(`${LOG_PREFIX} update_authorization_mode_failed`, {
      admin_user_id: req.user?.id || null,
      message: error?.message || null,
      code: error?.code || null,
    });
    return res.status(500).json({ ok: false, error: "admin_captives_authorization_mode_failed" });
  }
});

router.patch("/:id/preauth-notifications", requireAuth, requireAdmin, async (req, res) => {
  try {
    if (typeof req.body?.preauth_notifications_enabled !== "boolean") {
      return res.status(400).json({ ok: false, error: "invalid_preauth_notifications_enabled" });
    }

    const schema = await getAdminCaptivesSchema();
    const { numberColumns } = schema;
    if (!numberColumns.preauth_notifications_enabled) {
      console.warn(`${LOG_PREFIX} update_preauth_notifications_failed`, {
        admin_user_id: req.user?.id || null,
        reason: "missing_autopay_numbers_preauth_notifications_enabled_column",
      });
      return res.status(409).json({ ok: false, error: "migration_required" });
    }

    const id = String(req.params.id || "").trim();
    const updated = await query(
      `UPDATE public.autopay_numbers
          SET preauth_notifications_enabled = $2
        WHERE id = $1
        RETURNING id`,
      [id, req.body.preauth_notifications_enabled]
    );

    if (!updated.rowCount) {
      console.warn(`${LOG_PREFIX} update_preauth_notifications_failed`, {
        admin_user_id: req.user?.id || null,
        captive_id: id || null,
        reason: "not_found",
      });
      return res.status(404).json({ ok: false, error: "captive_not_found" });
    }

    const itemResult = await query(
      buildCaptivesBaseSql({
        ...schema,
        includeWhere: "WHERE an.id = $1",
      }),
      [id]
    );

    console.log(`${LOG_PREFIX} update_preauth_notifications`, {
      admin_user_id: req.user?.id || null,
      captive_id: id,
      autopay_number_id: id,
      enabled: req.body.preauth_notifications_enabled,
    });

    const item = mapRow(itemResult.rows[0]);
    return res.json({
      ok: true,
      id: item.autopay_number_id,
      preauth_notifications_enabled: item.preauth_notifications_enabled,
      preauth_notifications_label: item.preauth_notifications_label,
      item,
    });
  } catch (error) {
    console.error(`${LOG_PREFIX} update_preauth_notifications_failed`, {
      admin_user_id: req.user?.id || null,
      message: error?.message || null,
      code: error?.code || null,
    });
    return res.status(500).json({ ok: false, error: "admin_captives_preauth_notifications_failed" });
  }
});

export default router;
