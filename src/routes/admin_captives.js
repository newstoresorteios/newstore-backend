import { Router } from "express";
import { query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";

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

function mapRow(row) {
  const profileActive = row.profile_active === true;
  const numberActive = row.number_active !== false;
  return {
    id: String(row.id),
    user_id: Number(row.user_id),
    user_name: row.user_name || null,
    user_email: row.user_email || null,
    user_phone_masked: maskPhone(row.user_phone),
    captive_number: Number(row.captive_number),
    captive_number_label: String(Number(row.captive_number)).padStart(2, "0"),
    participation_active: profileActive && numberActive,
    profile_active: profileActive,
    number_active: numberActive,
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
        AND column_name IN ('active', 'created_at', 'updated_at')`
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
  return {
    numberColumns: {
      active: columns.has("active"),
      created_at: columns.has("created_at"),
      updated_at: columns.has("updated_at"),
    },
    hasCommunicationConsents: tableNames.has("communication_consents"),
    hasAutopayRuns: tableNames.has("autopay_runs"),
    runColumns: {
      status: runColumns.has("status"),
      created_at: runColumns.has("created_at"),
      updated_at: runColumns.has("updated_at"),
    },
  };
}

function buildCaptivesBaseSql({
  numberColumns,
  hasCommunicationConsents = true,
  hasAutopayRuns = true,
  runColumns = { status: true, created_at: true, updated_at: true },
  includeWhere = "",
} = {}) {
  const numberActiveExpr = numberColumns.active ? "an.active" : "true";
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
      items: (listResult.rows || []).map(mapRow),
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

export default router;
