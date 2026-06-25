import express from "express";
import { query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";

const router = express.Router();

router.use(requireAuth, requireAdmin);

function toPositiveInt(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? Math.trunc(n) : fallback;
}

function clampPageSize(value) {
  return Math.min(100, Math.max(1, toPositiveInt(value, 20)));
}

function buildLogsWhere(params = {}) {
  const clauses = [];
  const values = [];

  function add(value) {
    values.push(value);
    return `$${values.length}`;
  }

  const status = String(params.status || "").trim();
  if (status) {
    clauses.push(`LOWER(d.status) = LOWER(${add(status)})`);
  }

  const eventKey = String(params.event_key || "").trim();
  if (eventKey) {
    clauses.push(`LOWER(d.event_key) = LOWER(${add(eventKey)})`);
  }

  const userId = String(params.user_id || "").trim();
  if (userId && /^\d+$/.test(userId)) {
    clauses.push(`d.user_id = ${add(Number(userId))}`);
  }

  const q = String(params.q || "").trim();
  if (q) {
    const pattern = `%${q}%`;
    const qParam = add(pattern);
    const search = [
      `COALESCE(u.name, '') ILIKE ${qParam}`,
      `COALESCE(u.email, '') ILIKE ${qParam}`,
      `COALESCE(d.title, '') ILIKE ${qParam}`,
      `COALESCE(d.body, '') ILIKE ${qParam}`,
      `COALESCE(d.event_key, '') ILIKE ${qParam}`,
    ];
    if (/^\d+$/.test(q)) {
      search.push(`d.user_id = ${add(Number(q))}`);
    }
    clauses.push(`(${search.join(" OR ")})`);
  }

  return {
    where: clauses.length ? `WHERE ${clauses.join(" AND ")}` : "",
    values,
  };
}

router.get("/logs", async (req, res) => {
  try {
    const page = toPositiveInt(req.query.page, 1);
    const pageSize = clampPageSize(req.query.pageSize);
    const offset = (page - 1) * pageSize;
    const { where, values } = buildLogsWhere(req.query);

    const countResult = await query(
      `SELECT COUNT(*)::int AS total
         FROM public.notification_push_dispatches d
         LEFT JOIN public.users u ON u.id = d.user_id
         LEFT JOIN public.push_subscriptions ps ON ps.id = d.subscription_id
       ${where}`,
      values
    );

    const dataValues = [...values, pageSize, offset];
    const rowsResult = await query(
      `SELECT
          d.id,
          d.user_id,
          u.name AS user_name,
          u.email AS user_email,
          d.subscription_id,
          d.event_key,
          d.category,
          d.title,
          d.body,
          d.url,
          d.status,
          d.error_message,
          d.sent_at,
          d.created_at,
          ps.device_label,
          ps.is_active AS subscription_active,
          ps.last_success_at,
          ps.last_error_at
         FROM public.notification_push_dispatches d
         LEFT JOIN public.users u ON u.id = d.user_id
         LEFT JOIN public.push_subscriptions ps ON ps.id = d.subscription_id
       ${where}
        ORDER BY d.created_at DESC
        LIMIT $${dataValues.length - 1}
       OFFSET $${dataValues.length}`,
      dataValues
    );

    return res.json({
      ok: true,
      page,
      pageSize,
      total: countResult.rows?.[0]?.total || 0,
      items: rowsResult.rows || [],
    });
  } catch (error) {
    console.error("[admin/push/logs] list error", {
      code: error?.code || null,
      message: error?.message || null,
    });
    return res.status(500).json({ ok: false, error: "push_logs_failed" });
  }
});

router.get("/summary", async (_req, res) => {
  try {
    const [dispatches, subscribers] = await Promise.all([
      query(
        `SELECT
            COUNT(*)::int AS total_dispatches,
            COUNT(*) FILTER (WHERE LOWER(status) = 'sent')::int AS sent,
            COUNT(*) FILTER (WHERE LOWER(status) = 'failed')::int AS failed,
            COUNT(*) FILTER (WHERE LOWER(status) = 'pending')::int AS pending,
            MAX(sent_at) FILTER (WHERE LOWER(status) = 'sent') AS last_sent_at,
            MAX(created_at) FILTER (WHERE LOWER(status) = 'failed') AS last_failed_at
           FROM public.notification_push_dispatches`
      ),
      query(
        `SELECT
            COUNT(DISTINCT user_id) FILTER (WHERE is_active = true)::int AS active_subscribers,
            COUNT(*) FILTER (WHERE is_active = true)::int AS active_devices
           FROM public.push_subscriptions`
      ),
    ]);

    return res.json({
      ok: true,
      total_dispatches: dispatches.rows?.[0]?.total_dispatches || 0,
      sent: dispatches.rows?.[0]?.sent || 0,
      failed: dispatches.rows?.[0]?.failed || 0,
      pending: dispatches.rows?.[0]?.pending || 0,
      active_subscribers: subscribers.rows?.[0]?.active_subscribers || 0,
      active_devices: subscribers.rows?.[0]?.active_devices || 0,
      last_sent_at: dispatches.rows?.[0]?.last_sent_at || null,
      last_failed_at: dispatches.rows?.[0]?.last_failed_at || null,
    });
  } catch (error) {
    console.error("[admin/push/summary] error", {
      code: error?.code || null,
      message: error?.message || null,
    });
    return res.status(500).json({ ok: false, error: "push_summary_failed" });
  }
});

export default router;
