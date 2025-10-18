// backend/src/routes/analytics.js
// Monte no index.js:
//   import adminAnalyticsRouter from "./routes/analytics.js";
//   app.use("/api/admin/analytics", adminAnalyticsRouter);

import express from "express";
import { query as q, getPool } from "../db.js";

const router = express.Router();

/* -------------------------------------------------------------------------- */
/* Helpers                                                                    */
/* -------------------------------------------------------------------------- */

const toInt = (v, d = 0) => {
  const n = Number(v);
  return Number.isFinite(n) ? (n | 0) : d;
};
const toNum = (v, d = 0) => (Number.isFinite(+v) ? +v : d);
const toDate = (d) => (d ? new Date(d) : null);

// Padroniza saída de séries diárias (ISO curta)
const dayStr = (d) => new Date(d).toISOString().slice(0, 10);

/* -------------------------------------------------------------------------- */
/* Ping                                                                        */
/* -------------------------------------------------------------------------- */

router.get("/ping", (_req, res) => res.json({ ok: true }));

/* ============================================================================
 * 1) DRAW SUMMARY (por sorteio)
 * ============================================================================ */
router.get("/summary/:drawId", async (req, res, next) => {
  try {
    const drawId = Number(req.params.drawId);
    if (!Number.isFinite(drawId)) return res.status(400).json({ error: "drawId inválido" });

    const draw = (await q(
      `SELECT id, status, opened_at, closed_at, realized_at, product_name
         FROM public.draws WHERE id=$1`,
      [drawId]
    ))?.rows?.[0];
    if (!draw) return res.status(404).json({ error: "Sorteio não encontrado" });

    const sold = (await q(
      `SELECT SUM((status='sold')::int)      AS sold,
              SUM((status='reserved')::int)  AS reserved,
              SUM((status='available')::int) AS available
         FROM public.numbers WHERE draw_id=$1`,
      [drawId]
    ))?.rows?.[0] || { sold: 0, reserved: 0, available: 0 };

    const paid = (await q(
      `SELECT COALESCE(SUM(amount_cents),0) AS gmv_cents,
              COALESCE(AVG(amount_cents),0) AS avg_ticket_cents,
              COUNT(*) AS paid_orders,
              MAX(paid_at) AS last_paid_at
         FROM public.payments
        WHERE draw_id=$1 AND status='paid'`,
      [drawId]
    ))?.rows?.[0] || { gmv_cents: 0, avg_ticket_cents: 0, paid_orders: 0, last_paid_at: null };

    const expiredRes = (await q(
      `SELECT COUNT(*) AS expired_reservations
         FROM public.reservations WHERE draw_id=$1 AND status='expired'`,
      [drawId]
    ))?.rows?.[0] || { expired_reservations: 0 };

    const expiredPays = (await q(
      `SELECT COUNT(*) AS expired_payments
         FROM public.payments WHERE draw_id=$1 AND status='expired'`,
      [drawId]
    ))?.rows?.[0] || { expired_payments: 0 };

    const hourDist = (await q(
      `SELECT EXTRACT(HOUR FROM (paid_at AT TIME ZONE 'America/Sao_Paulo')) AS hour_br,
              COUNT(*) AS paid
         FROM public.payments
        WHERE status='paid' AND paid_at IS NOT NULL AND draw_id=$1
        GROUP BY 1 ORDER BY 1`,
      [drawId]
    ))?.rows ?? [];

    const numHeat = (await q(
      `SELECT n.n::int AS n, COUNT(*)::int AS sold_count
         FROM public.numbers n
         JOIN public.reservations r ON r.id=n.reservation_id AND r.status='captured'
         JOIN public.payments p     ON p.id=r.payment_id      AND p.status='paid'
        WHERE n.status='sold' AND n.draw_id=$1
        GROUP BY n.n
        ORDER BY n.n`,
      [drawId]
    ))?.rows ?? [];

    const soldCount = toInt(sold?.sold);
    const fill_rate = soldCount ? Number((soldCount / 100).toFixed(2)) : 0;

    let velocity_to_close_minutes = null;
    if (draw.opened_at && draw.closed_at) {
      velocity_to_close_minutes = Math.round(
        (new Date(draw.closed_at).getTime() - new Date(draw.opened_at).getTime()) / 60000
      );
    }

    let velocity_to_fill_minutes = null;
    if (soldCount === 100 && draw.opened_at && paid.last_paid_at) {
      velocity_to_fill_minutes = Math.round(
        (new Date(paid.last_paid_at).getTime() - new Date(draw.opened_at).getTime()) / 60000
      );
    }

    res.json({
      draw,
      funnel: {
        available: toInt(sold?.available),
        reserved: toInt(sold?.reserved),
        sold: soldCount,
      },
      paid: {
        gmv_cents: toInt(paid.gmv_cents),
        avg_ticket_cents: toInt(paid.avg_ticket_cents),
        paid_orders: toInt(paid.paid_orders),
      },
      expired: {
        reservations: toInt(expiredRes.expired_reservations),
        payments: toInt(expiredPays.expired_payments),
      },
      hourDist,
      numHeat,
      fill_rate,
      velocity_to_close_minutes,
      velocity_to_fill_minutes,
    });
  } catch (e) {
    next(e);
  }
});

/* ============================================================================
 * 1b) DRAW LIST SUMMARY (todos os sorteios)
 * ============================================================================ */
router.get("/draws-summary", async (_req, res, next) => {
  try {
    const { rows } = await q(
      `WITH sold_counts AS (
         SELECT draw_id, SUM((status='sold')::int) AS sold
           FROM public.numbers GROUP BY draw_id
       ),
       paid_gmv AS (
         SELECT draw_id,
                SUM(amount_cents) AS gmv_cents,
                AVG(amount_cents) AS avg_ticket_cents,
                COUNT(*)          AS paid_orders
           FROM public.payments
          WHERE status='paid'
          GROUP BY draw_id
       )
       SELECT d.id, d.status, d.opened_at, d.closed_at, d.realized_at, d.product_name,
              sc.sold,
              COALESCE(pg.gmv_cents,0)         AS gmv_cents,
              COALESCE(pg.avg_ticket_cents,0)  AS avg_ticket_cents,
              COALESCE(pg.paid_orders,0)       AS paid_orders,
              ROUND(COALESCE(sc.sold,0)/100.0,2) AS fill_rate
         FROM public.draws d
         LEFT JOIN sold_counts sc ON sc.draw_id=d.id
         LEFT JOIN paid_gmv     pg ON pg.draw_id=d.id
        ORDER BY d.id DESC`
    );
    res.json(rows);
  } catch (e) {
    next(e);
  }
});

/* ============================================================================
 * 2) FUNIL + VAZAMENTOS
 * ============================================================================ */
router.get("/funnel/:drawId", async (req, res, next) => {
  try {
    const drawId = Number(req.params.drawId);
    if (!Number.isFinite(drawId)) return res.status(400).json({ error: "drawId inválido" });

    const { rows } = await q(
      `SELECT
         SUM((status='available')::int) AS available,
         SUM((status='reserved')::int)  AS reserved,
         SUM((status='sold')::int)      AS sold
       FROM public.numbers
      WHERE draw_id=$1`,
      [drawId]
    );
    res.json(rows?.[0] || { available: 0, reserved: 0, sold: 0 });
  } catch (e) {
    next(e);
  }
});

router.get("/leaks/daily", async (req, res, next) => {
  try {
    const days = Math.min(Number(req.query.days) || 30, 365);
    const drawId = req.query.drawId ? Number(req.query.drawId) : null;

    const paramsR = [days];
    const paramsP = [days];
    let filterR = `WHERE status='expired' AND expires_at >= now() - ($1 || ' days')::interval`;
    let filterP = `WHERE status='expired' AND created_at >= now() - ($1 || ' days')::interval`;
    if (Number.isFinite(drawId)) {
      filterR += ` AND draw_id = $2`;
      filterP += ` AND draw_id = $2`;
      paramsR.push(drawId);
      paramsP.push(drawId);
    }

    const { rows: expired_reservations } = await q(
      `SELECT date_trunc('day', expires_at) AS day, COUNT(*)::int AS expired_reservations
         FROM public.reservations
       ${filterR}
        GROUP BY 1 ORDER BY 1 DESC`,
      paramsR
    );

    const { rows: expired_payments } = await q(
      `SELECT date_trunc('day', created_at) AS day, COUNT(*)::int AS expired_payments
         FROM public.payments
       ${filterP}
        GROUP BY 1 ORDER BY 1 DESC`,
      paramsP
    );

    res.json({ expired_reservations, expired_payments });
  } catch (e) {
    next(e);
  }
});

/* ============================================================================
 * 3) RFM
 * ============================================================================ */
router.get("/rfm", async (req, res, next) => {
  try {
    const limit = Math.min(Number(req.query.limit) || 200, 1000);
    const { rows } = await q(
      `WITH paid AS (
         SELECT user_id, SUM(amount_cents) AS m, COUNT(*) AS f, MAX(paid_at) AS last_paid
           FROM public.payments WHERE status='paid' GROUP BY user_id
       )
       SELECT u.id, u.name, u.email, u.phone,
              p.f::int AS freq,
              p.m::bigint AS monetary_cents,
              EXTRACT(EPOCH FROM (now() - p.last_paid))/86400.0 AS recency_days
         FROM paid p
         JOIN public.users u ON u.id=p.user_id
        ORDER BY p.m DESC
        LIMIT $1`,
      [limit]
    );

    const seg = (r, f) => {
      const rec = Number(r);
      const fr = Number(f);
      if (rec <= 7 && fr >= 3) return "Champions";
      if (fr >= 3 && rec <= 30) return "Leais";
      if (rec > 90) return "Em risco";
      if (rec >= 30 && rec <= 90) return "Quase perdidos";
      if (fr === 1 && rec <= 7) return "Alta oportunidade";
      return "Regulares";
    };

    res.json(rows.map((x) => ({ ...x, segment: seg(x.recency_days, x.freq) })));
  } catch (e) {
    next(e);
  }
});

/* ============================================================================
 * 4) COHORTS
 * ============================================================================ */
router.get("/cohorts", async (_req, res, next) => {
  try {
    const { rows } = await q(
      `WITH first_paid AS (
         SELECT user_id, MIN(paid_at) AS first_paid_at
           FROM public.payments
          WHERE status='paid'
          GROUP BY user_id
       ),
       cohort AS (
         SELECT user_id, date_trunc('month', first_paid_at) AS cohort_month
           FROM first_paid
       )
       SELECT c.cohort_month,
              date_trunc('month', p.paid_at) AS month,
              COUNT(DISTINCT p.user_id) AS active_buyers,
              SUM(p.amount_cents) AS gmv_cents
         FROM public.payments p
         JOIN cohort c ON c.user_id=p.user_id
        WHERE p.status='paid'
        GROUP BY 1,2
        ORDER BY 1 DESC, 2`
    );
    res.json(rows);
  } catch (e) {
    next(e);
  }
});

/* ============================================================================
 * 5) NÚMEROS
 * ============================================================================ */
router.get("/numbers/soldcount/:drawId", async (req, res, next) => {
  try {
    const drawId = Number(req.params.drawId);
    if (!Number.isFinite(drawId)) return res.status(400).json({ error: "drawId inválido" });

    const { rows } = await q(
      `SELECT n.n::int AS n, COUNT(*)::int AS sold_count
         FROM public.numbers n
         JOIN public.reservations r ON r.id=n.reservation_id AND r.status='captured'
         JOIN public.payments p     ON p.id=r.payment_id      AND p.status='paid'
        WHERE n.status='sold' AND n.draw_id=$1
        GROUP BY n.n
        ORDER BY n.n`,
      [drawId]
    );
    res.json(rows);
  } catch (e) {
    next(e);
  }
});

router.get("/numbers/favorites-by-user", async (_req, res, next) => {
  try {
    const { rows } = await q(
      `SELECT u.id AS user_id, u.name, x.n::int, COUNT(*)::int AS times_bought
         FROM public.payments p
         JOIN public.users u ON u.id=p.user_id
         JOIN LATERAL unnest(p.numbers) AS x(n) ON true
        WHERE p.status='paid'
        GROUP BY u.id, u.name, x.n
        ORDER BY times_bought DESC, u.id ASC, x.n ASC`
    );
    res.json(rows);
  } catch (e) {
    next(e);
  }
});

/* ============================================================================
 * 6) CUPONS
 * ============================================================================ */
router.get("/coupons/efficacy", async (_req, res, next) => {
  try {
    const { rows } = await q(
      `WITH enriched AS (
         SELECT p.*, u.coupon_code, u.coupon_value_cents, u.coupon_updated_at
           FROM public.payments p
           JOIN public.users u ON u.id=p.user_id
          WHERE p.status IN ('paid','expired','pending','processing')
       )
       SELECT coupon_code,
              COUNT(*) FILTER (WHERE status='paid')::float / NULLIF(COUNT(*),0) AS pay_rate,
              SUM(amount_cents) FILTER (WHERE status='paid') AS gmv_cents,
              AVG(amount_cents) FILTER (WHERE status='paid') AS avg_ticket_cents,
              AVG(coupon_value_cents) AS avg_coupon_cents
         FROM enriched
        GROUP BY coupon_code
        ORDER BY gmv_cents DESC NULLS LAST`
    );
    res.json(rows);
  } catch (e) {
    next(e);
  }
});

/* ============================================================================
 * 7) AUTOPAY
 * ============================================================================ */
router.get("/autopay/stats", async (_req, res, next) => {
  try {
    const { rows: daily } = await q(
      `SELECT date_trunc('day', created_at) AS day,
              COUNT(*)::int AS runs,
              SUM((status='ok')::int)::int AS ok_runs,
              COALESCE(SUM(amount_cents),0)::bigint AS gmv_cents
         FROM public.autopay_runs
        GROUP BY 1
        ORDER BY 1 DESC`
    );

    const { rows } = await q(
      `SELECT AVG( (COALESCE(array_length(tried_numbers,1),0)
                   - COALESCE(array_length(bought_numbers,1),0)) ) AS avg_missed
         FROM public.autopay_runs`
    );
    const avg = rows?.[0]?.avg_missed;
    res.json({ daily, avg_missed: avg !== null ? Number(avg) : null });
  } catch (e) {
    next(e);
  }
});

/* ============================================================================
 * 8) TEMPO & JANELAS
 * ============================================================================ */
router.get("/payments/hourly", async (req, res, next) => {
  try {
    const drawId = req.query.drawId ? Number(req.query.drawId) : null;
    const params = [];
    let filter = `WHERE status='paid' AND paid_at IS NOT NULL`;
    if (Number.isFinite(drawId)) {
      filter += ` AND draw_id=$1`;
      params.push(drawId);
    }
    const { rows } = await q(
      `SELECT EXTRACT(HOUR FROM (paid_at AT TIME ZONE 'America/Sao_Paulo')) AS hour_br,
              COUNT(*)::int AS paid
         FROM public.payments
       ${filter}
        GROUP BY 1 ORDER BY 1`,
      params
    );
    res.json(rows);
  } catch (e) {
    next(e);
  }
});

router.get("/payments/latency", async (req, res, next) => {
  try {
    const days = Math.min(Number(req.query.days) || 90, 365);
    const drawId = req.query.drawId ? Number(req.query.drawId) : null;

    const params = [days];
    let filter = `WHERE p.status='paid' AND p.paid_at >= now() - ($1 || ' days')::interval`;
    if (Number.isFinite(drawId)) {
      filter += ` AND p.draw_id=$2`;
      params.push(drawId);
    }

    const avg = (await q(
      `WITH link AS (
         SELECT p.id AS payment_id, p.paid_at, r.created_at AS reserved_at
           FROM public.payments p
           JOIN public.reservations r ON r.payment_id=p.id
         ${filter}
       )
       SELECT AVG(EXTRACT(EPOCH FROM (paid_at - reserved_at))/60.0) AS avg_minutes_to_pay
         FROM link`,
      params
    ))?.rows?.[0] || { avg_minutes_to_pay: null };

    const series = (await q(
      `WITH link AS (
         SELECT p.id AS payment_id, p.paid_at, r.created_at AS reserved_at
           FROM public.payments p
           JOIN public.reservations r ON r.payment_id=p.id
        ${filter}
       )
       SELECT date_trunc('week', paid_at) AS week,
              AVG(EXTRACT(EPOCH FROM (paid_at - reserved_at))/60.0) AS avg_minutes
         FROM link
        GROUP BY 1
        ORDER BY 1`,
      params
    ))?.rows ?? [];

    res.json({
      avg_minutes_to_pay: avg.avg_minutes_to_pay !== null ? Number(avg.avg_minutes_to_pay) : null,
      weekly: series,
    });
  } catch (e) {
    next(e);
  }
});

/* ============================================================================
 * SUPORTE — lista de sorteios
 * ============================================================================ */
router.get("/draws", async (_req, res, next) => {
  try {
    const { rows } = await q(
      `SELECT id, product_name, status, opened_at, closed_at
         FROM public.draws
        ORDER BY id DESC
        LIMIT 200`
    );
    res.json(rows);
  } catch (e) {
    next(e);
  }
});

/* ============================================================================
 * 0) KPIs GLOBAIS / OVERVIEW
 * ============================================================================ */

// Função reaproveitável para /overview e /stats
async function buildOverview(days = 30) {
  const totalsR = await q(
    `WITH paid AS (
       SELECT user_id, amount_cents, status
         FROM public.payments
     )
     SELECT
       COALESCE(SUM(CASE WHEN status='paid' THEN amount_cents END),0)         AS gmv_paid_cents,
       COUNT(*) FILTER (WHERE status='paid')                                  AS orders_paid,
       COALESCE(AVG(amount_cents) FILTER (WHERE status='paid'),0)             AS avg_ticket_paid_cents,
       COUNT(DISTINCT user_id) FILTER (WHERE status='paid')                   AS unique_buyers_paid,
       COALESCE(SUM(CASE WHEN status IN ('pending','processing')
                         THEN amount_cents END),0)                            AS gmv_intent_cents,
       COUNT(*) FILTER (WHERE status IN ('pending','processing'))             AS orders_intent,
       COALESCE(SUM(CASE WHEN status='expired' THEN amount_cents END),0)      AS gmv_expired_cents,
       COUNT(*) FILTER (WHERE status='expired')                               AS orders_expired,
       COALESCE(SUM(CASE WHEN status='cancelled' THEN amount_cents END),0)    AS gmv_cancelled_cents,
       COUNT(*) FILTER (WHERE status='cancelled')                             AS orders_cancelled
     FROM paid`
  );

  const avgOrdersR = await q(
    `WITH agg AS (
       SELECT user_id, COUNT(*) AS c
         FROM public.payments
        WHERE status='paid'
        GROUP BY user_id
     ) SELECT COALESCE(AVG(c),0) AS avg_orders FROM agg`
  );

  const quantR = await q(
    `SELECT
       PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY amount_cents) AS p25,
       PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY amount_cents) AS p50,
       PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY amount_cents) AS p75,
       PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY amount_cents) AS p90
     FROM public.payments
     WHERE status='paid'`
  );

  const series = (
    await q(
      `WITH base AS (
         SELECT date_trunc('day', created_at) AS day, status, amount_cents
           FROM public.payments
          WHERE created_at >= now() - ($1 || ' days')::interval
       )
       SELECT day,
              SUM(amount_cents) FILTER (WHERE status='paid')                      AS gmv_paid_cents,
              SUM(amount_cents) FILTER (WHERE status IN ('pending','processing')) AS gmv_intent_cents,
              SUM(amount_cents) FILTER (WHERE status='expired')                   AS gmv_expired_cents,
              COUNT(*)        FILTER (WHERE status='paid')                        AS orders_paid,
              COUNT(*)        FILTER (WHERE status IN ('pending','processing'))   AS orders_intent,
              COUNT(*)        FILTER (WHERE status='expired')                     AS orders_expired
         FROM base
        GROUP BY 1
        ORDER BY 1`,
      [days]
    )
  ).rows;

  const hourly = (
    await q(
      `SELECT EXTRACT(HOUR FROM (paid_at AT TIME ZONE 'America/Sao_Paulo')) AS hour_br,
              COUNT(*) AS paid
         FROM public.payments
        WHERE status='paid'
          AND paid_at >= now() - interval '90 days'
          AND paid_at IS NOT NULL
        GROUP BY 1 ORDER BY 1`
    )
  ).rows;

  const topBuyers = (
    await q(
      `SELECT u.id AS user_id, u.name, u.email,
              COUNT(*) AS orders, SUM(p.amount_cents) AS gmv_cents,
              AVG(p.amount_cents) AS avg_ticket_cents
         FROM public.payments p
         JOIN public.users u ON u.id=p.user_id
        WHERE p.status='paid'
        GROUP BY u.id, u.name, u.email
        ORDER BY gmv_cents DESC NULLS LAST
        LIMIT 20`
    )
  ).rows;

  const t = totalsR.rows?.[0] || {};
  const qn = quantR.rows?.[0] || { p25: 0, p50: 0, p75: 0, p90: 0 };

  return {
    totals: {
      gmv_paid_cents: toInt(t.gmv_paid_cents),
      orders_paid: toInt(t.orders_paid),
      avg_ticket_paid_cents: toInt(t.avg_ticket_paid_cents),
      unique_buyers_paid: toInt(t.unique_buyers_paid),
      avg_orders_per_buyer: toNum(avgOrdersR.rows?.[0]?.avg_orders, 0),
      p25_ticket_cents: toInt(qn.p25),
      p50_ticket_cents: toInt(qn.p50),
      p75_ticket_cents: toInt(qn.p75),
      p90_ticket_cents: toInt(qn.p90),
      gmv_intent_cents: toInt(t.gmv_intent_cents),
      orders_intent: toInt(t.orders_intent),
      gmv_expired_cents: toInt(t.gmv_expired_cents),
      orders_expired: toInt(t.orders_expired),
      gmv_cancelled_cents: toInt(t.gmv_cancelled_cents),
      orders_cancelled: toInt(t.orders_cancelled),
    },
    series,
    hourly,
    topBuyers,
  };
}

router.get("/overview", async (req, res, next) => {
  try {
    const days = Math.min(Number(req.query.days) || 30, 365);
    const data = await buildOverview(days);
    res.json(data);
  } catch (e) {
    next(e);
  }
});

// Alias simples sem fetch externo
router.get("/stats", async (req, res, next) => {
  try {
    const days = Math.min(Number(req.query.days) || 30, 365);
    const data = await buildOverview(days);
    res.json(data);
  } catch (e) {
    next(e);
  }
});

export default router;
