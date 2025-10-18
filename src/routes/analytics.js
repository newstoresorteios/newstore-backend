// backend/src/routes/analytics.js
// Monte no index.js:
//   import adminAnalyticsRouter from "./routes/analytics.js";
//   app.use("/api/admin/analytics", adminAnalyticsRouter);

import express from "express";
import { query as q } from "../db.js"; // ✅ usa o pool já configurado (SSL, etc)

const router = express.Router();

// opcional: rota de ping
router.get("/ping", (_req, res) => res.json({ ok: true }));

/* ============================================================================
 * 1) DRAW SUMMARY (por sorteio) — GMV, fill-rate, ticket, funil, horários, heatmap
 * ============================================================================ */
router.get("/summary/:drawId", async (req, res) => {
  const drawId = Number(req.params.drawId);
  if (!Number.isFinite(drawId)) return res.status(400).json({ error: "drawId inválido" });

  try {
    const draw = (await q(
      `SELECT id, status, opened_at, closed_at, realized_at, product_name
       FROM draws WHERE id=$1`,
      [drawId]
    ))?.rows?.[0];
    if (!draw) return res.status(404).json({ error: "Sorteio não encontrado" });

    const sold = (await q(
      `SELECT SUM((status='sold')::int) AS sold,
              SUM((status='reserved')::int) AS reserved,
              SUM((status='available')::int) AS available
       FROM numbers WHERE draw_id=$1`,
      [drawId]
    ))?.rows?.[0] || { sold: 0, reserved: 0, available: 0 };

    const paid = (await q(
      `SELECT COALESCE(SUM(amount_cents),0) AS gmv_cents,
              COALESCE(AVG(amount_cents),0) AS avg_ticket_cents,
              COUNT(*) AS paid_orders,
              MAX(paid_at) AS last_paid_at
       FROM payments WHERE draw_id=$1 AND status='paid'`,
      [drawId]
    ))?.rows?.[0] || { gmv_cents: 0, avg_ticket_cents: 0, paid_orders: 0, last_paid_at: null };

    const expiredRes = (await q(
      `SELECT COUNT(*) AS expired_reservations
         FROM reservations WHERE draw_id=$1 AND status='expired'`,
      [drawId]
    ))?.rows?.[0] || { expired_reservations: 0 };

    const expiredPays = (await q(
      `SELECT COUNT(*) AS expired_payments
         FROM payments WHERE draw_id=$1 AND status='expired'`,
      [drawId]
    ))?.rows?.[0] || { expired_payments: 0 };

    const hourDist = (await q(
      `SELECT EXTRACT(HOUR FROM (paid_at AT TIME ZONE 'America/Sao_Paulo')) AS hour_br,
              COUNT(*) AS paid
         FROM payments
        WHERE status='paid' AND paid_at IS NOT NULL AND draw_id=$1
        GROUP BY 1 ORDER BY 1`,
      [drawId]
    ))?.rows ?? [];

    const numHeat = (await q(
      `SELECT n.n::int AS n, COUNT(*)::int AS sold_count
         FROM numbers n
         JOIN reservations r ON r.id=n.reservation_id AND r.status='captured'
         JOIN payments p ON p.id=r.payment_id AND p.status='paid'
        WHERE n.status='sold' AND n.draw_id=$1
        GROUP BY n.n
        ORDER BY n.n`,
      [drawId]
    ))?.rows ?? [];

    const soldCount = Number(sold?.sold || 0);
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
        available: Number(sold?.available || 0),
        reserved: Number(sold?.reserved || 0),
        sold: soldCount,
      },
      paid: {
        gmv_cents: Number(paid.gmv_cents || 0),
        avg_ticket_cents: Number(paid.avg_ticket_cents || 0),
        paid_orders: Number(paid.paid_orders || 0),
      },
      expired: {
        reservations: Number(expiredRes.expired_reservations || 0),
        payments: Number(expiredPays.expired_payments || 0),
      },
      hourDist,
      numHeat,
      fill_rate,
      velocity_to_close_minutes,
      velocity_to_fill_minutes,
    });
  } catch (e) {
    console.error("[analytics/summary]", e);
    res.status(500).json({ error: "Falha ao obter summary" });
  }
});

/* ============================================================================
 * 1b) DRAW LIST SUMMARY (todos os sorteios)
 * ============================================================================ */
router.get("/draws-summary", async (_req, res) => {
  try {
    const { rows } = await q(
      `WITH sold_counts AS (
         SELECT draw_id, SUM((status='sold')::int) AS sold
         FROM numbers GROUP BY draw_id
       ),
       paid_gmv AS (
         SELECT draw_id,
                SUM(amount_cents) AS gmv_cents,
                AVG(amount_cents) AS avg_ticket_cents,
                COUNT(*)          AS paid_orders
           FROM payments
          WHERE status='paid'
          GROUP BY draw_id
       )
       SELECT d.id, d.status, d.opened_at, d.closed_at, d.realized_at, d.product_name,
              sc.sold,
              COALESCE(pg.gmv_cents,0) AS gmv_cents,
              COALESCE(pg.avg_ticket_cents,0) AS avg_ticket_cents,
              COALESCE(pg.paid_orders,0) AS paid_orders,
              ROUND(COALESCE(sc.sold,0)/100.0,2) AS fill_rate
         FROM draws d
         LEFT JOIN sold_counts sc ON sc.draw_id=d.id
         LEFT JOIN paid_gmv     pg ON pg.draw_id=d.id
        ORDER BY d.id DESC`
    );
    res.json(rows);
  } catch (e) {
    console.error("[analytics/draws-summary]", e);
    res.status(500).json({ error: "Falha ao listar draws-summary" });
  }
});

/* ============================================================================
 * 2) FUNIL + VAZAMENTOS
 * ============================================================================ */
router.get("/funnel/:drawId", async (req, res) => {
  const drawId = Number(req.params.drawId);
  if (!Number.isFinite(drawId)) return res.status(400).json({ error: "drawId inválido" });
  try {
    const { rows } = await q(
      `SELECT
         SUM((status='available')::int) AS available,
         SUM((status='reserved')::int)  AS reserved,
         SUM((status='sold')::int)      AS sold
       FROM numbers
      WHERE draw_id=$1`,
      [drawId]
    );
    res.json(rows?.[0] || { available: 0, reserved: 0, sold: 0 });
  } catch (e) {
    console.error("[analytics/funnel]", e);
    res.status(500).json({ error: "Falha ao obter funil" });
  }
});

router.get("/leaks/daily", async (req, res) => {
  const days = Math.min(Number(req.query.days) || 30, 365);
  const drawId = req.query.drawId ? Number(req.query.drawId) : null;

  try {
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
         FROM reservations
       ${filterR}
        GROUP BY 1 ORDER BY 1 DESC`,
      paramsR
    );

    const { rows: expired_payments } = await q(
      `SELECT date_trunc('day', created_at) AS day, COUNT(*)::int AS expired_payments
         FROM payments
       ${filterP}
        GROUP BY 1 ORDER BY 1 DESC`,
      paramsP
    );

    res.json({ expired_reservations, expired_payments });
  } catch (e) {
    console.error("[analytics/leaks/daily]", e);
    res.status(500).json({ error: "Falha ao obter leaks/daily" });
  }
});

/* ============================================================================
 * 3) RFM
 * ============================================================================ */
router.get("/rfm", async (req, res) => {
  const limit = Math.min(Number(req.query.limit) || 200, 1000);
  try {
    const { rows } = await q(
      `WITH paid AS (
         SELECT user_id, SUM(amount_cents) AS m, COUNT(*) AS f, MAX(paid_at) AS last_paid
           FROM payments WHERE status='paid' GROUP BY user_id
       )
       SELECT u.id, u.name, u.email, u.phone,
              p.f::int AS freq,
              p.m::bigint AS monetary_cents,
              EXTRACT(EPOCH FROM (now() - p.last_paid))/86400.0 AS recency_days
         FROM paid p
         JOIN users u ON u.id=p.user_id
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

    res.json(rows.map(x => ({ ...x, segment: seg(x.recency_days, x.freq) })));
  } catch (e) {
    console.error("[analytics/rfm]", e);
    res.status(500).json({ error: "Falha ao obter RFM" });
  }
});

/* ============================================================================
 * 4) COHORTS
 * ============================================================================ */
router.get("/cohorts", async (_req, res) => {
  try {
    const { rows } = await q(
      `WITH first_paid AS (
         SELECT user_id, MIN(paid_at) AS first_paid_at
           FROM payments
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
         FROM payments p
         JOIN cohort c ON c.user_id=p.user_id
        WHERE p.status='paid'
        GROUP BY 1,2
        ORDER BY 1 DESC, 2`
    );
    res.json(rows);
  } catch (e) {
    console.error("[analytics/cohorts]", e);
    res.status(500).json({ error: "Falha ao obter cohorts" });
  }
});

/* ============================================================================
 * 5) NÚMEROS
 * ============================================================================ */
router.get("/numbers/soldcount/:drawId", async (req, res) => {
  const drawId = Number(req.params.drawId);
  if (!Number.isFinite(drawId)) return res.status(400).json({ error: "drawId inválido" });
  try {
    const { rows } = await q(
      `SELECT n.n::int AS n, COUNT(*)::int AS sold_count
         FROM numbers n
         JOIN reservations r ON r.id=n.reservation_id AND r.status='captured'
         JOIN payments p ON p.id=r.payment_id AND p.status='paid'
        WHERE n.status='sold' AND n.draw_id=$1
        GROUP BY n.n
        ORDER BY n.n`,
      [drawId]
    );
    res.json(rows);
  } catch (e) {
    console.error("[analytics/numbers/soldcount]", e);
    res.status(500).json({ error: "Falha ao obter soldcount" });
  }
});

router.get("/numbers/favorites-by-user", async (_req, res) => {
  try {
    const { rows } = await q(
      `SELECT u.id AS user_id, u.name, x.n::int, COUNT(*)::int AS times_bought
         FROM payments p
         JOIN users u ON u.id=p.user_id
         JOIN LATERAL unnest(p.numbers) AS x(n) ON true
        WHERE p.status='paid'
        GROUP BY u.id, u.name, x.n
        ORDER BY times_bought DESC, u.id ASC, x.n ASC`
    );
    res.json(rows);
  } catch (e) {
    console.error("[analytics/numbers/favorites-by-user]", e);
    res.status(500).json({ error: "Falha ao obter favorites-by-user" });
  }
});

/* ============================================================================
 * 6) CUPONS
 * ============================================================================ */
router.get("/coupons/efficacy", async (_req, res) => {
  try {
    const { rows } = await q(
      `WITH enriched AS (
         SELECT p.*, u.coupon_code, u.coupon_value_cents, u.coupon_updated_at
           FROM payments p
           JOIN users u ON u.id=p.user_id
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
    console.error("[analytics/coupons/efficacy]", e);
    res.status(500).json({ error: "Falha ao obter coupons/efficacy" });
  }
});

/* ============================================================================
 * 7) AUTOPAY
 * ============================================================================ */
router.get("/autopay/stats", async (_req, res) => {
  try {
    const { rows: daily } = await q(
      `SELECT date_trunc('day', created_at) AS day,
              COUNT(*)::int AS runs,
              SUM((status='ok')::int)::int AS ok_runs,
              COALESCE(SUM(amount_cents),0)::bigint AS gmv_cents
         FROM autopay_runs
        GROUP BY 1
        ORDER BY 1 DESC`
    );

    const { rows } = await q(
      `SELECT AVG( (COALESCE(array_length(tried_numbers,1),0)
                   - COALESCE(array_length(bought_numbers,1),0)) ) AS avg_missed
         FROM autopay_runs`
    );
    const avg = rows?.[0]?.avg_missed;
    res.json({ daily, avg_missed: avg !== null ? Number(avg) : null });
  } catch (e) {
    console.error("[analytics/autopay/stats]", e);
    res.status(500).json({ error: "Falha ao obter autopay/stats" });
  }
});

/* ============================================================================
 * 8) TEMPO & JANELAS
 * ============================================================================ */
router.get("/payments/hourly", async (req, res) => {
  const drawId = req.query.drawId ? Number(req.query.drawId) : null;
  try {
    const params = [];
    let filter = `WHERE status='paid' AND paid_at IS NOT NULL`;
    if (Number.isFinite(drawId)) {
      filter += ` AND draw_id=$1`;
      params.push(drawId);
    }
    const { rows } = await q(
      `SELECT EXTRACT(HOUR FROM (paid_at AT TIME ZONE 'America/Sao_Paulo')) AS hour_br,
              COUNT(*)::int AS paid
         FROM payments
       ${filter}
        GROUP BY 1 ORDER BY 1`,
      params
    );
    res.json(rows);
  } catch (e) {
    console.error("[analytics/payments/hourly]", e);
    res.status(500).json({ error: "Falha ao obter payments/hourly" });
  }
});

router.get("/payments/latency", async (req, res) => {
  const days = Math.min(Number(req.query.days) || 90, 365);
  const drawId = req.query.drawId ? Number(req.query.drawId) : null;
  try {
    const params = [days];
    let filter = `WHERE p.status='paid' AND p.paid_at >= now() - ($1 || ' days')::interval`;
    if (Number.isFinite(drawId)) {
      filter += ` AND p.draw_id=$2`;
      params.push(drawId);
    }

    const avg = (await q(
      `WITH link AS (
         SELECT p.id AS payment_id, p.paid_at, r.created_at AS reserved_at
           FROM payments p
           JOIN reservations r ON r.payment_id=p.id
         ${filter}
       )
       SELECT AVG(EXTRACT(EPOCH FROM (paid_at - reserved_at))/60.0) AS avg_minutes_to_pay
         FROM link`,
      params
    ))?.rows?.[0] || { avg_minutes_to_pay: null };

    const series = (await q(
      `WITH link AS (
         SELECT p.id AS payment_id, p.paid_at, r.created_at AS reserved_at
           FROM payments p
           JOIN reservations r ON r.payment_id=p.id
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
    console.error("[analytics/payments/latency]", e);
    res.status(500).json({ error: "Falha ao obter payments/latency" });
  }
});

/* ============================================================================
 * SUPORTE — lista de sorteios
 * ============================================================================ */
router.get("/draws", async (_req, res) => {
  try {
    const { rows } = await q(
      `SELECT id, product_name, status, opened_at, closed_at
         FROM draws
        ORDER BY id DESC
        LIMIT 200`
    );
    res.json(rows);
  } catch (e) {
    console.error("[analytics/draws]", e);
    res.status(500).json({ error: "Falha ao listar draws" });
  }
});

/* ============================================================================
 * 0) KPIs GLOBAIS / OVERVIEW
 * ============================================================================ */

/** KPIs principais + séries auxiliares (para o Overview) */
router.get("/kpis/overview", async (_req, res) => {
  try {
    // Totais pagos
    const totals = (await q(
      `WITH paid AS (
         SELECT user_id, amount_cents
         FROM payments
         WHERE status='paid'
       )
       SELECT
         COALESCE(SUM(amount_cents),0)::bigint                 AS total_gmv_cents,
         COUNT(*)::int                                         AS total_orders,
         COUNT(DISTINCT user_id)::int                          AS unique_buyers,
         COALESCE(AVG(amount_cents),0)::bigint                 AS avg_ticket_cents
       FROM paid`
    ))?.rows?.[0] || {};

    // Média de pedidos por cliente
    const avgOrdersPerBuyer = (await q(
      `WITH agg AS (
         SELECT user_id, COUNT(*) AS f
         FROM payments
         WHERE status='paid'
         GROUP BY user_id
       )
       SELECT COALESCE(AVG(f),0)::float AS avg_orders_per_buyer
       FROM agg`
    ))?.rows?.[0]?.avg_orders_per_buyer ?? 0;

    // Últimos 30 dias: GMV & pedidos/dia
    const daily30 = (await q(
      `SELECT date_trunc('day', paid_at) AS day,
              SUM(amount_cents)::bigint  AS gmv_cents,
              COUNT(*)::int              AS orders
         FROM payments
        WHERE status='paid' AND paid_at >= now() - interval '30 days'
        GROUP BY 1 ORDER BY 1`
    ))?.rows ?? [];

    // Distribuição por hora (BR)
    const hourly = (await q(
      `SELECT EXTRACT(HOUR FROM (paid_at AT TIME ZONE 'America/Sao_Paulo')) AS hour_br,
              COUNT(*)::int AS paid
         FROM payments
        WHERE status='paid' AND paid_at IS NOT NULL
        GROUP BY 1 ORDER BY 1`
    ))?.rows ?? [];

    // Top compradores (GMV)
    const topBuyers = (await q(
      `SELECT u.id, u.name, u.email,
              SUM(p.amount_cents)::bigint AS gmv_cents,
              COUNT(*)::int               AS orders
         FROM payments p
         JOIN users u ON u.id=p.user_id
        WHERE p.status='paid'
        GROUP BY u.id, u.name, u.email
        ORDER BY gmv_cents DESC
        LIMIT 20`
    ))?.rows ?? [];

    // Top sorteios por GMV (usa pagamentos)
    const topDraws = (await q(
      `SELECT d.id, d.product_name, d.status,
              COALESCE(SUM(p.amount_cents),0)::bigint AS gmv_cents,
              COUNT(p.*)::int                         AS paid_orders
         FROM draws d
         LEFT JOIN payments p ON p.draw_id=d.id AND p.status='paid'
        GROUP BY d.id, d.product_name, d.status
        ORDER BY gmv_cents DESC
        LIMIT 20`
    ))?.rows ?? [];

    // Quantis do ticket
    const quantiles = (await q(
      `SELECT
         percentile_disc(0.25) WITHIN GROUP (ORDER BY amount_cents)::bigint AS p25,
         percentile_disc(0.50) WITHIN GROUP (ORDER BY amount_cents)::bigint AS p50,
         percentile_disc(0.75) WITHIN GROUP (ORDER BY amount_cents)::bigint AS p75,
         percentile_disc(0.90) WITHIN GROUP (ORDER BY amount_cents)::bigint AS p90
       FROM payments
       WHERE status='paid'`
    ))?.rows?.[0] || { p25: 0, p50: 0, p75: 0, p90: 0 };

    res.json({
      totals: {
        total_gmv_cents: Number(totals.total_gmv_cents || 0),
        total_orders: Number(totals.total_orders || 0),
        unique_buyers: Number(totals.unique_buyers || 0),
        avg_ticket_cents: Number(totals.avg_ticket_cents || 0),
        avg_orders_per_buyer: Number(avgOrdersPerBuyer || 0)
      },
      daily30,
      hourly,
      topBuyers,
      topDraws,
      quantiles
    });
  } catch (e) {
    console.error("[analytics/kpis/overview]", e);
    res.status(500).json({ error: "Falha ao obter KPIs do overview" });
  }
});

/** Série diária genérica (últimos N dias) */
router.get("/sales/daily", async (req, res) => {
  const days = Math.min(Number(req.query.days) || 90, 365);
  try {
    const rows = (await q(
      `SELECT date_trunc('day', paid_at) AS day,
              SUM(amount_cents)::bigint  AS gmv_cents,
              COUNT(*)::int              AS orders
         FROM payments
        WHERE status='paid' AND paid_at >= now() - ($1 || ' days')::interval
        GROUP BY 1 ORDER BY 1`,
      [days]
    ))?.rows ?? [];
    res.json(rows);
  } catch (e) {
    console.error("[analytics/sales/daily]", e);
    res.status(500).json({ error: "Falha ao obter série diária" });
  }
});

/** Top compradores (paginável) */
router.get("/buyers/top", async (req, res) => {
  const limit = Math.min(Number(req.query.limit) || 50, 500);
  try {
    const rows = (await q(
      `SELECT u.id, u.name, u.email, u.phone,
              SUM(p.amount_cents)::bigint AS gmv_cents,
              COUNT(*)::int               AS orders
         FROM payments p
         JOIN users u ON u.id=p.user_id
        WHERE p.status='paid'
        GROUP BY u.id, u.name, u.email, u.phone
        ORDER BY gmv_cents DESC
        LIMIT $1`,
      [limit]
    ))?.rows ?? [];
    res.json(rows);
  } catch (e) {
    console.error("[analytics/buyers/top]", e);
    res.status(500).json({ error: "Falha ao obter top compradores" });
  }
});

/** Leaderboard de sorteios (usa sua CTE existente para fill-rate + GMV) */
router.get("/draws/leaderboard", async (req, res) => {
  const limit = Math.min(Number(req.query.limit) || 30, 200);
  try {
    const rows = (await q(
      `WITH sold_counts AS (
         SELECT draw_id, SUM((status='sold')::int) AS sold
         FROM numbers GROUP BY draw_id
       ),
       paid_gmv AS (
         SELECT draw_id,
                SUM(amount_cents) AS gmv_cents,
                AVG(amount_cents) AS avg_ticket_cents,
                COUNT(*)          AS paid_orders
         FROM payments WHERE status='paid' GROUP BY draw_id
       )
       SELECT d.id, d.product_name, d.status,
              COALESCE(sc.sold,0) AS sold,
              COALESCE(pg.gmv_cents,0) AS gmv_cents,
              COALESCE(pg.paid_orders,0) AS paid_orders,
              ROUND(COALESCE(sc.sold,0)/100.0,2) AS fill_rate
       FROM draws d
       LEFT JOIN sold_counts sc ON sc.draw_id=d.id
       LEFT JOIN paid_gmv    pg ON pg.draw_id=d.id
       ORDER BY gmv_cents DESC NULLS LAST, id DESC
       LIMIT $1`,
      [limit]
    ))?.rows ?? [];
    res.json(rows);
  } catch (e) {
    console.error("[analytics/draws/leaderboard]", e);
    res.status(500).json({ error: "Falha ao obter leaderboard de sorteios" });
  }
});

// --- OVERVIEW GERAL (todos os sorteios) -------------------------------------
router.get("/overview", async (req, res) => {
  const days = Math.min(Number(req.query.days) || 30, 365);

  try {
    // Totais por status
    const totalsRow = (await q(
      `SELECT
         COALESCE(SUM(CASE WHEN status='paid' THEN amount_cents END),0)         AS gmv_paid_cents,
         COUNT(*) FILTER (WHERE status='paid')                                  AS orders_paid,
         COALESCE(AVG(amount_cents) FILTER (WHERE status='paid'),0)             AS avg_ticket_paid_cents,
         COUNT(DISTINCT user_id) FILTER (WHERE status='paid')                   AS unique_buyers_paid,
         COALESCE(SUM(CASE WHEN status IN ('pending','processing') THEN amount_cents END),0) AS gmv_intent_cents,
         COUNT(*) FILTER (WHERE status IN ('pending','processing'))             AS orders_intent,
         COALESCE(SUM(CASE WHEN status='expired' THEN amount_cents END),0)      AS gmv_expired_cents,
         COUNT(*) FILTER (WHERE status='expired')                               AS orders_expired,
         COALESCE(SUM(CASE WHEN status='cancelled' THEN amount_cents END),0)    AS gmv_cancelled_cents,
         COUNT(*) FILTER (WHERE status='cancelled')                             AS orders_cancelled
       FROM payments`
    ))?.rows?.[0] || {};

    // média pedidos/cliente (paid)
    const avgOrdersPerBuyer = (await q(
      `WITH agg AS (
         SELECT user_id, COUNT(*) AS c
           FROM payments
          WHERE status='paid'
          GROUP BY user_id
       ) SELECT COALESCE(AVG(c),0) AS avg_orders FROM agg`
    ))?.rows?.[0]?.avg_orders || 0;

    // quantis de ticket (paid)
    const quant = (await q(
      `SELECT
         PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY amount_cents) AS p25,
         PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY amount_cents) AS p50,
         PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY amount_cents) AS p75,
         PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY amount_cents) AS p90
       FROM payments
       WHERE status='paid'`
    ))?.rows?.[0] || { p25: 0, p50: 0, p75: 0, p90: 0 };

    // séries últimas N days (paid/intent/expired)
    const series = (await q(
      `WITH base AS (
         SELECT date_trunc('day', created_at) AS day, status, amount_cents
           FROM payments
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
    ))?.rows ?? [];

    // pagos por hora (últimos 90 dias)
    const hourly = (await q(
      `SELECT EXTRACT(HOUR FROM (paid_at AT TIME ZONE 'America/Sao_Paulo')) AS hour_br,
              COUNT(*) AS paid
         FROM payments
        WHERE status='paid'
          AND paid_at >= now() - interval '90 days'
          AND paid_at IS NOT NULL
        GROUP BY 1 ORDER BY 1`
    ))?.rows ?? [];

    // top compradores (paid)
    const topBuyers = (await q(
      `SELECT u.id AS user_id, u.name, u.email,
              COUNT(*) AS orders, SUM(p.amount_cents) AS gmv_cents,
              AVG(p.amount_cents) AS avg_ticket_cents
         FROM payments p
         JOIN users u ON u.id=p.user_id
        WHERE p.status='paid'
        GROUP BY u.id, u.name, u.email
        ORDER BY gmv_cents DESC NULLS LAST
        LIMIT 20`
    ))?.rows ?? [];

    res.json({
      totals: {
        gmv_paid_cents: Number(totalsRow.gmv_paid_cents || 0),
        orders_paid: Number(totalsRow.orders_paid || 0),
        avg_ticket_paid_cents: Number(totalsRow.avg_ticket_paid_cents || 0),
        unique_buyers_paid: Number(totalsRow.unique_buyers_paid || 0),
        avg_orders_per_buyer: Number(avgOrdersPerBuyer || 0),
        p25_ticket_cents: Number(quant.p25 || 0),
        p50_ticket_cents: Number(quant.p50 || 0),
        p75_ticket_cents: Number(quant.p75 || 0),
        p90_ticket_cents: Number(quant.p90 || 0),
        gmv_intent_cents: Number(totalsRow.gmv_intent_cents || 0),
        orders_intent: Number(totalsRow.orders_intent || 0),
        gmv_expired_cents: Number(totalsRow.gmv_expired_cents || 0),
        orders_expired: Number(totalsRow.orders_expired || 0),
        gmv_cancelled_cents: Number(totalsRow.gmv_cancelled_cents || 0),
        orders_cancelled: Number(totalsRow.orders_cancelled || 0),
      },
      series,
      hourly,
      topBuyers
    });
  } catch (e) {
    console.error("[analytics/overview]", e);
    res.status(500).json({ error: "Falha ao obter overview" });
  }
});

// alias simples se seu front já chama /stats
router.get("/stats", async (req, res) => {
  try {
    const o = await (await fetch(req.protocol + "://" + req.get("host") + `/api/admin/analytics/overview${req.url.includes('?') ? req.url.slice(req.url.indexOf('?')) : ''}`)).json();
    res.json(o);
  } catch (e) {
    console.error("[analytics/stats alias]", e);
    res.status(500).json({ error: "Falha ao obter stats" });
  }
});

export default router;
