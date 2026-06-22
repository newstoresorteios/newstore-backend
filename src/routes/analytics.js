// backend/src/routes/analytics.js
// Monte no index.js:
//   import adminAnalyticsRouter from "./routes/analytics.js";
//   app.use("/api/admin/analytics", adminAnalyticsRouter);

import express from "express";
import { query as q } from "../db.js"; // ✅ usa o pool já configurado (SSL, etc)
import { requireAuth, requireAdmin } from "../middleware/auth.js";

const router = express.Router();
router.use(requireAuth, requireAdmin);

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
              SUM((status='available')::int) AS available,
              COUNT(*)::int AS total
       FROM numbers WHERE draw_id=$1`,
      [drawId]
    ))?.rows?.[0] || { sold: 0, reserved: 0, available: 0, total: 0 };

    const paid = (await q(
      `SELECT COALESCE(SUM(amount_cents),0) AS gmv_cents,
              COALESCE(AVG(amount_cents),0) AS avg_ticket_cents,
              COUNT(*) AS paid_orders,
              MAX(COALESCE(paid_at, created_at)) AS last_paid_at
       FROM payments WHERE draw_id=$1 AND lower(status) IN ('approved','paid','pago')`,
      [drawId]
    ))?.rows?.[0] || { gmv_cents: 0, avg_ticket_cents: 0, paid_orders: 0, last_paid_at: null };

    const expiredRes = (await q(
      `SELECT COUNT(*) AS expired_reservations
         FROM reservations WHERE draw_id=$1 AND lower(status)='expired'`,
      [drawId]
    ))?.rows?.[0] || { expired_reservations: 0 };

    const expiredPays = (await q(
      `SELECT COUNT(*) AS expired_payments
         FROM payments WHERE draw_id=$1 AND lower(status)='expired'`,
      [drawId]
    ))?.rows?.[0] || { expired_payments: 0 };

    const hourDist = (await q(
      `SELECT EXTRACT(HOUR FROM (COALESCE(paid_at, created_at) AT TIME ZONE 'America/Sao_Paulo')) AS hour_br,
              COUNT(*) AS paid
         FROM payments
        WHERE lower(status) IN ('approved','paid','pago')
          AND COALESCE(paid_at, created_at) IS NOT NULL
          AND draw_id=$1
        GROUP BY 1 ORDER BY 1`,
      [drawId]
    ))?.rows ?? [];

    const numHeat = (await q(
      `SELECT sold_number.n::int AS n, COUNT(*)::int AS sold_count
         FROM payments p
         CROSS JOIN LATERAL unnest(p.numbers) AS sold_number(n)
        WHERE p.draw_id=$1
          AND lower(p.status) IN ('approved','paid','pago')
        GROUP BY sold_number.n
        ORDER BY sold_number.n`,
      [drawId]
    ))?.rows ?? [];

    const soldCount = Number(sold?.sold || 0);
    const totalCount = Number(sold?.total || 0);
    const fill_rate = totalCount > 0 ? Number((soldCount / totalCount).toFixed(4)) : 0;

    let velocity_to_close_minutes = null;
    if (draw.opened_at && draw.closed_at) {
      velocity_to_close_minutes = Math.round(
        (new Date(draw.closed_at).getTime() - new Date(draw.opened_at).getTime()) / 60000
      );
    }

    let velocity_to_fill_minutes = null;
    if (totalCount > 0 && soldCount === totalCount && draw.opened_at && paid.last_paid_at) {
      velocity_to_fill_minutes = Math.round(
        (new Date(paid.last_paid_at).getTime() - new Date(draw.opened_at).getTime()) / 60000
      );
    }

    res.json({
      draw,
      funnel: {
        total: totalCount,
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
         SELECT draw_id,
                COUNT(*)::int AS total,
                SUM((status='sold')::int) AS sold
         FROM numbers GROUP BY draw_id
       ),
       paid_gmv AS (
         SELECT draw_id,
                 SUM(amount_cents) AS gmv_cents,
                 AVG(amount_cents) AS avg_ticket_cents,
                 COUNT(*) AS paid_orders,
                 COUNT(DISTINCT user_id)::int AS unique_buyers
           FROM payments
          WHERE lower(status) IN ('approved','paid','pago')
          GROUP BY draw_id
       )
       SELECT d.id, d.status, d.draw_type, d.opened_at, d.closed_at, d.realized_at, d.product_name,
              COALESCE(sc.total,0) AS total,
              sc.sold,
              COALESCE(pg.gmv_cents,0) AS gmv_cents,
              COALESCE(pg.avg_ticket_cents,0) AS avg_ticket_cents,
              COALESCE(pg.paid_orders,0) AS paid_orders,
              ROUND(COALESCE(sc.sold,0)::numeric/NULLIF(COALESCE(sc.total,0),0),4) AS fill_rate
         FROM draws d
         LEFT JOIN sold_counts sc ON sc.draw_id=d.id
         LEFT JOIN paid_gmv     pg ON pg.draw_id=d.id
        ORDER BY d.id DESC`
    );
    res.json(rows.map((row) => {
      const fillRate = Number(row.fill_rate || 0);
      return {
        ...row,
        id: Number(row.id),
        total: Number(row.total || 0),
        numbers_count: Number(row.total || 0),
        sold: Number(row.sold || 0),
        sold_numbers: Number(row.sold || 0),
        gmv_cents: Number(row.gmv_cents || 0),
        avg_ticket_cents: Number(row.avg_ticket_cents || 0),
        average_ticket_cents: Number(row.avg_ticket_cents || 0),
        paid_orders: Number(row.paid_orders || 0),
        fill_rate: fillRate,
        progress_percent: Number((fillRate * 100).toFixed(2)),
      };
    }));
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
         COUNT(*)::int                    AS total,
         SUM((status='available')::int) AS available,
         SUM((status='reserved')::int)  AS reserved,
         SUM((status='sold')::int)      AS sold
       FROM numbers
      WHERE draw_id=$1`,
      [drawId]
    );
    res.json(rows?.[0] || { total: 0, available: 0, reserved: 0, sold: 0 });
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
    let filterR = `WHERE lower(status)='expired' AND expires_at >= now() - ($1 || ' days')::interval`;
    let filterP = `WHERE lower(status)='expired' AND created_at >= now() - ($1 || ' days')::interval`;
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
         SELECT user_id, SUM(amount_cents) AS m, COUNT(*) AS f, MAX(COALESCE(paid_at, created_at)) AS last_paid
           FROM payments WHERE lower(status) IN ('approved','paid','pago') GROUP BY user_id
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
         SELECT user_id, MIN(COALESCE(paid_at, created_at)) AS first_paid_at
           FROM payments
          WHERE lower(status) IN ('approved','paid','pago')
          GROUP BY user_id
       ),
       cohort AS (
         SELECT user_id, date_trunc('month', first_paid_at) AS cohort_month
           FROM first_paid
       )
       SELECT c.cohort_month,
              date_trunc('month', COALESCE(p.paid_at, p.created_at)) AS month,
              COUNT(DISTINCT p.user_id) AS active_buyers,
              SUM(p.amount_cents) AS gmv_cents
         FROM payments p
         JOIN cohort c ON c.user_id=p.user_id
        WHERE lower(p.status) IN ('approved','paid','pago')
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
      `SELECT sold_number.n::int AS n, COUNT(*)::int AS sold_count
         FROM payments p
         CROSS JOIN LATERAL unnest(p.numbers) AS sold_number(n)
        WHERE p.draw_id=$1
          AND lower(p.status) IN ('approved','paid','pago')
        GROUP BY sold_number.n
        ORDER BY sold_number.n`,
      [drawId]
    );
    res.json(rows.map((row) => ({
      ...row,
      n: Number(row.n),
      number: Number(row.n),
      sold_count: Number(row.sold_count || 0),
      frequency: Number(row.sold_count || 0),
    })));
  } catch (e) {
    console.error("[analytics/numbers/soldcount]", e);
    res.status(500).json({ error: "Falha ao obter soldcount" });
  }
});

router.get("/numbers/favorites-by-user", async (_req, res) => {
  try {
    const { rows } = await q(
      `SELECT u.id AS user_id,
              u.name,
              u.email,
              x.n::int,
              COUNT(*)::int AS times_bought,
              MAX(COALESCE(p.paid_at, p.created_at)) AS last_payment_at
         FROM payments p
         JOIN users u ON u.id=p.user_id
         JOIN LATERAL unnest(p.numbers) AS x(n) ON true
        WHERE lower(p.status) IN ('approved','paid','pago')
        GROUP BY u.id, u.name, u.email, x.n
        ORDER BY times_bought DESC, u.id ASC, x.n ASC`
    );
    res.json(rows.map((row) => ({
      ...row,
      user_id: Number(row.user_id),
      n: Number(row.n),
      number: Number(row.n),
      times_bought: Number(row.times_bought || 0),
      frequency: Number(row.times_bought || 0),
      numbers_count: Number(row.times_bought || 0),
    })));
  } catch (e) {
    console.error("[analytics/numbers/favorites-by-user]", e);
    res.status(500).json({ error: "Falha ao obter favorites-by-user" });
  }
});

router.get("/numbers/winning-frequency", async (_req, res) => {
  try {
    const { rows } = await q(
      `WITH winners AS (
         SELECT id,
                winner_number::int AS number,
                COALESCE(realized_at, closed_at, created_at) AS won_at
           FROM public.draws
          WHERE winner_number IS NOT NULL
       )
       SELECT number,
              COUNT(*)::int AS wins,
              ARRAY_AGG(id ORDER BY id)::int[] AS draws,
              (ARRAY_AGG(id ORDER BY won_at DESC NULLS LAST, id DESC))[1]::int AS last_draw_id,
              MAX(won_at) AS last_won_at
         FROM winners
        GROUP BY number
        ORDER BY wins DESC, last_won_at DESC NULLS LAST`
    );

    res.json(rows.map((row) => ({
      ...row,
      number: Number(row.number),
      wins: Number(row.wins || 0),
      frequency: Number(row.wins || 0),
      draws: Array.isArray(row.draws) ? row.draws.map(Number) : [],
      draws_count: Number(row.wins || 0),
      last_draw_id: row.last_draw_id === null ? null : Number(row.last_draw_id),
    })));
  } catch (e) {
    console.error("[analytics/numbers/winning-frequency]", e);
    res.status(500).json({ error: "Falha ao obter winning-frequency" });
  }
});

/* ============================================================================
 * 6) CUPONS
 * ============================================================================ */
router.get("/coupons/efficacy", async (_req, res) => {
  try {
    const { rows } = await q(
      `SELECT u.id AS user_id,
              COALESCE(NULLIF(u.name, ''), '-') AS name,
              COALESCE(NULLIF(u.email, ''), '-') AS email,
              u.coupon_code,
              COALESCE(sync.tray_coupon_id, u.tray_coupon_id) AS tray_coupon_id,
              sync.tray_sync_status,
              COUNT(p.id)::int AS total_orders,
              COUNT(p.id) FILTER (
                WHERE lower(p.status) IN ('approved','paid','pago')
              )::int AS paid_orders,
              COALESCE(
                COUNT(p.id) FILTER (
                  WHERE lower(p.status) IN ('approved','paid','pago')
                )::float / NULLIF(COUNT(p.id),0),
                0
              ) AS pay_rate,
              COALESCE(SUM(p.amount_cents) FILTER (
                WHERE lower(p.status) IN ('approved','paid','pago')
              ),0)::bigint AS gmv_cents,
              COALESCE(AVG(p.amount_cents) FILTER (
                WHERE lower(p.status) IN ('approved','paid','pago')
              ),0) AS avg_ticket_cents,
              u.coupon_value_cents,
              u.coupon_value_cents AS avg_coupon_cents,
              u.coupon_updated_at,
              MAX(COALESCE(p.paid_at, p.created_at)) FILTER (
                WHERE lower(p.status) IN ('approved','paid','pago')
              ) AS last_payment_at
         FROM public.users u
         LEFT JOIN public.payments p
           ON p.user_id=u.id
          AND lower(p.status) IN ('approved','paid','pago','expired','pending','processing')
         LEFT JOIN LATERAL (
           SELECT c.tray_coupon_id, c.tray_sync_status
             FROM public.coupon_tray_sync c
            WHERE c.user_id=u.id
            ORDER BY c.updated_at DESC NULLS LAST, c.created_at DESC NULLS LAST
            LIMIT 1
         ) sync ON true
        WHERE u.coupon_code IS NOT NULL
           OR u.tray_coupon_id IS NOT NULL
           OR sync.tray_coupon_id IS NOT NULL
        GROUP BY u.id, u.name, u.email, u.coupon_code,
                 u.tray_coupon_id, sync.tray_coupon_id, sync.tray_sync_status,
                 u.coupon_value_cents, u.coupon_updated_at
        ORDER BY gmv_cents DESC NULLS LAST`
    );
    res.json(rows.map((row) => ({
      ...row,
      user_id: Number(row.user_id),
      total_orders: Number(row.total_orders || 0),
      paid_orders: Number(row.paid_orders || 0),
      pay_rate: Number(row.pay_rate || 0),
      gmv_cents: Number(row.gmv_cents || 0),
      avg_ticket_cents: Number(row.avg_ticket_cents || 0),
      average_ticket_cents: Number(row.avg_ticket_cents || 0),
      coupon_value_cents: Number(row.coupon_value_cents || 0),
      avg_coupon_cents: Number(row.avg_coupon_cents || 0),
    })));
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
              SUM((lower(status) IN ('ok','charged_ok'))::int)::int AS ok_runs,
              COALESCE(SUM(amount_cents) FILTER (WHERE lower(status) IN ('ok','charged_ok')),0)::bigint AS gmv_cents
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
    let filter = `WHERE lower(status) IN ('approved','paid','pago') AND COALESCE(paid_at, created_at) IS NOT NULL`;
    if (Number.isFinite(drawId)) {
      filter += ` AND draw_id=$1`;
      params.push(drawId);
    }
    const { rows } = await q(
      `SELECT EXTRACT(HOUR FROM (COALESCE(paid_at, created_at) AT TIME ZONE 'America/Sao_Paulo')) AS hour_br,
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
    let filter = `WHERE lower(p.status) IN ('approved','paid','pago') AND COALESCE(p.paid_at, p.created_at) >= now() - ($1 || ' days')::interval`;
    if (Number.isFinite(drawId)) {
      filter += ` AND p.draw_id=$2`;
      params.push(drawId);
    }

    const avg = (await q(
      `WITH link AS (
         SELECT p.id AS payment_id, COALESCE(p.paid_at, p.created_at) AS paid_at, r.created_at AS reserved_at
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
         SELECT p.id AS payment_id, COALESCE(p.paid_at, p.created_at) AS paid_at, r.created_at AS reserved_at
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
      `SELECT id, product_name, status, draw_type, opened_at, closed_at
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
         SELECT user_id, amount_cents, COALESCE(paid_at, created_at) AS financial_at
         FROM payments
         WHERE lower(status) IN ('approved','paid','pago')
       )
       SELECT
         COALESCE(SUM(amount_cents),0)::bigint                 AS total_gmv_cents,
         COALESCE(SUM(amount_cents) FILTER (
           WHERE financial_at >= now() - interval '30 days'
         ),0)::bigint                                          AS gmv_30d_cents,
         COALESCE(SUM(amount_cents) FILTER (
           WHERE financial_at >= date_trunc('month', now())
         ),0)::bigint                                          AS gmv_current_month_cents,
         COALESCE(SUM(amount_cents) FILTER (
           WHERE financial_at >= date_trunc('year', now())
         ),0)::bigint                                          AS gmv_current_year_cents,
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
         WHERE lower(status) IN ('approved','paid','pago')
         GROUP BY user_id
       )
       SELECT COALESCE(AVG(f),0)::float AS avg_orders_per_buyer
       FROM agg`
    ))?.rows?.[0]?.avg_orders_per_buyer ?? 0;

    // Últimos 30 dias: GMV & pedidos/dia
    const daily30 = (await q(
      `SELECT date_trunc('day', COALESCE(paid_at, created_at)) AS day,
              SUM(amount_cents)::bigint  AS gmv_cents,
              COUNT(*)::int              AS orders
         FROM payments
        WHERE lower(status) IN ('approved','paid','pago')
          AND COALESCE(paid_at, created_at) >= now() - interval '30 days'
        GROUP BY 1 ORDER BY 1`
    ))?.rows ?? [];

    // Distribuição por hora (BR)
    const hourly = (await q(
      `SELECT EXTRACT(HOUR FROM (COALESCE(paid_at, created_at) AT TIME ZONE 'America/Sao_Paulo')) AS hour_br,
              COUNT(*)::int AS paid
         FROM payments
        WHERE lower(status) IN ('approved','paid','pago')
          AND COALESCE(paid_at, created_at) IS NOT NULL
        GROUP BY 1 ORDER BY 1`
    ))?.rows ?? [];

    // Top compradores (GMV)
    const topBuyers = (await q(
      `SELECT u.id, u.name, u.email,
              SUM(p.amount_cents)::bigint AS gmv_cents,
              COUNT(*)::int AS orders,
              COUNT(*)::int AS paid_orders,
              COALESCE(AVG(p.amount_cents),0) AS average_ticket_cents,
              COALESCE(SUM(cardinality(p.numbers)),0)::int AS numbers_count,
              MAX(COALESCE(p.paid_at, p.created_at)) AS last_payment_at
         FROM payments p
         JOIN users u ON u.id=p.user_id
        WHERE lower(p.status) IN ('approved','paid','pago')
        GROUP BY u.id, u.name, u.email
        ORDER BY gmv_cents DESC
        LIMIT 20`
    ))?.rows ?? [];

    // Top sorteios por GMV (usa pagamentos)
    const topDraws = (await q(
      `SELECT d.id, d.product_name, d.status, d.draw_type,
              COALESCE(SUM(p.amount_cents),0)::bigint AS gmv_cents,
              COUNT(p.*)::int AS paid_orders,
              COUNT(DISTINCT p.user_id)::int AS unique_buyers,
              COALESCE(AVG(p.amount_cents),0) AS average_ticket_cents,
              (SELECT COUNT(*)::int FROM numbers n WHERE n.draw_id=d.id) AS numbers_count
         FROM draws d
         LEFT JOIN payments p ON p.draw_id=d.id AND lower(p.status) IN ('approved','paid','pago')
        GROUP BY d.id, d.product_name, d.status, d.draw_type
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
       WHERE lower(status) IN ('approved','paid','pago')`
    ))?.rows?.[0] || { p25: 0, p50: 0, p75: 0, p90: 0 };

    res.json({
      totals: {
        total_gmv_cents: Number(totals.total_gmv_cents || 0),
        gmv_all_time_cents: Number(totals.total_gmv_cents || 0),
        gmv_30d_cents: Number(totals.gmv_30d_cents || 0),
        gmv_current_month_cents: Number(totals.gmv_current_month_cents || 0),
        gmv_current_year_cents: Number(totals.gmv_current_year_cents || 0),
        total_orders: Number(totals.total_orders || 0),
        unique_buyers: Number(totals.unique_buyers || 0),
        avg_ticket_cents: Number(totals.avg_ticket_cents || 0),
        avg_orders_per_buyer: Number(avgOrdersPerBuyer || 0)
      },
      daily30: daily30.map((row) => ({
        ...row,
        gmv_cents: Number(row.gmv_cents || 0),
        orders: Number(row.orders || 0),
      })),
      hourly,
      topBuyers: topBuyers.map((row) => ({
        ...row,
        id: Number(row.id),
        gmv_cents: Number(row.gmv_cents || 0),
        orders: Number(row.orders || 0),
        paid_orders: Number(row.paid_orders || 0),
        average_ticket_cents: Number(row.average_ticket_cents || 0),
        numbers_count: Number(row.numbers_count || 0),
      })),
      topDraws: topDraws.map((row) => ({
        ...row,
        id: Number(row.id),
        gmv_cents: Number(row.gmv_cents || 0),
        paid_orders: Number(row.paid_orders || 0),
        unique_buyers: Number(row.unique_buyers || 0),
        average_ticket_cents: Number(row.average_ticket_cents || 0),
        numbers_count: Number(row.numbers_count || 0),
      })),
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
      `SELECT date_trunc('day', COALESCE(paid_at, created_at)) AS day,
              SUM(amount_cents)::bigint  AS gmv_cents,
              COUNT(*)::int              AS orders
         FROM payments
        WHERE lower(status) IN ('approved','paid','pago')
          AND COALESCE(paid_at, created_at) >= now() - ($1 || ' days')::interval
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
              COUNT(*)::int AS orders,
              COUNT(*)::int AS paid_orders,
              COALESCE(AVG(p.amount_cents),0) AS average_ticket_cents,
              COALESCE(SUM(cardinality(p.numbers)),0)::int AS numbers_count,
              MAX(COALESCE(p.paid_at, p.created_at)) AS last_payment_at
         FROM payments p
         JOIN users u ON u.id=p.user_id
        WHERE lower(p.status) IN ('approved','paid','pago')
        GROUP BY u.id, u.name, u.email, u.phone
        ORDER BY gmv_cents DESC
        LIMIT $1`,
      [limit]
    ))?.rows ?? [];
    res.json(rows.map((row) => ({
      ...row,
      id: Number(row.id),
      gmv_cents: Number(row.gmv_cents || 0),
      orders: Number(row.orders || 0),
      paid_orders: Number(row.paid_orders || 0),
      average_ticket_cents: Number(row.average_ticket_cents || 0),
      numbers_count: Number(row.numbers_count || 0),
    })));
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
         SELECT draw_id,
                COUNT(*)::int AS total,
                SUM((status='sold')::int) AS sold
         FROM numbers GROUP BY draw_id
       ),
       paid_gmv AS (
         SELECT draw_id,
                SUM(amount_cents)::bigint AS gmv_cents,
                AVG(amount_cents) AS avg_ticket_cents,
                COUNT(*)::int AS paid_orders,
                COUNT(DISTINCT user_id)::int AS unique_buyers
         FROM payments WHERE lower(status) IN ('approved','paid','pago') GROUP BY draw_id
       )
       SELECT d.id, d.product_name, d.status, d.draw_type,
              COALESCE(sc.total,0) AS total,
              COALESCE(sc.sold,0) AS sold,
              COALESCE(pg.gmv_cents,0) AS gmv_cents,
              COALESCE(pg.avg_ticket_cents,0) AS avg_ticket_cents,
              COALESCE(pg.paid_orders,0) AS paid_orders,
              COALESCE(pg.unique_buyers,0) AS unique_buyers,
              COALESCE(pg.unique_buyers,0) AS unique_buyers,
              ROUND(COALESCE(sc.sold,0)::numeric/NULLIF(COALESCE(sc.total,0),0),4) AS fill_rate
       FROM draws d
       LEFT JOIN sold_counts sc ON sc.draw_id=d.id
       LEFT JOIN paid_gmv    pg ON pg.draw_id=d.id
       ORDER BY gmv_cents DESC NULLS LAST, id DESC
       LIMIT $1`,
      [limit]
    ))?.rows ?? [];
    res.json(rows.map((row) => {
      const fillRate = Number(row.fill_rate || 0);
      return {
        ...row,
        id: Number(row.id),
        total: Number(row.total || 0),
        numbers_count: Number(row.total || 0),
        sold: Number(row.sold || 0),
        sold_numbers: Number(row.sold || 0),
        gmv_cents: Number(row.gmv_cents || 0),
        avg_ticket_cents: Number(row.avg_ticket_cents || 0),
        average_ticket_cents: Number(row.avg_ticket_cents || 0),
        paid_orders: Number(row.paid_orders || 0),
        unique_buyers: Number(row.unique_buyers || 0),
        unique_buyers: Number(row.unique_buyers || 0),
        fill_rate: fillRate,
        progress_percent: Number((fillRate * 100).toFixed(2)),
      };
    }));
  } catch (e) {
    console.error("[analytics/draws/leaderboard]", e);
    res.status(500).json({ error: "Falha ao obter leaderboard de sorteios" });
  }
});

/* ============================================================================
 * DASHBOARD KPI CONSOLIDADO
 * ============================================================================ */
router.get("/kpi-dashboard", async (_req, res) => {
  try {
    const [
      summaryResult,
      currentDrawResult,
      monthlyResult,
      dailyResult,
      rankingResult,
      topBuyersResult,
      qualityResult,
      duplicateNumbersResult,
    ] = await Promise.all([
      q(
        `WITH paid AS (
           SELECT user_id,
                  amount_cents,
                  COALESCE(paid_at, created_at) AS financial_at
             FROM public.payments
            WHERE lower(status) IN ('approved','paid','pago')
         )
         SELECT
           COALESCE(SUM(amount_cents) FILTER (
             WHERE financial_at >= now() - interval '30 days'
           ),0)::bigint AS gmv_30d_cents,
           COALESCE(SUM(amount_cents),0)::bigint AS gmv_all_time_cents,
           COALESCE(SUM(amount_cents) FILTER (
             WHERE financial_at >= date_trunc('month', now())
           ),0)::bigint AS gmv_current_month_cents,
           COALESCE(SUM(amount_cents) FILTER (
             WHERE financial_at >= date_trunc('year', now())
           ),0)::bigint AS gmv_current_year_cents,
           COUNT(*) FILTER (
             WHERE financial_at >= now() - interval '30 days'
           )::int AS paid_orders_30d,
           COUNT(*)::int AS paid_orders_all_time,
           COUNT(DISTINCT user_id)::int AS unique_buyers_all_time,
           COALESCE(AVG(amount_cents),0)::bigint AS average_ticket_cents
         FROM paid`
      ),
      q(
        `WITH current_draw AS (
           SELECT id, status, draw_type, product_name, opened_at, closed_at, created_at
             FROM public.draws
            WHERE status = 'open'
              AND COALESCE(draw_type, 'principal') = 'principal'
            ORDER BY opened_at DESC NULLS LAST,
                     created_at DESC NULLS LAST,
                     id DESC
            LIMIT 1
         ),
         number_stats AS (
           SELECT n.draw_id,
                  COUNT(*)::int AS total_numbers,
                  COUNT(*) FILTER (WHERE lower(n.status)='sold')::int AS sold_numbers,
                  COUNT(*) FILTER (WHERE lower(n.status)='reserved')::int AS reserved_numbers,
                  COUNT(*) FILTER (WHERE lower(n.status)='available')::int AS available_numbers
             FROM public.numbers n
             JOIN current_draw d ON d.id=n.draw_id
            GROUP BY n.draw_id
         ),
         payment_stats AS (
           SELECT p.draw_id,
                  COALESCE(SUM(p.amount_cents),0)::bigint AS gmv_cents,
                  COUNT(*)::int AS paid_orders,
                  COUNT(DISTINCT p.user_id)::int AS unique_buyers
             FROM public.payments p
             JOIN current_draw d ON d.id=p.draw_id
            WHERE lower(p.status) IN ('approved','paid','pago')
            GROUP BY p.draw_id
         )
         SELECT d.id,
                d.status,
                COALESCE(d.draw_type, 'principal') AS draw_type,
                d.product_name,
                d.opened_at,
                d.closed_at,
                d.created_at,
                COALESCE(ns.total_numbers,0)::int AS total_numbers,
                COALESCE(ns.sold_numbers,0)::int AS sold_numbers,
                COALESCE(ns.reserved_numbers,0)::int AS reserved_numbers,
                COALESCE(ns.available_numbers,0)::int AS available_numbers,
                ROUND(
                  COALESCE(ns.sold_numbers,0)::numeric /
                  NULLIF(COALESCE(ns.total_numbers,0),0),
                  4
                ) AS fill_rate,
                COALESCE(ps.gmv_cents,0)::bigint AS gmv_cents,
                COALESCE(ps.paid_orders,0)::int AS paid_orders,
                COALESCE(ps.unique_buyers,0)::int AS unique_buyers
           FROM current_draw d
           LEFT JOIN number_stats ns ON ns.draw_id=d.id
           LEFT JOIN payment_stats ps ON ps.draw_id=d.id`
      ),
      q(
        `WITH paid AS (
           SELECT amount_cents,
                  COALESCE(paid_at, created_at) AS financial_at
             FROM public.payments
            WHERE lower(status) IN ('approved','paid','pago')
         )
         SELECT date_trunc('month', financial_at) AS month,
                COALESCE(SUM(amount_cents),0)::bigint AS gmv_cents,
                COUNT(*)::int AS paid_orders
           FROM paid
          WHERE financial_at IS NOT NULL
          GROUP BY 1
          ORDER BY 1`
      ),
      q(
        `WITH days AS (
           SELECT generate_series(
             date_trunc('day', now()) - interval '29 days',
             date_trunc('day', now()),
             interval '1 day'
           ) AS day
         ),
         paid AS (
           SELECT date_trunc('day', COALESCE(paid_at, created_at)) AS day,
                  SUM(amount_cents)::bigint AS gmv_cents,
                  COUNT(*)::int AS paid_orders
             FROM public.payments
            WHERE lower(status) IN ('approved','paid','pago')
              AND COALESCE(paid_at, created_at) >= date_trunc('day', now()) - interval '29 days'
            GROUP BY 1
         )
         SELECT d.day,
                COALESCE(p.gmv_cents,0)::bigint AS gmv_cents,
                COALESCE(p.paid_orders,0)::int AS paid_orders
           FROM days d
           LEFT JOIN paid p ON p.day=d.day
          ORDER BY d.day`
      ),
      q(
        `WITH number_stats AS (
           SELECT draw_id,
                  COUNT(*)::int AS total_numbers,
                  COUNT(*) FILTER (WHERE lower(status)='sold')::int AS sold_numbers
             FROM public.numbers
            GROUP BY draw_id
         ),
         payment_stats AS (
           SELECT draw_id,
                  COALESCE(SUM(amount_cents),0)::bigint AS gmv_cents,
                  COUNT(*)::int AS paid_orders,
                  COUNT(DISTINCT user_id)::int AS unique_buyers,
                  COALESCE(AVG(amount_cents),0) AS average_ticket_cents
             FROM public.payments
            WHERE lower(status) IN ('approved','paid','pago')
            GROUP BY draw_id
         )
         SELECT d.id AS draw_id,
                d.product_name,
                d.status,
                COALESCE(d.draw_type, 'principal') AS draw_type,
                COALESCE(ns.total_numbers,0)::int AS total_numbers,
                COALESCE(ns.sold_numbers,0)::int AS sold_numbers,
                ROUND(
                  COALESCE(ns.sold_numbers,0)::numeric /
                  NULLIF(COALESCE(ns.total_numbers,0),0),
                  4
                ) AS fill_rate,
                COALESCE(ps.gmv_cents,0)::bigint AS gmv_cents,
                COALESCE(ps.paid_orders,0)::int AS paid_orders,
                COALESCE(ps.unique_buyers,0)::int AS unique_buyers,
                COALESCE(ps.average_ticket_cents,0) AS average_ticket_cents
           FROM public.draws d
           LEFT JOIN number_stats ns ON ns.draw_id=d.id
           LEFT JOIN payment_stats ps ON ps.draw_id=d.id
          WHERE COALESCE(d.draw_type, 'principal') = 'principal'
          ORDER BY gmv_cents DESC, d.id DESC
          LIMIT 20`
      ),
      q(
        `SELECT u.id AS user_id,
                u.name,
                u.email,
                 COUNT(*)::int AS paid_orders,
                 SUM(p.amount_cents)::bigint AS gmv_cents,
                 AVG(p.amount_cents)::bigint AS average_ticket_cents,
                 COALESCE(SUM(cardinality(p.numbers)),0)::int AS numbers_count,
                 MAX(COALESCE(p.paid_at, p.created_at)) AS last_payment_at
           FROM public.payments p
           JOIN public.users u ON u.id=p.user_id
          WHERE lower(p.status) IN ('approved','paid','pago')
          GROUP BY u.id, u.name, u.email
          ORDER BY gmv_cents DESC
          LIMIT 20`
      ),
      q(
        `SELECT
           COUNT(*) FILTER (
             WHERE COALESCE(paid_at, created_at) IS NULL
           )::int AS missing_financial_date,
           COUNT(*) FILTER (
             WHERE amount_cents IS NULL OR amount_cents <= 0
           )::int AS nonpositive_amount,
           COUNT(*) FILTER (WHERE user_id IS NULL)::int AS missing_user,
           COUNT(*) FILTER (WHERE draw_id IS NULL)::int AS missing_draw
         FROM public.payments
         WHERE lower(status) IN ('approved','paid','pago')`
      ),
      q(
        `SELECT COUNT(*)::int AS duplicate_number_groups
           FROM (
             SELECT p.draw_id, paid_number.n
               FROM public.payments p
               CROSS JOIN LATERAL unnest(p.numbers) AS paid_number(n)
              WHERE lower(p.status) IN ('approved','paid','pago')
              GROUP BY p.draw_id, paid_number.n
             HAVING COUNT(*) > 1
           ) duplicated`
      ),
    ]);

    const rawSummary = summaryResult.rows?.[0] || {};
    const rawCurrentDraw = currentDrawResult.rows?.[0] || null;
    const quality = qualityResult.rows?.[0] || {};
    const duplicateNumberGroups = Number(
      duplicateNumbersResult.rows?.[0]?.duplicate_number_groups || 0
    );

    const dataQuality = [
      {
        key: "approved_payments_missing_financial_date",
        count: Number(quality.missing_financial_date || 0),
      },
      {
        key: "approved_payments_nonpositive_amount",
        count: Number(quality.nonpositive_amount || 0),
      },
      {
        key: "approved_payments_missing_user",
        count: Number(quality.missing_user || 0),
      },
      {
        key: "approved_payments_missing_draw",
        count: Number(quality.missing_draw || 0),
      },
      {
        key: "duplicate_approved_number_groups",
        count: duplicateNumberGroups,
      },
    ].map((item) => ({ ...item, status: item.count > 0 ? "warning" : "ok" }));

    const alerts = [];
    if (!rawCurrentDraw) {
      alerts.push({
        code: "NO_OPEN_PRINCIPAL_DRAW",
        severity: "warning",
        message: "Nenhum sorteio principal aberto foi encontrado.",
      });
    } else if (Number(rawCurrentDraw.total_numbers || 0) === 0) {
      alerts.push({
        code: "CURRENT_DRAW_WITHOUT_NUMBERS",
        severity: "warning",
        message: "O sorteio principal aberto não possui números cadastrados.",
      });
    }
    if (duplicateNumberGroups > 0) {
      alerts.push({
        code: "DUPLICATE_APPROVED_NUMBERS",
        severity: "warning",
        count: duplicateNumberGroups,
        message: "Existem números presentes em mais de um pagamento final no mesmo sorteio.",
      });
    }

    const currentDraw = rawCurrentDraw
      ? {
          ...rawCurrentDraw,
          id: Number(rawCurrentDraw.id),
          total_numbers: Number(rawCurrentDraw.total_numbers || 0),
          numbers_count: Number(rawCurrentDraw.total_numbers || 0),
          sold_numbers: Number(rawCurrentDraw.sold_numbers || 0),
          reserved_numbers: Number(rawCurrentDraw.reserved_numbers || 0),
          available_numbers: Number(rawCurrentDraw.available_numbers || 0),
          fill_rate: Number(rawCurrentDraw.fill_rate || 0),
          progress_percent: Number((Number(rawCurrentDraw.fill_rate || 0) * 100).toFixed(2)),
          gmv_cents: Number(rawCurrentDraw.gmv_cents || 0),
          paid_orders: Number(rawCurrentDraw.paid_orders || 0),
          unique_buyers: Number(rawCurrentDraw.unique_buyers || 0),
        }
      : {};

    return res.json({
      summary: {
        gmv_30d_cents: Number(rawSummary.gmv_30d_cents || 0),
        gmv_all_time_cents: Number(rawSummary.gmv_all_time_cents || 0),
        gmv_current_month_cents: Number(rawSummary.gmv_current_month_cents || 0),
        gmv_current_year_cents: Number(rawSummary.gmv_current_year_cents || 0),
        paid_orders_30d: Number(rawSummary.paid_orders_30d || 0),
        paid_orders_all_time: Number(rawSummary.paid_orders_all_time || 0),
        unique_buyers_all_time: Number(rawSummary.unique_buyers_all_time || 0),
        average_ticket_cents: Number(rawSummary.average_ticket_cents || 0),
      },
      current_draw: currentDraw,
      monthly_gmv: (monthlyResult.rows || []).map((row) => ({
        ...row,
        gmv_cents: Number(row.gmv_cents || 0),
        paid_orders: Number(row.paid_orders || 0),
      })),
      daily_gmv: (dailyResult.rows || []).map((row) => ({
        ...row,
        gmv_cents: Number(row.gmv_cents || 0),
        paid_orders: Number(row.paid_orders || 0),
      })),
      draw_ranking: (rankingResult.rows || []).map((row) => ({
        ...row,
        draw_id: Number(row.draw_id),
        total_numbers: Number(row.total_numbers || 0),
        numbers_count: Number(row.total_numbers || 0),
        sold_numbers: Number(row.sold_numbers || 0),
        fill_rate: Number(row.fill_rate || 0),
        progress_percent: Number((Number(row.fill_rate || 0) * 100).toFixed(2)),
        gmv_cents: Number(row.gmv_cents || 0),
        paid_orders: Number(row.paid_orders || 0),
        unique_buyers: Number(row.unique_buyers || 0),
        average_ticket_cents: Number(row.average_ticket_cents || 0),
      })),
      top_buyers: (topBuyersResult.rows || []).map((row) => ({
        ...row,
        user_id: Number(row.user_id),
        paid_orders: Number(row.paid_orders || 0),
        gmv_cents: Number(row.gmv_cents || 0),
        average_ticket_cents: Number(row.average_ticket_cents || 0),
        numbers_count: Number(row.numbers_count || 0),
      })),
      alerts,
      data_quality: dataQuality,
      gmv_basis: {
        statuses: ["approved", "paid", "pago"],
        financial_date: "COALESCE(paid_at, created_at)",
      },
    });
  } catch (e) {
    console.error("[analytics/kpi-dashboard]", e);
    return res.status(500).json({ error: "Falha ao obter kpi-dashboard" });
  }
});

// --- OVERVIEW GERAL (todos os sorteios) -------------------------------------
router.get("/overview", async (req, res) => {
  const days = Math.min(Number(req.query.days) || 30, 365);

  try {
    // Totais por status
    const totalsRow = (await q(
      `SELECT
         COALESCE(SUM(CASE WHEN lower(status) IN ('approved','paid','pago') THEN amount_cents END),0)         AS gmv_paid_cents,
         COALESCE(SUM(amount_cents) FILTER (
           WHERE lower(status) IN ('approved','paid','pago')
             AND COALESCE(paid_at, created_at) >= now() - interval '30 days'
         ),0) AS gmv_30d_cents,
         COALESCE(SUM(amount_cents) FILTER (
           WHERE lower(status) IN ('approved','paid','pago')
             AND COALESCE(paid_at, created_at) >= date_trunc('month', now())
         ),0) AS gmv_current_month_cents,
         COALESCE(SUM(amount_cents) FILTER (
           WHERE lower(status) IN ('approved','paid','pago')
             AND COALESCE(paid_at, created_at) >= date_trunc('year', now())
         ),0) AS gmv_current_year_cents,
         COUNT(*) FILTER (WHERE lower(status) IN ('approved','paid','pago'))                                  AS orders_paid,
         COUNT(*) FILTER (
           WHERE lower(status) IN ('approved','paid','pago')
             AND COALESCE(paid_at, created_at) >= now() - interval '30 days'
         ) AS paid_orders_30d,
         COALESCE(AVG(amount_cents) FILTER (WHERE lower(status) IN ('approved','paid','pago')),0)             AS avg_ticket_paid_cents,
         COUNT(DISTINCT user_id) FILTER (WHERE lower(status) IN ('approved','paid','pago'))                   AS unique_buyers_paid,
         COALESCE(SUM(CASE WHEN lower(status) IN ('pending','processing') THEN amount_cents END),0) AS gmv_intent_cents,
         COUNT(*) FILTER (WHERE lower(status) IN ('pending','processing'))             AS orders_intent,
         COALESCE(SUM(CASE WHEN lower(status)='expired' THEN amount_cents END),0)      AS gmv_expired_cents,
         COUNT(*) FILTER (WHERE lower(status)='expired')                               AS orders_expired,
         COALESCE(SUM(CASE WHEN lower(status) IN ('cancelled','canceled') THEN amount_cents END),0) AS gmv_cancelled_cents,
         COUNT(*) FILTER (WHERE lower(status) IN ('cancelled','canceled'))              AS orders_cancelled
       FROM payments`
    ))?.rows?.[0] || {};

    // média pedidos/cliente (paid)
    const avgOrdersPerBuyer = (await q(
      `WITH agg AS (
         SELECT user_id, COUNT(*) AS c
           FROM payments
         WHERE lower(status) IN ('approved','paid','pago')
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
       WHERE lower(status) IN ('approved','paid','pago')`
    ))?.rows?.[0] || { p25: 0, p50: 0, p75: 0, p90: 0 };

    // séries últimas N days (paid/intent/expired)
    const series = (await q(
      `WITH base AS (
         SELECT date_trunc('day', COALESCE(paid_at, created_at)) AS day,
                lower(status) AS status,
                amount_cents
           FROM payments
          WHERE COALESCE(paid_at, created_at) >= now() - ($1 || ' days')::interval
       )
       SELECT day,
              SUM(amount_cents) FILTER (WHERE lower(status) IN ('approved','paid','pago'))                      AS gmv_paid_cents,
              SUM(amount_cents) FILTER (WHERE status IN ('pending','processing')) AS gmv_intent_cents,
              SUM(amount_cents) FILTER (WHERE status='expired')                   AS gmv_expired_cents,
              COUNT(*)        FILTER (WHERE lower(status) IN ('approved','paid','pago'))                        AS orders_paid,
              COUNT(*)        FILTER (WHERE status IN ('pending','processing'))   AS orders_intent,
              COUNT(*)        FILTER (WHERE status='expired')                     AS orders_expired
         FROM base
        GROUP BY 1
        ORDER BY 1`,
      [days]
    ))?.rows ?? [];

    // pagos por hora (últimos 90 dias)
    const hourly = (await q(
      `SELECT EXTRACT(HOUR FROM (COALESCE(paid_at, created_at) AT TIME ZONE 'America/Sao_Paulo')) AS hour_br,
              COUNT(*) AS paid
         FROM payments
        WHERE lower(status) IN ('approved','paid','pago')
          AND COALESCE(paid_at, created_at) >= now() - interval '90 days'
          AND COALESCE(paid_at, created_at) IS NOT NULL
        GROUP BY 1 ORDER BY 1`
    ))?.rows ?? [];

    // top compradores (paid)
    const topBuyers = (await q(
      `SELECT u.id AS user_id, u.name, u.email,
              COUNT(*)::int AS orders,
              COUNT(*)::int AS paid_orders,
              SUM(p.amount_cents)::bigint AS gmv_cents,
              AVG(p.amount_cents) AS avg_ticket_cents,
              AVG(p.amount_cents) AS average_ticket_cents,
              COALESCE(SUM(cardinality(p.numbers)),0)::int AS numbers_count,
              MAX(COALESCE(p.paid_at, p.created_at)) AS last_payment_at
         FROM payments p
         JOIN users u ON u.id=p.user_id
        WHERE lower(p.status) IN ('approved','paid','pago')
        GROUP BY u.id, u.name, u.email
        ORDER BY gmv_cents DESC NULLS LAST
        LIMIT 20`
    ))?.rows ?? [];

    res.json({
      totals: {
        gmv_paid_cents: Number(totalsRow.gmv_paid_cents || 0),
        gmv_all_time_cents: Number(totalsRow.gmv_paid_cents || 0),
        gmv_30d_cents: Number(totalsRow.gmv_30d_cents || 0),
        gmv_current_month_cents: Number(totalsRow.gmv_current_month_cents || 0),
        gmv_current_year_cents: Number(totalsRow.gmv_current_year_cents || 0),
        orders_paid: Number(totalsRow.orders_paid || 0),
        paid_orders_30d: Number(totalsRow.paid_orders_30d || 0),
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
      topBuyers: topBuyers.map((row) => ({
        ...row,
        user_id: Number(row.user_id),
        orders: Number(row.orders || 0),
        paid_orders: Number(row.paid_orders || 0),
        gmv_cents: Number(row.gmv_cents || 0),
        avg_ticket_cents: Number(row.avg_ticket_cents || 0),
        average_ticket_cents: Number(row.average_ticket_cents || 0),
        numbers_count: Number(row.numbers_count || 0),
      }))
    });
  } catch (e) {
    console.error("[analytics/overview]", e);
    res.status(500).json({ error: "Falha ao obter overview" });
  }
});

// alias simples se seu front já chama /stats
router.get("/stats", async (req, res) => {
  try {
    const headers = {};
    if (req.headers.authorization) headers.authorization = req.headers.authorization;
    if (req.headers.cookie) headers.cookie = req.headers.cookie;

    const upstream = await fetch(
      req.protocol + "://" + req.get("host") + `/api/admin/analytics/overview${req.url.includes('?') ? req.url.slice(req.url.indexOf('?')) : ''}`,
      { headers }
    );
    const o = await upstream.json();
    res.status(upstream.status).json(o);
  } catch (e) {
    console.error("[analytics/stats alias]", e);
    res.status(500).json({ error: "Falha ao obter stats" });
  }
});

export default router;
