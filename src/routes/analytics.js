// backend/src/routes/analytics.js
// Exponha em app.js/server.js com: app.use('/api/admin/analytics', require('./routes/analytics'))

const express = require('express');
const router = express.Router();
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.PGSSL === 'true' ? { rejectUnauthorized: false } : false,
});

async function q(sql, params = []) {
  const cli = await pool.connect();
  try {
    const { rows } = await cli.query(sql, params);
    return rows;
  } finally {
    cli.release();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /summary/:drawId  → KPIs principais do sorteio (GMV, fill, ticket, funil)
// ─────────────────────────────────────────────────────────────────────────────
router.get('/summary/:drawId', async (req, res) => {
  const drawId = Number(req.params.drawId);
  if (!Number.isFinite(drawId)) return res.status(400).json({ error: 'drawId inválido' });
  try {
    const draw = (await q(`
      SELECT id, status, opened_at, closed_at, realized_at, product_name
      FROM draws WHERE id=$1
    `, [drawId]))?.[0];

    if (!draw) return res.status(404).json({ error: 'Sorteio não encontrado' });

    const sold = (await q(`
      SELECT SUM((status='sold')::int) AS sold,
             SUM((status='reserved')::int) AS reserved,
             SUM((status='available')::int) AS available
      FROM numbers WHERE draw_id=$1
    `, [drawId]))?.[0] || { sold: 0, reserved: 0, available: 0 };

    const paid = (await q(`
      SELECT COALESCE(SUM(amount_cents),0) AS gmv_cents,
             COALESCE(AVG(amount_cents),0) AS avg_ticket_cents,
             COUNT(*) AS paid_orders
      FROM payments WHERE draw_id=$1 AND status='paid'
    `, [drawId]))?.[0] || { gmv_cents: 0, avg_ticket_cents: 0, paid_orders: 0 };

    const expiredRes = (await q(`
      SELECT COUNT(*) AS expired_reservations
      FROM reservations WHERE draw_id=$1 AND status='expired'
    `, [drawId]))?.[0] || { expired_reservations: 0 };

    const expiredPays = (await q(`
      SELECT COUNT(*) AS expired_payments
      FROM payments WHERE draw_id=$1 AND status='expired'
    `, [drawId]))?.[0] || { expired_payments: 0 };

    const hourDist = await q(`
      SELECT EXTRACT(HOUR FROM (paid_at AT TIME ZONE 'America/Sao_Paulo')) AS hour_br,
             COUNT(*) AS paid
      FROM payments
      WHERE status='paid' AND paid_at IS NOT NULL AND draw_id=$1
      GROUP BY 1 ORDER BY 1
    `, [drawId]);

    const numHeat = await q(`
      SELECT n.n::int AS n, COUNT(*)::int AS sold_count
      FROM numbers n
      JOIN reservations r ON r.id=n.reservation_id AND r.status='captured'
      JOIN payments p ON p.id=r.payment_id AND p.status='paid'
      WHERE n.status='sold' AND n.draw_id=$1
      GROUP BY n.n
      ORDER BY n.n
    `, [drawId]);

    res.json({
      draw,
      funnel: {
        available: Number(sold?.available || 0),
        reserved: Number(sold?.reserved || 0),
        sold: Number(sold?.sold || 0),
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
      fill_rate: sold?.sold ? Number((Number(sold.sold) / 100).toFixed(2)) : 0,
    });
  } catch (e) {
    console.error('[analytics/summary]', e);
    res.status(500).json({ error: 'Falha ao obter summary' });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /rfm?limit=50 → lista RFM (quem atacar)
// ─────────────────────────────────────────────────────────────────────────────
router.get('/rfm', async (req, res) => {
  const limit = Math.min(Number(req.query.limit) || 50, 500);
  try {
    const rows = await q(`
      WITH paid AS (
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
      LIMIT $1
    `, [limit]);
    res.json(rows);
  } catch (e) {
    console.error('[analytics/rfm]', e);
    res.status(500).json({ error: 'Falha ao obter RFM' });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// GET /draws → últimos sorteios (para dropdown)
// ─────────────────────────────────────────────────────────────────────────────
router.get('/draws', async (_req, res) => {
  try {
    const rows = await q(`
      SELECT id, product_name, status, opened_at, closed_at
      FROM draws
      ORDER BY id DESC
      LIMIT 200
    `);
    res.json(rows);
  } catch (e) {
    console.error('[analytics/draws]', e);
    res.status(500).json({ error: 'Falha ao listar draws' });
  }
});

module.exports = router;
