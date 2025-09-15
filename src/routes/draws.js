// backend/src/routes/draws.js
import { Router } from 'express';
import { query } from '../db.js';

const router = Router();

/**
 * GET /api/draws
 * Lista sorteios. Aceita ?status=open|closed (opcional)
 */
router.get('/draws', async (req, res) => {
  try {
    const status = String(req.query.status || '').toLowerCase();
    const args = [];
    let where = '';
    if (status === 'open' || status === 'closed') {
      args.push(status);
      where = 'WHERE d.status = $1';
    }

    const sql = `
      SELECT
        d.id,
        d.status,
        COALESCE(d.opened_at, d.created_at) AS opened_at,
        d.closed_at,
        d.realized_at,
        COALESCE(d.winner_name, u.name, u.email, '-') AS winner_name
      FROM draws d
      LEFT JOIN users u ON u.id = d.winner_user_id
      ${where}
      ORDER BY d.id DESC
    `;

    const { rows } = await query(sql, args);
    return res.json({ draws: rows || [] });
  } catch (e) {
    console.error('[draws] list error', e);
    return res.status(500).json({ error: 'list_failed' });
  }
});

/**
 * GET /api/draws/history
 * Lista sorteios fechados (status = closed OU closed_at não-nulo)
 * Não exige auth (somente leitura)
 */
router.get('/draws/history', async (_req, res) => {
  try {
    const sql = `
      SELECT
        d.id,
        d.status,
        COALESCE(d.opened_at, d.created_at) AS opened_at,
        d.closed_at,
        d.realized_at,
        ROUND(
          EXTRACT(EPOCH FROM (COALESCE(d.closed_at, NOW()) - COALESCE(d.opened_at, d.created_at)))
          / 86400.0
        )::int AS days_open,
        COALESCE(d.winner_name, u.name, u.email, '-') AS winner_name
      FROM draws d
      LEFT JOIN users u ON u.id = d.winner_user_id
      WHERE d.status = 'closed' OR d.closed_at IS NOT NULL
      ORDER BY d.id DESC
    `;
    const { rows } = await query(sql, []);
    return res.json({ history: rows || [] });
  } catch (e) {
    console.error('[draws/history] error:', e);
    return res.status(500).json({ error: 'list_failed' });
  }
});

/**
 * (Opcional) GET /api/draws/:id/numbers
 * Lista números do sorteio.
 */
router.get('/draws/:id/numbers', async (req, res) => {
  try {
    const drawId = Number(req.params.id);
    if (!drawId) return res.json({ numbers: [] });
    const r = await query(
      'SELECT n, status FROM numbers WHERE draw_id = $1 ORDER BY n ASC',
      [drawId]
    );
    const numbers = (r.rows || []).map(x => ({ n: x.n, status: x.status }));
    return res.json({ numbers });
  } catch (e) {
    console.error('[draws/:id/numbers] error:', e);
    return res.status(500).json({ error: 'numbers_failed' });
  }
});

export default router;
