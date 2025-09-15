import { Router } from 'express';
import { query } from '../db.js';

const router = Router();

/**
 * GET /api/draws?status=open|closed
 * Lista sorteios (com filtro opcional por status)
 */
router.get('/', async (req, res) => {
  try {
    const status = String(req.query.status || '').toLowerCase();
    let sql = `
      select id, status, opened_at, closed_at, realized_at,
             winner_user_name as winner_name
        from draws
    `;
    const args = [];
    if (status === 'open' || status === 'closed') {
      sql += ` where status = $1`;
      args.push(status);
    }
    sql += ` order by id desc`;

    const { rows } = await query(sql, args);
    return res.json({ draws: rows || [] });
  } catch (e) {
    console.error('[draws] list error', e);
    return res.status(500).json({ error: 'list_failed' });
  }
});

/**
 * GET /api/draws/history
 * Alias: retorna somente fechados
 */
router.get('/history', async (_req, res) => {
  try {
    const { rows } = await query(
      `select id, status, opened_at, closed_at, realized_at,
              winner_user_name as winner_name
         from draws
        where status = 'closed' or closed_at is not null
        order by id desc`
    );
    return res.json({ draws: rows || [] });
  } catch (e) {
    console.error('[draws/history] error', e);
    return res.status(500).json({ error: 'history_failed' });
  }
});

/**
 * GET /api/draws/:id/numbers
 * Números de um sorteio específico
 */
router.get('/:id/numbers', async (req, res) => {
  try {
    const drawId = Number(req.params.id);
    if (!drawId) return res.json({ numbers: [] });

    const r = await query(
      'select n, status from numbers where draw_id=$1 order by n asc',
      [drawId]
    );
    const numbers = (r.rows || []).map(x => ({ n: x.n, status: x.status }));
    res.json({ numbers });
  } catch (e) {
    console.error('[draws/:id/numbers] error:', e);
    res.status(500).json({ error: 'numbers_failed' });
  }
});

export default router;
