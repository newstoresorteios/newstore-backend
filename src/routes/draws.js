import { Router } from 'express';
import { query } from '../db.js';

const router = Router();

/**
 * Função de listagem reaproveitável
 * - status: 'open' | 'closed' | null
 */
async function listDraws(status) {
  let sql = `
    select
      d.id,
      d.status,
      coalesce(d.opened_at, d.created_at) as opened_at,
      d.closed_at,
      d.realized_at,
      d.winner_name
    from draws d
  `;
  const params = [];

  if (status === 'open') {
    sql += ` where d.status = 'open'`;
  } else if (status === 'closed') {
    // considera fechado se status='closed' OU se já tem closed_at
    sql += ` where d.status = 'closed' or d.closed_at is not null`;
  }

  sql += ` order by d.id desc`;

  const { rows } = await query(sql, params);
  return rows || [];
}

/**
 * GET /api/draws?status=open|closed
 * Pública
 */
router.get('/draws', async (req, res) => {
  try {
    const status = String(req.query.status || '').toLowerCase();
    const rows = await listDraws(status || null);
    return res.json({ draws: rows });
  } catch (e) {
    console.error('[draws] list error', e);
    return res.status(500).json({ error: 'list_failed' });
  }
});

/**
 * GET /api/draws/history
 * Atalho para closed (pública)
 */
router.get('/draws/history', async (_req, res) => {
  try {
    const rows = await listDraws('closed');
    return res.json({ history: rows });
  } catch (e) {
    console.error('[draws/history] error', e);
    return res.status(500).json({ error: 'list_failed' });
  }
});

export default router;
