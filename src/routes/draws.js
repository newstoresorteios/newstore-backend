// backend/src/routes/draws.js
import { Router } from 'express';
import { query } from '../db.js';

const router = Router();

/**
 * GET /api/draws
 * Lista sorteios. Aceita ?status=open|closed (opcional).
 * Retorna campos padronizados que o front entende.
 */
router.get('/draws', async (req, res) => {
  try {
    const status = String(req.query.status || '').toLowerCase();

    let sql = `
      select
        d.id,
        d.status,
        coalesce(d.opened_at, d.created_at) as opened_at,
        d.closed_at,
        d.realized_at,
        -- usa winner_name que existe na tabela
        d.winner_name
      from draws d
    `;
    const args = [];

    if (status === 'open' || status === 'closed') {
      sql += ` where d.status = $1`;
      args.push(status);
    }

    sql += ` order by d.id desc`;

    const { rows } = await query(sql, args);

    // Normaliza payload
    const draws = (rows || []).map(r => ({
      id: r.id,
      status: r.status,
      opened_at: r.opened_at,
      closed_at: r.closed_at,
      realized_at: r.realized_at,
      winner_name: r.winner_name || null,
    }));

    return res.json({ draws });
  } catch (e) {
    console.error('[draws] list error', e);
    return res.status(500).json({ error: 'list_failed' });
  }
});

/**
 * GET /api/draws/history
 * Versão pública: só sorteios fechados.
 */
router.get('/draws/history', async (_req, res) => {
  try {
    const { rows } = await query(
      `
      select
        d.id,
        d.status,
        coalesce(d.opened_at, d.created_at) as opened_at,
        d.closed_at,
        d.realized_at,
        d.winner_name
      from draws d
      where d.status = 'closed' or d.closed_at is not null
      order by d.id desc
      `
    );

    const history = (rows || []).map(r => ({
      id: r.id,
      status: r.status,
      opened_at: r.opened_at,
      closed_at: r.closed_at,
      realized_at: r.realized_at,
      winner_name: r.winner_name || null,
    }));

    return res.json({ history });
  } catch (e) {
    console.error('[draws/history] error', e);
    return res.status(500).json({ error: 'history_failed' });
  }
});

export default router;
