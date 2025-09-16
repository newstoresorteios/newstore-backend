// src/routes/admin_winners.js
import { Router } from 'express';
import { query } from '../db.js';
import { requireAuth, requireAdmin } from '../middleware/auth.js';

const router = Router();

/**
 * GET /api/admin/winners
 * Lista sorteios com vencedor (realized_at IS NOT NULL).
 * Campos úteis para a tela:
 *  - id (nº do sorteio)
 *  - winner_name (nome do vencedor)
 *  - realized_at (data do sorteio)
 *  - closed_at   (se tem, foi resgatado)
 *  - prize_status ('RESGATADO' | 'NÃO RESGATADO')
 *  - days_since   (dias decorridos desde realized_at)
 */
router.get('/', requireAuth, requireAdmin, async (_req, res) => {
  try {
    const r = await query(`
      select
        d.id,
        coalesce(d.winner_name, '-') as winner_name,
        d.realized_at,
        d.closed_at,
        case when d.closed_at is not null
             then 'RESGATADO' else 'NÃO RESGATADO' end as prize_status,
        round(extract(epoch from (now() - d.realized_at)) / 86400.0)::int as days_since
      from draws d
      where d.realized_at is not null
      order by d.realized_at desc, d.id desc
    `);

    return res.json({ winners: r.rows || [] });
  } catch (e) {
    console.error('[admin/winners] error', e);
    return res.status(500).json({ error: 'list_failed' });
  }
});

export default router;
