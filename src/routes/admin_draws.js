import { Router } from 'express';
import { query } from '../db.js';
import { requireAuth } from '../middleware/auth.js';

const router = Router();

// Troque a lógica abaixo se seu flag de admin estiver em outro campo
async function requireAdmin(req, res, next) {
  try {
    const userId = req?.user?.id;
    if (!userId) return res.status(401).json({ error: 'unauthorized' });
    const r = await query('select is_admin from users where id = $1', [userId]);
    if (!r.rows.length || !r.rows[0].is_admin) {
      return res.status(403).json({ error: 'forbidden' });
    }
    return next();
  } catch (e) {
    console.error('[admin check] error', e);
    return res.status(500).json({ error: 'admin_check_failed' });
  }
}

/**
 * GET /api/admin/draws/history
 * Protegida (admin)
 */
router.get('/history', requireAuth, requireAdmin, async (_req, res) => {
  try {
    const r = await query(`
      select
        d.id,
        d.status,
        coalesce(d.opened_at, d.created_at)           as opened_at,
        d.closed_at,
        d.realized_at,
        round(
          extract(epoch from (coalesce(d.closed_at, now()) - coalesce(d.opened_at, d.created_at)))
          / 86400.0
        )::int                                        as days_open,
        coalesce(d.winner_name, '-')                  as winner_name
      from draws d
      where d.status = 'closed' or d.closed_at is not null
      order by d.id desc
    `);

    return res.json({ history: r.rows || [] });
  } catch (e) {
    console.error('[admin/draws/history] error', e);
    return res.status(500).json({ error: 'list_failed' });
  }
});

export default router;
