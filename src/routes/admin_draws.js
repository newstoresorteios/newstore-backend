import { Router } from 'express';
import { query } from '../db.js';
import { requireAuth } from '../middleware/auth.js';

const router = Router();

// Ajuste se o flag de admin for outro campo
async function requireAdmin(req, res, next) {
  try {
    const userId = req?.user?.id;
    if (!userId) return res.status(401).json({ error: 'unauthorized' });
    const r = await query('select is_admin from users where id = $1', [userId]);
    if (!r.rows.length || !r.rows[0].is_admin) {
      return res.status(403).json({ error: 'forbidden' });
    }
    next();
  } catch (e) {
    console.error('[admin check] error', e);
    res.status(500).json({ error: 'admin_check_failed' });
  }
}

/** GET /api/admin/draws/history */
router.get('/history', requireAuth, requireAdmin, async (_req, res) => {
  try {
    const r = await query(`
      select
        d.id,
        d.status,
        coalesce(d.opened_at, d.created_at) as opened_at,
        d.closed_at,
        d.realized_at,
        round(
          extract(epoch from (coalesce(d.closed_at, now()) - coalesce(d.opened_at, d.created_at)))
          / 86400.0
        )::int as days_open,
        coalesce(d.winner_name, '-') as winner_name
      from draws d
      where d.status = 'closed' or d.closed_at is not null
      order by d.id desc
    `);
    res.json({ history: r.rows || [] });
  } catch (e) {
    console.error('[admin/draws/history] error', e);
    res.status(500).json({ error: 'list_failed' });
  }
});

/** GET /api/admin/draws/:id/participants */
router.get('/:id/participants', requireAuth, requireAdmin, async (req, res) => {
  try {
    const drawId = Number(req.params.id);
    if (!Number.isFinite(drawId)) return res.status(400).json({ error: 'invalid_draw_id' });

    // Se quiser só "pagos", ative os filtros comentados.
    const sql = `
      select
        r.id as reservation_id,
        r.draw_id,
        r.user_id,
        r.number as number,
        r.status as status,
        r.created_at,
        coalesce(nullif(u.name,''), u.email, '-') as user_name,
        u.email as user_email
      from reservations r
      left join users u on u.id = r.user_id
      where r.draw_id = $1
        -- and coalesce(r.status,'') not in ('cancelled','canceled')
        -- and coalesce(r.paid,false) = true
      order by user_name asc, number asc
    `;
    const r = await query(sql, [drawId]);
    res.json({ draw_id: drawId, participants: r.rows || [] });
  } catch (e) {
    console.error('[admin/draws/:id/participants] error', e);
    res.status(500).json({ error: 'participants_failed' });
  }
});

// Alias opcional: /players -> mesmo payload
router.get('/:id/players', requireAuth, requireAdmin, async (req, res) => {
  req.url = req.url.replace('/players', '/participants');
  return router.handle(req, res);
});

export default router;
