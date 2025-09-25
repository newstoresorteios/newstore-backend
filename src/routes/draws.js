// backend/src/routes/draws.js
import { Router } from 'express';
import { query } from '../db.js';
import { requireAuth } from '../middleware/auth.js';

const router = Router();

/**
 * GET /api/draws
 *   ?status=open|closed (opcional)
 */
router.get('/', async (req, res) => {
  try {
    const status = String(req.query.status || '').toLowerCase();
    let sql = `
      select
        id,
        status,
        coalesce(opened_at, created_at) as opened_at,
        closed_at,
        realized_at,
        winner_name
      from draws
    `;
    const args = [];
    if (status === 'open' || status === 'closed') {
      sql += ` where status = $1`;
      args.push(status);
    }
    sql += ` order by id desc`;

    const { rows } = await query(sql, args);
    return res.json({ draws: rows });
  } catch (e) {
    console.error('[draws] list error', e);
    return res.status(500).json({ error: 'list_failed' });
  }
});

/**
 * GET /api/draws/history
 *   lista sorteios fechados
 */
router.get('/history', async (_req, res) => {
  try {
    const { rows } = await query(`
      select
        id,
        status,
        coalesce(opened_at, created_at) as opened_at,
        closed_at,
        realized_at,
        coalesce(winner_name, '-') as winner_name
      from draws
      where status = 'closed' or closed_at is not null
      order by id desc
    `);
    return res.json({ draws: rows });
  } catch (e) {
    console.error('[draws/history] error', e);
    return res.status(500).json({ error: 'history_failed' });
  }
});

/**
 * GET /api/draws/:id/numbers
 */
router.get('/:id/numbers', async (req, res) => {
  try {
    const drawId = Number(req.params.id);
    if (!drawId) return res.json({ numbers: [] });
    const r = await query(
      'select n, status from numbers where draw_id=$1 order by n asc',
      [drawId]
    );
    const numbers = r.rows.map(x => ({ n: x.n, status: x.status }));
    res.json({ numbers });
  } catch (e) {
    console.error('[draws/:id/numbers] error:', e);
    res.status(500).json({ error: 'numbers_failed' });
  }
});

/**
 * GET /api/draws/:id/participants
 * Lista participantes (nome/email) e seus respectivos números (explode o array reservations.numbers).
 * Requer usuário autenticado.
 */
router.get('/:id/participants', requireAuth, async (req, res) => {
  try {
    const drawId = Number(req.params.id);
    if (!Number.isFinite(drawId)) return res.status(400).json({ error: 'invalid_draw_id' });

    // Se quiser só pagos/aprovados, ative os filtros comentados.
    const sql = `
      select
        r.id as reservation_id,
        r.draw_id,
        r.user_id,
        num as number,
        r.status as status,
        r.created_at,
        coalesce(nullif(u.name,''), u.email, '-') as user_name,
        u.email as user_email
      from reservations r
      left join users u on u.id = r.user_id
      cross join lateral unnest(coalesce(r.numbers, '{}'::int[])) as num
      where r.draw_id = $1
        -- and coalesce(r.status,'') not in ('cancelled','canceled')
        -- and coalesce(r.paid,false) = true
      order by user_name asc, number asc
    `;
    const r = await query(sql, [drawId]);
    return res.json({ draw_id: drawId, participants: r.rows || [] });
  } catch (e) {
    console.error('[draws/:id/participants] error:', e);
    return res.status(500).json({ error: 'participants_failed' });
  }
});

/**
 * Alias: GET /api/draws/:id/players
 */
router.get('/:id/players', requireAuth, async (req, res) => {
  try {
    const drawId = Number(req.params.id);
    if (!Number.isFinite(drawId)) return res.status(400).json({ error: 'invalid_draw_id' });

    const sql = `
      select
        r.id as reservation_id,
        r.draw_id,
        r.user_id,
        num as number,
        r.status as status,
        r.created_at,
        coalesce(nullif(u.name,''), u.email, '-') as user_name,
        u.email as user_email
      from reservations r
      left join users u on u.id = r.user_id
      cross join lateral unnest(coalesce(r.numbers, '{}'::int[])) as num
      where r.draw_id = $1
      order by user_name asc, number asc
    `;
    const r = await query(sql, [drawId]);
    return res.json({ draw_id: drawId, participants: r.rows || [] });
  } catch (e) {
    console.error('[draws/:id/players] error:', e);
    return res.status(500).json({ error: 'participants_failed' });
  }
});

export default router;
