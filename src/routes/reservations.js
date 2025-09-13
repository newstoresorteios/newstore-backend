// src/routes/reservations.js
import { Router } from 'express';
import { v4 as uuid } from 'uuid';
import { query } from '../db.js';
import { requireAuth } from '../middleware/auth.js';

const router = Router();

async function cleanupExpired() {
  const expired = await query(
    `update reservations
       set status = 'expired'
     where status = 'active'
       and expires_at < now()
     returning id, draw_id, numbers`
  );

  for (const r of expired.rows) {
    await query(
      `update numbers
         set status = 'available',
             reservation_id = null
       where draw_id = $1
         and n = any($2)
         and status = 'reserved'
         and reservation_id = $3`,
      [r.draw_id, r.numbers, r.id]
    );
  }
}

router.post('/', requireAuth, async (req, res) => {
  const DBG = process.env.DEBUG_RESERVATIONS === 'true';

  try {
    if (DBG) {
      console.log('[reservations] origin =', req.headers.origin || '(none)');
      console.log('[reservations] auth header =', !!req.headers.authorization);
      console.log('[reservations] user =', req.user ? { id: req.user.id, email: req.user.email } : '(none)');
    }

    await cleanupExpired();

    const { numbers } = req.body || {};
    if (!Array.isArray(numbers) || numbers.length === 0) {
      return res.status(400).json({ error: 'no_numbers' });
    }

    const nums = Array.from(
      new Set(
        numbers.map(Number).filter((n) => Number.isInteger(n) && n >= 0 && n <= 99)
      )
    );
    if (!nums.length) return res.status(400).json({ error: 'numbers_invalid' });

    const ttlMin = Number(process.env.RESERVATION_TTL_MIN || 5);

    const dr = await query(
      `select id
         from draws
        where status = 'open'
     order by id desc
        limit 1`
    );
    if (!dr.rows.length) return res.status(400).json({ error: 'no_open_draw' });
    const drawId = dr.rows[0].id;

    await query('BEGIN');

    const check = await query(
      `select n, status
         from numbers
        where draw_id = $1
          and n = any($2)
        for update`,
      [drawId, nums]
    );

    const foundSet = new Set(check.rows.map((r) => r.n));
    const notFound = nums.filter((n) => !foundSet.has(n));
    if (notFound.length) {
      await query('ROLLBACK');
      return res.status(400).json({ error: 'numbers_not_found', numbers: notFound });
    }

    const conflicts = check.rows.filter((r) => r.status !== 'available').map((r) => r.n);
    if (conflicts.length) {
      await query('ROLLBACK');
      return res.status(409).json({ error: 'unavailable', conflicts });
    }

    const reservationId = uuid();
    const expiresAt = new Date(Date.now() + ttlMin * 60 * 1000);

    await query(
      `insert into reservations (id, user_id, draw_id, numbers, status, expires_at)
       values ($1, $2, $3, $4::int[], 'active', $5)`,
      [reservationId, req.user.id, drawId, nums, expiresAt]
    );

    await query(
      `update numbers
          set status = 'reserved',
              reservation_id = $3
        where draw_id = $1
          and n = any($2)`,
      [drawId, nums, reservationId]
    );

    await query('COMMIT');

    if (DBG) {
      console.log('[reservations] created', {
        reservationId,
        userId: req.user.id,
        drawId,
        numbers: nums,
        expiresAt: expiresAt.toISOString(),
      });
    }

    // Também devolvo "id" para facilitar clientes que esperam esse nome
    return res.status(201).json({ reservationId, id: reservationId, drawId, expiresAt, numbers: nums });
  } catch (e) {
    try { await query('ROLLBACK'); } catch {}
    console.error('[reservations] error:', e.code || e.message, e);
    return res.status(500).json({ error: 'reserve_failed' });
  }
});

export default router;
