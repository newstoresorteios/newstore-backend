// src/routes/reservations.js
import { Router } from 'express';
import { v4 as uuid } from 'uuid';
import { query } from '../db.js';
import { requireAuth } from '../middleware/auth.js';

const router = Router();

async function cleanupExpired() {
  // Marca reservas expiradas e libera números
  const expired = await query(
    `update reservations
        set status = 'expired'
      where status = 'active'
        and expires_at < now()
      returning id, draw_id, numbers`
  );

  if (expired.rows.length) {
    for (const r of expired.rows) {
      await query(
        `update numbers
            set status = 'available', reservation_id = null
          where draw_id = $1
            and n = any($2)
            and status = 'reserved'
            and reservation_id = $3`,
        [r.draw_id, r.numbers, r.id]
      );
    }
  }
}

router.post('/', requireAuth, async (req, res) => {
  try {
    const DBG = process.env.DEBUG_RESERVATIONS === 'true';

    if (DBG) {
      console.log('[reservations] origin =', req.headers.origin || '(none)');
      console.log('[reservations] authorization present =', Boolean(req.headers.authorization));
      console.log(
        '[reservations] cookie token/jwt present =',
        Boolean(req.cookies && (req.cookies.token || req.cookies.jwt))
      );
      console.log(
        '[reservations] user (JWT) =',
        req.user ? { id: req.user.id, email: req.user.email } : '(none)'
      );
    }

    await cleanupExpired();

    const { numbers } = req.body || {};
    if (DBG) console.log('[reservations] body.numbers =', numbers);

    if (!Array.isArray(numbers) || numbers.length === 0) {
      return res.status(400).json({ error: 'no_numbers' });
    }

    // Normaliza para inteiros
    const nums = numbers
      .map((n) => Number(n))
      .filter((n) => Number.isInteger(n));

    const ttl = Number(process.env.RESERVATION_TTL_MIN || 15);

    // Sorteio aberto mais recente
    const dr = await query(
      `select id from draws where status = 'open' order by id desc limit 1`
    );
    if (!dr.rows.length) return res.status(400).json({ error: 'no_open_draw' });
    const drawId = dr.rows[0].id;

    // Checa disponibilidade dos números
    const checks = await query(
      `select n, status from numbers where draw_id = $1 and n = any($2)`,
      [drawId, nums]
    );
    for (const row of checks.rows) {
      if (row.status !== 'available') {
        if (DBG) {
          console.log('[reservations] número indisponível:', row.n, 'status =', row.status);
        }
        return res.status(409).json({ error: 'unavailable', n: row.n });
      }
    }

    // Cria reserva
    const id = uuid();
    const expiresAt = new Date(Date.now() + ttl * 60 * 1000);

    await query(
      `insert into reservations(id, user_id, draw_id, numbers, expires_at)
       values ($1, $2, $3, $4, $5)`,
      [id, req.user.id, drawId, nums, expiresAt]
    );

    // Marca números como reservados
    await query(
      `update numbers
          set status = 'reserved', reservation_id = $3
        where draw_id = $1 and n = any($2)`,
      [drawId, nums, id]
    );

    if (DBG) {
      console.log('[reservations] created', {
        reservationId: id,
        userId: req.user.id,
        drawId,
        numbers: nums,
        expiresAt: expiresAt.toISOString(),
      });
    }

    return res.json({ reservationId: id, drawId, expiresAt });
  } catch (e) {
    console.error('[reservations] error:', e);
    return res.status(500).json({ error: 'reserve_failed' });
  }
});

export default router;
