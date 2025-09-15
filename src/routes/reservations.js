// src/routes/reservations.js
import { Router } from 'express';
import { v4 as uuid } from 'uuid';
import { query } from '../db.js';
import { requireAuth } from '../middleware/auth.js';

const router = Router();

/**
 * Expira reservas vencidas (best-effort, fora da transação principal).
 * Mantido para “limpeza geral”, mas a expiração crítica também é feita
 * novamente dentro da transação ao reservar (garante consistência).
 */
async function cleanupExpiredGlobal() {
  const expired = await query(
    `UPDATE reservations
        SET status = 'expired'
      WHERE status = 'active'
        AND expires_at IS NOT NULL
        AND expires_at < NOW()
      RETURNING id, draw_id, numbers`
  );

  for (const r of expired.rows) {
    await query(
      `UPDATE numbers
          SET status = 'available',
              reservation_id = NULL
        WHERE draw_id = $1
          AND n = ANY($2)
          AND reservation_id = $3`,
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
      console.log(
        '[reservations] user =',
        req.user ? { id: req.user.id, email: req.user.email } : '(none)'
      );
    }

    // limpeza “background” (não bloqueia o request)
    try { cleanupExpiredGlobal(); } catch {}

    const { numbers } = req.body || {};
    if (!Array.isArray(numbers) || numbers.length === 0) {
      return res.status(400).json({ error: 'no_numbers' });
    }

    // normaliza números
    const nums = Array.from(
      new Set(
        numbers.map(Number).filter((n) => Number.isInteger(n) && n >= 0 && n <= 99)
      )
    );
    if (!nums.length) return res.status(400).json({ error: 'numbers_invalid' });

    const ttlMin = Number(process.env.RESERVATION_TTL_MIN || 5);

    // draw aberto
    const dr = await query(
      `SELECT id
         FROM draws
        WHERE status = 'open'
     ORDER BY id DESC
        LIMIT 1`
    );
    if (!dr.rows.length) return res.status(400).json({ error: 'no_open_draw' });
    const drawId = dr.rows[0].id;

    // === INÍCIO TX ===========================================================
    await query('BEGIN');

    // 1) Lock nos números alvo
    const check = await query(
      `SELECT n, status, reservation_id
         FROM numbers
        WHERE draw_id = $1
          AND n = ANY($2)
        FOR UPDATE`,
      [drawId, nums]
    );

    // valida existência
    const foundSet = new Set(check.rows.map((r) => r.n));
    const notFound = nums.filter((n) => !foundSet.has(n));
    if (notFound.length) {
      await query('ROLLBACK');
      return res.status(400).json({ error: 'numbers_not_found', numbers: notFound });
    }

    // 2) Para cada número “reserved”, se a reserva estiver vencida, libera AGORA
    const byResId = new Map(); // agrupa números por reservation_id para liberar em lote
    for (const row of check.rows) {
      if (row.status === 'reserved' && row.reservation_id) {
        const rid = row.reservation_id;
        // lock na reserva para leitura consistente
        const rsv = await query(
          `SELECT id, status, expires_at
             FROM reservations
            WHERE id = $1
            FOR UPDATE`,
          [rid]
        );

        const r = rsv.rows[0];
        if (r) {
          const isActive = String(r.status).toLowerCase() === 'active';
          const isExpired =
            r.expires_at && new Date(r.expires_at).getTime() <= Date.now();

          if (isActive && isExpired) {
            // expira a reserva e libera os números que ela segurava
            await query(`UPDATE reservations SET status = 'expired' WHERE id = $1`, [rid]);

            if (!byResId.has(rid)) byResId.set(rid, []);
            byResId.get(rid).push(row.n);
          }
        }
      }
    }

    // libera números presos por reservas expiradas (em lote por reservation_id)
    for (const [rid, numsOfRid] of byResId) {
      await query(
        `UPDATE numbers
            SET status = 'available',
                reservation_id = NULL
          WHERE draw_id = $1
            AND n = ANY($2)
            AND reservation_id = $3`,
        [drawId, numsOfRid, rid]
      );
    }

    // 3) Números tomados por pagamento aprovado
    const pays = await query(
      `SELECT numbers
         FROM payments
        WHERE draw_id = $1
          AND lower(status) IN ('approved','paid','pago')`,
      [drawId]
    );
    const paidTaken = new Set();
    for (const p of pays.rows || []) {
      for (const n of p.numbers || []) paidTaken.add(Number(n));
    }

    // 4) Revalida os números (após possíveis liberações) e detecta conflitos
    const after = await query(
      `SELECT n, status, reservation_id
         FROM numbers
        WHERE draw_id = $1
          AND n = ANY($2)
        FOR UPDATE`,
      [drawId, nums]
    );

    const conflicts = [];
    for (const row of after.rows) {
      // conflito se já “sold/taken/reserved” OU pago aprovado
      const st = String(row.status).toLowerCase();
      const isBusy =
        st !== 'available' || paidTaken.has(Number(row.n));
      if (isBusy) conflicts.push(row.n);
    }

    if (conflicts.length) {
      await query('ROLLBACK');
      return res.status(409).json({ error: 'unavailable', conflicts });
    }

    // 5) Cria reserva e marca números como reserved
    const reservationId = uuid();
    const expiresAt = new Date(Date.now() + ttlMin * 60 * 1000);

    await query(
      `INSERT INTO reservations (id, user_id, draw_id, numbers, status, expires_at)
       VALUES ($1, $2, $3, $4::int[], 'active', $5)`,
      [reservationId, req.user.id, drawId, nums, expiresAt]
    );

    await query(
      `UPDATE numbers
          SET status = 'reserved',
              reservation_id = $3
        WHERE draw_id = $1
          AND n = ANY($2)`,
      [drawId, nums, reservationId]
    );

    await query('COMMIT');
    // === FIM TX ==============================================================

    if (DBG) {
      console.log('[reservations] created', {
        reservationId,
        userId: req.user.id,
        drawId,
        numbers: nums,
        expiresAt: expiresAt.toISOString(),
      });
    }

    return res
      .status(201)
      .json({ reservationId, id: reservationId, drawId, expiresAt, numbers: nums });
  } catch (e) {
    try { await query('ROLLBACK'); } catch {}
    console.error('[reservations] error:', e.code || e.message, e);
    return res.status(500).json({ error: 'reserve_failed' });
  }
});

export default router;
