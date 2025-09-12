import { Router } from 'express';
import { query } from '../db.js';
import { requireAuth } from '../middleware/auth.js';

const router = Router();

router.get('/reservations', requireAuth, async (req, res) => {
  try {
    const userId = req.user.id;
    const r = await query(
      `select id, draw_id, numbers, status, created_at, expires_at
         from reservations
        where user_id = $1
        order by created_at desc`,
      [userId]
    );

    const priceCents = Number(process.env.PRICE_CENTS || 5500);
    const reservations = r.rows.map(row => ({
      id: row.id,
      draw_id: row.draw_id,
      numbers: row.numbers,
      amount_cents: (Array.isArray(row.numbers) ? row.numbers.length : 0) * priceCents,
      status: row.status,
      created_at: row.created_at,
      expires_at: row.expires_at
    }));

    res.json({ reservations });
  } catch (e) {
    console.error('[me/reservations] error:', e);
    res.status(500).json({ error: 'me_list_failed' });
  }
});

export default router;
