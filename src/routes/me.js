// src/routes/me.js
import { Router } from 'express';
import { query } from '../db.js';
import { requireAuth } from '../middleware/auth.js';
import { getTicketPriceCents } from '../services/config.js';

const router = Router();

/**
 * GET /api/me
 * Retorna o usuário logado (id, name, email, is_admin).
 */
router.get('/', requireAuth, async (req, res) => {
  try {
    const userId = req.user.id;
    // busca no banco pra garantir dados atualizados
    const r = await query(
      `select id, name, email, phone, is_admin, winner_balance_cents, winner_balance_updated_at
         from users
        where id = $1`,
      [userId]
    );
    const u = r.rows[0] || req.user;
    const user = {
      id: u.id,
      name: u.name || null,
      email: u.email || null,
      phone: u.phone || null,
      is_admin: !!u.is_admin,
    };
    const winnerBalanceCents = u.winner_balance_cents == null ? null : Number(u.winner_balance_cents);
    if (Number.isFinite(winnerBalanceCents) && winnerBalanceCents > 0) {
      user.winner_balance_cents = winnerBalanceCents;
      user.winner_balance_updated_at = u.winner_balance_updated_at || null;
    }

    return res.json({ user });
  } catch (e) {
    console.error('[me] error:', e);
    return res.status(500).json({ error: 'me_failed' });
  }
});

function normalizePhoneInput(value) {
  return String(value || '').replace(/\D/g, '');
}

function isValidBrazilianPhone(phone) {
  if (phone.length === 10 || phone.length === 11) return true;
  return (phone.length === 12 || phone.length === 13) && phone.startsWith('55');
}

/**
 * PATCH /api/me/phone
 * Atualiza somente o telefone do usuario autenticado.
 */
router.patch('/phone', requireAuth, async (req, res) => {
  try {
    const userId = req.user.id;
    const phone = normalizePhoneInput(req.body?.phone);

    if (!isValidBrazilianPhone(phone)) {
      return res.status(400).json({ ok: false, error: 'invalid_phone' });
    }

    const r = await query(
      `update users
          set phone = $2
        where id = $1
        returning id, name, email, phone, is_admin`,
      [userId, phone]
    );

    const u = r.rows[0];
    if (!u) {
      return res.status(404).json({ ok: false, error: 'user_not_found' });
    }

    return res.json({
      ok: true,
      user: {
        id: u.id,
        name: u.name || null,
        email: u.email || null,
        phone: u.phone || null,
        is_admin: !!u.is_admin,
      },
    });
  } catch (e) {
    console.error('[me/phone] error:', e);
    return res.status(500).json({ ok: false, error: 'phone_update_failed' });
  }
});

/**
 * GET /api/me/reservations
 */
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

    const priceCents = await getTicketPriceCents();
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
