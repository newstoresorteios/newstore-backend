// src/routes/admin_clients.js
import { Router } from "express";
import { query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";

const router = Router();

/**
 * GET /api/admin/clients/active
 * Lista clientes com saldo ativo (última compra aprovada < 6 meses)
 */
router.get("/active", requireAuth, requireAdmin, async (_req, res) => {
  try {
    const r = await query(
      `
      WITH pays AS (
        SELECT
          p.user_id,
          COUNT(*) FILTER (
            WHERE lower(trim(coalesce(p.status,''))) = 'approved'
          )                                   AS compras,

          -- seu schema usa amount_cents
          COALESCE(
            SUM(p.amount_cents) FILTER (
              WHERE lower(trim(coalesce(p.status,''))) = 'approved'
            ), 0
          )::bigint                           AS total_cents,

          MAX(
            COALESCE(p.paid_at, p.created_at)
          ) FILTER (
            WHERE lower(trim(coalesce(p.status,''))) = 'approved'
          )                                   AS last_buy
        FROM payments p
        GROUP BY p.user_id
      ),
      wins AS (
        SELECT winner_user_id AS user_id, COUNT(*) AS wins
        FROM draws
        WHERE winner_user_id IS NOT NULL
        GROUP BY winner_user_id
      )
      SELECT
        u.id,
        COALESCE(NULLIF(u.name,''), u.email, '-') AS name,
        u.email,
        u.created_at,
        pa.compras,
        pa.total_cents,
        pa.last_buy,
        COALESCE(w.wins, 0)                       AS wins,
        (pa.last_buy + INTERVAL '6 months')::date AS expires_at,
        ((pa.last_buy + INTERVAL '6 months')::date - NOW()::date) AS days_to_expire
      FROM users u
      JOIN pays pa ON pa.user_id = u.id
      LEFT JOIN wins w ON w.user_id = u.id
      -- saldo ativo: última compra aprovada há menos de 6 meses
      WHERE pa.last_buy >= NOW() - INTERVAL '6 months'
      ORDER BY expires_at ASC, pa.total_cents DESC
      `
    );

    const items = (r.rows || []).map((row) => ({
      user_id: row.id,
      name: row.name,
      email: row.email,
      created_at: row.created_at,
      purchases_count: row.compras || 0,
      total_brl: Number(((row.total_cents || 0) / 100).toFixed(2)),
      last_buy: row.last_buy,
      wins: row.wins || 0,
      expires_at: row.expires_at,
      days_to_expire: Math.max(0, Number(row.days_to_expire) || 0),
    }));

    return res.json({ clients: items });
  } catch (e) {
    console.error("[admin/clients/active] error:", e);
    return res.status(500).json({ error: "list_failed" });
  }
});

/**
 * GET /api/admin/clients/:userId/coupon
 * Retorna o cupom atual do usuário a partir da TABELA USERS.
 * Resposta esperada pelo front: { user_id, code, cents }
 */
router.get("/:userId/coupon", requireAuth, requireAdmin, async (req, res) => {
  const userId = Number(req.params.userId);
  if (!Number.isFinite(userId) || userId <= 0) {
    return res.status(400).json({ error: "invalid_user_id" });
  }

  try {
    const r = await query(
      `
      SELECT
        u.coupon_code,
        COALESCE(u.coupon_cents, 0)::bigint AS coupon_cents
      FROM users u
      WHERE u.id = $1
      LIMIT 1
      `,
      [userId]
    );

    if (!r.rowCount) {
      return res.status(404).json({ error: "user_not_found" });
    }

    const row = r.rows[0];
    return res.json({
      user_id: userId,
      code: row.coupon_code || null,
      cents: Number(row.coupon_cents || 0),
    });
  } catch (e) {
    console.error("[admin/clients/:userId/coupon] error:", e);
    return res.status(500).json({ error: "coupon_lookup_failed" });
  }
});

export default router;
