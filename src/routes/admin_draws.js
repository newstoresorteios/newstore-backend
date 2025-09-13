// src/routes/admin_draws.js
import { Router } from "express";
import { query } from "../db.js";
import { requireAuth } from "../middleware/auth.js";

const router = Router();

// pequeno guard de admin (usa req.user.is_admin; ajuste o nome do campo se diferente)
function requireAdmin(req, res, next) {
  try {
    if (req?.user?.is_admin) return next();
    return res.status(403).json({ error: "forbidden" });
  } catch {
    return res.status(403).json({ error: "forbidden" });
  }
}

/**
 * GET /api/admin/draws/history
 * Lista sorteios fechados (ou que tenham closed_at), com datas e vencedor.
 * Campos retornados: id, opened_at, closed_at, realized_at, days_open, winner_name
 */
router.get("/history", requireAuth, requireAdmin, async (_req, res) => {
  try {
    const r = await query(
      `
      select
        d.id,
        d.status,
        coalesce(d.opened_at, d.created_at)           as opened_at,
        d.closed_at,
        d.realized_at,
        -- dias aberto: de opened_at (ou created_at) até closed_at (ou now se faltar)
        round(
          extract(epoch from (coalesce(d.closed_at, now()) - coalesce(d.opened_at, d.created_at)))
          / 86400.0
        )::int                                        as days_open,
        coalesce(d.winner_name, u.name, u.email, '-') as winner_name
      from draws d
      left join users u on u.id = d.winner_user_id
      where d.status = 'closed' or d.closed_at is not null
      order by d.id desc
      `
    );

    return res.json({ history: r.rows || [] });
  } catch (e) {
    console.error("[admin/draws/history] error:", e);
    return res.status(500).json({ error: "list_failed" });
  }
});

export default router;
