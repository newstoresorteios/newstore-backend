// backend/src/routes/admin_winners.js
import { Router } from "express";
import { query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";

const router = Router();

/**
 * GET /api/admin/winners
 * Lista sorteios com realized_at (já houve vencedor)
 */
router.get("/", requireAuth, requireAdmin, async (_req, res) => {
  try {
    const r = await query(
      `
      select
        d.id                                                as draw_id,
        coalesce(nullif(d.winner_name,''), u.name, u.email, '-') as winner_name,
        d.realized_at,
        d.closed_at
      from draws d
      left join users u on u.id = d.winner_user_id
      where d.realized_at is not null
      order by d.realized_at desc, d.id desc
      `
    );

    const now = Date.now();
    const winners = (r.rows || []).map(row => {
      const realized = row.realized_at ? new Date(row.realized_at) : null;
      const daysSince = realized ? Math.max(0, Math.floor((now - realized.getTime()) / 86400000)) : 0;
      const redeemed = !!row.closed_at;
      return {
        draw_id: row.draw_id,
        winner_name: row.winner_name || "-",
        realized_at: row.realized_at,
        closed_at: row.closed_at,
        redeemed,
        status: redeemed ? "RESGATADO" : "NÃO RESGATADO",
        days_since: daysSince,
      };
    });

    // força 200 + JSON mesmo se vazio (evita 204 em alguma camada)
    return res.status(200).json({ winners });
  } catch (e) {
    console.error("[admin/winners] error:", e);
    return res.status(500).json({ error: "list_failed" });
  }
});

export default router;
