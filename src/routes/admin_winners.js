// backend/src/routes/admin_winners.js
import { Router } from "express";
import { query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";

const router = Router();

/**
 * GET /api/admin/winners
 * Lista sorteios realizados (realized_at IS NOT NULL)
 */
router.get("/", requireAuth, requireAdmin, async (req, res) => {
  try {
    console.log("[admin/winners] IN", { userId: req.user?.id, at: new Date().toISOString() }); // LOG

    const r = await query(
      `
      select
        d.id                                                as draw_id,
        coalesce(nullif(d.winner_name,''), u.name, u.email, '-') as winner_name,
        d.winner_number,                                    -- <=== NOVO
        d.realized_at,
        d.closed_at
      from public.draws d
      left join public.users u on u.id = d.winner_user_id
      where d.realized_at is not null
      order by d.realized_at desc, d.id desc
      `
    );

    console.log("[admin/winners] rows:", r.rows?.length ?? 0); // LOG

    const now = Date.now();
    const winners = (r.rows || []).map((row) => {
      const realized = row.realized_at ? new Date(row.realized_at) : null;
      const daysSince = realized ? Math.max(0, Math.floor((now - realized.getTime()) / 86400000)) : 0;
      const redeemed = !!row.closed_at;
      return {
        draw_id: row.draw_id,
        winner_name: row.winner_name || "-",
        winner_number: row.winner_number ?? null,   // <=== NOVO
        realized_at: row.realized_at,
        closed_at: row.closed_at,
        redeemed,
        status: redeemed ? "RESGATADO" : "NÃO RESGATADO",
        days_since: daysSince,
      };
    });

    return res.json({ winners });
  } catch (e) {
    console.error("[admin/winners] error:", e);
    return res.status(500).json({ error: "list_failed" });
  }
});

export default router;
