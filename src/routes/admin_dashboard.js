// src/routes/admin_dashboard.js
import { Router } from "express";
import { query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import { getTicketPriceCents } from "../services/config.js"; // <- caminho correto

const router = Router();

/**
 * GET /api/admin/dashboard/summary
 * Retorna:
 *  - draw_id: id do sorteio aberto (ou null)
 *  - sold: quantidade vendida
 *  - remaining: quantidade restante
 *  - price_cents: preço atual do ticket (centavos)
 */
router.get("/summary", requireAuth, requireAdmin, async (_req, res) => {
  try {
    // sorteio aberto (se houver)
    const d = await query(
      `select id
         from draws
        where status = 'open'
        order by id asc
        limit 1`
    );

    let drawId = d.rows[0]?.id ?? null;
    let sold = 0;
    let remaining = 0;

    if (drawId != null) {
      const nr = await query(
        `select
             count(*)::int                                           as total,
             count(*) filter (where status = 'sold')::int            as sold
           from numbers
          where draw_id = $1`,
        [drawId]
      );
      const total = nr.rows[0]?.total ?? 0;
      sold = nr.rows[0]?.sold ?? 0;
      remaining = Math.max(0, total - sold);
    }

    const price_cents = await getTicketPriceCents();

    return res.json({
      draw_id: drawId,
      sold,
      remaining,
      price_cents,
    });
  } catch (e) {
    console.error("[admin/dashboard/summary] error:", e);
    return res.status(500).json({ error: "summary_failed" });
  }
});

export default router;
