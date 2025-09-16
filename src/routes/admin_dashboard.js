// src/routes/admin_dashboard.js
import { Router } from "express";
import { query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import { getTicketPriceCents, setTicketPriceCents } from "../lib/app_config.js";

const router = Router();

/**
 * GET /api/admin/dashboard/summary
 * Resumo do sorteio atual (aberto) + preço do ticket (em centavos)
 */
router.get("/summary", requireAuth, requireAdmin, async (_req, res) => {
  try {
    const r = await query(`
      with cur as (
        select id
          from draws
         where status = 'open'
         order by id asc
         limit 1
      ),
      counts as (
        select
          n.draw_id,
          count(*) filter (where n.status = 'sold')        as sold,
          count(*) filter (where n.status = 'available')   as available,
          count(*) filter (where n.status = 'reserved')    as reserved
        from numbers n
        join cur on cur.id = n.draw_id
        group by n.draw_id
      )
      select
        coalesce(c.draw_id, 0)                         as draw_id,
        coalesce(c.sold, 0)                            as sold,
        coalesce(c.available, 0)                       as available,
        coalesce(c.reserved, 0)                        as reserved
      from counts c
      right join cur on cur.id = c.draw_id
    `);

    const row = r.rows?.[0] || { draw_id: 0, sold: 0, available: 0, reserved: 0 };
    const priceCents = await getTicketPriceCents();

    const payload = {
      draw_id: Number(row.draw_id) || 0,
      sold: Number(row.sold) || 0,
      remaining: Number(row.available) || 0, // “disponíveis”
      reserved: Number(row.reserved) || 0,
      price_cents: Number(priceCents) || 0,
    };

    console.log("[admin/dashboard/summary] payload:", payload);
    return res.json(payload);
  } catch (e) {
    console.error("[admin/dashboard/summary] error:", e);
    return res.status(500).json({ error: "summary_failed" });
  }
});

/**
 * POST /api/admin/dashboard/price
 * Body: { price_cents: number }
 * Atualiza o valor do ticket em centavos
 */
router.post("/price", requireAuth, requireAdmin, async (req, res) => {
  try {
    const v = req.body?.price_cents;
    const saved = await setTicketPriceCents(v);
    console.log("[admin/dashboard/price] set to:", saved);
    return res.json({ price_cents: saved });
  } catch (e) {
    console.error("[admin/dashboard/price] error:", e);
    return res.status(400).json({ error: "invalid_price" });
  }
});

export default router;
