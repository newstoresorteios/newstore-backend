// src/routes/admin_dashboard.js
import { Router } from "express";
import { query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import { getTicketPriceCents, setTicketPriceCents } from "../config.js";

const router = Router();

/**
 * GET /api/admin/dashboard/summary
 * - draw_id aberto
 * - total, sold, remaining
 * - price_cents (preço atual do ticket)
 */
router.get("/summary", requireAuth, requireAdmin, async (_req, res) => {
  try {
    console.log("[admin/dashboard/summary] IN");
    const r = await query(`
      with d as (
        select id
          from draws
         where lower(coalesce(status,'')) = 'open'
         order by opened_at desc nulls last, id desc
         limit 1
      ),
      c as (
        select
          (select id from d)                           as draw_id,
          count(*)::int                                as total,
          count(*) filter (
            where lower(coalesce(n.status,'')) in ('sold','paid','approved','completed')
          )::int                                       as sold,
          count(*) filter (
            where lower(coalesce(n.status,'')) in ('available','free','open')
          )::int                                       as available
        from numbers n
        join d on n.draw_id = d.id
      )
      select * from c
    `);

    const row = r.rows?.[0] || null;
    const price_cents = getTicketPriceCents();

    if (!row || row.draw_id == null) {
      return res.json({ draw_id: null, total: 0, sold: 0, remaining: 0, price_cents });
    }
    const remaining = Math.max(0, (row.available ?? (row.total - row.sold)));

    return res.json({
      draw_id: row.draw_id,
      total: row.total,
      sold: row.sold,
      remaining,
      price_cents,
    });
  } catch (e) {
    console.error("[admin/dashboard/summary] error:", e);
    return res.status(500).json({ error: "summary_failed" });
  }
});

/**
 * GET /api/admin/dashboard/ticket-price
 * PUT /api/admin/dashboard/ticket-price  { price_cents }
 */
router.get("/ticket-price", requireAuth, requireAdmin, (_req, res) => {
  return res.json({ price_cents: getTicketPriceCents() });
});

router.put("/ticket-price", requireAuth, requireAdmin, async (req, res) => {
  try {
    const cents = setTicketPriceCents(req.body?.price_cents);
    return res.json({ ok: true, price_cents: cents });
  } catch (e) {
    console.error("[admin/dashboard/ticket-price] error:", e?.message || e);
    return res.status(400).json({ error: "invalid_price" });
  }
});

export default router;
