// src/routes/admin_dashboard.js
import { Router } from "express";
import { query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import { getTicketPriceCents, setTicketPriceCents } from "../services/config.js";

const router = Router();
const log = (...a) => console.log("[admin/dashboard]", ...a);

/** GET /api/admin/dashboard/summary */
router.get("/summary", requireAuth, requireAdmin, async (_req, res) => {
  try {
    log("GET /summary");
    const d = await query(`
      select id, opened_at
        from draws
       where status='open'
    order by id desc limit 1`);
    const cur = d.rows[0] || null;

    let sold = 0, remaining = 0;
    if (cur?.id != null) {
      const r = await query(`
        select
          sum(case when status='sold' then 1 else 0 end)::int as sold,
          sum(case when status='available' then 1 else 0 end)::int as available
          from numbers where draw_id=$1`, [cur.id]);
      sold = r.rows[0]?.sold ?? 0;
      remaining = r.rows[0]?.available ?? 0;
    }
    const price_cents = await getTicketPriceCents();
    return res.json({ draw_id: cur?.id ?? null, sold, remaining, price_cents });
  } catch (e) {
    console.error("[admin/dashboard] /summary error:", e);
    return res.status(500).json({ error: "summary_failed" });
  }
});

/** POST /api/admin/dashboard/new */
router.post("/new", requireAuth, requireAdmin, async (_req, res) => {
  try {
    log("POST /new");
    await query(`update draws set status='closed', closed_at=now() where status='open'`);
    const ins = await query(
      `insert into draws(status, opened_at) values('open', now()) returning id`
    );
    const newId = ins.rows[0].id;
    const tuples = [];
    for (let i = 0; i < 100; i++) tuples.push(`(${newId}, ${i}, 'available', null)`);
    await query(`insert into numbers(draw_id, n, status, reservation_id) values ${tuples.join(",")}`);
    return res.json({ draw_id: newId, sold: 0, remaining: 100 });
  } catch (e) {
    console.error("[admin/dashboard] /new error:", e);
    return res.status(500).json({ error: "new_draw_failed" });
  }
});

/** POST /api/admin/dashboard/price  Body:{price_cents} */
router.post("/price", requireAuth, requireAdmin, async (req, res) => {
  try {
    const raw = req.body?.price_cents;
    log("POST /price", raw);
    const saved = await setTicketPriceCents(raw);
    return res.json({ ok: true, price_cents: saved });
  } catch (e) {
    console.error("[admin/dashboard] /price error:", e);
    return res.status(400).json({ error: "invalid_price" });
  }
});

export default router;
