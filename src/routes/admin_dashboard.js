// backend/src/routes/admin_dashboard.js
import { Router } from "express";
import { query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import { getTicketPriceCents, setTicketPriceCents } from "../services/config.js";
import { runAutopayForDraw } from "../services/autopayRunner.js";

const router = Router();

function log(...a) {
  console.log("[admin/dashboard]", ...a);
}

/**
 * GET /api/admin/dashboard/summary
 * -> { draw_id, sold, remaining, price_cents }
 */
router.get("/summary", requireAuth, requireAdmin, async (_req, res) => {
  try {
    log("GET /summary");

    const d = await query(
      `select id, opened_at
         from draws
        where status = 'open'
        order by id desc
        limit 1`
    );
    const current = d.rows[0] || null;

    let sold = 0;
    let remaining = 0;

    if (current?.id != null) {
      const r = await query(
        `select
           sum(case when status = 'sold' then 1 else 0 end)::int as sold,
           sum(case when status = 'available' then 1 else 0 end)::int as available
         from numbers
        where draw_id = $1`,
        [current.id]
      );
      sold = r.rows[0]?.sold ?? 0;
      remaining = r.rows[0]?.available ?? 0;
    }

    const price_cents = await getTicketPriceCents();

    return res.json({
      draw_id: current?.id ?? null,
      sold,
      remaining,
      price_cents,
    });
  } catch (e) {
    console.error("[admin/dashboard] /summary error:", e);
    return res.status(500).json({ error: "summary_failed" });
  }
});

/**
 * POST /api/admin/dashboard/new
 * Fecha sorteios 'open', cria um novo, popula 0..99 'available'
 * e DISPARA o Autopay oficial (services/autopayRunner.js).
 */
router.post("/new", requireAuth, requireAdmin, async (_req, res) => {
  try {
    log("POST /new");

    // fecha os abertos anteriores
    await query(
      `update draws
          set status = 'closed', closed_at = now()
        where status = 'open'`
    );

    // cria draw novo
    const ins = await query(
      `insert into draws(status, opened_at, autopay_ran_at)
       values('open', now(), null)
       returning id`
    );
    const newId = ins.rows[0].id;
    log("novo draw id =", newId);

    // popula números 00..99
    const tuples = Array.from({ length: 100 }, (_, i) => `(${newId}, ${i}, 'available', null)`);
    await query(
      `insert into numbers(draw_id, n, status, reservation_id)
       values ${tuples.join(",")}`
    );

    // dispara o AUTOPAY oficial — gera logs [autopayRunner]
    const autopay = await runAutopayForDraw(newId);

    // resposta inclui o resultado do autopay para depuração
    if (!autopay?.ok) {
      console.warn("[admin/dashboard] autopay falhou", autopay);
      return res.status(500).json({ ok: false, draw_id: newId, sold: 0, remaining: 100, autopay });
    }

    return res.json({ ok: true, draw_id: newId, sold: 0, remaining: 100, autopay });
  } catch (e) {
    console.error("[admin/dashboard] /new error:", e);
    return res.status(500).json({ error: "new_draw_failed" });
  }
});

/**
 * POST /api/admin/dashboard/price
 * Body: { price_cents }
 */
router.post("/price", requireAuth, requireAdmin, async (req, res) => {
  try {
    const saved = await setTicketPriceCents(req.body?.price_cents);
    return res.json({ ok: true, price_cents: saved });
  } catch (e) {
    console.error("[admin/dashboard] /price error:", e);
    return res.status(400).json({ error: "invalid_price" });
  }
});

/**
 * Alias: POST /api/admin/dashboard/ticket-price
 */
router.post("/ticket-price", requireAuth, requireAdmin, async (req, res) => {
  try {
    const saved = await setTicketPriceCents(req.body?.price_cents);
    return res.json({ ok: true, price_cents: saved });
  } catch (e) {
    console.error("[admin/dashboard] /ticket-price error:", e);
    return res.status(400).json({ error: "invalid_price" });
  }
});

export default router;
