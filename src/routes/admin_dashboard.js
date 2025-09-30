// backend/src/routes/admin_dashboard.js
import { Router } from "express";
import { query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import { getTicketPriceCents, setTicketPriceCents } from "../services/config.js";
import { ensureAutopayForDraw } from "../services/autopayRunner.js";

const router = Router();

function log(...a) {
  console.log("[admin/dashboard]", ...a);
}

/**
 * GET /api/admin/dashboard/summary
 * -> { draw_id, sold, remaining, price_cents }
 * Agora conta vendidos a partir de reservations 'paid'
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
    if (current?.id != null) {
      const r = await query(
        `select coalesce(sum(cardinality(numbers)),0)::int as sold
           from reservations
          where draw_id = $1
            and (lower(coalesce(status,'')) = 'paid' or coalesce(paid,false) = true)`,
        [current.id]
      );
      sold = r.rows[0]?.sold ?? 0;
    }
    const remaining = Math.max(0, 100 - sold);

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
 * Roda o AutoPay, mas nunca retorna 500 por causa dele (devolve payload com o resultado).
 */
router.post("/new", requireAuth, requireAdmin, async (_req, res) => {
  try {
    log("POST /new");

    // fecha abertos
    await query(
      `update draws
          set status = 'closed', closed_at = now()
        where status = 'open'`
    );

    // cria novo sorteio
    const ins = await query(
      `insert into draws(status, opened_at) values('open', now())
       returning id`
    );
    const newId = ins.rows[0].id;
    log("novo draw id =", newId);

    // popula tabela auxiliar 'numbers' (usada só no painel)
    const tuples = Array.from({ length: 100 }, (_, i) => `(${newId}, ${i}, 'available', null)`);
    await query(
      `insert into numbers(draw_id, n, status, reservation_id)
       values ${tuples.join(",")}`
    );

    // dispara autopay, mas não deixa o painel quebrar se falhar
    let autopay = null;
    try {
      autopay = await ensureAutopayForDraw(newId, { force: false });
    } catch (e) {
      console.error("[admin/dashboard] autopay exception:", e?.message || e);
      autopay = { ok: false, error: "autopay_exception", message: String(e?.message || e) };
    }

    if (!autopay?.ok) {
      console.warn("[admin/dashboard] autopay_run_failed", autopay);
    }

    // responde 200 sempre com os dados do novo draw e o resultado do autopay
    return res.json({
      ok: true,
      draw_id: newId,
      sold: 0,
      remaining: 100,
      autopay,
    });
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

/** Alias de compatibilidade */
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
