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
 * -> { draw_id, sold, remaining, price_cents, sold_by_payments, sold_by_numbers, available_by_numbers }
 *
 * Agora:
 * - "sold" = quantidade de números vendidos APENAS por payments aprovados (approved/paid/pago)
 * - "remaining" = 100 - sold
 * Mantive também contagens da tabela numbers como campos auxiliares (debug).
 */
router.get("/summary", requireAuth, requireAdmin, async (_req, res) => {
  try {
    console.log("[admin/dashboard] GET /summary");

    // sorteio aberto mais recente
    const d = await query(
      `SELECT id, opened_at
         FROM draws
        WHERE status = 'open'
        ORDER BY id DESC
        LIMIT 1`
    );
    const current = d.rows[0] || null;

    const price_cents = await getTicketPriceCents();

    if (!current?.id) {
      return res.json({
        draw_id: null,
        sold: 0,
        remaining: 0,
        price_cents,
        sold_by_payments: 0,
        sold_by_numbers: 0,
        available_by_numbers: 0,
      });
    }

    // 1) vendidos por payments aprovados (distinct em payments.numbers)
    // 2) métricas da tabela numbers (mantidas para diagnóstico)
    const agg = await query(
      `
      WITH approved AS (
        SELECT DISTINCT t.n
          FROM payments p
          CROSS JOIN LATERAL unnest(p.numbers) AS t(n)
         WHERE p.draw_id = $1
           AND lower(p.status) IN ('approved','paid','pago')
      ),
      nums AS (
        SELECT
          SUM(CASE WHEN status = 'sold'      THEN 1 ELSE 0 END)::int AS sold_numbers,
          SUM(CASE WHEN status = 'available' THEN 1 ELSE 0 END)::int AS available_numbers
          FROM numbers
         WHERE draw_id = $1
      )
      SELECT
        (SELECT COUNT(*)::int FROM approved)        AS sold_by_payments,
        (SELECT sold_numbers       FROM nums)       AS sold_by_numbers,
        (SELECT available_numbers  FROM nums)       AS available_by_numbers
      `,
      [current.id]
    );

    const row = agg.rows[0] || {};
    const sold_by_payments   = Number(row.sold_by_payments || 0);
    const sold_by_numbers    = Number(row.sold_by_numbers  || 0);
    const available_numbers  = Number(row.available_by_numbers || 0);

    // contador exibido: somente aprovados
    const sold = sold_by_payments;
    const remaining = Math.max(0, 100 - sold);

    return res.json({
      draw_id: current.id,
      sold,
      remaining,
      price_cents,
      // campos extras para conferência/depuração (não usados pelo front)
      sold_by_payments,
      sold_by_numbers,
      available_by_numbers,
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
