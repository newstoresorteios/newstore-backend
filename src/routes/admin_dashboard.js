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
 * - "remaining" = total de números cadastrados - sold
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
          AND COALESCE(draw_type, 'principal') = 'principal'
        ORDER BY opened_at DESC NULLS LAST,
                 created_at DESC NULLS LAST,
                 id DESC
        LIMIT 1`
    );
    const current = d.rows[0] || null;

    const price_cents = await getTicketPriceCents();

    if (!current?.id) {
      return res.json({
        draw_id: null,
        total: 0,
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
          SUM(CASE WHEN status = 'available' THEN 1 ELSE 0 END)::int AS available_numbers,
          COUNT(*)::int AS total_numbers
          FROM numbers
         WHERE draw_id = $1
      )
      SELECT
        (SELECT COUNT(*)::int FROM approved)        AS sold_by_payments,
        (SELECT sold_numbers       FROM nums)       AS sold_by_numbers,
        (SELECT available_numbers  FROM nums)       AS available_by_numbers,
        (SELECT total_numbers      FROM nums)       AS total_numbers
      `,
      [current.id]
    );

    const row = agg.rows[0] || {};
    const sold_by_payments     = Number(row.sold_by_payments || 0);
    const sold_by_numbers      = Number(row.sold_by_numbers  || 0);
    const available_by_numbers = Number(row.available_by_numbers || 0);
    const total = Number(row.total_numbers || 0);

    // contador exibido: somente aprovados
    const sold = sold_by_payments;
    const remaining = Math.max(0, total - sold);

    return res.json({
      draw_id: current.id,
      total,
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
 * Fecha sorteios principais 'open', cria um novo principal e popula os numeros.
 * e DISPARA o Autopay oficial (services/autopayRunner.js).
 */
router.post("/new", requireAuth, requireAdmin, async (req, res) => {
  try {
    log("POST /new");
    const numberCount = Number(req.body?.number_count ?? 100);
    if (!Number.isInteger(numberCount) || numberCount <= 0 || numberCount > 10000) {
      return res.status(400).json({ error: "invalid_number_count" });
    }

    // fecha os abertos anteriores
    await query(
      `update draws
          set status = 'closed', closed_at = now()
        where status = 'open'
          and COALESCE(draw_type, 'principal') = 'principal'`
    );

    // cria draw novo
    const ins = await query(
      `insert into draws(status, draw_type, opened_at, autopay_ran_at)
       values('open', 'principal', now(), null)
       returning id`
    );
    const newId = ins.rows[0].id;
    log("novo draw id =", newId);

    // popula numeros do sorteio principal
    await query(
      `insert into numbers(draw_id, n, status, reservation_id)
       select $1, gs::int, 'available', null
         from generate_series(0, $2::int - 1) as gs`,
      [newId, numberCount]
    );

    // dispara o AUTOPAY oficial — gera logs [autopayRunner]
    const autopay = await runAutopayForDraw(newId);

    // resposta inclui o resultado do autopay para depuração
    if (!autopay?.ok) {
      console.warn("[admin/dashboard] autopay falhou", autopay);
      return res.status(500).json({ ok: false, draw_id: newId, sold: 0, remaining: numberCount, autopay });
    }

    return res.json({ ok: true, draw_id: newId, sold: 0, remaining: numberCount, autopay });
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

// === NOVO: compradores do sorteio aberto (apenas payments aprovados)
router.get("/open-buyers", requireAuth, requireAdmin, async (_req, res) => {
  try {
    const d = await query(
      `SELECT id
         FROM draws
        WHERE status = 'open'
          AND COALESCE(draw_type, 'principal') = 'principal'
        ORDER BY opened_at DESC NULLS LAST,
                 created_at DESC NULLS LAST,
                 id DESC
        LIMIT 1`
    );
    const cur = d.rows[0];
    if (!cur?.id) {
      return res.json({
        draw_id: null,
        total: 0,
        sold: 0,
        remaining: 100,
        buyers: [],
        numbers: [],
      });
    }

    // Agregado por comprador
    const sql = `
      WITH p_ok AS (
        SELECT p.user_id, p.numbers, p.amount_cents::int AS amount_cents, p.paid_at
          FROM payments p
         WHERE p.draw_id = $1
           AND lower(p.status) IN ('approved','paid','pago')
      ),
      unn AS (
        SELECT user_id, unnest(numbers)::int AS n
          FROM p_ok
      ),
      per_user AS (
        SELECT u.user_id,
               array_agg(DISTINCT u.n ORDER BY u.n) AS numbers,
               COUNT(DISTINCT u.n)::int            AS count
          FROM unn u
         GROUP BY u.user_id
      ),
      totals AS (
        SELECT user_id,
               COALESCE(SUM(amount_cents),0)::int AS total_cents,
               MAX(paid_at)                       AS last_paid_at
          FROM p_ok
         GROUP BY user_id
      ),
      taken AS ( SELECT DISTINCT n FROM unn )
      SELECT
        (SELECT COUNT(*)::int FROM taken) AS sold_approved,
        (SELECT COUNT(*)::int FROM numbers WHERE draw_id = $1) AS total_numbers,
        json_agg(
          json_build_object(
            'user_id', pu.user_id,
            'name',    COALESCE(us.name, us.email),      -- << apenas colunas existentes
            'email',   us.email,
            'numbers', pu.numbers,
            'count',   pu.count,
            'total_cents', COALESCE(t.total_cents,0),
            'last_paid_at', t.last_paid_at
          )
          ORDER BY lower(COALESCE(us.name, us.email, ''))
        ) FILTER (WHERE pu.user_id IS NOT NULL) AS buyers_json
      FROM per_user pu
      LEFT JOIN totals t ON t.user_id = pu.user_id
      LEFT JOIN users  us ON us.id     = pu.user_id
    `;
    const agg = await query(sql, [cur.id]);
    const sold = Number(agg.rows[0]?.sold_approved || 0);
    const total = Number(agg.rows[0]?.total_numbers || 0);
    const buyers = agg.rows[0]?.buyers_json || [];

    // Mapa número -> comprador
    const nums = await query(
      `
      WITH p_ok AS (
        SELECT p.user_id, p.numbers
          FROM payments p
         WHERE p.draw_id = $1
           AND lower(p.status) IN ('approved','paid','pago')
      ),
      unn AS ( SELECT user_id, unnest(numbers)::int AS n FROM p_ok )
      SELECT u.n,
             us.id   AS user_id,
             COALESCE(us.name, us.email) AS name,  -- << apenas colunas existentes
             us.email
        FROM unn u
        LEFT JOIN users us ON us.id = u.user_id
       ORDER BY u.n
      `,
      [cur.id]
    );

    return res.json({
      draw_id: cur.id,
      total,
      sold,
      remaining: Math.max(0, total - sold),
      buyers,
      numbers: nums.rows || [],
    });
  } catch (e) {
    console.error("[admin/dashboard] /open-buyers error:", e);
    return res.status(500).json({ error: "open_buyers_failed" });
  }
});


export default router;
