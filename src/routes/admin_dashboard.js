// backend/src/routes/admin_dashboard.js
import { Router } from "express";
import { getPool, query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import { getTicketPriceCents, setTicketPriceCents } from "../services/config.js";
import { runAutopayForDraw } from "../services/autopayRunner.js";
import {
  createCaptivePreAuthorizationsForDraw,
  isCaptivePreauthEnabled,
  resolveCaptivePreauthDrawRequirement,
} from "../services/autopay/captivePreauthService.js";
import { handlePushAutomationEvent } from "../services/notifications/pushAutomationEvents.js";

const router = Router();

function log(...a) {
  console.log("[admin/dashboard]", ...a);
}

function normalizePrincipalConfigPayload(body = {}) {
  if (
    body.ticket_price_cents === undefined ||
    body.banner_title === undefined ||
    body.max_numbers_per_selection === undefined
  ) {
    return { error: "principal_config_fields_required" };
  }

  const ticketPriceCents = Number(body.ticket_price_cents);
  if (!Number.isInteger(ticketPriceCents) || ticketPriceCents <= 0) {
    return { error: "invalid_ticket_price_cents" };
  }
  if (typeof body.banner_title !== "string") {
    return { error: "invalid_banner_title" };
  }
  const bannerTitle = body.banner_title.trim();
  if (bannerTitle.length > 255) {
    return { error: "invalid_banner_title" };
  }
  const maxNumbersPerSelection = Number(body.max_numbers_per_selection);
  if (!Number.isInteger(maxNumbersPerSelection) || maxNumbersPerSelection <= 0) {
    return { error: "invalid_max_numbers_per_selection" };
  }
  return {
    value: {
      ticket_price_cents: ticketPriceCents,
      banner_title: bannerTitle,
      max_numbers_per_selection: maxNumbersPerSelection,
    },
  };
}

function normalizePersistedPrincipalConfig(globalRows, drawRow) {
  const global = new Map((globalRows || []).map((row) => [String(row.key), row.value]));
  return {
    global: {
      ticket_price_cents: Number(global.get("ticket_price_cents")),
      banner_title: String(global.get("banner_title") ?? "").trim(),
      max_numbers_per_selection: Number(global.get("max_numbers_per_selection")),
    },
    draw: {
      ticket_price_cents: Number(drawRow?.ticket_price_cents),
      banner_title: String(drawRow?.banner_title ?? "").trim(),
      max_numbers_per_selection: Number(drawRow?.max_numbers_per_selection),
    },
  };
}

function samePrincipalConfig(left, right) {
  return (
    left.ticket_price_cents === right.ticket_price_cents &&
    left.banner_title === right.banner_title &&
    left.max_numbers_per_selection === right.max_numbers_per_selection
  );
}

export function createPrincipalDrawConfigHandler(options = {}) {
  const getPoolFn = options.getPoolFn || getPool;
  return async function savePrincipalDrawConfig(req, res) {
    const drawId = Number(req.params.drawId);
    if (!Number.isInteger(drawId) || drawId <= 0) {
      return res.status(400).json({ error: "invalid_draw_id" });
    }
    const normalized = normalizePrincipalConfigPayload(req.body || {});
    if (normalized.error) return res.status(400).json({ error: normalized.error });

    const fields = ["ticket_price_cents", "banner_title", "max_numbers_per_selection"];
    console.log("[admin-draw-config] update_started", { draw_id: drawId, fields });

    let client;
    let transactionOpen = false;
    try {
      const pool = await getPoolFn();
      client = await pool.connect();
      await client.query("BEGIN");
      transactionOpen = true;

      const drawResult = await client.query(
        `SELECT id, status, draw_type
           FROM public.draws
          WHERE id = $1
          FOR UPDATE`,
        [drawId]
      );
      const draw = drawResult.rows?.[0] || null;
      if (!draw) {
        await client.query("ROLLBACK");
        transactionOpen = false;
        console.warn("[admin-draw-config] update_rolled_back", { draw_id: drawId, reason: "draw_not_found" });
        return res.status(404).json({ error: "draw_not_found" });
      }
      if (String(draw.draw_type || "principal").trim().toLowerCase() !== "principal") {
        await client.query("ROLLBACK");
        transactionOpen = false;
        console.warn("[admin-draw-config] update_rolled_back", { draw_id: drawId, reason: "principal_draw_required" });
        return res.status(400).json({ error: "principal_draw_required" });
      }

      const currentPriceResult = await client.query(
        `SELECT value
           FROM public.app_config
          WHERE key = 'ticket_price_cents'
          LIMIT 1`
      );
      const currentPrice = Number(
        currentPriceResult.rows?.[0]?.value ?? process.env.PRICE_CENTS ?? 5500
      );
      if (normalized.value.ticket_price_cents !== currentPrice) {
        const activityResult = await client.query(
          `SELECT
             EXISTS (SELECT 1 FROM public.payments WHERE draw_id = $1) AS has_payment,
             EXISTS (
               SELECT 1 FROM public.reservations
                WHERE draw_id = $1
                  AND lower(coalesce(status, '')) IN ('active', 'pending', 'reserved', 'paid')
             ) AS has_active_reservation,
             EXISTS (
               SELECT 1 FROM public.numbers
                WHERE draw_id = $1
                  AND lower(coalesce(status, 'available')) IN ('sold', 'reserved')
             ) AS has_sold_or_reserved_number,
             EXISTS (SELECT 1 FROM public.autopay_draw_authorizations WHERE draw_id = $1) AS has_preauthorization,
             EXISTS (SELECT 1 FROM public.autopay_runs WHERE draw_id = $1) AS has_autopay_run`,
          [drawId]
        );
        const activity = activityResult.rows?.[0] || {};
        if (Object.values(activity).some(Boolean)) {
          console.warn("[admin-draw-config] ticket_price_locked", { draw_id: drawId });
          await client.query("ROLLBACK");
          transactionOpen = false;
          console.warn("[admin-draw-config] update_rolled_back", { draw_id: drawId, reason: "draw_ticket_price_locked" });
          return res.status(409).json({
            error: "draw_ticket_price_locked",
            message: "O valor da cota não pode ser alterado após o início das vendas.",
          });
        }
      }

      const config = normalized.value;
      await client.query(
        `INSERT INTO public.app_config (key, value, updated_at)
         SELECT entry.key, entry.value, NOW()
           FROM unnest($1::text[], $2::text[]) AS entry(key, value)
         ON CONFLICT (key) DO UPDATE
           SET value = EXCLUDED.value,
               updated_at = NOW()`,
        [
          ["ticket_price_cents", "banner_title", "max_numbers_per_selection"],
          [String(config.ticket_price_cents), config.banner_title, String(config.max_numbers_per_selection)],
        ]
      );
      await client.query(
        `INSERT INTO public.app_config_new AS cfg
           (id, banner_title, ticket_price_cents, max_numbers_per_selection)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (id) DO UPDATE
           SET banner_title = EXCLUDED.banner_title,
               ticket_price_cents = EXCLUDED.ticket_price_cents,
               max_numbers_per_selection = EXCLUDED.max_numbers_per_selection`,
        [String(drawId), config.banner_title, config.ticket_price_cents, config.max_numbers_per_selection]
      );

      const [globalResult, individualResult] = await Promise.all([
        client.query(
          `SELECT key, value
             FROM public.app_config
            WHERE key = ANY($1::text[])`,
          [["ticket_price_cents", "banner_title", "max_numbers_per_selection"]]
        ),
        client.query(
          `SELECT banner_title, ticket_price_cents, max_numbers_per_selection
             FROM public.app_config_new
            WHERE id = $1
            LIMIT 1`,
          [String(drawId)]
        ),
      ]);
      const persisted = normalizePersistedPrincipalConfig(
        globalResult.rows,
        individualResult.rows?.[0]
      );
      if (
        !samePrincipalConfig(persisted.global, config) ||
        !samePrincipalConfig(persisted.draw, config)
      ) {
        const error = new Error("draw_config_sync_failed");
        error.code = "draw_config_sync_failed";
        throw error;
      }

      await client.query("COMMIT");
      transactionOpen = false;
      console.log("[admin-draw-config] update_committed", {
        draw_id: drawId,
        sync: { global: true, draw: true },
      });
      return res.json({
        ok: true,
        draw: {
          id: Number(draw.id),
          status: draw.status,
          draw_type: draw.draw_type || "principal",
        },
        config,
        sync: { global: true, draw: true },
      });
    } catch (error) {
      if (transactionOpen && client) {
        try { await client.query("ROLLBACK"); } catch {}
      }
      console.warn("[admin-draw-config] update_rolled_back", {
        draw_id: drawId,
        reason: error?.code || error?.message || "principal_config_update_failed",
      });
      const syncFailed = error?.code === "draw_config_sync_failed";
      return res.status(500).json({
        error: syncFailed ? "draw_config_sync_failed" : "principal_config_update_failed",
      });
    } finally {
      if (client) client.release();
    }
  };
}

async function createCaptivePreauthIfEnabled(drawId, adminUserId, context, amountCents) {
  if (!isCaptivePreauthEnabled()) {
    return { ok: true, skipped: true, reason: "captive_preauth_disabled" };
  }
  try {
    const requirement = await resolveCaptivePreauthDrawRequirement(drawId, { amountCents });
    if (!requirement.required) {
      return {
        ok: true,
        skipped: true,
        reason: "amount_not_above_default",
        ...requirement,
      };
    }
    return await createCaptivePreAuthorizationsForDraw(drawId, { adminUserId, amountCents });
  } catch (error) {
    console.error(`[${context}] captive preauth failed`, {
      draw_id: drawId,
      admin_user_id: adminUserId || null,
      message: error?.message || null,
      code: error?.code || null,
    });
    return {
      ok: false,
      error: error?.message || "captive_preauth_create_failed",
      code: error?.code || null,
    };
  }
}

async function emitAdminNewDrawPublished(drawId) {
  if (process.env.PUSH_ALLOW_ENGINE_EVENTS !== "true") return;
  try {
    await handlePushAutomationEvent({
      eventKey: "NEW_DRAW_PUBLISHED",
      source: "admin",
      referenceType: "draw",
      referenceKey: `draw:${drawId}`,
      metadata: {
        draw_id: Number(drawId),
        origin: "admin",
      },
      actor: { type: "admin_dashboard" },
      dryRun: process.env.PUSH_ENGINE_DRY_RUN !== "false",
    });
  } catch (error) {
    console.warn("[admin/dashboard] push automation event skipped", {
      draw_id: drawId,
      code: error?.code || null,
      message: error?.message || null,
    });
  }
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

    // principal aberto; se nao houver, ultimo principal como historico
    const d = await query(
      `SELECT id, status, draw_type, opened_at, closed_at, realized_at
         FROM draws
        WHERE COALESCE(draw_type, 'principal') = 'principal'
        ORDER BY CASE WHEN status = 'open' THEN 0 ELSE 1 END,
                 id DESC
        LIMIT 1`
    );
    const current = d.rows[0] || null;

    const price_cents = await getTicketPriceCents();

    if (!current?.id) {
      return res.json({
        draw_id: null,
        status: null,
        draw_type: null,
        closed_at: null,
        realized_at: null,
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
      status: current.status,
      draw_type: current.draw_type || "principal",
      closed_at: current.closed_at || null,
      realized_at: current.realized_at || null,
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

router.patch(
  "/draws/:drawId/config",
  requireAuth,
  requireAdmin,
  createPrincipalDrawConfigHandler()
);


/**
 * POST /api/admin/dashboard/new
 * Cria um novo principal somente quando nenhum principal esta aberto e popula os numeros.
 * e DISPARA o Autopay oficial (services/autopayRunner.js).
 */
router.post("/new", requireAuth, requireAdmin, async (req, res) => {
  let client;
  let transactionOpen = false;
  try {
    log("POST /new");
    const numberCount = Number(req.body?.number_count ?? 100);
    if (!Number.isInteger(numberCount) || numberCount <= 0 || numberCount > 10000) {
      return res.status(400).json({ error: "invalid_number_count" });
    }
    const ticketPriceCents = Number(req.body?.ticket_price_cents);
    if (!Number.isInteger(ticketPriceCents) || ticketPriceCents <= 0) {
      return res.status(400).json({ error: "invalid_ticket_price" });
    }
    const pool = await getPool();
    client = await pool.connect();
    await client.query("BEGIN");
    transactionOpen = true;
    await client.query(
      `SELECT pg_advisory_xact_lock(hashtext('newstore_principal_creation'))`
    );

    const currentPrincipalResult = await client.query(
      `SELECT id, status, draw_type, opened_at
         FROM public.draws
        WHERE status = 'open'
          AND COALESCE(draw_type, 'principal') = 'principal'
        ORDER BY id DESC
        LIMIT 1`
    );
    const currentPrincipal = currentPrincipalResult.rows?.[0] || null;
    if (currentPrincipal) {
      await client.query("ROLLBACK");
      transactionOpen = false;
      console.warn("[admin-dashboard] principal_creation_blocked", {
        current_draw_id: Number(currentPrincipal.id),
        current_status: currentPrincipal.status,
        admin_user_id: req.user?.id ?? null,
      });
      return res.status(409).json({
        error: "principal_draw_already_open",
        message: "Já existe um sorteio principal em andamento. Ele não foi alterado.",
        current_draw: {
          id: Number(currentPrincipal.id),
          status: currentPrincipal.status,
        },
      });
    }

    const principalTicketPriceCents = await setTicketPriceCents(ticketPriceCents);

    // cria draw novo
    const ins = await client.query(
      `insert into draws(status, draw_type, opened_at, autopay_ran_at)
       values('open', 'principal', now(), null)
       returning id`
    );
    const newId = ins.rows[0].id;
    log("novo draw id =", newId);

    // popula numeros do sorteio principal
    await client.query(
      `insert into numbers(draw_id, n, status, reservation_id)
       select $1, gs::int, 'available', null
         from generate_series(0, $2::int - 1) as gs`,
      [newId, numberCount]
    );

    await client.query("COMMIT");
    transactionOpen = false;

    // dispara o AUTOPAY oficial — gera logs [autopayRunner]
    const captivePreauth = await createCaptivePreauthIfEnabled(
      newId,
      req.user?.id ?? null,
      "admin/dashboard/new",
      principalTicketPriceCents
    );
    if (!captivePreauth?.ok) {
      return res.status(500).json({
        ok: false,
        error: "captive_preauth_create_failed",
        draw_id: newId,
        ticket_price_cents: principalTicketPriceCents,
        sold: 0,
        remaining: numberCount,
        captive_preauth: captivePreauth,
      });
    }

    const autopay = await runAutopayForDraw(newId);

    // resposta inclui o resultado do autopay para depuração
    if (!autopay?.ok) {
      console.warn("[admin/dashboard] autopay falhou", autopay);
      return res.status(500).json({
        ok: false,
        draw_id: newId,
        ticket_price_cents: principalTicketPriceCents,
        sold: 0,
        remaining: numberCount,
        captive_preauth: captivePreauth,
        autopay,
      });
    }

    await emitAdminNewDrawPublished(newId);
    return res.json({
      ok: true,
      draw_id: newId,
      ticket_price_cents: principalTicketPriceCents,
      sold: 0,
      remaining: numberCount,
      captive_preauth: captivePreauth,
      autopay,
    });
  } catch (e) {
    if (transactionOpen && client) {
      try { await client.query("ROLLBACK"); } catch {}
    }
    console.error("[admin/dashboard] /new error:", e);
    return res.status(500).json({ error: "new_draw_failed" });
  } finally {
    if (client) client.release();
  }
});

/**
 * POST /api/admin/dashboard/draws/:drawId/close
 * Fecha manualmente sorteio principal legado sem alterar resultado ou numeros.
 */
router.post("/draws/:drawId/close", requireAuth, requireAdmin, async (req, res) => {
  const drawId = Number(req.params.drawId);
  if (!Number.isInteger(drawId) || drawId <= 0) {
    return res.status(400).json({ error: "invalid_draw_id" });
  }

  try {
    const current = await query(
      `SELECT id, status, draw_type, closed_at, realized_at, winner_number, winner_user_id, winner_name
         FROM public.draws
        WHERE id = $1
          AND COALESCE(draw_type, 'principal') = 'principal'`,
      [drawId]
    );

    if (!current.rowCount) return res.status(404).json({ error: "draw_not_found" });
    if (current.rows[0].status !== "open") {
      return res.status(409).json({ error: "draw_not_open", draw: current.rows[0] });
    }

    const updated = await query(
      `UPDATE public.draws
          SET status = 'closed',
              closed_at = COALESCE(closed_at, NOW())
        WHERE id = $1
          AND status = 'open'
          AND COALESCE(draw_type, 'principal') = 'principal'
        RETURNING id, status, draw_type, closed_at, realized_at, winner_number, winner_user_id, winner_name`,
      [drawId]
    );

    return res.json({ ok: true, draw: updated.rows[0] });
  } catch (e) {
    console.error("[admin/dashboard] /draws/:drawId/close error:", e);
    return res.status(500).json({ error: "close_draw_failed" });
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
