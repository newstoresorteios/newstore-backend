import { Router } from "express";
import { v4 as uuid } from "uuid";
import { getPool, query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import { getTicketPriceCents } from "../services/config.js";
import { formatAdditionalDraw, loadDrawConfigs } from "./additional_draws.js";

const router = Router();
const VALID_STATUSES = new Set(["draft", "open", "closed", "cancelled"]);

router.use(requireAuth, requireAdmin);

function toOptionalString(value, maxLength) {
  if (value === undefined) return undefined;
  if (value === null) return null;
  return String(value).trim().slice(0, maxLength);
}

function normalizeNumbers(input) {
  if (!Array.isArray(input)) return { error: "numbers_must_be_array", numbers: [] };

  const parsed = input.map(Number);
  const invalid = parsed.filter((n) => !Number.isInteger(n) || n < 0);
  if (invalid.length) return { error: "invalid_numbers", numbers: [] };

  const numbers = Array.from(new Set(parsed)).sort((a, b) => a - b);
  if (!numbers.length) return { error: "no_numbers", numbers: [] };

  return { numbers };
}

function normalizeNumberCount(value) {
  const count = Number(value ?? 100);
  if (!Number.isInteger(count) || count <= 0 || count > 10000) {
    return { error: "invalid_number_count" };
  }
  return { count };
}

async function ensureDrawNumbers(client, drawId, numberCount = 100) {
  const count = Math.max(0, Number(numberCount || 0));
  if (!Number.isInteger(count) || count <= 0) return;

  await client.query(
    `INSERT INTO public.numbers (draw_id, n, status)
     SELECT $1, gs::int, 'available'
       FROM generate_series(0, $2::int - 1) AS gs
     ON CONFLICT DO NOTHING`,
    [drawId, count]
  );
}

async function loadStats(drawId) {
  const stats = await query(
    `SELECT
        COUNT(*) FILTER (WHERE lower(status) IN ('sold', 'paid', 'approved', 'unavailable', 'indisponivel'))::int AS sold,
        COUNT(*) FILTER (WHERE status = 'available')::int AS available,
        COUNT(*) FILTER (WHERE status = 'reserved')::int AS reserved,
        COUNT(*) FILTER (WHERE status = 'blocked')::int AS blocked,
        COUNT(*)::int AS total
       FROM public.numbers
      WHERE draw_id = $1`,
    [drawId]
  );

  const row = stats.rows[0] || {};
  const sold = Number(row.sold || 0);
  const available = Number(row.available || 0);
  const reserved = Number(row.reserved || 0);
  const blocked = Number(row.blocked || 0);
  const total = Number(row.total || 0);

  return {
    sold,
    available,
    reserved,
    blocked,
    total,
    remaining: Math.max(0, available),
  };
}

async function loadBuyers(drawId) {
  const buyers = await query(
    `WITH paid_numbers AS (
       SELECT p.id AS payment_id,
              p.user_id,
              u.name,
              u.email,
              num.n::int AS n
         FROM public.payments p
    LEFT JOIN public.users u ON u.id = p.user_id
   CROSS JOIN LATERAL unnest(p.numbers) AS num(n)
        WHERE p.draw_id = $1
          AND lower(p.status) IN ('approved', 'paid', 'pago', 'sold')
     )
     SELECT user_id,
            max(name) AS name,
            max(email) AS email,
            COUNT(DISTINCT n)::int AS numbers_count,
            array_agg(DISTINCT n ORDER BY n) AS numbers,
            array_agg(DISTINCT payment_id ORDER BY payment_id) AS payments
       FROM paid_numbers
      GROUP BY user_id
      ORDER BY max(name) ASC NULLS LAST, max(email) ASC NULLS LAST, user_id ASC NULLS LAST`,
    [drawId]
  );

  return (buyers.rows || []).map((row) => ({
    user_id: row.user_id ?? null,
    name: row.name || null,
    email: row.email || null,
    numbers_count: Number(row.numbers_count || 0),
    numbers: (row.numbers || []).map(Number),
    payments: row.payments || [],
  }));
}

async function loadConfig(drawId) {
  const config = await query(
    `SELECT *
       FROM public.app_config_new
      WHERE id = $1`,
    [String(drawId)]
  );

  return config.rows[0] || null;
}

async function upsertConfig(client, drawId, values, fallbackProductName) {
  const hasConfigUpdate =
    values.banner_title !== undefined ||
    values.ticket_price_cents !== undefined ||
    values.max_numbers_per_selection !== undefined;

  if (!hasConfigUpdate) return;

  const fallbackPrice = await getTicketPriceCents();
  const bannerTitle = toOptionalString(values.banner_title ?? fallbackProductName ?? "SORTEIO ADICIONAL", 255);
  const ticketPriceCents =
    values.ticket_price_cents === undefined || values.ticket_price_cents === null
      ? Number(fallbackPrice)
      : Number(values.ticket_price_cents);
  const maxNumbers =
    values.max_numbers_per_selection === undefined || values.max_numbers_per_selection === null
      ? null
      : Number(values.max_numbers_per_selection);

  if (!Number.isInteger(ticketPriceCents) || ticketPriceCents < 0) {
    throw Object.assign(new Error("invalid_ticket_price_cents"), { status: 400 });
  }
  if (maxNumbers !== null && (!Number.isInteger(maxNumbers) || maxNumbers <= 0)) {
    throw Object.assign(new Error("invalid_max_numbers_per_selection"), { status: 400 });
  }

  await client.query(
    `INSERT INTO public.app_config_new AS cfg (id, banner_title, ticket_price_cents, max_numbers_per_selection)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (id) DO UPDATE
       SET banner_title = COALESCE(EXCLUDED.banner_title, cfg.banner_title),
           ticket_price_cents = COALESCE(EXCLUDED.ticket_price_cents, cfg.ticket_price_cents),
           max_numbers_per_selection = COALESCE(EXCLUDED.max_numbers_per_selection, cfg.max_numbers_per_selection)`,
    [String(drawId), bannerTitle, ticketPriceCents, maxNumbers]
  );
}

router.get("/", async (_req, res) => {
  try {
    const draws = await query(
      `SELECT *
         FROM public.draws
        WHERE draw_type IN ('adicional', 'secundario')
        ORDER BY id DESC`
    );

    const items = [];
    for (const draw of draws.rows || []) {
      const config = await loadConfig(draw.id);
      items.push({
        draw: await formatAdditionalDraw(draw, config),
        config,
        stats: await loadStats(draw.id),
        buyers: await loadBuyers(draw.id),
      });
    }

    return res.json({ draws: items });
  } catch (e) {
    console.error("[admin_additional_draws/list] error:", e?.code || e?.message || e);
    return res.status(500).json({ error: "additional_draws_list_failed" });
  }
});

router.post("/", async (req, res) => {
  const status = String(req.body?.status ?? "open").trim();
  if (!VALID_STATUSES.has(status)) return res.status(400).json({ error: "invalid_status" });

  const drawType = String(req.body?.draw_type ?? "adicional").trim().toLowerCase();
  if (!["adicional", "secundario"].includes(drawType)) {
    return res.status(400).json({ error: "invalid_draw_type" });
  }

  const numberCount = normalizeNumberCount(req.body?.number_count);
  if (numberCount.error) return res.status(400).json({ error: numberCount.error });

  let client;
  try {
    const productName = toOptionalString(
      req.body?.product_name ?? req.body?.promo_phrase ?? req.body?.banner_title ?? "SORTEIO ADICIONAL",
      255
    );
    const productLink = toOptionalString(req.body?.product_link ?? null, 1024);
    const bannerTitle = toOptionalString(req.body?.banner_title ?? req.body?.promo_phrase ?? productName, 255);
    const ticketPriceCents =
      req.body?.ticket_price_cents === undefined || req.body?.ticket_price_cents === null
        ? await getTicketPriceCents()
        : Number(req.body.ticket_price_cents);
    const maxNumbers =
      req.body?.max_numbers_per_selection === undefined || req.body?.max_numbers_per_selection === null
        ? null
        : Number(req.body.max_numbers_per_selection);

    if (!Number.isInteger(Number(ticketPriceCents)) || Number(ticketPriceCents) < 0) {
      return res.status(400).json({ error: "invalid_ticket_price_cents" });
    }
    if (maxNumbers !== null && (!Number.isInteger(maxNumbers) || maxNumbers <= 0)) {
      return res.status(400).json({ error: "invalid_max_numbers_per_selection" });
    }

    const pool = await getPool();
    client = await pool.connect();
    await client.query("BEGIN");

    if (status === "open") {
      await client.query(
        `UPDATE public.draws
            SET status = 'closed',
                closed_at = COALESCE(closed_at, NOW())
          WHERE status = 'open'
            AND draw_type = $1`,
        [drawType]
      );
    }

    const inserted = await client.query(
      `INSERT INTO public.draws (status, draw_type, product_name, product_link, opened_at)
       VALUES ($1, $2, $3, $4, CASE WHEN $1 = 'open' THEN NOW() ELSE NULL END)
       RETURNING id, status, draw_type, product_name, product_link, opened_at,
                 closed_at, realized_at, winner_user_id, winner_name, winner_number`,
      [status, drawType, productName, productLink]
    );

    const draw = inserted.rows[0];
    await upsertConfig(
      client,
      draw.id,
      { banner_title: bannerTitle, ticket_price_cents: Number(ticketPriceCents), max_numbers_per_selection: maxNumbers },
      productName
    );
    await ensureDrawNumbers(client, draw.id, numberCount.count);

    await client.query("COMMIT");
    const configMap = await loadDrawConfigs([draw.id]);
    return res.status(201).json({
      draw: await formatAdditionalDraw(draw, configMap.get(String(draw.id))),
      stats: await loadStats(draw.id),
    });
  } catch (e) {
    if (client) {
      try { await client.query("ROLLBACK"); } catch {}
    }
    console.error("[admin_additional_draws/create] error:", {
      code: e?.code,
      message: e?.message,
      constraint: e?.constraint,
      table: e?.table,
      detail: e?.detail,
    });
    if (e?.code === "23514") {
      return res.status(409).json({ error: "draw_type_adicional_not_allowed" });
    }
    if (e?.code === "23505") {
      return res.status(409).json({ error: "additional_draw_duplicate" });
    }
    return res.status(500).json({ error: "additional_draw_create_failed" });
  } finally {
    if (client) client.release();
  }
});

router.patch("/:id", async (req, res) => {
  const drawId = Number(req.params.id);
  if (!Number.isInteger(drawId) || drawId <= 0) {
    return res.status(400).json({ error: "invalid_draw_id" });
  }

  const updates = [];
  const params = [];
  const addUpdate = (column, value) => {
    params.push(value);
    updates.push(`${column} = $${params.length}`);
  };

  const productName = toOptionalString(req.body?.product_name ?? req.body?.promo_phrase, 255);
  if (productName !== undefined) addUpdate("product_name", productName);

  const productLink = toOptionalString(req.body?.product_link, 1024);
  if (productLink !== undefined) addUpdate("product_link", productLink);

  if (req.body?.winner_name !== undefined) {
    addUpdate("winner_name", toOptionalString(req.body.winner_name, 255));
  }

  if (req.body?.winner_number !== undefined) {
    const winnerNumber = req.body.winner_number === null ? null : Number(req.body.winner_number);
    if (winnerNumber !== null && (!Number.isInteger(winnerNumber) || winnerNumber < 0)) {
      return res.status(400).json({ error: "invalid_winner_number" });
    }
    addUpdate("winner_number", winnerNumber);
  }

  if (req.body?.winner_user_id !== undefined) {
    const winnerUserId = req.body.winner_user_id === null ? null : Number(req.body.winner_user_id);
    if (winnerUserId !== null && (!Number.isInteger(winnerUserId) || winnerUserId <= 0)) {
      return res.status(400).json({ error: "invalid_winner_user_id" });
    }
    addUpdate("winner_user_id", winnerUserId);
  }

  if (req.body?.status !== undefined) {
    const status = String(req.body.status).trim();
    if (!VALID_STATUSES.has(status)) return res.status(400).json({ error: "invalid_status" });
    addUpdate("status", status);
    if (status === "open") updates.push("opened_at = COALESCE(opened_at, NOW())");
  }

  const configValues = {
    banner_title: req.body?.banner_title ?? req.body?.promo_phrase,
    ticket_price_cents: req.body?.ticket_price_cents,
    max_numbers_per_selection: req.body?.max_numbers_per_selection,
  };

  const pool = await getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const current = await client.query(
      `SELECT id, product_name
         FROM public.draws
        WHERE id = $1
          AND draw_type IN ('adicional', 'secundario')
        FOR UPDATE`,
      [drawId]
    );
    if (!current.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "draw_not_found" });
    }

    let updatedRow;
    if (updates.length) {
      params.push(drawId);
      const updated = await client.query(
        `UPDATE public.draws
            SET ${updates.join(", ")}
          WHERE id = $${params.length}
            AND draw_type IN ('adicional', 'secundario')
          RETURNING id, status, draw_type, product_name, product_link, opened_at,
                    closed_at, realized_at, winner_user_id, winner_name, winner_number`,
        params
      );
      updatedRow = updated.rows[0];
    } else {
      const unchanged = await client.query(
        `SELECT id, status, draw_type, product_name, product_link, opened_at,
                closed_at, realized_at, winner_user_id, winner_name, winner_number
           FROM public.draws
          WHERE id = $1`,
        [drawId]
      );
      updatedRow = unchanged.rows[0];
    }

    await upsertConfig(client, drawId, configValues, updatedRow.product_name || current.rows[0].product_name);

    await client.query("COMMIT");
    const configMap = await loadDrawConfigs([drawId]);
    return res.json({
      draw: await formatAdditionalDraw(updatedRow, configMap.get(String(drawId))),
      stats: await loadStats(drawId),
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[admin_additional_draws/update] error:", e?.code || e?.message || e);
    if (e?.status === 400) return res.status(400).json({ error: e.message });
    return res.status(500).json({ error: "additional_draw_update_failed" });
  } finally {
    client.release();
  }
});

router.get("/:id/numbers", async (req, res) => {
  const drawId = Number(req.params.id);
  if (!Number.isInteger(drawId) || drawId <= 0) {
    return res.status(400).json({ error: "invalid_draw_id" });
  }

  try {
    const drawExists = await query(
      `SELECT id
         FROM public.draws
        WHERE id = $1
          AND draw_type IN ('adicional', 'secundario')`,
      [drawId]
    );
    if (!drawExists.rowCount) return res.status(404).json({ error: "draw_not_found" });

    const numbers = await query(
      `WITH paid_numbers AS (
         SELECT DISTINCT ON (num.n)
                num.n::int AS n,
                p.user_id,
                p.id AS payment_id,
                u.name AS user_name,
                u.email AS user_email
           FROM public.payments p
      LEFT JOIN public.users u ON u.id = p.user_id
     CROSS JOIN LATERAL unnest(p.numbers) AS num(n)
          WHERE p.draw_id = $1
            AND lower(p.status) IN ('approved','paid','pago','sold')
       ORDER BY num.n, p.created_at DESC NULLS LAST, p.id DESC
       )
       SELECT n.n,
              CASE WHEN pn.payment_id IS NOT NULL THEN 'sold' ELSE n.status END AS status,
              COALESCE(pn.user_id, r.user_id) AS user_id,
              COALESCE(pn.user_name, ru.name) AS user_name,
              COALESCE(pn.user_email, ru.email) AS user_email,
              n.reservation_id,
              pn.payment_id
         FROM public.numbers n
    LEFT JOIN public.reservations r ON r.id = n.reservation_id
    LEFT JOIN public.users ru ON ru.id = r.user_id
    LEFT JOIN paid_numbers pn ON pn.n = n.n
        WHERE n.draw_id = $1
        ORDER BY n.n ASC`,
      [drawId]
    );

    return res.json({ draw_id: drawId, numbers: numbers.rows || [] });
  } catch (e) {
    console.error("[admin_additional_draws/numbers] error:", e?.code || e?.message || e);
    return res.status(500).json({ error: "numbers_failed" });
  }
});

router.post("/:id/assign-numbers", async (req, res) => {
  const drawId = Number(req.params.id);
  if (!Number.isInteger(drawId) || drawId <= 0) {
    return res.status(400).json({ error: "invalid_draw_id" });
  }

  const userId = Number(req.body?.user_id);
  if (!Number.isInteger(userId) || userId <= 0) {
    return res.status(400).json({ error: "invalid_user_id" });
  }

  const normalized = normalizeNumbers(req.body?.numbers);
  if (normalized.error) return res.status(400).json({ error: normalized.error });

  const nums = normalized.numbers;
  const pool = await getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const userRes = await client.query(
      `SELECT id, email, name
         FROM public.users
        WHERE id = $1
        FOR UPDATE`,
      [userId]
    );
    if (!userRes.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "user_not_found" });
    }

    const drawRes = await client.query(
      `SELECT id, status
         FROM public.draws
        WHERE id = $1
          AND draw_type IN ('adicional', 'secundario')
        FOR UPDATE`,
      [drawId]
    );
    if (!drawRes.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "draw_not_found" });
    }
    if (drawRes.rows[0].status !== "open") {
      await client.query("ROLLBACK");
      return res.status(400).json({ error: "draw_not_open" });
    }

    const locked = await client.query(
      `SELECT n, status
         FROM public.numbers
        WHERE draw_id = $1
          AND n = ANY($2::int[])
        FOR UPDATE`,
      [drawId, nums]
    );

    const found = new Set(locked.rows.map((row) => Number(row.n)));
    const notFound = nums.filter((n) => !found.has(n));
    if (notFound.length) {
      await client.query("ROLLBACK");
      return res.status(400).json({ error: "numbers_not_found", numbers: notFound });
    }

    const conflicts = locked.rows
      .filter((row) => String(row.status).toLowerCase() !== "available")
      .map((row) => Number(row.n));
    if (conflicts.length) {
      await client.query("ROLLBACK");
      return res.status(409).json({ error: "numbers_unavailable", conflicts });
    }

    const paymentId = `manual-additional-${uuid()}`;
    await client.query(
      `INSERT INTO public.payments (id, user_id, draw_id, numbers, amount_cents, status, provider)
       VALUES ($1, $2, $3, $4::int[], 0, 'approved', 'manual')`,
      [paymentId, userId, drawId, nums]
    );

    const updated = await client.query(
      `UPDATE public.numbers
          SET status = 'sold',
              reservation_id = NULL
        WHERE draw_id = $1
          AND n = ANY($2::int[])
        RETURNING n, status, reservation_id`,
      [drawId, nums]
    );

    await client.query("COMMIT");

    return res.status(201).json({
      draw_id: drawId,
      user: userRes.rows[0],
      payment_id: paymentId,
      numbers: updated.rows || [],
      generate_balance_requested: Boolean(req.body?.generate_balance),
      balance_generated: false,
      balance_reason: "not_implemented_for_additional_draw",
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[admin_additional_draws/assign] error:", e?.code || e?.message || e);
    return res.status(500).json({ error: "assign_failed" });
  } finally {
    client.release();
  }
});

export default router;
