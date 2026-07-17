import { Router } from "express";
import { v4 as uuid } from "uuid";
import { getPool, query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import { getTicketPriceCents } from "../services/config.js";
import { closeDrawIfSoldOut } from "../services/drawLifecycle.js";
import {
  assertCanOpenAdditionalDraw,
  getOpenDrawLimitResponse,
  isDrawTypeConstraintViolation,
  isOneOpenPerTypeConstraint,
  lockOpenDrawSlots,
} from "../services/openDrawLimits.js";

const router = Router();
const VALID_STATUSES = new Set(["draft", "open", "closed", "cancelled"]);

router.use(requireAuth, requireAdmin);

function toOptionalString(value, maxLength) {
  if (value === undefined) return undefined;
  if (value === null) return null;
  return String(value).slice(0, maxLength);
}

function normalizeNumbers(input) {
  if (!Array.isArray(input)) return { error: "numbers_must_be_array", numbers: [] };

  const parsed = input.map(Number);
  const invalid = parsed.filter((n) => !Number.isInteger(n) || n < 0 || n > 99);
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
  await client.query(
    `INSERT INTO numbers (draw_id, n, status)
     SELECT $1, gs::int, 'available'
       FROM generate_series(0, $2::int - 1) AS gs
     ON CONFLICT DO NOTHING`,
    [drawId, numberCount]
  );
}

async function loadStats(drawId) {
  const stats = await query(
    `SELECT
        COUNT(*) FILTER (WHERE status = 'sold')::int AS sold,
        COUNT(*) FILTER (WHERE status = 'available')::int AS available,
        COUNT(*) FILTER (WHERE status = 'reserved')::int AS reserved,
        COUNT(*)::int AS total
       FROM numbers
      WHERE draw_id = $1`,
    [drawId]
  );

  return {
    sold: Number(stats.rows[0]?.sold || 0),
    available: Number(stats.rows[0]?.available || 0),
    reserved: Number(stats.rows[0]?.reserved || 0),
    total: Number(stats.rows[0]?.total || 0),
  };
}

async function drawResponse(row) {
  if (!row) return null;
  const config = (await query(
    `SELECT banner_title, ticket_price_cents, max_numbers_per_selection
       FROM app_config_new
      WHERE id = $1`,
    [String(row.id)]
  ))?.rows?.[0] || null;
  const fallbackPriceCents = await getTicketPriceCents();
  const priceCents = Number(config?.ticket_price_cents || fallbackPriceCents);
  const productName = row.product_name || config?.banner_title || "Sorteio Secundario";
  return {
    id: row.id,
    status: row.status,
    draw_type: row.draw_type,
    product_name: productName,
    product_link: row.product_link || null,
    banner_title: config?.banner_title || productName,
    promo_phrase: config?.banner_title || productName,
    ticket_price_cents: priceCents,
    price_cents: priceCents,
    max_numbers_per_selection: config?.max_numbers_per_selection ?? null,
    max_tickets_per_user: config?.max_numbers_per_selection ?? null,
    opened_at: row.opened_at || null,
    closed_at: row.closed_at || null,
    realized_at: row.realized_at || null,
    winner_user_id: row.winner_user_id ?? null,
    winner_name: row.winner_name || null,
    winner_number: row.winner_number ?? null,
  };
}

async function upsertConfig(db, drawId, values, fallbackProductName) {
  const fallbackPriceCents = await getTicketPriceCents();
  const bannerTitle = toOptionalString(values.banner_title ?? fallbackProductName ?? "Sorteio Adicional", 255);
  const ticketPriceCents =
    values.ticket_price_cents === undefined || values.ticket_price_cents === null
      ? Number(fallbackPriceCents)
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

  await db.query(
    `INSERT INTO app_config_new (id, banner_title, ticket_price_cents, max_numbers_per_selection)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (id) DO UPDATE
       SET banner_title = COALESCE(EXCLUDED.banner_title, app_config_new.banner_title),
           ticket_price_cents = COALESCE(EXCLUDED.ticket_price_cents, app_config_new.ticket_price_cents),
           max_numbers_per_selection = COALESCE(EXCLUDED.max_numbers_per_selection, app_config_new.max_numbers_per_selection)`,
    [String(drawId), bannerTitle, ticketPriceCents, maxNumbers]
  );
}

router.get("/current", async (_req, res) => {
  const pool = await getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const drawRes = await client.query(
      `SELECT id, status, draw_type, product_name, product_link, opened_at,
              closed_at, realized_at, winner_user_id, winner_name, winner_number
         FROM draws
        WHERE status = 'open'
          AND draw_type IN ('adicional', 'secundario')
        ORDER BY id ASC
        LIMIT 1`
    );

    const draw = drawRes.rows[0] || null;
    if (!draw) {
      await client.query("COMMIT");
      return res.json({
        draw: null,
        stats: { sold: 0, available: 0, reserved: 0, total: 0 },
      });
    }

    await client.query("COMMIT");
    return res.json({ draw: await drawResponse(draw), stats: await loadStats(draw.id) });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[admin_secondary_draws/current] error:", e?.code || e?.message || e);
    return res.status(500).json({ error: "secondary_draw_current_failed" });
  } finally {
    client.release();
  }
});

router.patch("/:id", async (req, res) => {
  const drawId = Number(req.params.id);
  if (!Number.isInteger(drawId) || drawId <= 0) {
    return res.status(400).json({ error: "invalid_draw_id" });
  }

  const updates = [];
  const params = [];
  let requestedStatus;
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
    if (winnerNumber !== null && (!Number.isInteger(winnerNumber) || winnerNumber < 0 || winnerNumber > 99)) {
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
    requestedStatus = String(req.body.status).trim();
    if (!VALID_STATUSES.has(requestedStatus)) return res.status(400).json({ error: "invalid_status" });
    addUpdate("status", requestedStatus);
    if (requestedStatus === "closed") updates.push("closed_at = COALESCE(closed_at, NOW())");
  }

  const configValues = {
    banner_title: req.body?.banner_title ?? req.body?.promo_phrase,
    ticket_price_cents: req.body?.ticket_price_cents,
    max_numbers_per_selection: req.body?.max_numbers_per_selection,
  };
  const hasConfigUpdate =
    configValues.banner_title !== undefined ||
    configValues.ticket_price_cents !== undefined ||
    configValues.max_numbers_per_selection !== undefined;

  if (!updates.length && !hasConfigUpdate) return res.status(400).json({ error: "no_fields_to_update" });

  const pool = await getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    if (requestedStatus === "open") {
      await lockOpenDrawSlots(client);
    }

    const current = await client.query(
      `SELECT id, status, product_name
         FROM draws
        WHERE id = $1
          AND draw_type IN ('adicional', 'secundario')
        FOR UPDATE`,
      [drawId]
    );
    if (!current.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "draw_not_found" });
    }
    const previousStatus = current.rows[0].status;

    if (previousStatus !== "open" && requestedStatus === "open") {
      await assertCanOpenAdditionalDraw(client);
      updates.push("opened_at = COALESCE(opened_at, NOW())");
    }

    let updated;
    if (updates.length) {
      params.push(drawId);
      updated = await client.query(
        `UPDATE draws
            SET ${updates.join(", ")}
          WHERE id = $${params.length}
            AND draw_type IN ('adicional', 'secundario')
          RETURNING id, status, draw_type, product_name, product_link, opened_at,
                    closed_at, realized_at, winner_user_id, winner_name, winner_number`,
        params
      );
    } else {
      updated = await client.query(
        `SELECT id, status, draw_type, product_name, product_link, opened_at,
                closed_at, realized_at, winner_user_id, winner_name, winner_number
           FROM draws
          WHERE id = $1
            AND draw_type IN ('adicional', 'secundario')`,
        [drawId]
      );
    }

    if (hasConfigUpdate) {
      await upsertConfig(client, drawId, configValues, updated.rows[0]?.product_name || current.rows[0].product_name);
    }
    await client.query("COMMIT");
    return res.json({ draw: await drawResponse(updated.rows[0]) });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[admin_secondary_draws/update] error:", e?.code || e?.message || e);
    const limitResponse = getOpenDrawLimitResponse(e);
    if (limitResponse) return res.status(409).json(limitResponse);
    if (isOneOpenPerTypeConstraint(e)) {
      return res.status(409).json({
        error: "additional_draw_database_limit",
        message: "O banco ainda está configurado para permitir apenas um sorteio aberto por tipo.",
      });
    }
    if (e?.status === 400) return res.status(400).json({ error: e.message });
    return res.status(500).json({ error: "secondary_draw_update_failed" });
  } finally {
    client.release();
  }
});

router.post("/", async (req, res) => {
  const status = String(req.body?.status ?? "open").trim();
  if (!VALID_STATUSES.has(status)) return res.status(400).json({ error: "invalid_status" });
  const numberCount = normalizeNumberCount(req.body?.number_count);
  if (numberCount.error) return res.status(400).json({ error: numberCount.error });

  const pool = await getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    if (status === "open") {
      await lockOpenDrawSlots(client);
      await assertCanOpenAdditionalDraw(client);
    }

    const productName = toOptionalString(
      req.body?.product_name ?? req.body?.promo_phrase ?? req.body?.banner_title ?? "Sorteio Adicional",
      255
    );
    const bannerTitle = toOptionalString(req.body?.banner_title ?? req.body?.promo_phrase ?? productName, 255);

    const inserted = await client.query(
      `INSERT INTO draws (status, draw_type, product_name, product_link, opened_at)
       VALUES ($1, 'adicional', $2, $3, CASE WHEN $1 = 'open' THEN NOW() ELSE NULL END)
       RETURNING id, status, draw_type, product_name, product_link, opened_at,
                 closed_at, realized_at, winner_user_id, winner_name, winner_number`,
      [status, productName, toOptionalString(req.body?.product_link ?? null, 1024)]
    );

    const draw = inserted.rows[0];
    await ensureDrawNumbers(client, draw.id, numberCount.count);
    await upsertConfig(
      client,
      draw.id,
      {
        banner_title: bannerTitle,
        ticket_price_cents: req.body?.ticket_price_cents,
        max_numbers_per_selection: req.body?.max_numbers_per_selection,
      },
      productName
    );

    await client.query("COMMIT");
    return res.status(201).json({ draw: await drawResponse(draw), stats: await loadStats(draw.id) });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[admin_secondary_draws/create] error:", e?.code || e?.message || e);
    const limitResponse = getOpenDrawLimitResponse(e);
    if (limitResponse) {
      return res.status(409).json(limitResponse);
    }
    if (isDrawTypeConstraintViolation(e)) {
      return res.status(409).json({ error: "draw_type_adicional_not_allowed" });
    }
    if (isOneOpenPerTypeConstraint(e)) {
      return res.status(409).json({
        error: "additional_draw_database_limit",
        message: "O banco ainda está configurado para permitir apenas um sorteio aberto por tipo.",
      });
    }
    return res.status(500).json({ error: "secondary_draw_create_failed" });
  } finally {
    client.release();
  }
});

router.get("/:id/numbers", async (req, res) => {
  const drawId = Number(req.params.id);
  if (!Number.isInteger(drawId) || drawId <= 0) {
    return res.status(400).json({ error: "invalid_draw_id" });
  }

  const pool = await getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const drawExists = await client.query(
      `SELECT id
         FROM draws
        WHERE id = $1
          AND draw_type IN ('adicional', 'secundario')`,
      [drawId]
    );
    if (!drawExists.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "draw_not_found" });
    }

    const numbers = await client.query(
      `WITH paid_numbers AS (
         SELECT DISTINCT ON (num.n)
                num.n::int AS n,
                p.user_id,
                p.id AS payment_id,
                u.name AS user_name,
                u.email AS user_email
           FROM payments p
      LEFT JOIN users u ON u.id = p.user_id
     CROSS JOIN LATERAL unnest(p.numbers) AS num(n)
          WHERE p.draw_id = $1
            AND lower(p.status) IN ('approved','paid','pago')
       ORDER BY num.n, p.created_at DESC NULLS LAST, p.id DESC
       )
       SELECT n.n,
              n.status,
              COALESCE(pn.user_id, r.user_id) AS user_id,
              pn.user_name,
              pn.user_email,
              n.reservation_id,
              pn.payment_id
         FROM numbers n
    LEFT JOIN reservations r ON r.id = n.reservation_id
    LEFT JOIN paid_numbers pn ON pn.n = n.n
        WHERE n.draw_id = $1
        ORDER BY n.n ASC`,
      [drawId]
    );

    await client.query("COMMIT");
    return res.json({ draw_id: drawId, secondary_draw_id: drawId, numbers: numbers.rows || [] });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[admin_secondary_draws/numbers] error:", e?.code || e?.message || e);
    return res.status(500).json({ error: "numbers_failed" });
  } finally {
    client.release();
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
         FROM users
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
         FROM draws
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

    await ensureDrawNumbers(client, drawId);

    const locked = await client.query(
      `SELECT n, status
         FROM numbers
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
      `INSERT INTO payments (id, user_id, draw_id, numbers, amount_cents, status, provider)
       VALUES ($1, $2, $3, $4::int[], 0, 'approved', 'manual')`,
      [paymentId, userId, drawId, nums]
    );

    const updated = await client.query(
      `UPDATE numbers
          SET status = 'sold',
              reservation_id = NULL
        WHERE draw_id = $1
          AND n = ANY($2::int[])
        RETURNING n, status, reservation_id`,
      [drawId, nums]
    );

    await closeDrawIfSoldOut(drawId, client);

    await client.query("COMMIT");

    return res.status(201).json({
      draw_id: drawId,
      secondary_draw_id: drawId,
      user: userRes.rows[0],
      payment_id: paymentId,
      numbers: updated.rows || [],
      generate_balance_requested: Boolean(req.body?.generate_balance),
      balance_generated: false,
      balance_reason: "not_implemented_for_additional_draw",
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[admin_secondary_draws/assign] error:", e?.code || e?.message || e);
    return res.status(500).json({ error: "assign_failed" });
  } finally {
    client.release();
  }
});

export default router;
