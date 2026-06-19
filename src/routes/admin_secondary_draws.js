import { Router } from "express";
import { v4 as uuid } from "uuid";
import { getPool, query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import { getTicketPriceCents } from "../services/config.js";

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

async function ensureDrawNumbers(client, drawId) {
  await client.query(
    `INSERT INTO numbers (draw_id, n, status)
     SELECT $1, gs::int, 'available'
       FROM generate_series(0, 99) AS gs
     ON CONFLICT DO NOTHING`,
    [drawId]
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
  const priceCents = await getTicketPriceCents();
  const productName = row.product_name || "Sorteio Secundario";
  return {
    id: row.id,
    status: row.status,
    draw_type: row.draw_type,
    product_name: productName,
    product_link: row.product_link || null,
    promo_phrase: productName,
    price_cents: priceCents,
    max_tickets_per_user: null,
    opened_at: row.opened_at || null,
    closed_at: row.closed_at || null,
    realized_at: row.realized_at || null,
    winner_user_id: row.winner_user_id ?? null,
    winner_name: row.winner_name || null,
    winner_number: row.winner_number ?? null,
  };
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
    const status = String(req.body.status).trim();
    if (!VALID_STATUSES.has(status)) return res.status(400).json({ error: "invalid_status" });
    addUpdate("status", status);
    if (status === "open") updates.push("opened_at = COALESCE(opened_at, NOW())");
  }

  if (!updates.length) return res.status(400).json({ error: "no_fields_to_update" });

  try {
    params.push(drawId);
    const updated = await query(
      `UPDATE draws
          SET ${updates.join(", ")}
        WHERE id = $${params.length}
          AND draw_type IN ('adicional', 'secundario')
        RETURNING id, status, draw_type, product_name, product_link, opened_at,
                  closed_at, realized_at, winner_user_id, winner_name, winner_number`,
      params
    );

    if (!updated.rowCount) return res.status(404).json({ error: "draw_not_found" });
    if (productName !== undefined) {
      const ticketPriceCents = await getTicketPriceCents();
      await query(
        `INSERT INTO app_config_new (id, banner_title, ticket_price_cents, max_numbers_per_selection)
         VALUES ($1, $2, $3, NULL)
         ON CONFLICT (id) DO UPDATE
           SET banner_title = EXCLUDED.banner_title`,
        [String(drawId), productName || "Sorteio Adicional", Number(ticketPriceCents)]
      );
    }
    return res.json({ draw: await drawResponse(updated.rows[0]) });
  } catch (e) {
    console.error("[admin_secondary_draws/update] error:", e?.code || e?.message || e);
    return res.status(500).json({ error: "secondary_draw_update_failed" });
  }
});

router.post("/", async (req, res) => {
  const status = String(req.body?.status ?? "open").trim();
  if (!VALID_STATUSES.has(status)) return res.status(400).json({ error: "invalid_status" });

  const pool = await getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const productName = toOptionalString(
      req.body?.product_name ?? req.body?.promo_phrase ?? "Sorteio Adicional",
      255
    );

    const inserted = await client.query(
      `INSERT INTO draws (status, draw_type, product_name, product_link, opened_at)
       VALUES ($1, 'adicional', $2, $3, CASE WHEN $1 = 'open' THEN NOW() ELSE NULL END)
       RETURNING id, status, draw_type, product_name, product_link, opened_at,
                 closed_at, realized_at, winner_user_id, winner_name, winner_number`,
      [status, productName, toOptionalString(req.body?.product_link ?? null, 1024)]
    );

    const draw = inserted.rows[0];
    await ensureDrawNumbers(client, draw.id);
    await client.query(
      `INSERT INTO app_config_new (id, banner_title, ticket_price_cents, max_numbers_per_selection)
       VALUES ($1, $2, $3, NULL)
       ON CONFLICT (id) DO UPDATE
         SET banner_title = EXCLUDED.banner_title,
             ticket_price_cents = COALESCE(app_config_new.ticket_price_cents, EXCLUDED.ticket_price_cents)`,
      [String(draw.id), productName || "Sorteio Adicional", Number(await getTicketPriceCents())]
    );

    await client.query("COMMIT");
    return res.status(201).json({ draw: await drawResponse(draw), stats: await loadStats(draw.id) });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[admin_secondary_draws/create] error:", e?.code || e?.message || e);
    if (e?.code === "23514") {
      return res.status(409).json({ error: "draw_type_adicional_not_allowed" });
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
