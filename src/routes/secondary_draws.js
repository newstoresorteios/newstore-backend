import { Router } from "express";
import { v4 as uuid } from "uuid";
import { getPool } from "../db.js";
import { requireAuth } from "../middleware/auth.js";
import { getTicketPriceCents } from "../services/config.js";
import { pendingCaptivePreauthReservationGuardSql } from "../services/reservationExpiry.js";

const router = Router();

const RESERVATION_TTL_MIN = Math.max(
  1,
  Number(process.env.RESERVATION_TTL_MIN || process.env.SECONDARY_RESERVATION_TTL_MIN || 30)
);

function normalizeDrawType(value) {
  return value === "adicional" || value === "secundario" ? value : "principal";
}

function isAdditionalDrawType(value) {
  return value === "adicional" || value === "secundario";
}

function normalizeNewDrawType(value) {
  return value === "adicional" ? "adicional" : "principal";
}

function initialsFromNameOrEmail(name, email) {
  const nm = String(name || "").trim();
  if (nm) {
    const parts = nm.split(/\s+/).filter(Boolean);
    const first = parts[0]?.[0] || "";
    const last = parts.length > 1 ? parts[parts.length - 1][0] : (parts[0]?.[1] || "");
    return (first + last).toUpperCase();
  }
  const mail = String(email || "").trim();
  const user = mail.includes("@") ? mail.split("@")[0] : mail;
  return user.slice(0, 2).toUpperCase();
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

async function expireDrawReservations(client, drawId = null) {
  const params = [];
  const drawFilter = drawId ? "AND r.draw_id = $1" : "";
  if (drawId) params.push(drawId);

  const expired = await client.query(
    `UPDATE reservations r
        SET status = 'expired'
      WHERE r.status = 'active'
        AND r.expires_at < NOW()
        AND ${pendingCaptivePreauthReservationGuardSql("r")}
        ${drawFilter}
      RETURNING r.id, r.draw_id`,
    params
  );

  for (const row of expired.rows || []) {
    await client.query(
      `UPDATE numbers
          SET status = 'available',
              reservation_id = NULL
        WHERE draw_id = $1
          AND reservation_id = $2
          AND status = 'reserved'`,
      [row.draw_id, row.id]
    );
  }
}

function publicDraw(row, config = null, fallbackPriceCents = null) {
  if (!row) return null;
  const productName = row.product_name || config?.banner_title || "Sorteio Secundario";
  const priceCents = Number(config?.ticket_price_cents || fallbackPriceCents || 0);
  return {
    id: row.id,
    status: row.status,
    draw_type: normalizeDrawType(row.draw_type),
    product_name: productName,
    product_link: row.product_link || null,
    banner_title: config?.banner_title || productName,
    promo_phrase: config?.banner_title || productName,
    ticket_price_cents: priceCents,
    price_cents: priceCents,
    max_numbers_per_selection: config?.max_numbers_per_selection ?? null,
    opened_at: row.opened_at || null,
  };
}

router.get("/current", async (_req, res) => {
  const pool = await getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const { rows } = await client.query(
      `SELECT id, status, draw_type, product_name, product_link, opened_at
         FROM draws
        WHERE status = 'open'
          AND draw_type IN ('adicional', 'secundario')
        ORDER BY id ASC
        LIMIT 1`
    );

    const draw = rows[0] || null;
    if (!draw) {
      await client.query("COMMIT");
      return res.json({ draw: null, numbers: [] });
    }

    const config = await client.query(
      `SELECT id, banner_title, ticket_price_cents, max_numbers_per_selection
         FROM app_config_new
        WHERE id = $1`,
      [String(draw.id)]
    );

    await client.query("COMMIT");

    const fallbackPriceCents = await getTicketPriceCents();
    return res.json({ draw: publicDraw(draw, config.rows[0] || null, fallbackPriceCents) });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[secondary_draws/current] error:", e?.code || e?.message || e);
    return res.status(500).json({ error: "secondary_draw_current_failed" });
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
    await expireDrawReservations(client, drawId);

    const draw = await client.query(
      `SELECT id
         FROM draws
        WHERE id = $1
          AND draw_type IN ('adicional', 'secundario')`,
      [drawId]
    );
    if (!draw.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "draw_not_found" });
    }

    const numbersResult = await client.query(
      `SELECT n, status, reservation_id
         FROM numbers
        WHERE draw_id = $1
        ORDER BY n ASC`,
      [drawId]
    );

    const paidRows = await client.query(
      `SELECT
         num.n::int AS n,
         u.name AS owner_name,
         u.email AS owner_email
       FROM public.payments p
       LEFT JOIN public.users u ON u.id = p.user_id
       CROSS JOIN LATERAL unnest(p.numbers) AS num(n)
       WHERE p.draw_id = $1
         AND lower(p.status) IN ('approved', 'paid', 'pago')`,
      [drawId]
    );

    const initialsByNumber = new Map();
    const ownerNameByNumber = new Map();

    for (const row of paidRows.rows || []) {
      const n = Number(row.n);
      const initials = initialsFromNameOrEmail(row.owner_name, row.owner_email);

      if (Number.isFinite(n) && initials) {
        initialsByNumber.set(n, initials);
      }

      if (Number.isFinite(n) && row.owner_name) {
        ownerNameByNumber.set(n, row.owner_name);
      }
    }

    const rows = (numbersResult.rows || []).map((row) => {
      const n = Number(row.n);

      return {
        ...row,
        n,
        owner_initials: initialsByNumber.get(n) || null,
        buyer_initials: initialsByNumber.get(n) || null,
        owner_name: ownerNameByNumber.get(n) || null,
      };
    });

    await client.query("COMMIT");
    return res.json({ draw_id: drawId, numbers: rows });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[secondary_draws/numbers] error:", e?.code || e?.message || e);
    return res.status(500).json({ error: "numbers_failed" });
  } finally {
    client.release();
  }
});

router.post("/:id/reserve", requireAuth, async (req, res) => {
  const drawId = Number(req.params.id);
  if (!Number.isInteger(drawId) || drawId <= 0) {
    return res.status(400).json({ error: "invalid_draw_id" });
  }

  const normalized = normalizeNumbers(req.body?.numbers);
  if (normalized.error) return res.status(400).json({ error: normalized.error });

  const nums = normalized.numbers;
  const pool = await getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    await expireDrawReservations(client, drawId);

    const drawRes = await client.query(
      `SELECT id, status
         FROM draws
        WHERE id = $1
          AND draw_type IN ('adicional', 'secundario')
        FOR UPDATE`,
      [drawId]
    );
    const draw = drawRes.rows[0];
    if (!draw) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "draw_not_found" });
    }
    if (draw.status !== "open") {
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

    const reservationId = uuid();
    const expiresAt = new Date(Date.now() + RESERVATION_TTL_MIN * 60 * 1000);

    await client.query(
      `INSERT INTO reservations (id, user_id, draw_id, numbers, status, expires_at)
       VALUES ($1, $2, $3, $4::int[], 'active', $5)`,
      [reservationId, req.user.id, drawId, nums, expiresAt]
    );

    await client.query(
      `UPDATE numbers
          SET status = 'reserved',
              reservation_id = $3
        WHERE draw_id = $1
          AND n = ANY($2::int[])`,
      [drawId, nums, reservationId]
    );

    const config = await client.query(
      `SELECT ticket_price_cents
         FROM app_config_new
        WHERE id = $1`,
      [String(drawId)]
    );

    await client.query("COMMIT");

    const fallbackPriceCents = await getTicketPriceCents();
    const priceCents = Number(config.rows[0]?.ticket_price_cents || fallbackPriceCents);
    return res.status(201).json({
      reservation_id: reservationId,
      draw_id: drawId,
      secondary_draw_id: drawId,
      numbers: nums,
      expires_at: expiresAt.toISOString(),
      amount_cents: Number(priceCents) * nums.length,
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[secondary_draws/reserve] error:", e?.code || e?.message || e);
    return res.status(500).json({ error: "reserve_failed" });
  } finally {
    client.release();
  }
});

export {
  ensureDrawNumbers,
  expireDrawReservations,
  isAdditionalDrawType,
  normalizeDrawType,
  normalizeNewDrawType,
};
export default router;
