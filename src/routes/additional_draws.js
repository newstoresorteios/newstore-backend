import { Router } from "express";
import { v4 as uuid } from "uuid";
import { getPool, query } from "../db.js";
import { requireAuth } from "../middleware/auth.js";

const router = Router();

const RESERVATION_TTL_MIN = Math.max(1, Number(process.env.RESERVATION_TTL_MIN || 30));
const AUTH_COOKIE_NAMES = [
  process.env.AUTH_COOKIE_NAME || "ns_auth",
  "ns_auth_token",
  "token",
  "jwt",
];

const ERROR_MESSAGES = {
  unauthorized: "Faca login para reservar numeros.",
  invalid_draw_id: "Sorteio adicional invalido.",
  numbers_must_be_array: "Selecione ao menos um numero do sorteio adicional.",
  invalid_numbers: "Selecao de numeros invalida.",
  no_numbers: "Selecione ao menos um numero do sorteio adicional.",
  draw_not_found: "Sorteio adicional nao encontrado.",
  draw_not_open: "Esse sorteio adicional nao esta mais disponivel.",
  numbers_not_found: "Numero nao encontrado neste sorteio adicional.",
  numbers_reserved: "Esse numero ja esta reservado temporariamente.",
  numbers_unavailable: "Esse numero nao esta mais disponivel.",
  additional_config_not_found: "Configuracao do sorteio adicional indisponivel.",
  invalid_ticket_price: "Valor do sorteio adicional indisponivel.",
  reserve_failed: "Nao foi possivel reservar numeros do adicional.",
};

function jsonError(res, status, error, extra = {}) {
  return res.status(status).json({
    ok: false,
    error,
    message: ERROR_MESSAGES[error] || ERROR_MESSAGES.reserve_failed,
    ...extra,
  });
}

function isAdditionalDrawType(value) {
  return value === "adicional" || value === "secundario";
}

function sanitizeToken(value) {
  return String(value || "")
    .trim()
    .replace(/^Bearer\s+/i, "")
    .replace(/^['"]|['"]$/g, "");
}

function hasAdditionalReserveCredential(req) {
  const authorization = sanitizeToken(req.headers?.authorization);
  if (authorization) return true;
  const cookies = req.cookies || {};
  for (const name of AUTH_COOKIE_NAMES) {
    const token = sanitizeToken(cookies[name]);
    if (token) return true;
  }
  return false;
}

function requireAdditionalReserveCredential(req, res, next) {
  if (!hasAdditionalReserveCredential(req)) {
    return res.status(401).json({
      ok: false,
      error: "unauthorized",
      message: "Faça login para reservar números.",
    });
  }
  return next();
}

function normalizeNewDrawType(value) {
  return value === "adicional" ? "adicional" : "principal";
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

async function expireDrawReservations(client, drawId = null) {
  const params = [];
  const drawFilter = drawId ? "AND r.draw_id = $1" : "";
  if (drawId) params.push(drawId);

  const expired = await client.query(
    `UPDATE public.reservations r
        SET status = 'expired'
      WHERE r.status = 'active'
        AND r.expires_at < NOW()
        ${drawFilter}
      RETURNING r.id, r.draw_id, r.numbers`,
    params
  );

  for (const row of expired.rows || []) {
    const nums = Array.isArray(row.numbers)
      ? row.numbers.map(Number).filter((n) => Number.isInteger(n) && n >= 0)
      : [];
    if (!nums.length) continue;

    await client.query(
      `UPDATE public.numbers
          SET status = 'available',
              reservation_id = NULL
        WHERE draw_id = $1
          AND n = ANY($2::int[])
          AND reservation_id = $3
          AND status = 'reserved'`,
      [row.draw_id, nums, row.id]
    );
  }
}

async function loadDrawConfigs(drawIds) {
  if (!drawIds.length) return new Map();
  const configs = await query(
    `SELECT id, banner_title, ticket_price_cents, max_numbers_per_selection
       FROM public.app_config_new
      WHERE id = ANY($1::text[])`,
    [drawIds.map(String)]
  );
  return new Map((configs.rows || []).map((row) => [String(row.id), row]));
}

async function formatAdditionalDraw(row, config = null) {
  if (!row) return null;
  const productName = row.product_name || config?.banner_title || "SORTEIO ADICIONAL";
  const configuredPrice = Number(config?.ticket_price_cents);
  const ticketPriceCents =
    Number.isInteger(configuredPrice) && configuredPrice > 0 ? configuredPrice : null;

  return {
    id: row.id,
    status: row.status,
    draw_type: row.draw_type,
    product_name: productName,
    product_link: row.product_link || null,
    banner_title: config?.banner_title || productName,
    promo_phrase: config?.banner_title || productName,
    config_missing: !config,
    ticket_price_cents: ticketPriceCents,
    price_cents: ticketPriceCents,
    max_numbers_per_selection: config?.max_numbers_per_selection ?? null,
    opened_at: row.opened_at || null,
    closed_at: row.closed_at || null,
    realized_at: row.realized_at || null,
    winner_user_id: row.winner_user_id ?? null,
    winner_name: row.winner_name || null,
    winner_number: row.winner_number ?? null,
  };
}

router.get("/open", async (_req, res) => {
  try {
    const draws = await query(
      `SELECT id, status, draw_type, product_name, product_link, opened_at,
              closed_at, realized_at, winner_user_id, winner_name, winner_number
         FROM public.draws
        WHERE status = 'open'
          AND draw_type IN ('adicional', 'secundario')
        ORDER BY id ASC`
    );

    const configMap = await loadDrawConfigs((draws.rows || []).map((row) => row.id));
    const formatted = [];
    for (const draw of draws.rows || []) {
      formatted.push(await formatAdditionalDraw(draw, configMap.get(String(draw.id))));
    }

    return res.json({ draws: formatted });
  } catch (e) {
    console.error("[additional_draws/open] error:", e?.code || e?.message || e);
    return res.status(500).json({ error: "additional_draws_open_failed" });
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
         FROM public.draws
        WHERE id = $1
          AND draw_type IN ('adicional', 'secundario')`,
      [drawId]
    );
    if (!draw.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "draw_not_found" });
    }

    const numbers = await client.query(
      `SELECT n, status, reservation_id
         FROM public.numbers
        WHERE draw_id = $1
        ORDER BY n ASC`,
      [drawId]
    );

    await client.query("COMMIT");
    return res.json({ draw_id: drawId, numbers: numbers.rows || [] });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[additional_draws/numbers] error:", e?.code || e?.message || e);
    return res.status(500).json({ error: "numbers_failed" });
  } finally {
    client.release();
  }
});

router.post("/:id/reserve", requireAdditionalReserveCredential, requireAuth, async (req, res) => {
  const drawId = Number(req.params.id);
  if (!Number.isInteger(drawId) || drawId <= 0) {
    return jsonError(res, 400, "invalid_draw_id");
  }

  const normalized = normalizeNumbers(req.body?.numbers);
  if (normalized.error) return jsonError(res, 400, normalized.error);

  const nums = normalized.numbers;
  const pool = await getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    await expireDrawReservations(client, drawId);

    const drawRes = await client.query(
      `SELECT id, status, draw_type
         FROM public.draws
        WHERE id = $1
          AND draw_type IN ('adicional', 'secundario')
        FOR UPDATE`,
      [drawId]
    );
    const draw = drawRes.rows[0];
    if (!draw) {
      await client.query("ROLLBACK");
      return jsonError(res, 404, "draw_not_found");
    }
    if (draw.status !== "open") {
      await client.query("ROLLBACK");
      return jsonError(res, 400, "draw_not_open");
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
      return jsonError(res, 400, "numbers_not_found", { numbers: notFound });
    }

    const conflicts = locked.rows
      .filter((row) => String(row.status).toLowerCase() !== "available")
      .map((row) => ({
        n: Number(row.n),
        status: String(row.status || "").toLowerCase(),
      }));
    if (conflicts.length) {
      await client.query("ROLLBACK");
      const hasReserved = conflicts.some((item) => item.status === "reserved");
      const error = hasReserved ? "numbers_reserved" : "numbers_unavailable";
      return jsonError(res, 409, error, {
        conflicts: conflicts.map((item) => item.n),
      });
    }

    const reservationId = uuid();
    const expiresAt = new Date(Date.now() + RESERVATION_TTL_MIN * 60 * 1000);

    await client.query(
      `INSERT INTO public.reservations (id, user_id, draw_id, numbers, status, expires_at)
       VALUES ($1, $2, $3, $4::int[], 'active', $5)`,
      [reservationId, req.user.id, drawId, nums, expiresAt]
    );

    await client.query(
      `UPDATE public.numbers
          SET status = 'reserved',
              reservation_id = $3
        WHERE draw_id = $1
          AND n = ANY($2::int[])`,
      [drawId, nums, reservationId]
    );

    const config = await client.query(
      `SELECT ticket_price_cents
         FROM public.app_config_new
        WHERE id = $1`,
      [String(drawId)]
    );
    if (!config.rowCount) {
      await client.query("ROLLBACK");
      return jsonError(res, 404, "additional_config_not_found");
    }

    const priceCents = Number(config.rows[0]?.ticket_price_cents);
    if (!Number.isInteger(priceCents) || priceCents <= 0) {
      await client.query("ROLLBACK");
      return jsonError(res, 400, "invalid_ticket_price");
    }

    await client.query("COMMIT");

    return res.status(201).json({
      reservation_id: reservationId,
      draw_id: drawId,
      numbers: nums,
      expires_at: expiresAt.toISOString(),
      amount_cents: priceCents * nums.length,
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[additional_draws/reserve] error:", e?.code || e?.message || e);
    return jsonError(res, 500, "reserve_failed");
  } finally {
    client.release();
  }
});

export {
  ensureDrawNumbers,
  expireDrawReservations,
  formatAdditionalDraw,
  isAdditionalDrawType,
  loadDrawConfigs,
  normalizeNewDrawType,
};
export default router;
