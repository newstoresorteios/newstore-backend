import crypto from "node:crypto";
import { getPool as defaultGetPool } from "../db.js";
import { pendingCaptivePreauthReservationGuardSql } from "./reservationExpiry.js";

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const ALLOWED_ITEM_KEYS = new Set(["draw_id", "numbers"]);
const MULTI_DRAW_REQUIRED_MESSAGE =
  "O pagamento agrupado exige seleções em pelo menos dois sorteios diferentes.";
const COUNTING_RESERVATION_STATUSES = [
  "reservado", "pago", "pendente", "aprovado", "vendido", "indisponivel",
  "confirmado", "processando", "aguardando", "reserved", "paid", "pending",
  "approved", "sold", "taken", "confirmed", "processing", "awaiting",
];

export class CheckoutBatchError extends Error {
  constructor(code, status = 400, payload = {}) {
    super(code);
    this.name = "CheckoutBatchError";
    this.code = code;
    this.status = status;
    this.payload = payload;
  }
}

export function normalizeCheckoutSelection(items) {
  if (!Array.isArray(items) || items.length === 0) {
    throw new CheckoutBatchError("items_required", 400);
  }

  const normalized = items.map((item) => {
    if (!item || typeof item !== "object" || Array.isArray(item)) {
      throw new CheckoutBatchError("invalid_item", 400);
    }
    const forbidden = Object.keys(item).filter((key) => !ALLOWED_ITEM_KEYS.has(key));
    if (forbidden.length) {
      throw new CheckoutBatchError("forbidden_checkout_fields", 400, { fields: forbidden });
    }
    const drawId = item.draw_id;
    if (!Number.isInteger(drawId) || drawId <= 0) {
      throw new CheckoutBatchError("invalid_draw_id", 400);
    }
    if (!Array.isArray(item.numbers)) {
      throw new CheckoutBatchError("numbers_required", 400, { draw_id: drawId });
    }
    const parsed = item.numbers;
    if (parsed.some((number) => !Number.isInteger(number) || number < 0)) {
      throw new CheckoutBatchError("invalid_numbers", 400, { draw_id: drawId });
    }
    const numbers = [...new Set(parsed)].sort((a, b) => a - b);
    return { draw_id: drawId, numbers };
  }).filter((item) => item.numbers.length > 0)
    .sort((a, b) => a.draw_id - b.draw_id);

  for (let index = 1; index < normalized.length; index += 1) {
    if (normalized[index - 1].draw_id === normalized[index].draw_id) {
      throw new CheckoutBatchError("duplicate_draw_id", 400, {
        draw_id: normalized[index].draw_id,
      });
    }
  }

  const selectionHash = crypto
    .createHash("sha256")
    .update(JSON.stringify({ items: normalized }))
    .digest("hex");
  return { items: normalized, selectionHash };
}

export function assertMultiDrawCheckout(items) {
  const distinctDrawIds = new Set(
    (items || [])
      .filter((item) => Array.isArray(item?.numbers) && item.numbers.length > 0)
      .map((item) => Number(item.draw_id))
  );
  if (distinctDrawIds.size < 2) {
    throw new CheckoutBatchError(
      "multi_draw_checkout_requires_multiple_draws",
      422,
      { message: MULTI_DRAW_REQUIRED_MESSAGE }
    );
  }
  return items;
}

export function validateIdempotencyKey(value) {
  const key = String(value || "").trim();
  if (!key) throw new CheckoutBatchError("idempotency_key_required", 400);
  if (!UUID_RE.test(key)) throw new CheckoutBatchError("invalid_idempotency_key", 400);
  return key.toLowerCase();
}

function normalizeDrawType(value) {
  return String(value || "principal").toLowerCase();
}

function intConfig(value, code) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new CheckoutBatchError(code, 409);
  }
  return parsed;
}

function batchItemJson(row) {
  return {
    draw_id: Number(row.draw_id),
    draw_type: row.draw_type,
    reservation_id: row.reservation_id,
    numbers: (row.numbers || []).map(Number),
    unit_price_cents: Number(row.unit_price_cents),
    amount_cents: Number(row.amount_cents),
    child_payment_id: row.child_payment_id || null,
  };
}

export function checkoutBatchJson(batch, items = []) {
  const normalizedItems = items.map(batchItemJson);
  const settled = Boolean(batch.settled_at) || batch.status === "settled";
  const qrCode = batch.qr_code || null;
  return {
    batch_id: batch.id,
    payment_id: batch.provider_payment_id || null,
    paymentId: batch.provider_payment_id || null,
    payment_type: "checkout_batch",
    status: batch.status,
    amount_cents: Number(batch.amount_cents),
    total_numbers: normalizedItems.reduce((sum, item) => sum + item.numbers.length, 0),
    expires_at: new Date(batch.expires_at).toISOString(),
    paid: settled,
    settled,
    qr_code: qrCode,
    qr_code_base64: batch.qr_code_base64 || null,
    copy_paste_code: qrCode,
    error_code: batch.error_code || null,
    items: normalizedItems,
  };
}

export async function loadCheckoutBatch(batchId, runner, { userId, forUpdate = false } = {}) {
  const params = [batchId];
  let ownerFilter = "";
  if (userId != null) {
    params.push(userId);
    ownerFilter = `AND user_id = $${params.length}`;
  }
  const batchResult = await runner.query(
    `SELECT * FROM public.checkout_batches
      WHERE id = $1 ${ownerFilter}
      ${forUpdate ? "FOR UPDATE" : ""}`,
    params
  );
  const batch = batchResult.rows[0] || null;
  if (!batch) return null;
  const itemResult = await runner.query(
    `SELECT * FROM public.checkout_batch_items
      WHERE batch_id = $1
      ORDER BY draw_id ASC${forUpdate ? " FOR UPDATE" : ""}`,
    [batchId]
  );
  return { batch, items: itemResult.rows || [] };
}

async function expireReservationsForDraws(client, drawIds) {
  const expired = await client.query(
    `UPDATE public.reservations r
        SET status = 'expired'
      WHERE r.draw_id = ANY($1::int[])
        AND r.expires_at <= now()
        AND lower(coalesce(r.status, '')) IN ('active', 'pending', 'reserved', '')
        AND ${pendingCaptivePreauthReservationGuardSql("r")}
      RETURNING r.id`,
    [drawIds]
  );
  const reservationIds = (expired.rows || []).map((row) => row.id);
  if (reservationIds.length) {
    await client.query(
      `UPDATE public.numbers
          SET status = 'available', reservation_id = NULL
        WHERE reservation_id = ANY($1::uuid[])
          AND status = 'reserved'`,
      [reservationIds]
    );
  }
}

async function loadPricing(client, drawsById, normalizedItems) {
  const principal = normalizedItems.find((item) => drawsById.get(item.draw_id).draw_type === "principal");
  let principalPrice = null;
  let principalSelectionMax = null;
  if (principal) {
    const config = await client.query(
      `SELECT key, value FROM public.app_config
        WHERE key IN ('ticket_price_cents', 'max_numbers_per_selection')`
    );
    const byKey = new Map((config.rows || []).map((row) => [row.key, row.value]));
    principalPrice = intConfig(byKey.get("ticket_price_cents"), "principal_ticket_price_missing");
    principalSelectionMax = intConfig(
      byKey.get("max_numbers_per_selection"),
      "principal_selection_limit_missing"
    );
  }

  const additionalIds = normalizedItems
    .filter((item) => drawsById.get(item.draw_id).draw_type !== "principal")
    .map((item) => String(item.draw_id));
  const additionalConfigs = additionalIds.length
    ? await client.query(
        `SELECT id, ticket_price_cents, max_numbers_per_selection
           FROM public.app_config_new
          WHERE id = ANY($1::text[])`,
        [additionalIds]
      )
    : { rows: [] };
  const additionalById = new Map((additionalConfigs.rows || []).map((row) => [String(row.id), row]));

  return normalizedItems.map((item) => {
    const draw = drawsById.get(item.draw_id);
    if (draw.draw_type === "principal") {
      if (item.numbers.length > principalSelectionMax) {
        throw new CheckoutBatchError("max_numbers_per_selection", 409, {
          draw_id: item.draw_id,
          max: principalSelectionMax,
        });
      }
      return { ...item, draw_type: draw.draw_type, unit_price_cents: principalPrice };
    }
    const config = additionalById.get(String(item.draw_id));
    if (!config) {
      throw new CheckoutBatchError("additional_config_not_found", 409, { draw_id: item.draw_id });
    }
    const unitPrice = intConfig(config.ticket_price_cents, "invalid_ticket_price");
    const max = intConfig(config.max_numbers_per_selection, "invalid_selection_limit");
    if (item.numbers.length > max) {
      throw new CheckoutBatchError("max_numbers_per_selection", 409, {
        draw_id: item.draw_id,
        max,
      });
    }
    return { ...item, draw_type: draw.draw_type, unit_price_cents: unitPrice };
  }).map((item) => ({
    ...item,
    amount_cents: item.unit_price_cents * item.numbers.length,
  }));
}

async function enforcePrincipalPurchaseLimit(client, userId, pricedItems) {
  const principal = pricedItems.find((item) => item.draw_type === "principal");
  if (!principal) return;
  const result = await client.query(
    `SELECT COALESCE(SUM(cardinality(numbers)), 0)::int AS count
       FROM public.reservations
      WHERE user_id = $1
        AND draw_id = $2
        AND lower(coalesce(status, '')) = ANY($3::text[])`,
    [userId, principal.draw_id, COUNTING_RESERVATION_STATUSES]
  );
  const current = Number(result.rows?.[0]?.count || 0);
  const max = Number(process.env.MAX_NUMBERS_PER_USER || 20);
  if (current >= max || current + principal.numbers.length > max) {
    throw new CheckoutBatchError("max_numbers_reached", 409, { current, max });
  }
}

function groupConflicts(rows) {
  const grouped = new Map();
  for (const row of rows) {
    const drawId = Number(row.draw_id);
    if (!grouped.has(drawId)) grouped.set(drawId, []);
    grouped.get(drawId).push(Number(row.n));
  }
  return [...grouped.entries()].map(([draw_id, numbers]) => ({
    draw_id,
    numbers: numbers.sort((a, b) => a - b),
  }));
}

export async function reserveCheckoutBatch(
  { userId, items, idempotencyKey },
  { getPool = defaultGetPool, now = () => new Date(), uuid = () => crypto.randomUUID() } = {}
) {
  const uid = Number(userId);
  if (!Number.isInteger(uid) || uid <= 0) throw new CheckoutBatchError("unauthorized", 401);
  const key = validateIdempotencyKey(idempotencyKey);
  const normalized = normalizeCheckoutSelection(items);
  assertMultiDrawCheckout(normalized.items);
  const pool = await getPool();
  const client = await pool.connect();
  let transactionOpen = false;

  try {
    await client.query("BEGIN");
    transactionOpen = true;
    await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [`checkout:${uid}:${key}`]);

    const existing = await client.query(
      `SELECT id, selection_hash FROM public.checkout_batches
        WHERE user_id = $1 AND idempotency_key = $2
        FOR UPDATE`,
      [uid, key]
    );
    if (existing.rowCount) {
      if (existing.rows[0].selection_hash !== normalized.selectionHash) {
        throw new CheckoutBatchError("idempotency_key_reused", 409);
      }
      const loaded = await loadCheckoutBatch(existing.rows[0].id, client);
      await client.query("COMMIT");
      transactionOpen = false;
      return checkoutBatchJson(loaded.batch, loaded.items);
    }

    const drawIds = normalized.items.map((item) => item.draw_id);
    const drawResult = await client.query(
      `SELECT id, status, coalesce(draw_type, 'principal') AS draw_type
         FROM public.draws
        WHERE id = ANY($1::int[])
        ORDER BY id ASC
        FOR UPDATE`,
      [drawIds]
    );
    if (drawResult.rowCount !== drawIds.length) {
      const found = new Set(drawResult.rows.map((row) => Number(row.id)));
      throw new CheckoutBatchError("draw_not_found", 404, {
        draw_ids: drawIds.filter((id) => !found.has(id)),
      });
    }
    const drawsById = new Map(drawResult.rows.map((row) => [Number(row.id), {
      ...row,
      draw_type: normalizeDrawType(row.draw_type),
    }]));
    for (const draw of drawsById.values()) {
      if (draw.status !== "open") {
        throw new CheckoutBatchError("draw_not_open", 409, { draw_id: Number(draw.id) });
      }
      if (!["principal", "adicional", "secundario"].includes(draw.draw_type)) {
        throw new CheckoutBatchError("draw_type_not_allowed", 409, { draw_id: Number(draw.id) });
      }
    }
    const principalItems = normalized.items.filter(
      (item) => drawsById.get(item.draw_id).draw_type === "principal"
    );
    if (principalItems.length > 1) throw new CheckoutBatchError("multiple_principal_draws", 409);
    if (principalItems.length === 1) {
      const current = await client.query(
        `SELECT id FROM public.draws
          WHERE status = 'open' AND coalesce(draw_type, 'principal') = 'principal'
          ORDER BY id DESC LIMIT 1`
      );
      if (Number(current.rows?.[0]?.id) !== principalItems[0].draw_id) {
        throw new CheckoutBatchError("principal_draw_not_current", 409);
      }
    }

    await expireReservationsForDraws(client, drawIds);
    const pricedItems = await loadPricing(client, drawsById, normalized.items);
    await enforcePrincipalPurchaseLimit(client, uid, pricedItems);

    const flattenedDrawIds = [];
    const flattenedNumbers = [];
    for (const item of pricedItems) {
      for (const number of item.numbers) {
        flattenedDrawIds.push(item.draw_id);
        flattenedNumbers.push(number);
      }
    }
    const locked = await client.query(
      `WITH requested AS (
         SELECT * FROM unnest($1::int[], $2::int[]) AS value(draw_id, n)
       )
       SELECT requested.draw_id, requested.n, numbers.status, numbers.reservation_id
         FROM requested
         JOIN public.numbers
           ON numbers.draw_id = requested.draw_id AND numbers.n = requested.n
        ORDER BY requested.draw_id, requested.n
        FOR UPDATE OF numbers`,
      [flattenedDrawIds, flattenedNumbers]
    );
    const foundKeys = new Set(locked.rows.map((row) => `${Number(row.draw_id)}:${Number(row.n)}`));
    const missing = flattenedNumbers
      .map((number, index) => ({ draw_id: flattenedDrawIds[index], n: number }))
      .filter((row) => !foundKeys.has(`${row.draw_id}:${row.n}`));
    if (missing.length) {
      throw new CheckoutBatchError("numbers_not_found", 400, {
        conflicts: groupConflicts(missing),
      });
    }
    const unavailable = locked.rows.filter(
      (row) => String(row.status || "").toLowerCase() !== "available"
    );
    if (unavailable.length) {
      throw new CheckoutBatchError("batch_numbers_unavailable", 409, {
        conflicts: groupConflicts(unavailable),
      });
    }

    const batchId = uuid();
    const ttlMin = Math.max(1, Number(process.env.RESERVATION_TTL_MIN || 5));
    const expiresAt = new Date(now().getTime() + ttlMin * 60 * 1000);
    const amountCents = pricedItems.reduce((sum, item) => sum + item.amount_cents, 0);
    await client.query(
      `INSERT INTO public.checkout_batches
        (id, user_id, idempotency_key, selection_hash, status, provider, amount_cents, expires_at)
       VALUES ($1, $2, $3, $4, 'reserved', 'mercadopago', $5, $6)`,
      [batchId, uid, key, normalized.selectionHash, amountCents, expiresAt]
    );

    const responseItems = [];
    for (const item of pricedItems) {
      const reservationId = uuid();
      const itemId = uuid();
      await client.query(
        `INSERT INTO public.reservations
          (id, user_id, draw_id, numbers, status, expires_at)
         VALUES ($1, $2, $3, $4::int[], 'active', $5)`,
        [reservationId, uid, item.draw_id, item.numbers, expiresAt]
      );
      await client.query(
        `INSERT INTO public.checkout_batch_items
          (id, batch_id, reservation_id, draw_id, draw_type, numbers, unit_price_cents, amount_cents)
         VALUES ($1, $2, $3, $4, $5, $6::int[], $7, $8)`,
        [itemId, batchId, reservationId, item.draw_id, item.draw_type, item.numbers,
          item.unit_price_cents, item.amount_cents]
      );
      const updated = await client.query(
        `UPDATE public.numbers
            SET status = 'reserved', reservation_id = $3
          WHERE draw_id = $1 AND n = ANY($2::int[]) AND status = 'available'`,
        [item.draw_id, item.numbers, reservationId]
      );
      if (updated.rowCount !== item.numbers.length) {
        throw new CheckoutBatchError("batch_numbers_unavailable", 409, {
          conflicts: [{ draw_id: item.draw_id, numbers: item.numbers }],
        });
      }
      responseItems.push({ ...item, reservation_id: reservationId, child_payment_id: null });
    }

    await client.query("COMMIT");
    transactionOpen = false;
    return checkoutBatchJson({
      id: batchId,
      status: "reserved",
      amount_cents: amountCents,
      expires_at: expiresAt,
    }, responseItems);
  } catch (error) {
    if (transactionOpen) await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}
