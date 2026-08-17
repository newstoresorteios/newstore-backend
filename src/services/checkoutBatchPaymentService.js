import { getPool as defaultGetPool } from "../db.js";
import { closeDrawIfSoldOut as defaultCloseDrawIfSoldOut } from "./drawLifecycle.js";
import { creditCouponOnApprovedPayment as defaultCreditCoupon } from "./couponBalance.js";
import { mpCreatePixPayment as defaultCreatePix, mpGetPayment as defaultGetPayment } from "./mercadopago.js";
import { checkoutBatchJson, CheckoutBatchError, loadCheckoutBatch } from "./checkoutBatchService.js";
import { pendingCaptivePreauthReservationGuardSql } from "./reservationExpiry.js";

const ACTIVE_RESERVATION_STATUSES = new Set(["active", "reserved", "pending", ""]);
const FINAL_BATCH_STATUSES = new Set(["settled", "expired", "manual_review"]);
const PAYMENT_CREATION_STALE_MS = 30_000;
const FALLBACK_PAYER_EMAIL = "comprador@example.com";

export function normalizeCheckoutProviderStatus(value) {
  const status = String(value || "pending").toLowerCase();
  if (["approved", "paid", "pago"].includes(status)) return "approved";
  if (["expired", "cancelled", "canceled"].includes(status)) return "expired";
  if (["rejected", "failed", "refunded", "charged_back"].includes(status)) return "failed";
  return "pending";
}

function safeErrorCode(error) {
  return String(error?.code || error?.response?.error || error?.message || "payment_create_failed")
    .slice(0, 240);
}

function safeProviderErrorDetails(error) {
  const causes = Array.isArray(error?.response?.cause)
    ? error.response.cause
        .map((cause) => cause?.description || cause?.message || cause?.code)
        .filter(Boolean)
        .join(" | ")
    : "";
  return String(
    causes ||
      error?.response?.message ||
      error?.response?.error?.message ||
      error?.message ||
      error?.response?.error ||
      "mercado_pago_error"
  ).slice(0, 500);
}

function validEmail(value) {
  const email = String(value || "").trim();
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) ? email : null;
}

function isStalePaymentCreation(batch, nowMs) {
  const startedAt = Date.parse(batch?.payment_create_started_at || "");
  return !Number.isFinite(startedAt) || nowMs - startedAt >= PAYMENT_CREATION_STALE_MS;
}

export function resolveBatchNotificationUrl(value = process.env.PUBLIC_URL) {
  const publicUrl = String(value || "").trim().replace(/\/+$/, "");
  if (!publicUrl) return undefined;

  try {
    const parsed = new URL(publicUrl);
    const hostname = parsed.hostname.toLowerCase();
    const isLocal = hostname === "localhost" || hostname === "127.0.0.1" ||
      hostname === "::1" || hostname === "0.0.0.0";
    if (isLocal || parsed.protocol !== "https:") return undefined;
    return `${publicUrl}/api/payments/webhook`;
  } catch {
    return undefined;
  }
}

function normalizeQr(providerPayment) {
  const data = providerPayment?.point_of_interaction?.transaction_data || {};
  return {
    qrCode: typeof data.qr_code === "string" ? data.qr_code.trim() : null,
    qrCodeBase64: typeof data.qr_code_base64 === "string"
      ? data.qr_code_base64.replace(/\s+/g, "")
      : null,
  };
}

async function withTransaction(getPool, callback) {
  const pool = await getPool();
  const client = await pool.connect();
  let transactionOpen = false;
  try {
    await client.query("BEGIN");
    transactionOpen = true;
    const result = await callback(client);
    await client.query("COMMIT");
    transactionOpen = false;
    return result;
  } catch (error) {
    if (transactionOpen) await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

async function expireCheckoutBatchWithClient(client, batch, items) {
  if (["approved", "settled", "manual_review"].includes(batch.status) || batch.paid_at) {
    return false;
  }
  const reservationIds = items.map((item) => item.reservation_id);
  if (reservationIds.length) {
    await client.query(
      `UPDATE public.reservations reservation
          SET status = 'expired'
        WHERE reservation.id = ANY($1::uuid[])
          AND lower(coalesce(reservation.status, '')) IN ('active', 'pending', 'reserved', '')
          AND ${pendingCaptivePreauthReservationGuardSql("reservation")}`,
      [reservationIds]
    );
    await client.query(
      `UPDATE public.numbers number
          SET status = 'available', reservation_id = NULL
        WHERE number.status = 'reserved'
          AND number.reservation_id = ANY($1::uuid[])
          AND EXISTS (
            SELECT 1 FROM public.reservations reservation
             WHERE reservation.id = number.reservation_id
               AND reservation.status = 'expired'
          )`,
      [reservationIds]
    );
  }
  await client.query(
    `UPDATE public.checkout_batches
        SET status = 'expired', updated_at = now(), error_code = COALESCE(error_code, 'reservation_expired')
      WHERE id = $1`,
    [batch.id]
  );
  batch.status = "expired";
  return true;
}

export async function expireCheckoutBatch(batchId, {
  getPool = defaultGetPool,
  force = false,
} = {}) {
  return withTransaction(getPool, async (client) => {
    const loaded = await loadCheckoutBatch(batchId, client, { forUpdate: true });
    if (!loaded) return null;
    if (loaded.batch.status === "settled") return checkoutBatchJson(loaded.batch, loaded.items);
    if (!force && new Date(loaded.batch.expires_at).getTime() > Date.now()) {
      return checkoutBatchJson(loaded.batch, loaded.items);
    }
    await expireCheckoutBatchWithClient(client, loaded.batch, loaded.items);
    return checkoutBatchJson(loaded.batch, loaded.items);
  });
}

async function validateBatchForPix(client, loaded, userId, requestPayerEmail, nowMs) {
  const { batch, items } = loaded;
  if (!String(batch.id || "").trim()) throw new CheckoutBatchError("batch_id_missing", 409);
  if (Number(batch.user_id) !== Number(userId)) throw new CheckoutBatchError("batch_not_owned", 403);
  if (FINAL_BATCH_STATUSES.has(batch.status)) {
    throw new CheckoutBatchError(`batch_${batch.status}`, 409);
  }
  const expiresAtMs = new Date(batch.expires_at).getTime();
  if (!Number.isFinite(expiresAtMs) || expiresAtMs <= nowMs) {
    throw new CheckoutBatchError("batch_expired", 409);
  }
  if (!items.length) throw new CheckoutBatchError("batch_items_missing", 409);
  const distinctDrawIds = new Set(items.map((item) => Number(item.draw_id)));
  if (distinctDrawIds.size < 2) {
    throw new CheckoutBatchError("multi_draw_checkout_requires_multiple_draws", 422, {
      message: "O pagamento agrupado exige seleções em pelo menos dois sorteios diferentes.",
    });
  }
  const amountCents = Number(batch.amount_cents);
  if (!Number.isInteger(amountCents) || amountCents <= 0) {
    throw new CheckoutBatchError("batch_amount_invalid", 409);
  }
  const reservationIds = items.map((item) => item.reservation_id);
  const reservations = await client.query(
    `SELECT * FROM public.reservations
      WHERE id = ANY($1::uuid[])
      ORDER BY draw_id ASC FOR UPDATE`,
    [reservationIds]
  );
  const byId = new Map(reservations.rows.map((row) => [String(row.id), row]));
  let sum = 0;
  for (const item of items) {
    const reservation = byId.get(String(item.reservation_id));
    if (!reservation || Number(reservation.user_id) !== Number(userId)) {
      throw new CheckoutBatchError("batch_reservation_invalid", 409);
    }
    if (!ACTIVE_RESERVATION_STATUSES.has(String(reservation.status || "").toLowerCase())) {
      throw new CheckoutBatchError("batch_reservation_not_active", 409);
    }
    if (new Date(reservation.expires_at).getTime() <= nowMs) {
      throw new CheckoutBatchError("batch_expired", 409);
    }
    if (
      Number(reservation.draw_id) !== Number(item.draw_id) ||
      JSON.stringify((reservation.numbers || []).map(Number).sort((a, b) => a - b)) !==
        JSON.stringify((item.numbers || []).map(Number).sort((a, b) => a - b)) ||
      Number(item.amount_cents) !== Number(item.unit_price_cents) * item.numbers.length
    ) {
      throw new CheckoutBatchError("batch_item_inconsistent", 409);
    }
    sum += Number(item.amount_cents);
  }
  if (sum !== Number(batch.amount_cents)) throw new CheckoutBatchError("batch_amount_inconsistent", 409);

  const flatDrawIds = [];
  const flatNumbers = [];
  const itemByNumber = new Map();
  for (const item of items) {
    for (const number of item.numbers) {
      const normalizedNumber = Number(number);
      flatDrawIds.push(Number(item.draw_id));
      flatNumbers.push(normalizedNumber);
      itemByNumber.set(`${Number(item.draw_id)}:${normalizedNumber}`, item);
    }
  }
  const numberRows = await client.query(
    `WITH requested AS (
       SELECT * FROM unnest($1::int[], $2::int[]) AS value(draw_id, n)
     )
     SELECT requested.draw_id, requested.n, numbers.status, numbers.reservation_id
       FROM requested
       JOIN public.numbers
         ON numbers.draw_id = requested.draw_id AND numbers.n = requested.n
      ORDER BY requested.draw_id, requested.n
      FOR UPDATE OF numbers`,
    [flatDrawIds, flatNumbers]
  );
  if (numberRows.rowCount !== flatNumbers.length) {
    throw new CheckoutBatchError("batch_item_inconsistent", 409);
  }
  for (const row of numberRows.rows) {
    const item = itemByNumber.get(`${Number(row.draw_id)}:${Number(row.n)}`);
    if (
      String(row.status || "").toLowerCase() !== "reserved" ||
      String(row.reservation_id) !== String(item?.reservation_id)
    ) {
      throw new CheckoutBatchError("batch_reservation_not_active", 409);
    }
  }

  const userResult = await client.query(
    `SELECT email FROM public.users WHERE id = $1 LIMIT 1`,
    [batch.user_id]
  );
  const payerEmail =
    validEmail(userResult.rows?.[0]?.email) ||
    validEmail(requestPayerEmail) ||
    validEmail(FALLBACK_PAYER_EMAIL);
  if (!payerEmail) throw new CheckoutBatchError("payer_email_invalid", 409);
  return { payerEmail };
}

export function buildCheckoutBatchPixPayload({ batch, items, payerEmail, notificationUrl }) {
  const amountCents = Number(batch?.amount_cents);
  if (!String(batch?.id || "").trim()) throw new CheckoutBatchError("batch_id_missing", 409);
  if (!Number.isInteger(amountCents) || amountCents <= 0) {
    throw new CheckoutBatchError("batch_amount_invalid", 409);
  }
  const transactionAmount = Number((amountCents / 100).toFixed(2));
  if (typeof transactionAmount !== "number" || !Number.isFinite(transactionAmount) || transactionAmount <= 0) {
    throw new CheckoutBatchError("batch_transaction_amount_invalid", 409);
  }
  const expiresAt = new Date(batch.expires_at);
  if (!Number.isFinite(expiresAt.getTime()) || expiresAt.getTime() <= Date.now()) {
    throw new CheckoutBatchError("batch_expired", 409);
  }
  const drawIds = new Set((items || []).map((item) => Number(item.draw_id)));
  if (drawIds.size < 2) {
    throw new CheckoutBatchError("multi_draw_checkout_requires_multiple_draws", 422);
  }
  const email = validEmail(payerEmail);
  if (!email) throw new CheckoutBatchError("payer_email_invalid", 409);
  const totalNumbers = items.reduce((sum, item) => sum + (item.numbers || []).length, 0);

  return {
    transaction_amount: transactionAmount,
    description: `New Store — ${totalNumbers} participações em ${items.length} sorteios`,
    payerEmail: email,
    external_reference: String(batch.id),
    metadata: {
      source: "multi_draw_checkout",
      checkout_batch_id: String(batch.id),
      batch_id: String(batch.id),
      user_id: Number(batch.user_id),
      total_numbers: Number(totalNumbers),
      total_draws: Number(items.length),
    },
    notification_url: notificationUrl,
    date_of_expiration: expiresAt.toISOString(),
    idempotencyKey: String(batch.id),
  };
}

export async function createCheckoutBatchPix(
  { batchId, userId, payerEmail, notificationUrl },
  { getPool = defaultGetPool, createPix = defaultCreatePix, now = () => new Date() } = {}
) {
  const nowDate = now();
  const nowMs = nowDate.getTime();
  const prepared = await withTransaction(getPool, async (client) => {
    const loaded = await loadCheckoutBatch(batchId, client, { forUpdate: true });
    if (!loaded) throw new CheckoutBatchError("batch_not_found", 404);
    if (Number(loaded.batch.user_id) !== Number(userId)) {
      throw new CheckoutBatchError("batch_not_owned", 403);
    }
    if (
      new Date(loaded.batch.expires_at).getTime() <= nowMs &&
      !["approved", "settled", "manual_review"].includes(loaded.batch.status)
    ) {
      await expireCheckoutBatchWithClient(client, loaded.batch, loaded.items);
      return { action: "expired", response: checkoutBatchJson(loaded.batch, loaded.items) };
    }
    if (loaded.batch.provider_payment_id) {
      return { action: "existing", response: checkoutBatchJson(loaded.batch, loaded.items) };
    }
    if (
      loaded.batch.status === "creating_payment" &&
      !isStalePaymentCreation(loaded.batch, nowMs)
    ) {
      return { action: "creating", response: checkoutBatchJson(loaded.batch, loaded.items) };
    }
    const validation = await validateBatchForPix(
      client,
      loaded,
      userId,
      payerEmail,
      nowMs
    );
    await client.query(
      `UPDATE public.checkout_batches
          SET status = 'creating_payment', payment_create_started_at = now(),
              error_code = NULL, updated_at = now()
        WHERE id = $1`,
      [batchId]
    );
    loaded.batch.status = "creating_payment";
    return {
      action: "create",
      batch: loaded.batch,
      items: loaded.items,
      payerEmail: validation.payerEmail,
    };
  });

  if (prepared.action === "expired") throw new CheckoutBatchError("batch_expired", 409);
  if (prepared.action !== "create") return prepared;
  let pixPayload;
  let providerPayment;
  try {
    pixPayload = buildCheckoutBatchPixPayload({
      batch: prepared.batch,
      items: prepared.items,
      payerEmail: prepared.payerEmail,
      notificationUrl,
    });
    providerPayment = await createPix(pixPayload);
  } catch (error) {
    console.error("[checkout_batches/pix][mercadopago] error", {
      batchId: String(batchId),
      userId: Number(userId),
      amountCents: Number(prepared.batch.amount_cents),
      transactionAmount: pixPayload?.transaction_amount || null,
      notificationUrl: notificationUrl || null,
      status: error?.status || null,
      code: error?.code || null,
      message: error?.message || null,
      response: error?.response || null,
    });
    await withTransaction(getPool, async (client) => {
      await client.query(
        `UPDATE public.checkout_batches
            SET status = 'reserved', payment_create_started_at = NULL,
                error_code = $2, updated_at = now()
          WHERE id = $1 AND provider_payment_id IS NULL AND status = 'creating_payment'`,
        [batchId, safeErrorCode(error)]
      );
    });
    throw new CheckoutBatchError("mp_pix_create_failed", 502, {
      details: safeProviderErrorDetails(error),
      retryable: true,
      batch_id: String(batchId),
    });
  }

  const paymentId = providerPayment?.id != null ? String(providerPayment.id) : "";
  const qr = normalizeQr(providerPayment);
  if (!paymentId || !qr.qrCode) {
    const missingCode = !paymentId ? "provider_payment_id_missing" : "provider_qr_code_missing";
    await withTransaction(getPool, (client) => client.query(
      `UPDATE public.checkout_batches
          SET status = 'reserved', payment_create_started_at = NULL,
              error_code = $2, updated_at = now()
        WHERE id = $1 AND provider_payment_id IS NULL`,
      [batchId, missingCode]
    ));
    throw new CheckoutBatchError("mp_pix_create_failed", 502, {
      details: "Mercado Pago did not return complete PIX data",
      retryable: true,
      batch_id: String(batchId),
    });
  }
  const providerStatus = normalizeCheckoutProviderStatus(providerPayment.status);
  const persisted = await withTransaction(getPool, async (client) => {
    const loaded = await loadCheckoutBatch(batchId, client, { forUpdate: true });
    if (!loaded) throw new CheckoutBatchError("batch_not_found", 404);
    if (loaded.batch.provider_payment_id && String(loaded.batch.provider_payment_id) !== paymentId) {
      await client.query(
        `UPDATE public.checkout_batches SET status = 'manual_review',
                error_code = 'provider_payment_id_mismatch', updated_at = now() WHERE id = $1`,
        [batchId]
      );
      throw new CheckoutBatchError("provider_payment_id_mismatch", 409);
    }
    await client.query(
      `UPDATE public.checkout_batches
          SET provider_payment_id = $2, status = $3,
              qr_code = COALESCE($4, qr_code),
              qr_code_base64 = COALESCE($5, qr_code_base64),
              paid_at = CASE WHEN $3 = 'approved' THEN COALESCE(paid_at, now()) ELSE paid_at END,
              payment_create_started_at = NULL, error_code = NULL, updated_at = now()
        WHERE id = $1`,
      [batchId, paymentId, providerStatus, qr.qrCode, qr.qrCodeBase64]
    );
    loaded.batch.provider_payment_id = paymentId;
    loaded.batch.status = providerStatus;
    loaded.batch.qr_code = qr.qrCode || loaded.batch.qr_code;
    loaded.batch.qr_code_base64 = qr.qrCodeBase64 || loaded.batch.qr_code_base64;
    return checkoutBatchJson(loaded.batch, loaded.items);
  });
  if (providerStatus === "approved") {
    return { action: "created", response: await settleApprovedCheckoutBatch(batchId, { getPool }) };
  }
  return { action: "created", response: persisted };
}

async function markManualReview(client, batchId, code) {
  await client.query(
    `UPDATE public.checkout_batches
        SET status = 'manual_review', error_code = $2, updated_at = now()
      WHERE id = $1`,
    [batchId, code]
  );
}

export async function settleApprovedCheckoutBatch(batchId, {
  getPool = defaultGetPool,
  closeDrawIfSoldOut = defaultCloseDrawIfSoldOut,
  creditCoupon = defaultCreditCoupon,
} = {}) {
  let settlement;
  try {
    settlement = await withTransaction(getPool, async (client) => {
    const loaded = await loadCheckoutBatch(batchId, client, { forUpdate: true });
    if (!loaded) throw new CheckoutBatchError("batch_not_found", 404);
    const { batch, items } = loaded;
    if (batch.settled_at || batch.status === "settled") {
      return {
        response: checkoutBatchJson(batch, items),
        childPayments: items
          .filter((item) => item.child_payment_id)
          .map((item) => ({ id: item.child_payment_id, item })),
      };
    }
    if (batch.status !== "approved" || !batch.provider_payment_id) {
      throw new CheckoutBatchError("batch_not_approved", 409);
    }
    const reservationIds = items.map((item) => item.reservation_id);
    const reservations = await client.query(
      `SELECT * FROM public.reservations
        WHERE id = ANY($1::uuid[]) ORDER BY draw_id ASC FOR UPDATE`,
      [reservationIds]
    );
    const flatDrawIds = [];
    const flatNumbers = [];
    for (const item of items) for (const number of item.numbers) {
      flatDrawIds.push(Number(item.draw_id));
      flatNumbers.push(Number(number));
    }
    const numbersResult = await client.query(
      `WITH requested AS (
         SELECT * FROM unnest($1::int[], $2::int[]) AS value(draw_id, n)
       )
       SELECT requested.draw_id, requested.n, numbers.status, numbers.reservation_id
         FROM requested
         JOIN public.numbers
           ON numbers.draw_id = requested.draw_id AND numbers.n = requested.n
        ORDER BY requested.draw_id, requested.n
        FOR UPDATE OF numbers`,
      [flatDrawIds, flatNumbers]
    );
    const reservationById = new Map(reservations.rows.map((row) => [String(row.id), row]));
    const itemByKey = new Map();
    let sum = 0;
    for (const item of items) {
      const reservation = reservationById.get(String(item.reservation_id));
      if (
        !reservation ||
        Number(reservation.user_id) !== Number(batch.user_id) ||
        Number(reservation.draw_id) !== Number(item.draw_id) ||
        JSON.stringify((reservation.numbers || []).map(Number).sort((a, b) => a - b)) !==
          JSON.stringify((item.numbers || []).map(Number).sort((a, b) => a - b)) ||
        Number(item.amount_cents) !== Number(item.unit_price_cents) * item.numbers.length
      ) {
        await markManualReview(client, batchId, "settlement_reservation_mismatch");
        return { response: { ...checkoutBatchJson(batch, items), status: "manual_review", error_code: "settlement_reservation_mismatch" }, childPayments: [] };
      }
      sum += Number(item.amount_cents);
      for (const number of item.numbers) itemByKey.set(`${item.draw_id}:${Number(number)}`, item);
    }
    if (sum !== Number(batch.amount_cents) || numbersResult.rowCount !== flatNumbers.length) {
      await markManualReview(client, batchId, "settlement_amount_or_number_mismatch");
      return { response: { ...checkoutBatchJson(batch, items), status: "manual_review", error_code: "settlement_amount_or_number_mismatch" }, childPayments: [] };
    }
    for (const number of numbersResult.rows) {
      const item = itemByKey.get(`${Number(number.draw_id)}:${Number(number.n)}`);
      if (number.status !== "reserved" || String(number.reservation_id) !== String(item?.reservation_id)) {
        await markManualReview(client, batchId, "settlement_reservation_lost");
        return { response: { ...checkoutBatchJson(batch, items), status: "manual_review", error_code: "settlement_reservation_lost" }, childPayments: [] };
      }
    }

    const childPayments = [];
    const paidAt = batch.paid_at || new Date();
    for (const item of items) {
      const childId = `batch:${batch.provider_payment_id}:${item.reservation_id}`;
      const existing = await client.query(
        `SELECT id, user_id, draw_id, numbers, amount_cents FROM public.payments WHERE id = $1 FOR UPDATE`,
        [childId]
      );
      if (existing.rowCount) {
        const payment = existing.rows[0];
        if (
          Number(payment.user_id) !== Number(batch.user_id) ||
          Number(payment.draw_id) !== Number(item.draw_id) ||
          Number(payment.amount_cents) !== Number(item.amount_cents)
        ) {
          await markManualReview(client, batchId, "child_payment_mismatch");
          return { response: { ...checkoutBatchJson(batch, items), status: "manual_review", error_code: "child_payment_mismatch" }, childPayments: [] };
        }
      } else {
        await client.query(
          `INSERT INTO public.payments
            (id, user_id, draw_id, numbers, amount_cents, status, provider, created_at, paid_at, coupon_credited)
           VALUES ($1, $2, $3, $4::int[], $5, 'approved', 'mercadopago', COALESCE($6, now()), $7, false)`,
          [childId, batch.user_id, item.draw_id, item.numbers, item.amount_cents, batch.created_at, paidAt]
        );
      }
      await client.query(
        `UPDATE public.checkout_batch_items SET child_payment_id = $2 WHERE id = $1`,
        [item.id, childId]
      );
      await client.query(
        `UPDATE public.reservations SET payment_id = $2, status = 'paid'
          WHERE id = $1 AND user_id = $3 AND draw_id = $4`,
        [item.reservation_id, childId, batch.user_id, item.draw_id]
      );
      const sold = await client.query(
        `UPDATE public.numbers SET status = 'sold', reservation_id = NULL
          WHERE draw_id = $1 AND n = ANY($2::int[]) AND reservation_id = $3 AND status = 'reserved'`,
        [item.draw_id, item.numbers, item.reservation_id]
      );
      if (sold.rowCount !== item.numbers.length) throw new Error("settlement_number_update_race");
      await closeDrawIfSoldOut(item.draw_id, client);
      item.child_payment_id = childId;
      childPayments.push({ id: childId, item });

      const credit = await creditCoupon(childId, {
        channel: "PIX",
        source: "checkout_batch",
        pgClient: client,
        runTraceId: null,
        meta: {
          checkout_batch_id: batchId,
          provider_payment_id: String(batch.provider_payment_id),
          draw_id: Number(item.draw_id),
          draw_type: item.draw_type,
          pricing_source: "payments.amount_cents",
        },
      });
      if (credit?.ok === false) {
        throw new CheckoutBatchError("checkout_batch_coupon_credit_failed", 500);
      }
    }
    await client.query(
      `UPDATE public.checkout_batches
          SET status = 'settled', settled_at = COALESCE(settled_at, now()),
              paid_at = COALESCE(paid_at, $2), error_code = NULL, updated_at = now()
        WHERE id = $1`,
      [batchId, paidAt]
    );
    batch.status = "settled";
    batch.settled_at = new Date();
    batch.paid_at = paidAt;
    return { response: checkoutBatchJson(batch, items), childPayments };
    });
  } catch (error) {
    if (error?.message !== "settlement_number_update_race") throw error;
    settlement = await withTransaction(getPool, async (client) => {
      const loaded = await loadCheckoutBatch(batchId, client, { forUpdate: true });
      if (!loaded) throw new CheckoutBatchError("batch_not_found", 404);
      await markManualReview(client, batchId, "settlement_number_update_race");
      loaded.batch.status = "manual_review";
      loaded.batch.error_code = "settlement_number_update_race";
      return { response: checkoutBatchJson(loaded.batch, loaded.items), childPayments: [] };
    });
  }

  return settlement.response;
}

async function findBatchForProviderPayment(providerPayment, getPool) {
  const paymentId = providerPayment?.id != null ? String(providerPayment.id) : null;
  const externalReference = String(providerPayment?.external_reference || "").trim();
  const metadataSource = String(providerPayment?.metadata?.source || "").trim();
  const metadataBatchId = metadataSource === "multi_draw_checkout"
    ? String(
        providerPayment?.metadata?.checkout_batch_id ||
        providerPayment?.metadata?.batch_id ||
        ""
      ).trim()
    : "";
  try {
    return await withTransaction(getPool, async (client) => {
      const result = await client.query(
        `SELECT id FROM public.checkout_batches
          WHERE ($1::text IS NOT NULL AND provider_payment_id = $1)
             OR ($2::text <> '' AND id::text = $2)
             OR ($3::text <> '' AND id::text = $3)
          ORDER BY CASE WHEN provider_payment_id = $1 THEN 0 ELSE 1 END
          LIMIT 1`,
        [paymentId, externalReference, metadataBatchId]
      );
      return result.rows?.[0]?.id || null;
    });
  } catch (error) {
    if (error?.code === "42P01") return null;
    throw error;
  }
}

export async function syncCheckoutBatchFromProviderPayment(providerPayment, {
  getPool = defaultGetPool,
  closeDrawIfSoldOut = defaultCloseDrawIfSoldOut,
  creditCoupon = defaultCreditCoupon,
} = {}) {
  const batchId = await findBatchForProviderPayment(providerPayment, getPool);
  if (!batchId) return { handled: false, batch: null };
  const paymentId = String(providerPayment.id);
  const status = normalizeCheckoutProviderStatus(providerPayment.status);
  const qr = normalizeQr(providerPayment);
  const updated = await withTransaction(getPool, async (client) => {
    const loaded = await loadCheckoutBatch(batchId, client, { forUpdate: true });
    if (!loaded) return null;
    if (loaded.batch.status === "settled") return checkoutBatchJson(loaded.batch, loaded.items);
    const statusUpdate = await client.query(
      `UPDATE public.checkout_batches
          SET provider_payment_id = COALESCE(provider_payment_id, $2),
              status = CASE
                WHEN status IN ('settled', 'manual_review') THEN status
                WHEN status IN ('approved', 'expired') AND $3 <> 'approved' THEN status
                ELSE $3
              END,
              qr_code = COALESCE($4, qr_code), qr_code_base64 = COALESCE($5, qr_code_base64),
              paid_at = CASE WHEN $3 = 'approved' THEN COALESCE(paid_at, now()) ELSE paid_at END,
              updated_at = now()
        WHERE id = $1
        RETURNING status, provider_payment_id, qr_code, qr_code_base64, paid_at`,
      [batchId, paymentId, status, qr.qrCode, qr.qrCodeBase64]
    );
    const saved = statusUpdate.rows[0];
    loaded.batch.provider_payment_id = saved.provider_payment_id;
    loaded.batch.status = saved.status;
    loaded.batch.qr_code = saved.qr_code;
    loaded.batch.qr_code_base64 = saved.qr_code_base64;
    loaded.batch.paid_at = saved.paid_at;
    return checkoutBatchJson(loaded.batch, loaded.items);
  });
  let response = updated;
  if (updated.status === "approved") {
    response = await settleApprovedCheckoutBatch(batchId, { getPool, closeDrawIfSoldOut, creditCoupon });
  } else if (updated.status === "expired") {
    response = await expireCheckoutBatch(batchId, { getPool, force: true });
  }
  return { handled: true, batch: response };
}

export async function getCheckoutBatchStatus(batchId, userId, {
  getPool = defaultGetPool,
  getPayment = defaultGetPayment,
  closeDrawIfSoldOut = defaultCloseDrawIfSoldOut,
  creditCoupon = defaultCreditCoupon,
} = {}) {
  const lookup = await withTransaction(getPool, async (client) => {
    const loaded = await loadCheckoutBatch(batchId, client, { userId });
    if (loaded) return { loaded, exists: true };
    const exists = await client.query(`SELECT 1 FROM public.checkout_batches WHERE id = $1`, [batchId]);
    return { loaded: null, exists: exists.rowCount > 0 };
  });
  const loaded = lookup.loaded;
  if (!loaded) throw new CheckoutBatchError(lookup.exists ? "batch_not_owned" : "batch_not_found", lookup.exists ? 403 : 404);
  if (loaded.batch.status === "settled" || loaded.batch.status === "manual_review") {
    return checkoutBatchJson(loaded.batch, loaded.items);
  }
  if (loaded.batch.provider_payment_id) {
    const providerPayment = await getPayment(loaded.batch.provider_payment_id);
    const synced = await syncCheckoutBatchFromProviderPayment(providerPayment, {
      getPool, closeDrawIfSoldOut, creditCoupon,
    });
    return synced.batch;
  }
  if (new Date(loaded.batch.expires_at).getTime() <= Date.now()) {
    return expireCheckoutBatch(batchId, { getPool, force: true });
  }
  return checkoutBatchJson(loaded.batch, loaded.items);
}

export async function reconcilePendingCheckoutBatches({
  getPool = defaultGetPool,
  getPayment = defaultGetPayment,
  closeDrawIfSoldOut = defaultCloseDrawIfSoldOut,
  creditCoupon = defaultCreditCoupon,
  lookbackMinutes = Number(process.env.RECONCILE_LOOKBACK_MIN || 1440),
} = {}) {
  let rows;
  try {
    rows = await withTransaction(getPool, async (client) => {
      const result = await client.query(
        `SELECT id, provider_payment_id, status, expires_at
           FROM public.checkout_batches
          WHERE settled_at IS NULL
            AND (
              status IN ('creating_payment', 'pending', 'approved')
              OR (status IN ('reserved', 'failed') AND expires_at <= now())
            )
            AND created_at >= now() - ($1::int || ' minutes')::interval
          ORDER BY created_at ASC
          LIMIT 100`,
        [Math.max(5, Number(lookbackMinutes || 1440))]
      );
      return result.rows || [];
    });
  } catch (error) {
    if (error?.code === "42P01") return { scanned: 0, updated: 0, settled: 0, expired: 0, failed: 0 };
    throw error;
  }
  const summary = { scanned: rows.length, updated: 0, settled: 0, expired: 0, failed: 0 };
  for (const row of rows) {
    try {
      if (new Date(row.expires_at).getTime() <= Date.now() && row.status !== "approved") {
        const expired = await expireCheckoutBatch(row.id, { getPool, force: true });
        if (expired?.status === "expired") summary.expired += 1;
        continue;
      }
      if (row.status === "approved") {
        const settled = await settleApprovedCheckoutBatch(row.id, { getPool, closeDrawIfSoldOut, creditCoupon });
        if (settled?.status === "settled") summary.settled += 1;
        continue;
      }
      if (row.status === "creating_payment" && !row.provider_payment_id) {
        await withTransaction(getPool, (client) => client.query(
          `UPDATE public.checkout_batches
              SET status = 'reserved', payment_create_started_at = NULL,
                  error_code = 'payment_creation_recovery', updated_at = now()
            WHERE id = $1 AND status = 'creating_payment' AND provider_payment_id IS NULL
              AND payment_create_started_at < now() - interval '30 seconds'`,
          [row.id]
        ));
        continue;
      }
      if (!row.provider_payment_id) continue;
      const providerPayment = await getPayment(row.provider_payment_id);
      const synced = await syncCheckoutBatchFromProviderPayment(providerPayment, {
        getPool, closeDrawIfSoldOut, creditCoupon,
      });
      summary.updated += synced.handled ? 1 : 0;
      if (synced.batch?.status === "settled") summary.settled += 1;
      if (synced.batch?.status === "expired") summary.expired += 1;
    } catch (error) {
      summary.failed += 1;
      console.warn("[checkout-batch][reconcile] failed", { batch_id: row.id, error: safeErrorCode(error) });
    }
  }
  return summary;
}
