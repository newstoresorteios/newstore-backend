import { Router } from "express";
import { getPool } from "../db.js";
import { requireAuth } from "../middleware/auth.js";
import { getMercadoPagoAccessToken, mpCreatePixPayment } from "../services/mercadopago.js";
import { expireDrawReservations, isAdditionalDrawType } from "./additional_draws.js";

const router = Router();
const ACTIVE_RESERVATION_STATUSES = new Set(["active", "reserved", "pending"]);

function amountFromCents(cents) {
  return Number((Math.max(0, Number(cents || 0)) / 100).toFixed(2));
}

function resolveBaseUrl(req) {
  const publicUrl = process.env.PUBLIC_URL
    ? String(process.env.PUBLIC_URL).replace(/\/$/, "")
    : "";
  if (publicUrl) return publicUrl;

  const protoRaw = req.headers["x-forwarded-proto"] || req.protocol || "https";
  const proto = String(protoRaw).split(",")[0].trim() || "https";
  const host = req.get("host");
  let fallback = `${proto}://${host}`.replace(/\/$/, "");
  if (process.env.NODE_ENV === "production" && !fallback.startsWith("https://")) {
    fallback = fallback.replace(/^http:\/\//, "https://");
  }
  return fallback;
}

function safeMpErrorMessage(err) {
  return String(
    err?.response?.message ||
      err?.response?.error ||
      err?.message ||
      "mercado_pago_error"
  ).slice(0, 240);
}

router.post("/pix", requireAuth, async (req, res) => {
  const reservationId = req.body?.reservation_id ?? req.body?.reservationId ?? req.body?.id;
  if (!reservationId) return res.status(400).json({ error: "missing_reservation_id" });

  if (!getMercadoPagoAccessToken()) {
    console.warn("[additional_payments/pix] Mercado Pago token missing");
    return res.status(503).json({ error: "mp_token_missing" });
  }

  let client;
  try {
    const pool = await getPool();
    client = await pool.connect();

    await client.query("BEGIN");
    await expireDrawReservations(client);

    const reservationRes = await client.query(
      `SELECT r.id,
              r.user_id,
              r.draw_id,
              r.numbers,
              r.status,
              r.expires_at,
              r.payment_id,
              d.draw_type,
              d.product_name,
              c.id AS config_id,
              c.banner_title,
              c.ticket_price_cents,
              c.max_numbers_per_selection,
              u.email AS user_email,
              u.name AS user_name
         FROM public.reservations r
         JOIN public.draws d ON d.id = r.draw_id
    LEFT JOIN public.app_config_new c ON c.id = r.draw_id::text
    LEFT JOIN public.users u ON u.id = r.user_id
        WHERE r.id = $1
        FOR UPDATE OF r`,
      [reservationId]
    );

    const reservation = reservationRes.rows[0];
    if (!reservation) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "reservation_not_found" });
    }

    if (Number(reservation.user_id) !== Number(req.user.id)) {
      await client.query("ROLLBACK");
      return res.status(403).json({ error: "reservation_not_owned" });
    }

    if (!isAdditionalDrawType(reservation.draw_type)) {
      await client.query("ROLLBACK");
      return res.status(400).json({ error: "draw_not_additional" });
    }

    const reservationStatus = String(reservation.status || "").toLowerCase();
    if (!ACTIVE_RESERVATION_STATUSES.has(reservationStatus)) {
      await client.query("ROLLBACK");
      return res.status(409).json({ error: `reservation_${reservationStatus || "invalid"}` });
    }

    if (new Date(reservation.expires_at).getTime() <= Date.now()) {
      await client.query("ROLLBACK");
      return res.status(409).json({ error: "reservation_expired" });
    }

    const numbers = Array.isArray(reservation.numbers)
      ? reservation.numbers.map(Number).filter((n) => Number.isInteger(n) && n >= 0)
      : [];
    if (!numbers.length) {
      await client.query("ROLLBACK");
      return res.status(400).json({ error: "invalid_numbers" });
    }

    if (!reservation.config_id) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "additional_config_not_found" });
    }

    const ticketPriceCents = Number(reservation.ticket_price_cents);
    if (!Number.isInteger(ticketPriceCents) || ticketPriceCents <= 0) {
      await client.query("ROLLBACK");
      return res.status(400).json({ error: "invalid_ticket_price" });
    }

    const amountCents = ticketPriceCents * numbers.length;
    if (!Number.isInteger(amountCents) || amountCents <= 0) {
      await client.query("ROLLBACK");
      return res.status(400).json({ error: "invalid_amount" });
    }

    if (reservation.payment_id) {
      const existing = await client.query(
        `SELECT id, status, qr_code, qr_code_base64, amount_cents
           FROM public.payments
          WHERE id = $1`,
        [String(reservation.payment_id)]
      );
      const payment = existing.rows[0];
      if (!payment) {
        await client.query("ROLLBACK");
        return res.status(409).json({ error: "reservation_payment_missing" });
      }

      const paymentStatus = String(payment.status || "").toLowerCase();
      if (["approved", "paid", "pago"].includes(paymentStatus)) {
        await client.query("ROLLBACK");
        return res.status(409).json({ error: "reservation_already_paid" });
      }

      if (paymentStatus === "pending") {
        await client.query("COMMIT");
        return res.json({
          payment_id: String(payment.id),
          paymentId: String(payment.id),
          reservation_id: reservation.id,
          draw_id: reservation.draw_id,
          status: payment.status,
          amount_cents: Number(payment.amount_cents || amountCents),
          qr_code: payment.qr_code || null,
          qr_code_base64: payment.qr_code_base64 || null,
          numbers,
        });
      }

      await client.query("ROLLBACK");
      return res.status(409).json({ error: `payment_${paymentStatus || "invalid"}` });
    }

    await client.query("COMMIT");
    client.release();
    client = null;

    const description = `${reservation.product_name || reservation.banner_title || "Sorteio adicional New Store"} - ${numbers
      .map((n) => n.toString().padStart(2, "0"))
      .join(", ")}`;
    const notificationUrl = `${resolveBaseUrl(req)}/api/payments/webhook`;
    const payerEmail = reservation.user_email || req.user?.email || "comprador@example.com";
    const transactionAmount = amountFromCents(amountCents);

    let mpBody;
    try {
      mpBody = await mpCreatePixPayment({
        transaction_amount: transactionAmount,
        description,
        payerEmail,
        external_reference: String(reservation.id),
        metadata: {
          source: "additional_draw",
          draw_id: Number(reservation.draw_id),
          reservation_id: String(reservation.id),
          user_id: Number(req.user.id),
          numbers,
        },
        notification_url: notificationUrl,
        date_of_expiration: new Date(reservation.expires_at).toISOString(),
        idempotencyKey: String(reservation.id),
      });
    } catch (e) {
      console.error("[additional_payments/pix][mercadopago] error:", {
        status: e?.status,
        code: e?.code,
        message: e?.message,
      });
      return res.status(502).json({
        error: "mp_pix_create_failed",
        details: safeMpErrorMessage(e),
      });
    }

    const td = mpBody?.point_of_interaction?.transaction_data || {};
    const paymentId = mpBody?.id != null ? String(mpBody.id) : null;
    const status = mpBody?.status || "pending";
    const qrCode = typeof td.qr_code === "string" ? td.qr_code.trim() : null;
    const qrCodeBase64 =
      typeof td.qr_code_base64 === "string" ? td.qr_code_base64.replace(/\s+/g, "") : null;

    if (!paymentId || !qrCode) {
      return res.status(502).json({
        error: "mp_pix_create_failed",
        details: "Mercado Pago did not return PIX data",
      });
    }

    try {
      const pool = await getPool();
      client = await pool.connect();
      await client.query("BEGIN");

      await client.query(
        `INSERT INTO public.payments AS pay
          (id, user_id, draw_id, numbers, amount_cents, status, qr_code, qr_code_base64, provider, created_at)
         VALUES ($1, $2, $3, $4::int[], $5, $6, $7, $8, 'mercadopago', NOW())
         ON CONFLICT (id) DO UPDATE
           SET status = EXCLUDED.status,
               qr_code = COALESCE(EXCLUDED.qr_code, pay.qr_code),
               qr_code_base64 = COALESCE(EXCLUDED.qr_code_base64, pay.qr_code_base64),
               provider = COALESCE(EXCLUDED.provider, pay.provider)`,
        [
          paymentId,
          req.user.id,
          reservation.draw_id,
          numbers,
          amountCents,
          status,
          qrCode,
          qrCodeBase64,
        ]
      );

      await client.query(
        `UPDATE public.reservations
            SET payment_id = $1
          WHERE id = $2
            AND user_id = $3
            AND draw_id = $4`,
        [paymentId, reservation.id, req.user.id, reservation.draw_id]
      );

      await client.query("COMMIT");
    } catch (e) {
      if (client) {
        try { await client.query("ROLLBACK"); } catch {}
      }
      console.error("[additional_payments/pix][persist] error:", {
        code: e?.code,
        message: e?.message,
        table: e?.table,
        column: e?.column,
      });
      return res.status(500).json({ error: "payment_create_failed" });
    }

    return res.status(201).json({
      payment_id: paymentId,
      paymentId,
      reservation_id: reservation.id,
      draw_id: reservation.draw_id,
      status,
      amount_cents: amountCents,
      qr_code: qrCode,
      qr_code_base64: qrCodeBase64,
      numbers,
    });
  } catch (e) {
    if (client) {
      try { await client.query("ROLLBACK"); } catch {}
    }
    console.error("[additional_payments/pix] error:", {
      code: e?.code,
      message: e?.message,
      table: e?.table,
      column: e?.column,
    });
    return res.status(500).json({ error: "additional_pix_failed" });
  } finally {
    if (client) client.release();
  }
});

export default router;
