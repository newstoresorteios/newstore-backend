import { Router } from "express";
import { MercadoPagoConfig, Payment } from "mercadopago";
import { v4 as uuid } from "uuid";
import { getPool } from "../db.js";
import { requireAuth } from "../middleware/auth.js";
import { getTicketPriceCents } from "../services/config.js";
import { getMercadoPagoAccessToken } from "../services/mercadopago.js";
import { expireDrawReservations, isAdditionalDrawType } from "./secondary_draws.js";

const router = Router();

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

router.post("/pix", requireAuth, async (req, res) => {
  const { reservation_id: reservationId } = req.body || {};
  if (!reservationId) return res.status(400).json({ error: "missing_reservation_id" });

  const pool = await getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    await expireDrawReservations(client);

    const reservationRes = await client.query(
      `SELECT r.id,
              r.user_id,
              r.draw_id,
              r.numbers,
              r.status,
              r.expires_at,
              d.draw_type,
              d.product_name,
              c.ticket_price_cents,
              u.email AS user_email,
              u.name AS user_name
         FROM reservations r
         JOIN draws d ON d.id = r.draw_id
    LEFT JOIN app_config_new c ON c.id = r.draw_id::text
    LEFT JOIN users u ON u.id = r.user_id
        WHERE r.id = $1
        FOR UPDATE OF r`,
      [reservationId]
    );

    const reservation = reservationRes.rows[0];
    if (!reservation) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "reservation_not_found" });
    }
    if (!isAdditionalDrawType(reservation.draw_type)) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "draw_not_found" });
    }
    if (Number(reservation.user_id) !== Number(req.user.id)) {
      await client.query("ROLLBACK");
      return res.status(403).json({ error: "reservation_not_owned" });
    }
    if (reservation.status !== "active") {
      await client.query("ROLLBACK");
      return res.status(400).json({ error: "reservation_not_active" });
    }
    if (new Date(reservation.expires_at).getTime() <= Date.now()) {
      await client.query("ROLLBACK");
      return res.status(400).json({ error: "reservation_expired" });
    }
    const token = getMercadoPagoAccessToken();
    if (!token) {
      await client.query("ROLLBACK");
      return res.status(503).json({ error: "mp_token_missing" });
    }

    const numbers = (reservation.numbers || []).map(Number);
    const fallbackPrice = await getTicketPriceCents();
    const priceCents = Number(reservation.ticket_price_cents || fallbackPrice);
    const amountCents = Number(priceCents) * numbers.length;

    await client.query("COMMIT");

    const description = `Sorteio adicional New Store - ${numbers
      .map((n) => n.toString().padStart(2, "0"))
      .join(", ")}`;
    const notificationUrl = `${resolveBaseUrl(req)}/api/payments/webhook`;
    const payerEmail = reservation.user_email || req.user?.email || "comprador@example.com";
    const mpPayment = new Payment(new MercadoPagoConfig({ accessToken: token }));

    let mpResp;
    try {
      mpResp = await mpPayment.create({
        body: {
          transaction_amount: amountFromCents(amountCents),
          description,
          payment_method_id: "pix",
          payer: { email: payerEmail },
          external_reference: String(reservation.id),
          metadata: {
            source: "additional_draw",
            draw_id: Number(reservation.draw_id),
            reservation_id: reservation.id,
            user_id: Number(req.user.id),
            numbers,
          },
          notification_url: notificationUrl,
          date_of_expiration: new Date(reservation.expires_at).toISOString(),
        },
        requestOptions: { idempotencyKey: uuid() },
      });
    } catch (e) {
      console.error("[secondary_payments/pix][mercadopago] error:", e?.status || e?.code || e?.message || e);
      return res.status(502).json({ error: "mercado_pago_payment_failed" });
    }

    const body = mpResp?.body || mpResp;
    const td = body?.point_of_interaction?.transaction_data || {};
    const paymentId = body?.id != null ? String(body.id) : null;
    const status = body?.status || "pending";
    const qrCode = typeof td.qr_code === "string" ? td.qr_code.trim() : null;
    const qrCodeBase64 =
      typeof td.qr_code_base64 === "string" ? td.qr_code_base64.replace(/\s+/g, "") : null;

    if (!paymentId) {
      return res.status(502).json({ error: "mercado_pago_payment_failed" });
    }

    try {
      await pool.query(
        `INSERT INTO payments
          (id, user_id, draw_id, numbers, amount_cents, status, qr_code, qr_code_base64, provider)
         VALUES ($1, $2, $3, $4::int[], $5, $6, $7, $8, 'mercadopago')
         ON CONFLICT (id) DO UPDATE
           SET status = EXCLUDED.status,
               qr_code = COALESCE(EXCLUDED.qr_code, payments.qr_code),
               qr_code_base64 = COALESCE(EXCLUDED.qr_code_base64, payments.qr_code_base64),
               provider = COALESCE(EXCLUDED.provider, payments.provider)`,
        [
          paymentId,
          reservation.user_id || req.user.id,
          reservation.draw_id,
          numbers,
          amountCents,
          status,
          qrCode,
          qrCodeBase64,
        ]
      );

      await pool.query(
        `UPDATE reservations
            SET payment_id = $2
          WHERE id = $1`,
        [reservation.id, paymentId]
      );
    } catch (e) {
      console.error("[secondary_payments/pix][persist] error:", e?.code || e?.message || e);
      return res.status(500).json({ error: "payment_create_failed" });
    }

    return res.status(201).json({
      payment_id: paymentId,
      paymentId,
      status,
      amount_cents: amountCents,
      qr_code: qrCode,
      qr_code_base64: qrCodeBase64,
      expires_at: reservation.expires_at,
      numbers,
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[secondary_payments/pix] error:", e?.status || e?.code || e?.message || e);
    return res.status(500).json({ error: "secondary_pix_failed" });
  } finally {
    client.release();
  }
});

export default router;
