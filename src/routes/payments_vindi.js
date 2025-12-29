// backend/src/routes/payments_vindi.js
// Webhook handler para eventos Vindi
import express from "express";
import { getPool } from "../db.js";
import { parseWebhook, getBill, getCharge } from "../services/vindi.js";

const router = express.Router();

/**
 * POST /api/payments/vindi/webhook
 * Recebe webhooks da Vindi e atualiza status de pagamentos
 */
router.post("/vindi/webhook", express.raw({ type: "application/json" }), async (req, res) => {
  const pool = await getPool();
  const client = await pool.connect();

  try {
    // Parse do payload (pode vir como string ou objeto)
    let payload;
    try {
      if (typeof req.body === "string") {
        payload = JSON.parse(req.body);
      } else {
        payload = req.body;
      }
    } catch (e) {
      console.error("[vindi/webhook] erro ao parsear payload:", e?.message);
      return res.status(400).json({ error: "invalid_payload" });
    }

    // Validação básica (se houver secret, validar aqui)
    const webhookSecret = process.env.VINDI_WEBHOOK_SECRET;
    if (webhookSecret) {
      // TODO: Implementar validação de assinatura se Vindi fornecer
      // Por enquanto, apenas logamos que recebemos
      console.log("[vindi/webhook] recebido (secret configurado mas não validado)");
    }

    // Parse do evento
    let event;
    try {
      event = parseWebhook(payload);
    } catch (e) {
      console.error("[vindi/webhook] erro ao parsear evento:", e?.message);
      return res.status(400).json({ error: "invalid_event" });
    }

    console.log("[vindi/webhook] evento recebido", {
      type: event.type,
      billId: event.billId,
      chargeId: event.chargeId,
    });

    await client.query("BEGIN");

    // Busca payment pelo bill_id ou charge_id
    let payment = null;
    if (event.billId) {
      const result = await client.query(
        `select * from public.payments where vindi_bill_id = $1 limit 1`,
        [event.billId]
      );
      if (result.rows.length) {
        payment = result.rows[0];
      }
    }

    if (!payment && event.chargeId) {
      const result = await client.query(
        `select * from public.payments where vindi_charge_id = $1 limit 1`,
        [event.chargeId]
      );
      if (result.rows.length) {
        payment = result.rows[0];
      }
    }

    if (!payment) {
      console.warn("[vindi/webhook] payment não encontrado", {
        billId: event.billId,
        chargeId: event.chargeId,
      });
      await client.query("COMMIT");
      return res.json({ ok: true, message: "payment_not_found" });
    }

    // Verifica se já processou este evento (idempotência)
    // Por enquanto, atualizamos sempre, mas podemos adicionar uma tabela de eventos processados

    // Atualiza status baseado no tipo de evento
    let newStatus = payment.status;
    let updateFields = {};
    let shouldReconcile = false;

    switch (event.type) {
      case "bill.paid":
      case "charge.paid":
        newStatus = "approved";
        updateFields.vindi_status = "paid";
        shouldReconcile = true;
        break;

      case "bill.failed":
      case "charge.rejected":
        newStatus = "rejected";
        updateFields.vindi_status = "rejected";
        break;

      case "charge.refunded":
        newStatus = "refunded";
        updateFields.vindi_status = "refunded";
        // Se foi refundado, pode precisar liberar números
        shouldReconcile = true;
        break;

      case "bill.canceled":
        newStatus = "canceled";
        updateFields.vindi_status = "canceled";
        break;

      default:
        console.log("[vindi/webhook] evento não mapeado", { type: event.type });
        // Atualiza apenas vindi_status se disponível
        if (event.status) {
          updateFields.vindi_status = event.status;
        }
    }

    // Atualiza payment
    if (Object.keys(updateFields).length > 0 || newStatus !== payment.status) {
      const setClauses = [];
      const values = [];
      let paramIndex = 1;

      if (newStatus !== payment.status) {
        setClauses.push(`status = $${paramIndex++}`);
        values.push(newStatus);
      }

      Object.entries(updateFields).forEach(([key, value]) => {
        setClauses.push(`${key} = $${paramIndex++}`);
        values.push(value);
      });

      if (setClauses.length > 0) {
        values.push(payment.id);
        await client.query(
          `update public.payments set ${setClauses.join(", ")} where id = $${paramIndex}`,
          values
        );
      }

      // Se foi pago, atualiza paid_at
      if (newStatus === "approved" && !payment.paid_at) {
        await client.query(
          `update public.payments set paid_at = now() where id = $1`,
          [payment.id]
        );
      }
    }

    // Reconcilição: se necessário, atualiza números/reservas
    if (shouldReconcile && payment.draw_id && payment.numbers) {
      if (newStatus === "approved") {
        // Garante que números estão marcados como sold
        // (já deveriam estar, mas garante consistência)
        const reservation = await client.query(
          `select id from public.reservations 
           where draw_id = $1 and payment_id = $2 limit 1`,
          [payment.draw_id, payment.id]
        );

        if (reservation.rows.length) {
          const resvId = reservation.rows[0].id;
          await client.query(
            `update public.numbers 
             set status = 'sold', reservation_id = $1
             where draw_id = $2 and n = any($3::int2[])`,
            [resvId, payment.draw_id, payment.numbers]
          );
        }
      } else if (newStatus === "refunded") {
        // Se foi refundado, libera números (se ainda não foram usados)
        // CUIDADO: isso pode ser perigoso se o sorteio já foi realizado
        // Por enquanto, apenas logamos
        console.warn("[vindi/webhook] refund detectado - números podem precisar ser liberados manualmente", {
          paymentId: payment.id,
          drawId: payment.draw_id,
        });
      }
    }

    await client.query("COMMIT");

    console.log("[vindi/webhook] processado com sucesso", {
      paymentId: payment.id,
      eventType: event.type,
      newStatus,
    });

    res.json({ ok: true, processed: true, paymentId: payment.id, status: newStatus });
  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    console.error("[vindi/webhook] erro ao processar:", e?.message || e);
    res.status(500).json({ error: "webhook_processing_failed" });
  } finally {
    client.release();
  }
});

export default router;

