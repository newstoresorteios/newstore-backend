// src/routes/payments.js
import { Router } from 'express';
import { MercadoPagoConfig, Payment } from 'mercadopago';
import { query } from '../db.js';
import { requireAuth } from '../middleware/auth.js';
import { v4 as uuidv4 } from 'uuid';

const router = Router();

const mpClient = new MercadoPagoConfig({
  accessToken: process.env.MP_ACCESS_TOKEN
});
const mpPayment = new Payment(mpClient);

/**
 * POST /api/payments/pix
 * Body: { reservationId }
 * Auth: Bearer
 */
router.post('/pix', requireAuth, async (req, res) => {
  try {
    const { reservationId } = req.body || {};
    if (!reservationId) {
      return res.status(400).json({ error: 'missing_reservation' });
    }

    // Carrega a reserva + usuário
    const r = await query(
      `select r.id, r.user_id, r.draw_id, r.numbers, r.status, r.expires_at, u.email, u.name
         from reservations r
         join users u on u.id = r.user_id
        where r.id = $1`,
      [reservationId]
    );
    if (!r.rows.length) return res.status(404).json({ error: 'reservation_not_found' });

    const rs = r.rows[0];

    if (rs.status !== 'active') {
      return res.status(400).json({ error: 'reservation_not_active' });
    }
    if (new Date(rs.expires_at).getTime() < Date.now()) {
      return res.status(400).json({ error: 'reservation_expired' });
    }

    // Valor
    const priceCents = Number(process.env.PRICE_CENTS || 5500);
    const amount = Number(((rs.numbers.length * priceCents) / 100).toFixed(2));

    // Descrição e webhook
    const description = `Sorteio New Store - números ${rs.numbers
      .map((n) => n.toString().padStart(2, '0'))
      .join(', ')}`;

    // URL pública do backend para o webhook
    const baseUrl =
      process.env.PUBLIC_URL ||
      `${req.protocol}://${req.get('host')}`;
    const notification_url = `${baseUrl.replace(/\/$/, '')}/api/payments/webhook`;

    // Cria pagamento PIX no MP (idempotente)
    const mpResp = await mpPayment.create({
      body: {
        transaction_amount: amount,
        description,
        payment_method_id: 'pix',
        payer: { email: rs.email },
        external_reference: String(reservationId),
        notification_url,
        date_of_expiration: new Date(Date.now() + 30 * 60 * 1000).toISOString()
      },
      requestOptions: { idempotencyKey: uuidv4() }
    });

    const body = mpResp?.body || mpResp;
    const { id, status, point_of_interaction } = body || {};
    const td = point_of_interaction?.transaction_data || {};

    // Normaliza QR/copia-e-cola
    let { qr_code, qr_code_base64 } = td;
    if (typeof qr_code_base64 === 'string') qr_code_base64 = qr_code_base64.replace(/\s+/g, '');
    if (typeof qr_code === 'string') qr_code = qr_code.replace(/\s+/g, '');

    // Persiste o pagamento
    await query(
      `insert into payments(id, user_id, draw_id, numbers, amount_cents, status, qr_code, qr_code_base64)
       values($1,$2,$3,$4,$5,$6,$7,$8)
       on conflict (id) do update
         set status = excluded.status,
             qr_code = coalesce(excluded.qr_code, payments.qr_code),
             qr_code_base64 = coalesce(excluded.qr_code_base64, payments.qr_code_base64)`,
      [
        String(id),
        rs.user_id,
        rs.draw_id,
        rs.numbers,
        rs.numbers.length * priceCents,
        status,
        qr_code || null,
        qr_code_base64 || null
      ]
    );

    // Amarra a reserva ao pagamento
    await query(
      `update reservations set payment_id = $2 where id = $1`,
      [reservationId, String(id)]
    );

    return res.json({
      paymentId: String(id),
      status,
      qr_code,
      qr_code_base64
    });
  } catch (e) {
    console.error('[pix] error:', e);
    return res.status(500).json({ error: 'pix_failed' });
  }
});

/**
 * GET /api/payments/:id/status
 * Auth: Bearer
 */
router.get('/:id/status', requireAuth, async (req, res) => {
  try {
    const { id } = req.params;
    const resp = await mpPayment.get({ id: String(id) });
    const body = resp?.body || resp;

    await query(
      `update payments set status = $2 where id = $1`,
      [id, body.status]
    );

    if (body.status === 'approved') {
      const pr = await query(
        `select draw_id, numbers from payments where id = $1`,
        [id]
      );
      if (pr.rows.length) {
        const { draw_id, numbers } = pr.rows[0];

        // marca números como vendidos
        await query(
          `update numbers
              set status = 'sold', reservation_id = null
            where draw_id = $1 and n = any($2)`,
          [draw_id, numbers]
        );

        // marca reserva como paga
        await query(`update reservations set status = 'paid' where payment_id = $1`, [id]);

        // se vendeu 100, fecha o sorteio e abre um novo
        const cnt = await query(
          `select count(*)::int as sold
             from numbers
            where draw_id = $1 and status = 'sold'`,
          [draw_id]
        );
        if (cnt.rows[0]?.sold === 100) {
          await query(`update draws set status = 'closed', closed_at = now() where id = $1`, [draw_id]);
          const newDraw = await query(`insert into draws(status) values('open') returning id`);
          const newId = newDraw.rows[0].id;
          const tuples = [];
          for (let i = 0; i < 100; i++) tuples.push(`($1, ${i}, 'available', null)`);
          await query(
            `insert into numbers(draw_id, n, status, reservation_id) values ${tuples.join(', ')}`,
            [newId]
          );
        }
      }
    }

    return res.json({ id, status: body.status });
  } catch (e) {
    console.error('[status] error:', e);
    return res.status(500).json({ error: 'status_failed' });
  }
});

/**
 * POST /api/payments/webhook
 * Body: evento do Mercado Pago
 */
router.post('/webhook', async (req, res) => {
  try {
    const paymentId = req.body?.data?.id || req.query?.id || req.body?.id;
    const type = req.body?.type || req.query?.type;

    // Ignora eventos não relacionados a pagamento
    if (type && type !== 'payment') {
      return res.sendStatus(200);
    }
    if (!paymentId) {
      return res.sendStatus(200);
    }

    const resp = await mpPayment.get({ id: String(paymentId) });
    const body = resp?.body || resp;

    const id = String(body.id);
    const status = body.status;

    // Atualiza status + paid_at
    await query(
      `update payments
          set status = $2,
              paid_at = case when $2 = 'approved' then now() else paid_at end
        where id = $1`,
      [id, status]
    );

    if (status === 'approved') {
      const pr = await query(`select draw_id, numbers from payments where id = $1`, [id]);
      if (pr.rows.length) {
        const { draw_id, numbers } = pr.rows[0];

        await query(
          `update numbers
              set status = 'sold', reservation_id = null
            where draw_id = $1 and n = any($2)`,
          [draw_id, numbers]
        );

        // fecha sorteio se vendeu 100
        const cnt = await query(
          `select count(*)::int as sold
             from numbers
            where draw_id = $1 and status = 'sold'`,
          [draw_id]
        );
        if (cnt.rows[0]?.sold === 100) {
          await query(`update draws set status = 'closed', closed_at = now() where id = $1`, [draw_id]);
          const newDraw = await query(`insert into draws(status) values('open') returning id`);
          const newId = newDraw.rows[0].id;
          const tuples = [];
          for (let i = 0; i < 100; i++) tuples.push(`($1, ${i}, 'available', null)`);
          await query(
            `insert into numbers(draw_id, n, status, reservation_id) values ${tuples.join(', ')}`,
            [newId]
          );
        }
      }
    }

    return res.sendStatus(200);
  } catch (e) {
    console.error('[webhook] error:', e);
    // sempre 200 para o MP não reenfileirar indefinidamente
    return res.sendStatus(200);
  }
});

export default router;
