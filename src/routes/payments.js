// src/routes/payments.js
import { Router } from 'express';
import { MercadoPagoConfig, Payment } from 'mercadopago';
import { query } from '../db.js';
import { requireAuth } from '../middleware/auth.js';
import { v4 as uuidv4 } from 'uuid';

const router = Router();

// Aceita MP_ACCESS_TOKEN (backend) ou REACT_APP_MP_ACCESS_TOKEN (Render)
const mpClient = new MercadoPagoConfig({
  accessToken: process.env.MP_ACCESS_TOKEN || process.env.REACT_APP_MP_ACCESS_TOKEN,
});
const mpPayment = new Payment(mpClient);

/**
 * POST /api/payments/pix
 * Body: { reservationId }
 * Auth: Bearer
 */
router.post('/pix', requireAuth, async (req, res) => {
  console.log('[payments/pix] user=', req.user?.id, 'body=', req.body);
  try {
    if (!req.user?.id) return res.status(401).json({ error: 'unauthorized' });

    const { reservationId } = req.body || {};
    if (!reservationId) {
      return res.status(400).json({ error: 'missing_reservation' });
    }

    // Corrige reservas antigas sem user_id (anexa ao usuário atual)
    await query(
      `update reservations set user_id = $2
         where id = $1 and user_id is null`,
      [reservationId, req.user.id]
    );

    // Carrega a reserva + (opcional) usuário
    const r = await query(
      `select r.id, r.user_id, r.draw_id, r.numbers, r.status, r.expires_at,
              u.email as user_email, u.name as user_name
         from reservations r
    left join users u on u.id = r.user_id
        where r.id = $1`,
      [reservationId]
    );
    if (!r.rows.length) return res.status(404).json({ error: 'reservation_not_found' });

    const rs = r.rows[0];

    if (rs.status !== 'active') return res.status(400).json({ error: 'reservation_not_active' });
    if (new Date(rs.expires_at).getTime() < Date.now()) {
      return res.status(400).json({ error: 'reservation_expired' });
    }

    // Valor (preço * quantidade)
    const priceCents = Number(
      process.env.PRICE_CENTS ||
        (Number(process.env.REACT_APP_PIX_PRICE) * 100) ||
        5500
    );
    const amount = Number(((rs.numbers.length * priceCents) / 100).toFixed(2));

    // Descrição e webhook
    const description = `Sorteio New Store - números ${rs.numbers
      .map((n) => n.toString().padStart(2, '0'))
      .join(', ')}`;

    const baseUrl = (process.env.PUBLIC_URL || `${req.protocol}://${req.get('host')}`).replace(/\/$/, '');
    const notification_url = `${baseUrl}/api/payments/webhook`;

    // E-mail do pagador
    const payerEmail = rs.user_email || req.user?.email || 'comprador@example.com';

    // Cria pagamento PIX no Mercado Pago (idempotente)
    const mpResp = await mpPayment.create({
      body: {
        transaction_amount: amount,
        description,
        payment_method_id: 'pix',
        payer: { email: payerEmail },
        external_reference: String(reservationId),
        notification_url,
        date_of_expiration: new Date(Date.now() + 30 * 60 * 1000).toISOString(),
      },
      requestOptions: { idempotencyKey: uuidv4() },
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
        rs.user_id || req.user.id,
        rs.draw_id,
        rs.numbers,
        rs.numbers.length * priceCents,
        status,
        qr_code || null,
        qr_code_base64 || null,
      ]
    );

    // Amarra a reserva ao pagamento (status segue 'active' até aprovar)
    await query(`update reservations set payment_id = $2 where id = $1`, [reservationId, String(id)]);

    return res.json({ paymentId: String(id), status, qr_code, qr_code_base64 });
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

    await query(`update payments set status = $2 where id = $1`, [id, body.status]);

    if (body.status === 'approved') {
      const pr = await query(`select draw_id, numbers from payments where id = $1`, [id]);
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

        // fecha sorteio se vendeu 100 e abre novo
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

    if (type && type !== 'payment') return res.sendStatus(200);
    if (!paymentId) return res.sendStatus(200);

    const resp = await mpPayment.get({ id: String(paymentId) });
    const body = resp?.body || resp;

    const id = String(body.id);
    const status = body.status;

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

        // fecha sorteio se vendeu 100 e abre novo
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

    // Sempre 200 para o MP não reenfileirar indefinidamente
    return res.sendStatus(200);
  } catch (e) {
    console.error('[webhook] error:', e);
    return res.sendStatus(200);
  }
});

// === LISTA MEUS PAGAMENTOS (para a conta) ===
// GET /api/payments/me  -> { payments: [...] }
router.get('/me', requireAuth, async (req, res) => {
  try {
    const r = await query(
      `select id,
              user_id,
              draw_id,
              numbers,
              amount_cents,
              status,
              created_at,
              paid_at
         from payments
        where user_id = $1
        order by coalesce(paid_at, created_at) asc`,
      [req.user.id]
    );
    return res.json({ payments: r.rows || [] });
  } catch (e) {
    console.error('[payments/me] error:', e);
    return res.status(500).json({ error: 'list_failed' });
  }
});

export default router;
