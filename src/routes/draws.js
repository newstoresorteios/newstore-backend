// backend/src/routes/draws.js
import { Router } from 'express';
import { query, getPool } from '../db.js';
import { requireAuth } from '../middleware/auth.js';
import { mpChargeCard } from '../services/mercadopago.js';

const router = Router();

/* ------------------------------------------------------------------ *
 * Helpers
 * ------------------------------------------------------------------ */
async function requireAdmin(req, res, next) {
  try {
    const userId = req?.user?.id;
    if (!userId) return res.status(401).json({ error: 'unauthorized' });
    const r = await query('select is_admin from users where id = $1', [userId]);
    if (!r.rows.length || !r.rows[0].is_admin) {
      return res.status(403).json({ error: 'forbidden' });
    }
    return next();
  } catch (e) {
    console.error('[admin check] error', e);
    return res.status(500).json({ error: 'admin_check_failed' });
  }
}

async function getTicketPriceCents(client) {
  const r = await client.query(
    `select value from kv_store where key in ('ticket_price_cents','price_cents') limit 1`
  );
  const v = r.rows?.[0]?.value;
  if (v && Number(v) > 0) return Number(v);
  const r2 = await client.query(`select price_cents from app_config order by id desc limit 1`);
  return Number(r2.rows?.[0]?.price_cents || 300);
}

async function isNumberFree(client, draw_id, n) {
  const q = `
    with
    p as (
      select 1 from payments
       where draw_id=$1
         and lower(status) in ('approved','paid','pago')
         and $2 = any(numbers) limit 1
    ),
    r as (
      select 1 from reservations
       where draw_id=$1
         and lower(status) in ('active','pending','paid')
         and $2 = any(numbers) limit 1
    )
    select
      coalesce((select 1 from p),0) as taken_pay,
      coalesce((select 1 from r),0) as taken_resv
  `;
  const r = await client.query(q, [draw_id, n]);
  return !(r.rows[0].taken_pay || r.rows[0].taken_resv);
}

/* ------------------------------------------------------------------ *
 * LISTAGENS EXISTENTES
 * ------------------------------------------------------------------ */

router.get('/', async (req, res) => {
  try {
    const status = String(req.query.status || '').toLowerCase();
    let sql = `
      select id,status,
             coalesce(opened_at, created_at) as opened_at,
             closed_at, realized_at, winner_name
      from draws`;
    const args = [];
    if (status === 'open' || status === 'closed') {
      sql += ` where status = $1`;
      args.push(status);
    }
    sql += ` order by id desc`;
    const { rows } = await query(sql, args);
    return res.json({ draws: rows });
  } catch (e) {
    console.error('[draws] list error', e);
    return res.status(500).json({ error: 'list_failed' });
  }
});

router.get('/history', async (_req, res) => {
  try {
    const { rows } = await query(`
      select id,status,
             coalesce(opened_at, created_at) as opened_at,
             closed_at, realized_at,
             coalesce(winner_name,'-') as winner_name
      from draws
      where status = 'closed' or closed_at is not null
      order by id desc
    `);
    return res.json({ draws: rows });
  } catch (e) {
    console.error('[draws/history] error', e);
    return res.status(500).json({ error: 'history_failed' });
  }
});

router.get('/:id/numbers', async (req, res) => {
  try {
    const drawId = Number(req.params.id);
    if (!drawId) return res.json({ numbers: [] });
    const r = await query(
      'select n, status from numbers where draw_id=$1 order by n asc',
      [drawId]
    );
    res.json({ numbers: r.rows.map(x => ({ n: x.n, status: x.status })) });
  } catch (e) {
    console.error('[draws/:id/numbers] error', e);
    res.status(500).json({ error: 'numbers_failed' });
  }
});

router.get('/:id/participants', requireAuth, async (req, res) => {
  try {
    const drawId = Number(req.params.id);
    if (!Number.isFinite(drawId)) return res.status(400).json({ error: 'invalid_draw_id' });

    const sql = `
      select
        p.id as payment_id,
        p.draw_id,
        p.user_id,
        num as number,
        case when lower(coalesce(p.status,'')) = 'approved' then 'paid'
             else lower(coalesce(p.status,'')) end as status,
        p.created_at,
        coalesce(nullif(u.name,''), u.email, '-') as user_name,
        u.email as user_email
      from payments p
      left join users u on u.id = p.user_id
      cross join lateral unnest(coalesce(p.numbers, '{}'::int[])) as num
      where p.draw_id = $1
        and lower(coalesce(p.status,'')) in ('approved','paid','confirmed')
      order by user_name asc, number asc
    `;
    const r = await query(sql, [drawId]);
    res.json({ draw_id: drawId, participants: r.rows || [] });
  } catch (e) {
    console.error('[draws/:id/participants] error:', e);
    res.status(500).json({ error: 'participants_failed' });
  }
});

router.get('/:id/players', requireAuth, async (req, res) => {
  try {
    const drawId = Number(req.params.id);
    if (!Number.isFinite(drawId)) return res.status(400).json({ error: 'invalid_draw_id' });
    const sql = `
      select
        p.id as payment_id,
        p.draw_id,
        p.user_id,
        num as number,
        case when lower(coalesce(p.status,'')) = 'approved' then 'paid'
             else lower(coalesce(p.status,'')) end as status,
        p.created_at,
        coalesce(nullif(u.name,''), u.email, '-') as user_name,
        u.email as user_email
      from payments p
      left join users u on u.id = p.user_id
      cross join lateral unnest(coalesce(p.numbers, '{}'::int[])) as num
      where p.draw_id = $1
        and lower(coalesce(p.status,'')) in ('approved','paid','confirmed')
      order by user_name asc, number asc
    `;
    const r = await query(sql, [drawId]);
    res.json({ draw_id: drawId, participants: r.rows || [] });
  } catch (e) {
    console.error('[draws/:id/players] error:', e);
    res.status(500).json({ error: 'participants_failed' });
  }
});

/* ------------------------------------------------------------------ *
 * NOVO: ABRIR SORTEIO + AUTO-PAY
 * ------------------------------------------------------------------ */
/**
 * POST /api/admin/draws/open
 * body: { product_name?, product_link? }
 */
router.post('/admin/open', requireAuth, requireAdmin, async (req, res) => {
  const pool = await getPool();
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // 1) cria sorteio
    const { rows: drows } = await client.query(
      `insert into draws (status, product_name, product_link, opened_at)
       values ('open', $1, $2, now())
       returning id`,
      [req.body?.product_name || null, req.body?.product_link || null]
    );
    const drawId = drows[0].id;

    // 2) busca perfis autopay ativos
    const { rows: profiles } = await client.query(`
      select ap.*, array(
        select n from autopay_numbers an where an.autopay_id = ap.id order by n
      ) as numbers
      from autopay_profiles ap
      where ap.active = true
        and ap.mp_customer_id is not null
        and ap.mp_card_id is not null
    `);

    const price_cents = await getTicketPriceCents(client);
    const results = [];

    // 3) para cada perfil, filtra números livres, cobra e grava
    for (const p of profiles) {
      const wants = (p.numbers || []).map(Number).filter(n => n >= 0 && n <= 99);
      if (!wants.length) {
        results.push({ user_id: p.user_id, status: 'skipped', reason: 'no_numbers' });
        continue;
      }
      const free = [];
      for (const n of wants) {
        // eslint-disable-next-line no-await-in-loop
        const ok = await isNumberFree(client, drawId, n);
        if (ok) free.push(n);
      }
      if (!free.length) {
        results.push({ user_id: p.user_id, status: 'skipped', reason: 'none_available' });
        continue;
      }

      const amount_cents = free.length * price_cents;
      try {
        // eslint-disable-next-line no-await-in-loop
        const charge = await mpChargeCard({
          customerId: p.mp_customer_id,
          cardId: p.mp_card_id,
          amount_cents,
          description: `Sorteio ${drawId} – números: ${free.join(', ')}`,
          metadata: { user_id: p.user_id, draw_id: drawId, numbers: free },
        });

        if (!charge || String(charge.status).toLowerCase() !== 'approved') {
          results.push({ user_id: p.user_id, status: 'error', error: 'not_approved' });
          continue;
        }

        // registra pagamento/reserva
        const pay = await client.query(
          `insert into payments (user_id, draw_id, numbers, amount_cents, status, created_at)
           values ($1,$2,$3::int2[],$4,'approved',now()) returning id`,
          [p.user_id, drawId, free, amount_cents]
        );
        await client.query(
          `insert into reservations (id, user_id, draw_id, numbers, status, created_at, expires_at)
           values (gen_random_uuid(), $1,$2,$3::int2[],'paid',now(),now())`,
          [p.user_id, drawId, free]
        );

        results.push({ user_id: p.user_id, status: 'ok', numbers: free, payment_id: pay.rows[0].id });
      } catch (e) {
        console.error('[autopay charge error]', e);
        results.push({ user_id: p.user_id, status: 'error', error: 'charge_failed' });
      }
    }

    await client.query('COMMIT');
    res.json({ ok: true, draw_id: drawId, results, price_cents });
  } catch (e) {
    try { await client.query('ROLLBACK'); } catch {}
    console.error('[admin/draws/open] error', e);
    res.status(500).json({ error: 'open_failed' });
  } finally {
    client.release();
  }
});

export default router;
