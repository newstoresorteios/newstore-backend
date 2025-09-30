// backend/src/routes/admin_draws.js
import { Router } from "express";
import { getPool, query } from "../db.js";
import { requireAuth } from "../middleware/auth.js";
import { mpChargeCard } from "../services/mercadopago.js";

const router = Router();

/* ------------------------------------------------------------------ *
 * Middleware: admin
 * ------------------------------------------------------------------ */
async function requireAdmin(req, res, next) {
  try {
    const userId = req?.user?.id;
    if (!userId) return res.status(401).json({ error: "unauthorized" });
    const r = await query("select is_admin from users where id = $1", [userId]);
    if (!r.rows.length || !r.rows[0].is_admin) {
      return res.status(403).json({ error: "forbidden" });
    }
    return next();
  } catch (e) {
    console.error("[admin check] error", e);
    return res.status(500).json({ error: "admin_check_failed" });
  }
}

/* ------------------------------------------------------------------ *
 * Utils (robustas para seus esquemas atuais)
 * ------------------------------------------------------------------ */
async function getTicketPriceCents(client) {
  // 1) app_config (key/value)
  try {
    const r = await client.query(
      `select value
         from public.app_config
        where key in ('ticket_price_cents','price_cents')
        order by updated_at desc
        limit 1`
    );
    if (r.rowCount) {
      const v = Number(r.rows[0].value);
      if (Number.isFinite(v) && v > 0) return v | 0;
    }
  } catch {}

  // 2) kv_store (detecta variantes k/key e v/value)
  try {
    const { rows: cols } = await client.query(
      `select column_name
         from information_schema.columns
        where table_schema='public'
          and table_name='kv_store'
          and column_name in ('k','key','v','value')`
    );
    const hasKey = cols.some(c => c.column_name === "key");
    const hasK   = cols.some(c => c.column_name === "k");
    const hasVal = cols.some(c => c.column_name === "value");
    const hasV   = cols.some(c => c.column_name === "v");

    if (hasKey && hasVal) {
      const r = await client.query(
        `select value
           from public.kv_store
          where key in ('ticket_price_cents','price_cents')
          limit 1`
      );
      if (r.rowCount) {
        const v = Number(r.rows[0].value);
        if (Number.isFinite(v) && v > 0) return v | 0;
      }
    } else if (hasK && hasV) {
      const r = await client.query(
        `select v as value
           from public.kv_store
          where k in ('ticket_price_cents','price_cents')
          limit 1`
      );
      if (r.rowCount) {
        const v = Number(r.rows[0].value);
        if (Number.isFinite(v) && v > 0) return v | 0;
      }
    }
  } catch {}

  // 3) legado: app_config.price_cents (se existir)
  try {
    const r = await client.query(
      `select price_cents
         from public.app_config
     order by id desc
        limit 1`
    );
    if (r.rowCount) {
      const v = Number(r.rows[0].price_cents);
      if (Number.isFinite(v) && v > 0) return v | 0;
    }
  } catch {}

  return 300; // fallback seguro
}

async function isNumberFree(client, draw_id, n) {
  // livre se NÃO está em payments aprovados e NÃO está em reservas ativas/pagas
  const q = `
    with
    p as (
      select 1 from public.payments
       where draw_id=$1
         and lower(status) in ('approved','paid','pago')
         and $2 = any(numbers) limit 1
    ),
    r as (
      select 1 from public.reservations
       where draw_id=$1
         and lower(status) in ('active','pending','paid')
         and $2 = any(numbers)
       limit 1
    )
    select
      coalesce((select 1 from p),0) as taken_pay,
      coalesce((select 1 from r),0) as taken_resv
  `;
  const r = await client.query(q, [draw_id, n]);
  return !(r.rows[0].taken_pay || r.rows[0].taken_resv);
}

/**
 * Executa a cobrança automática para todos os perfis ativos
 * e grava em payments/reservations quando aprovado.
 * Também marca os números como 'sold' na tabela numbers (quando existir).
 * Retorna: { results, price_cents }
 */
async function runAutopayForDraw(client, draw_id) {
  const { rows: profiles } = await client.query(
    `select ap.*, array(
       select n from public.autopay_numbers an where an.autopay_id=ap.id order by n
     ) numbers
     from public.autopay_profiles ap
     where ap.active = true
       and ap.mp_customer_id is not null
       and ap.mp_card_id is not null`
  );

  const price_cents = await getTicketPriceCents(client);
  const results = [];

  for (const p of profiles) {
    const user_id = p.user_id;
    const wants = (p.numbers || [])
      .map(Number)
      .filter((n) => n >= 0 && n <= 99);

    if (!wants.length) {
      results.push({ user_id, status: "skipped", reason: "no_numbers" });
      continue;
    }

    // filtra apenas os números ainda livres
    const free = [];
    for (const n of wants) {
      // eslint-disable-next-line no-await-in-loop
      const ok = await isNumberFree(client, draw_id, n);
      if (ok) free.push(n);
    }
    if (!free.length) {
      results.push({ user_id, status: "skipped", reason: "none_available" });
      continue;
    }

    const amount_cents = free.length * price_cents;

    // cobra no cartão do Mercado Pago
    let charge;
    try {
      // eslint-disable-next-line no-await-in-loop
      charge = await mpChargeCard({
        customerId: p.mp_customer_id,
        cardId: p.mp_card_id,
        amount_cents,
        description: `Sorteio ${draw_id} – números: ${free
          .map((n) => String(n).padStart(2, "0"))
          .join(", ")}`,
        metadata: { user_id, draw_id, numbers: free },
      });
    } catch (e) {
      const msg = String(e?.message || e || "");
      const status = /security_code/i.test(msg) ? "skipped" : "error";
      const error = /security_code/i.test(msg) ? "cvv_required" : "charge_failed";
      await client.query(
        `insert into public.autopay_runs (autopay_id,user_id,draw_id,tried_numbers,status,error)
         values ($1,$2,$3,$4,$5,$6)`,
        [p.id, user_id, draw_id, free, status, error]
      );
      results.push({ user_id, status, error });
      continue;
    }

    if (!charge || String(charge.status).toLowerCase() !== "approved") {
      await client.query(
        `insert into public.autopay_runs (autopay_id,user_id,draw_id,tried_numbers,status,error)
         values ($1,$2,$3,$4,'error','not_approved')`,
        [p.id, user_id, draw_id, free]
      );
      results.push({ user_id, status: "error", error: "not_approved" });
      continue;
    }

    // grava payment + reservation (status 'approved'/'paid')
    const pay = await client.query(
      `insert into public.payments (user_id, draw_id, numbers, amount_cents, status, created_at)
       values ($1,$2,$3::int2[],$4,'approved', now())
       returning id`,
      [user_id, draw_id, free, amount_cents]
    );
    const resv = await client.query(
      `insert into public.reservations (id, user_id, draw_id, numbers, status, created_at, expires_at)
       values (gen_random_uuid(), $1, $2, $3::int2[], 'paid', now(), now())
       returning id`,
      [user_id, draw_id, free]
    );

    // reflete no grid: numbers.status = 'sold', vincula reservation_id
    try {
      await client.query(
        `update public.numbers n
            set status = 'sold',
                reservation_id = $1
          where n.draw_id = $2
            and n.n = any($3::int2[])`,
        [resv.rows[0].id, draw_id, free]
      );
    } catch (_) {
      // se a tabela numbers não existir/estiver vazia, apenas ignora
    }

    await client.query(
      `insert into public.autopay_runs (autopay_id,user_id,draw_id,tried_numbers,bought_numbers,amount_cents,status,payment_id,reservation_id)
       values ($1,$2,$3,$4,$5,$6,'ok',$7,$8)`,
      [
        p.id,
        user_id,
        draw_id,
        free,
        free,
        amount_cents,
        pay.rows[0].id,
        resv.rows[0].id,
      ]
    );

    results.push({ user_id, status: "ok", numbers: free, amount_cents });
  }

  return { results, price_cents };
}

/* ------------------------------------------------------------------ *
 * LISTAGENS
 * ------------------------------------------------------------------ */

/** GET /api/admin/draws/history */
router.get("/history", requireAuth, requireAdmin, async (_req, res) => {
  try {
    const r = await query(`
      select
        d.id,
        d.status,
        coalesce(d.opened_at, d.created_at) as opened_at,
        d.closed_at,
        d.realized_at,
        round(
          extract(epoch from (coalesce(d.closed_at, now()) - coalesce(d.opened_at, d.created_at)))
          / 86400.0
        )::int as days_open,
        coalesce(d.winner_name, '-') as winner_name
      from public.draws d
      where d.status = 'closed' or d.closed_at is not null
      order by d.id desc
    `);
    res.json({ history: r.rows || [] });
  } catch (e) {
    console.error("[admin/draws/history] error", e);
    res.status(500).json({ error: "list_failed" });
  }
});

/** GET /api/admin/draws/:id/participants — somente pagos (reservations) */
router.get("/:id/participants", requireAuth, requireAdmin, async (req, res) => {
  try {
    const drawId = Number(req.params.id);
    if (!Number.isFinite(drawId))
      return res.status(400).json({ error: "invalid_draw_id" });

    const sql = `
      select
        r.id as reservation_id,
        r.draw_id,
        r.user_id,
        num as number,
        r.status as status,
        r.created_at,
        coalesce(nullif(u.name,''), u.email, '-') as user_name,
        u.email as user_email
      from reservations r
      left join users u on u.id = r.user_id
      cross join lateral unnest(coalesce(r.numbers, '{}'::int[])) as num
      where r.draw_id = $1
        and (lower(coalesce(r.status,'')) = 'paid' or coalesce(r.paid,false) = true)
      order by user_name asc, number asc
    `;
    const r = await query(sql, [drawId]);
    res.json({ draw_id: drawId, participants: r.rows || [] });
  } catch (e) {
    console.error("[admin/draws/:id/participants] error", e);
    res.status(500).json({ error: "participants_failed" });
  }
});

/** Alias /players — somente pagos */
router.get("/:id/players", requireAuth, requireAdmin, async (req, res) => {
  try {
    const drawId = Number(req.params.id);
    if (!Number.isFinite(drawId))
      return res.status(400).json({ error: "invalid_draw_id" });

    const sql = `
      select
        r.id as reservation_id,
        r.draw_id,
        r.user_id,
        num as number,
        r.status as status,
        r.created_at,
        coalesce(nullif(u.name,''), u.email, '-') as user_name,
        u.email as user_email
      from reservations r
      left join users u on u.id = r.user_id
      cross join lateral unnest(coalesce(r.numbers, '{}'::int[])) as num
      where r.draw_id = $1
        and (lower(coalesce(r.status,'')) = 'paid' or coalesce(r.paid,false) = true)
      order by user_name asc, number asc
    `;
    const r = await query(sql, [drawId]);
    res.json({ draw_id: drawId, participants: r.rows || [] });
  } catch (e) {
    console.error("[admin/draws/:id/players] error", e);
    res.status(500).json({ error: "participants_failed" });
  }
});

/* ------------------------------------------------------------------ *
 * ABERTURA + AUTOPAY
 * ------------------------------------------------------------------ */

/**
 * POST /api/admin/draws/new
 * Abre um novo sorteio (status 'open'), popula 0..99 na tabela numbers
 * (se existir) e roda AutoPay imediatamente.
 */
router.post("/new", requireAuth, requireAdmin, async (req, res) => {
  const pool = await getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // cria sorteio
    const d = await client.query(
      `insert into public.draws (status, opened_at, product_name, product_link)
       values ('open', now(), $1, $2)
       returning id`,
      [req.body?.product_name || null, req.body?.product_link || null]
    );
    const draw_id = d.rows[0].id;

    // popula numbers 0..99 (ignora falha se tabela não existir)
    try {
      const tuples = Array.from({ length: 100 }, (_, i) => `(${draw_id}, ${i}, 'available', null)`);
      await client.query(
        `insert into public.numbers(draw_id, n, status, reservation_id) values ${tuples.join(",")}`
      );
    } catch {}

    // roda AutoPay
    const { results, price_cents } = await runAutopayForDraw(client, draw_id);

    // marca autopay_ran_at (se coluna existir)
    try {
      await client.query(`update public.draws set autopay_ran_at = now() where id=$1`, [draw_id]);
    } catch {}

    await client.query("COMMIT");
    console.log("[admin/draws] novo draw id =", draw_id);
    return res.json({ ok: true, draw_id, autopay: { results, price_cents } });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[admin/draws/new] error", e?.message || e);
    return res.status(500).json({ error: "open_failed" });
  } finally {
    client.release();
  }
});

/**
 * POST /api/admin/draws/:id/open
 * Garante que o sorteio esteja 'open' (ou reabre) e roda AutoPay.
 */
router.post("/:id/open", requireAuth, requireAdmin, async (req, res) => {
  const pool = await getPool();
  const client = await pool.connect();
  const draw_id = Number(req.params.id);
  if (!Number.isInteger(draw_id)) {
    client.release();
    return res.status(400).json({ error: "bad_draw_id" });
  }
  try {
    await client.query("BEGIN");

    await client.query(
      `update public.draws
          set status='open',
              opened_at = coalesce(opened_at, now()),
              autopay_ran_at = null
        where id=$1`,
      [draw_id]
    );

    const { results, price_cents } = await runAutopayForDraw(client, draw_id);

    try {
      await client.query(`update public.draws set autopay_ran_at = now() where id=$1`, [draw_id]);
    } catch {}

    await client.query("COMMIT");
    return res.json({ ok: true, draw_id, autopay: { results, price_cents } });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[admin/draws/:id/open] error", e?.message || e);
    return res.status(500).json({ error: "open_failed" });
  } finally {
    client.release();
  }
});

export default router;
