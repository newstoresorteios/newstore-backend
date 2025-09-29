// backend/src/services/autopayRunner.js
import { getPool } from "../db.js";
import { mpChargeCard } from "./mercadopago.js";

// helpers locais (copiados do seu routes/autopay.js)
async function getTicketPriceCents(client) {
  try {
    const r1 = await client.query(
      `select value from public.kv_store where key in ('ticket_price_cents','price_cents') limit 1`
    );
    if (r1.rowCount) {
      const v = Number(r1.rows[0].value);
      if (Number.isFinite(v) && v > 0) return v | 0;
    }
  } catch {}
  try {
    const r2 = await client.query(
      `select price_cents from public.app_config order by id desc limit 1`
    );
    if (r2.rowCount) {
      const v = Number(r2.rows[0].price_cents);
      if (Number.isFinite(v) && v > 0) return v | 0;
    }
  } catch {}
  return 300;
}

async function isNumberFree(client, draw_id, n) {
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
         and ($2 = any(numbers) or n = $2)
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
 * Executa a cobrança automática para um sorteio aberto.
 * - Filtra perfis ativos com cartão salvo
 * - Cobra somente números livres
 * - Grava payments(approved) e reservations(paid)
 * - Marca draws.autopay_ran_at
 * Retorna { ok, results, price_cents }
 */
export async function runAutopayForDraw(draw_id) {
  const pool = await getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // valida sorteio
    const d = await client.query(
      `select id, status, autopay_ran_at from public.draws where id=$1 for update`,
      [draw_id]
    );
    if (!d.rowCount) {
      await client.query("ROLLBACK");
      return { ok: false, error: "draw_not_found" };
    }
    const st = String(d.rows[0].status || "").toLowerCase();
    if (!["open", "aberto"].includes(st)) {
      await client.query("ROLLBACK");
      return { ok: false, error: "draw_not_open" };
    }
    if (d.rows[0].autopay_ran_at) {
      await client.query("ROLLBACK");
      return { ok: false, error: "autopay_already_ran" };
    }

    // perfis
    const { rows: profiles } = await client.query(
      `select ap.*, array(
         select n from public.autopay_numbers an
          where an.autopay_id=ap.id order by n
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
      const wants = (p.numbers || []).map(Number).filter(n => n>=0 && n<=99);

      if (!wants.length) {
        results.push({ user_id, status: "skipped", reason: "no_numbers" });
        continue;
      }

      // filtra livres
      const free = [];
      for (const n of wants) {
        // eslint-disable-next-line no-await-in-loop
        if (await isNumberFree(client, draw_id, n)) free.push(n);
      }
      if (!free.length) {
        results.push({ user_id, status: "skipped", reason: "none_available" });
        continue;
      }

      const amount_cents = free.length * price_cents;

      // cobra
      let charge;
      try {
        // eslint-disable-next-line no-await-in-loop
        charge = await mpChargeCard({
          customerId: p.mp_customer_id,
          cardId: p.mp_card_id,
          amount_cents,
          description: `Sorteio ${draw_id} – números: ${free.map(n=>String(n).padStart(2,"0")).join(", ")}`,
          metadata: { user_id, draw_id, numbers: free },
        });
      } catch (e) {
        await client.query(
          `insert into public.autopay_runs (autopay_id,user_id,draw_id,tried_numbers,status,error)
           values ($1,$2,$3,$4,'error',$5)`,
          [p.id, user_id, draw_id, free, String(e?.message || e)]
        );
        results.push({ user_id, status: "error", error: "charge_failed" });
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

      // grava payment + reservation
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

      await client.query(
        `insert into public.autopay_runs (autopay_id,user_id,draw_id,tried_numbers,bought_numbers,amount_cents,status,payment_id,reservation_id)
         values ($1,$2,$3,$4,$5,$6,'ok',$7,$8)`,
        [p.id, user_id, draw_id, free, free, amount_cents, pay.rows[0].id, resv.rows[0].id]
      );

      results.push({ user_id, status: "ok", numbers: free, amount_cents });
    }

    // marca draw como processado
    await client.query(
      `update public.draws set autopay_ran_at = now() where id=$1`,
      [draw_id]
    );

    await client.query("COMMIT");
    return { ok: true, draw_id, results, price_cents };
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[autopayRunner] error:", e?.message || e);
    return { ok: false, error: "run_failed" };
  } finally {
    client.release();
  }
}
