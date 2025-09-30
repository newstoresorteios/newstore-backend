// backend/src/services/autopayRunner.js
import crypto from "node:crypto";
import { getPool } from "../db.js";
import { mpChargeCard } from "./mercadopago.js";

/* ------------------------------------------------------- *
 * Logging helpers
 * ------------------------------------------------------- */
const LP = "[autopayRunner]";
const log  = (...a) => console.log(LP, ...a);
const warn = (...a) => console.warn(LP, ...a);
const err  = (...a) => console.error(LP, ...a);

const t0 = () => Date.now();
const dt = (ms) => `${ms}ms`;

/** sanitiza valores para log (evita prints gigantes / dados sensíveis) */
function scrub(v) {
  if (v == null) return v;
  if (Array.isArray(v)) return v.length > 10 ? `[len=${v.length}]` : v;
  if (typeof v === "string" && v.length > 64) return v.slice(0, 61) + "...";
  return v;
}

/** wrapper p/ logar SQL + duração + detalhes do erro */
async function runQ(client, label, sql, params = []) {
  const t = t0();
  log(`SQL ${label} -> start`, { params: params.map(scrub) });
  try {
    const r = await client.query(sql, params);
    log(`SQL ${label} -> ok`, { rows: r.rowCount, time: dt(t0() - t) });
    return r;
  } catch (e) {
    err(`SQL ${label} -> FAIL`, {
      time: dt(t0() - t),
      msg: e?.message,
      code: e?.code,
      detail: e?.detail,
      position: e?.position,
      constraint: e?.constraint,
    });
    throw e;
  }
}

/* ------------------------------------------------------- *
 * Config helpers
 * ------------------------------------------------------- */
async function getTicketPriceCents(client) {
  const t = t0();
  try {
    const r1 = await runQ(
      client,
      "getTicketPriceCents/kv_store",
      `select value
         from public.kv_store
        where key in ('ticket_price_cents','price_cents')
        limit 1`
    );
    if (r1.rowCount) {
      const v = Number(r1.rows[0].value);
      if (Number.isFinite(v) && v > 0) return v | 0;
    }
  } catch {}
  try {
    const r2 = await runQ(
      client,
      "getTicketPriceCents/app_config",
      `select price_cents
         from public.app_config
     order by id desc
        limit 1`
    );
    if (r2.rowCount) {
      const v = Number(r2.rows[0].price_cents);
      if (Number.isFinite(v) && v > 0) return v | 0;
    }
  } catch {}
  const v = 300;
  log("getTicketPriceCents -> fallback", { value: v, time: dt(t0() - t) });
  return v;
}

/* ------------------------------------------------------- *
 * Checagem de disponibilidade
 * ------------------------------------------------------- */
async function isNumberFree(client, draw_id, n) {
  const q = `
    with
    p as (
      select 1
        from public.payments
       where draw_id = $1
         and lower(status) in ('approved','paid','pago')
         and $2::int2 = any(numbers)
       limit 1
    ),
    r as (
      select 1
        from public.reservations
       where draw_id = $1
         and lower(status) in ('active','pending','paid')
         and $2::int2 = any(numbers)
       limit 1
    )
    select
      coalesce((select 1 from p),0) as taken_pay,
      coalesce((select 1 from r),0) as taken_resv
  `;
  const r = await runQ(client, `isNumberFree(n=${n})`, q, [draw_id, n]);
  const busy = !!(r.rows[0].taken_pay || r.rows[0].taken_resv);
  log(`isNumberFree -> n=${n} free=${!busy}`);
  return !busy;
}

/* ------------------------------------------------------- *
 * Núcleo
 * ------------------------------------------------------- */
export async function runAutopayForDraw(draw_id) {
  const pool = await getPool();
  const client = await pool.connect();
  const tAll = t0();
  log("RUN start", { draw_id });

  try {
    log("TX BEGIN");
    await client.query("BEGIN");

    const d = await runQ(
      client,
      "lock_draw",
      `select id, status, autopay_ran_at
         from public.draws
        where id=$1
        for update`,
      [draw_id]
    );

    if (!d.rowCount) {
      await client.query("ROLLBACK");
      warn("abort: draw_not_found", { draw_id });
      return { ok: false, error: "draw_not_found" };
    }

    const st = String(d.rows[0].status || "").toLowerCase();
    const ran = !!d.rows[0].autopay_ran_at;

    if (!["open", "aberto"].includes(st)) {
      await client.query("ROLLBACK");
      warn("abort: draw_not_open", { draw_id, status: st });
      return { ok: false, error: "draw_not_open" };
    }
    if (ran) {
      await client.query("ROLLBACK");
      warn("abort: autopay_already_ran", { draw_id });
      return { ok: false, error: "autopay_already_ran" };
    }

    const profilesR = await runQ(
      client,
      "eligible_profiles",
      `select ap.*, array(
         select n from public.autopay_numbers an
          where an.autopay_id=ap.id
          order by n
       ) numbers
       from public.autopay_profiles ap
       where ap.active = true
         and ap.mp_customer_id is not null
         and ap.mp_card_id is not null`
    );
    const profiles = profilesR.rows || [];
    log("eligible profiles", { count: profiles.length });

    const price_cents = await getTicketPriceCents(client);
    const results = [];

    for (const p of profiles) {
      const stepT = t0();
      const user_id = p.user_id;
      const wants = (p.numbers || []).map(Number).filter(n => n >= 0 && n <= 99);
      log("USER begin", { user_id, wants });

      if (!wants.length) {
        results.push({ user_id, status: "skipped", reason: "no_numbers" });
        log("USER end (skipped no_numbers)", { user_id, time: dt(t0() - stepT) });
        continue;
      }

      // filtra livres
      const free = [];
      for (const n of wants) {
        // eslint-disable-next-line no-await-in-loop
        const livre = await isNumberFree(client, draw_id, n);
        if (livre) free.push(n);
      }
      log("USER free filtered", { user_id, free });

      if (!free.length) {
        results.push({ user_id, status: "skipped", reason: "none_available" });
        log("USER end (none_available)", { user_id, time: dt(t0() - stepT) });
        continue;
      }

      const amount_cents = free.length * price_cents;

      // Mercado Pago
      let charge;
      try {
        log("MP charge -> start", {
          user_id,
          customerId: String(p.mp_customer_id).slice(0, 6) + "...",
          cardId: String(p.mp_card_id).slice(0, 6) + "...",
          amount_cents,
          numbers: free,
        });
        // eslint-disable-next-line no-await-in-loop
        charge = await mpChargeCard({
          customerId: p.mp_customer_id,
          cardId: p.mp_card_id,
          amount_cents,
          description: `Sorteio ${draw_id} – números: ${free.map(n => String(n).padStart(2, "0")).join(", ")}`,
          metadata: { user_id, draw_id, numbers: free },
        });
        log("MP charge -> done", { user_id, status: charge?.status, id: charge?.paymentId });
      } catch (e) {
        const emsg = String(e?.message || e);
        await runQ(
          client,
          "autopay_runs/charge_failed",
          `insert into public.autopay_runs (autopay_id,user_id,draw_id,tried_numbers,status,error)
           values ($1,$2,$3,$4,'error',$5)`,
          [p.id, user_id, draw_id, free, emsg]
        );
        err("MP charge -> ERROR", { user_id, emsg });
        results.push({ user_id, status: "error", error: "charge_failed" });
        log("USER end (charge_failed)", { user_id, time: dt(t0() - stepT) });
        continue;
      }

      if (!charge || String(charge.status).toLowerCase() !== "approved") {
        await runQ(
          client,
          "autopay_runs/not_approved",
          `insert into public.autopay_runs (autopay_id,user_id,draw_id,tried_numbers,status,error)
           values ($1,$2,$3,$4,'error','not_approved')`,
          [p.id, user_id, draw_id, free]
        );
        warn("payment not approved", { user_id, draw_id });
        results.push({ user_id, status: "error", error: "not_approved" });
        log("USER end (not_approved)", { user_id, time: dt(t0() - stepT) });
        continue;
      }

      // grava payment + reservation (UUID no Node)
      const pay = await runQ(
        client,
        "payments/insert",
        `insert into public.payments (user_id, draw_id, numbers, amount_cents, status, created_at)
         values ($1,$2,$3::int2[],$4,'approved', now())
         returning id`,
        [user_id, draw_id, free, amount_cents]
      );

      const reservationId = crypto.randomUUID();
      const resv = await runQ(
        client,
        "reservations/insert",
        `insert into public.reservations (id, user_id, draw_id, numbers, status, created_at, expires_at)
         values ($1, $2, $3, $4::int2[], 'paid', now(), now())
         returning id`,
        [reservationId, user_id, draw_id, free]
      );

      await runQ(
        client,
        "autopay_runs/ok",
        `insert into public.autopay_runs
           (autopay_id,user_id,draw_id,tried_numbers,bought_numbers,amount_cents,status,payment_id,reservation_id)
         values ($1,$2,$3,$4,$5,$6,'ok',$7,$8)`,
        [p.id, user_id, draw_id, free, free, amount_cents, pay.rows[0].id, resv.rows[0].id]
      );

      log("USER end (ok)", {
        user_id,
        payment_id: pay.rows[0].id,
        reservation_id: resv.rows[0].id,
        numbers: free,
        amount_cents,
        time: dt(t0() - stepT),
      });

      results.push({ user_id, status: "ok", numbers: free, amount_cents });
    }

    await runQ(
      client,
      "draws/mark_autopay_ran",
      `update public.draws set autopay_ran_at = now() where id=$1`,
      [draw_id]
    );

    log("TX COMMIT");
    await client.query("COMMIT");

    log("RUN end", { draw_id, time: dt(t0() - tAll) });
    return { ok: true, draw_id, results, price_cents };
  } catch (e) {
    try { log("TX ROLLBACK"); await client.query("ROLLBACK"); } catch {}
    err("RUN error:", { msg: e?.message, code: e?.code });
    return { ok: false, error: "run_failed" };
  } finally {
    client.release();
  }
}

/* ------------------------------------------------------- *
 * Batch & ensure
 * ------------------------------------------------------- */
export async function runAutopayForOpenDraws({ force = false, limit = 50 } = {}) {
  const pool = await getPool();
  const client = await pool.connect();

  try {
    const where = force
      ? `status in ('open','aberto')`
      : `status in ('open','aberto') and autopay_ran_at is null`;

    const rows = (await runQ(
      client,
      "scan_open_draws",
      `select id from public.draws
        where ${where}
        order by id asc
        limit $1`,
      [limit]
    )).rows;

    if (!rows.length) {
      log("scan: no open draws", { force, limit });
      return { ok: true, processed: 0, results: [] };
    }

    log("scan: processing draws", rows.map(r => r.id));
    const results = [];
    for (const r of rows) {
      // eslint-disable-next-line no-await-in-loop
      const out = await runAutopayForDraw(r.id);
      results.push(out);
    }
    return { ok: true, processed: rows.length, results };
  } catch (e) {
    err("scan error:", e?.message || e);
    return { ok: false, error: "scan_failed" };
  } finally {
    client.release();
  }
}

export async function ensureAutopayForDraw(draw_id, { force = false } = {}) {
  const pool = await getPool();
  const client = await pool.connect();

  try {
    const r = await runQ(
      client,
      "ensure/read_draw",
      `select id, status, autopay_ran_at
         from public.draws
        where id = $1`,
      [draw_id]
    );
    if (!r.rowCount) {
      warn("ensure: draw_not_found", { draw_id });
      return { ok: false, error: "draw_not_found" };
    }
    const st = String(r.rows[0].status || "").toLowerCase();
    const already = !!r.rows[0].autopay_ran_at;

    if (!["open", "aberto"].includes(st)) {
      warn("ensure: draw_not_open", { draw_id, status: st });
      return { ok: false, error: "draw_not_open" };
    }
    if (already && !force) {
      log("ensure: already ran; skipping", { draw_id });
      return { ok: true, skipped: true, reason: "already_ran" };
    }

    return await runAutopayForDraw(draw_id);
  } catch (e) {
    err("ensure error:", e?.message || e);
    return { ok: false, error: "ensure_failed" };
  } finally {
    client.release();
  }
}

export default {
  runAutopayForDraw,
  runAutopayForOpenDraws,
  ensureAutopayForDraw,
};
