// backend/src/services/autopayRunner.js
import { getPool } from "../db.js";
// MP desabilitado para autopay (mantido apenas para compatibilidade de imports, não usado)
// import { mpChargeCard } from "./mercadopago.js";
import { createBill, chargeBill, refundCharge, getBill } from "./vindi.js";

/* ------------------------------------------------------- *
 * Logging enxuto com contexto
 * ------------------------------------------------------- */
const LP = "[autopayRunner]";
const log  = (msg, extra = null) => console.log(`${LP} ${msg}`, extra ?? "");
const warn = (msg, extra = null) => console.warn(`${LP} ${msg}`, extra ?? "");
const err  = (msg, extra = null) => console.error(`${LP} ${msg}`, extra ?? "");

/* ------------------------------------------------------- *
 * Preço do ticket — compatível com seus schemas
 * ------------------------------------------------------- */
async function getTicketPriceCents(client) {
  // 1) app_config (key/value) – existe no seu banco
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

  // 2) kv_store – detecta esquema (k/v vs key/value)
  try {
    const { rows: cols } = await client.query(
      `select column_name
         from information_schema.columns
        where table_schema='public'
          and table_name='kv_store'
          and column_name in ('k','key','v','value')`
    );
    const hasKey = cols.some(c => c.column_name === 'key');
    const hasK   = cols.some(c => c.column_name === 'k');
    const hasVal = cols.some(c => c.column_name === 'value');
    const hasV   = cols.some(c => c.column_name === 'v');

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

  // 3) compat com app_config antigo (coluna price_cents)
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

/* ------------------------------------------------------- *
 * Número livre?
 * ------------------------------------------------------- */
async function isNumberFree(client, draw_id, n) {
  const t0 = Date.now();
  try {
    log("SQL isNumberFree(n) -> start", { params: [draw_id, n] });
    const q = `
      with
      p as (
        select 1
          from public.payments
         where draw_id = $1
           and lower(status) in ('approved','paid','pago')
           and $2 = any(numbers)
         limit 1
      ),
      r as (
        select 1
          from public.reservations
         where draw_id = $1
           and lower(status) in ('active','pending','paid')
           and $2 = any(numbers)
         limit 1
      )
      select
        coalesce((select 1 from p),0) as taken_pay,
        coalesce((select 1 from r),0) as taken_resv
    `;
    const r = await client.query(q, [draw_id, n]);
    const livre = !(r.rows[0].taken_pay || r.rows[0].taken_resv);
    log("SQL isNumberFree(n) -> ok", { time: `${Date.now() - t0}ms`, livre });
    return livre;
  } catch (e) {
    err("SQL isNumberFree(n) -> FAIL", {
      time: `${Date.now() - t0}ms`,
      msg: e?.message,
      code: e?.code,
    });
    throw e;
  }
}

/* ------------------------------------------------------- *
 * Autopay para UM sorteio aberto
 * ------------------------------------------------------- */
export async function runAutopayForDraw(draw_id) {
  const pool = await getPool();
  const client = await pool.connect();
  log("RUN start", { draw_id });

  try {
    await client.query("BEGIN");
    log("TX BEGIN");

    // 1) Validação + lock do sorteio
    const d = await client.query(
      `select id, status, autopay_ran_at
         from public.draws
        where id=$1
        for update`,
      [draw_id]
    );
    log("SQL lock_draw -> ok", { rows: d.rowCount });

    if (!d.rowCount) {
      await client.query("ROLLBACK");
      warn("draw não encontrado", draw_id);
      return { ok: false, error: "draw_not_found" };
    }
    const st = String(d.rows[0].status || "").toLowerCase();
    if (!["open", "aberto"].includes(st)) {
      await client.query("ROLLBACK");
      warn("draw não está open", { draw_id, status: st });
      return { ok: false, error: "draw_not_open" };
    }
    if (d.rows[0].autopay_ran_at) {
      await client.query("ROLLBACK");
      warn("autopay já processado para draw", draw_id);
      return { ok: false, error: "autopay_already_ran" };
    }

    // 2) Perfis elegíveis (apenas Vindi - MP desabilitado para autopay)
    // Feature flag para permitir MP (default: false - MP desabilitado)
    const mpEnabled = process.env.AUTOPAY_ENABLE_MP === "true";
    
    // Query dinâmica baseada na feature flag
    let querySQL = `
      select ap.*,
              array(select n
                      from public.autopay_numbers an
                     where an.autopay_id = ap.id
                     order by n) as numbers
         from public.autopay_profiles ap
        where ap.active = true
          and (
            (ap.vindi_customer_id is not null and ap.vindi_payment_profile_id is not null)
    `;
    
    if (mpEnabled) {
      querySQL += ` or (ap.mp_customer_id is not null and ap.mp_card_id is not null)`;
    }
    
    querySQL += `)`;
    
    const { rows: profiles } = await client.query(querySQL);
    log("eligible profiles", { count: profiles.length, mpEnabled });

    // 3) Preço
    const price_cents = await getTicketPriceCents(client);

    const results = [];

    // 4) Loop usuários
    for (const p of profiles) {
      const user_id = p.user_id;
      const wants = (p.numbers || []).map(Number).filter(n => n >= 0 && n <= 99);
      log("USER begin", { user_id, wants });

      if (!wants.length) {
        results.push({ user_id, status: "skipped", reason: "no_numbers" });
        continue;
      }

      // filtra números ainda livres
      const free = [];
      for (const n of wants) {
        // eslint-disable-next-line no-await-in-loop
        if (await isNumberFree(client, draw_id, n)) free.push(n);
      }
      log("USER free numbers", { user_id, free });

      if (!free.length) {
        results.push({ user_id, status: "skipped", reason: "none_available" });
        continue;
      }

      const amount_cents = free.length * price_cents;

      // 5) Determina provider (Vindi-first, MP desabilitado por padrão)
      const useVindi = !!(p.vindi_customer_id && p.vindi_payment_profile_id);
      const hasMP = !!(p.mp_customer_id && p.mp_card_id);

      // Se não tem Vindi configurado, marca como skipped
      if (!useVindi) {
        let skipReason = "needs_vindi_setup";
        let skipError = "needs_vindi_setup";
        
        // Se tem MP mas MP está desabilitado, marca especificamente
        if (hasMP && !mpEnabled) {
          skipReason = "mp_disabled_for_autopay";
          skipError = "mp_disabled_for_autopay";
          warn("SKIP perfil com MP - MP desabilitado para autopay", { user_id, draw_id });
        } else {
          warn("SKIP missing vindi_payment_profile_id", { user_id, draw_id });
        }

        // Registra skip no autopay_runs
        await client.query(
          `insert into public.autopay_runs (autopay_id,user_id,draw_id,tried_numbers,status,error,provider)
           values ($1,$2,$3,$4,'skipped',$5,'none')`,
          [p.id, user_id, draw_id, free, skipError]
        );

        results.push({ user_id, status: "skipped", reason: skipReason });
        continue;
      }

      // 6) Cobrança Vindi (único provider suportado)
      let charge;
      let provider = "vindi";
      let billId = null;
      let chargeId = null;

      try {
        const description = `Sorteio ${draw_id} – números: ${free.map(n => String(n).padStart(2, "0")).join(", ")}`;
        
        // eslint-disable-next-line no-await-in-loop
        const bill = await createBill({
          customerId: p.vindi_customer_id,
          amount: amount_cents,
          description,
          metadata: { user_id, draw_id, numbers: free },
          paymentProfileId: p.vindi_payment_profile_id,
        });

        billId = bill.billId;
        chargeId = bill.chargeId;

        // Se não foi cobrado automaticamente, cobra agora
        if (!chargeId || bill.status !== "paid") {
          // eslint-disable-next-line no-await-in-loop
          const chargeResult = await chargeBill(billId);
          chargeId = chargeResult.chargeId;
        }

        // Verifica status da bill
        // eslint-disable-next-line no-await-in-loop
        const billInfo = await getBill(billId);
        const billStatus = billInfo?.status?.toLowerCase();

        if (billStatus === "paid") {
          charge = { status: "approved", paymentId: chargeId || billId };
        } else {
          throw new Error(`Bill não paga: status=${billStatus}`);
        }

        log("Vindi charge ->", { user_id, billId, chargeId, status: billStatus });
      } catch (e) {
        const emsg = String(e?.message || e);

        await client.query(
          `insert into public.autopay_runs (autopay_id,user_id,draw_id,tried_numbers,status,error,provider)
           values ($1,$2,$3,$4,'error',$5,$6)`,
          [p.id, user_id, draw_id, free, emsg, provider]
        );

        // Se foi Vindi e falhou, tenta refund se já cobrou
        if (billId && chargeId) {
          try {
            // eslint-disable-next-line no-await-in-loop
            await refundCharge(chargeId, true);
            warn("Vindi: refund executado após falha", { user_id, billId, chargeId });
          } catch (refundErr) {
            err("Vindi: falha ao fazer refund", { user_id, billId, chargeId, msg: refundErr?.message });
          }
        }

        err("falha ao cobrar Vindi", { user_id, provider, msg: emsg });
        results.push({ user_id, status: "error", error: "charge_failed", provider });
        continue;
      }

      if (!charge || String(charge.status).toLowerCase() !== "approved") {
        await client.query(
          `insert into public.autopay_runs (autopay_id,user_id,draw_id,tried_numbers,status,error,provider)
           values ($1,$2,$3,$4,'error','not_approved',$5)`,
          [p.id, user_id, draw_id, free, provider]
        );
        warn("pagamento não aprovado", { user_id, draw_id, provider });
        results.push({ user_id, status: "error", error: "not_approved", provider });
        continue;
      }

      // 7) Grava payment + reservation (com provider)
      const pay = await client.query(
        `insert into public.payments (user_id, draw_id, numbers, amount_cents, status, created_at, provider, vindi_bill_id, vindi_charge_id)
         values ($1,$2,$3::int2[],$4,'approved', now(), $5, $6, $7)
         returning id`,
        [user_id, draw_id, free, amount_cents, provider, billId, chargeId]
      );
      const reservation = await client.query(
        `insert into public.reservations
           (id, user_id, draw_id, numbers, status, created_at, expires_at)
         values (gen_random_uuid(), $1, $2, $3::int2[], 'paid', now(), now())
         returning id`,
        [user_id, draw_id, free]
      );
      const resv_id = reservation.rows[0].id;

      // 8) Atualiza números vendidos
      await client.query(
        `update public.numbers n
            set status = 'sold',
                reservation_id = $1
          where n.draw_id = $2
            and n.n = any($3::int2[])`,
        [resv_id, draw_id, free]
      );

      // 9) Audita
      await client.query(
        `insert into public.autopay_runs
           (autopay_id,user_id,draw_id,tried_numbers,bought_numbers,amount_cents,status,payment_id,reservation_id,provider)
         values ($1,$2,$3,$4,$5,$6,'ok',$7,$8,$9)`,
        [p.id, user_id, draw_id, free, free, amount_cents, pay.rows[0].id, resv_id, provider]
      );

      log("gravado payment/reservation", {
        user_id,
        payment_id: pay.rows[0].id,
        reservation_id: resv_id,
        free,
        amount_cents,
      });

      results.push({ user_id, status: "ok", numbers: free, amount_cents });
    }

    // 10) Marca draw como processado
    await client.query(
      `update public.draws set autopay_ran_at = now() where id=$1`,
      [draw_id]
    );

    await client.query("COMMIT");
    log("TX COMMIT");
    log("RUN done", { draw_id });

    return { ok: true, draw_id, results, price_cents };
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    err("RUN error", { msg: e?.message, code: e?.code });
    return { ok: false, error: "run_failed" };
  } finally {
    client.release();
  }
}

/* ------------------------------------------------------- *
 * Em lote
 * ------------------------------------------------------- */
export async function runAutopayForOpenDraws({ force = false, limit = 50 } = {}) {
  const pool = await getPool();
  const client = await pool.connect();

  try {
    const where = force
      ? `status in ('open','aberto')`
      : `status in ('open','aberto') and autopay_ran_at is null`;

    const { rows } = await client.query(
      `select id from public.draws
        where ${where}
        order by id asc
        limit $1`,
      [limit]
    );

    if (!rows.length) {
      log("nenhum sorteio aberto pendente para autopay", { force, limit });
      return { ok: true, processed: 0, results: [] };
    }

    log("executando autopay em lote para draws", rows.map(r => r.id));

    const results = [];
    for (const r of rows) {
      // eslint-disable-next-line no-await-in-loop
      results.push(await runAutopayForDraw(r.id));
    }
    return { ok: true, processed: rows.length, results };
  } catch (e) {
    err("erro ao varrer draws abertos", e?.message || e);
    return { ok: false, error: "scan_failed" };
  } finally {
    client.release();
  }
}

/* ------------------------------------------------------- *
 * Idempotente p/ um sorteio
 * ------------------------------------------------------- */
export async function ensureAutopayForDraw(draw_id, { force = false } = {}) {
  const pool = await getPool();
  const client = await pool.connect();

  try {
    const { rows } = await client.query(
      `select id, status, autopay_ran_at
         from public.draws
        where id = $1`,
      [draw_id]
    );
    if (!rows.length) {
      warn("ensureAutopay: draw não encontrado", draw_id);
      return { ok: false, error: "draw_not_found" };
    }
    const st = String(rows[0].status || "").toLowerCase();
    const already = !!rows[0].autopay_ran_at;

    if (!["open", "aberto"].includes(st)) {
      warn("ensureAutopay: draw não está open", { draw_id, status: st });
      return { ok: false, error: "draw_not_open" };
    }
    if (already && !force) {
      log("ensureAutopay: já executado e force=false; ignorando", draw_id);
      return { ok: true, skipped: true, reason: "already_ran" };
    }

    return await runAutopayForDraw(draw_id);
  } catch (e) {
    err("ensureAutopay erro", e?.message || e);
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
