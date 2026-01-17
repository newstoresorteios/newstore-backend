// backend/src/services/autopayRunner.js
import { getPool } from "../db.js";
// MP desabilitado para autopay (mantido apenas para compatibilidade de imports, não usado)
// import { mpChargeCard } from "./mercadopago.js";
import { createBill, chargeBill, refundCharge, getBill } from "./vindi.js";
import crypto from "node:crypto";

/* ------------------------------------------------------- *
 * Logging enxuto com contexto
 * ------------------------------------------------------- */
const LP = "[autopayRunner]";
const log  = (msg, extra = null) => console.log(`${LP} ${msg}`, extra ?? "");
const warn = (msg, extra = null) => console.warn(`${LP} ${msg}`, extra ?? "");
const err  = (msg, extra = null) => console.error(`${LP} ${msg}`, extra ?? "");

/* ------------------------------------------------------- *
 * AutopayRun upsert (compatível com UNIQUE(user_id,draw_id) se existir)
 * ------------------------------------------------------- */
async function writeAutopayRun(client, run) {
  const {
    autopay_id,
    user_id,
    draw_id,
    tried_numbers,
    bought_numbers = null,
    amount_cents = null,
    status,
    error = null,
    provider = "vindi",
    payment_id = null,
    reservation_id = null,
  } = run;

  try {
    await client.query(
      `insert into public.autopay_runs
         (autopay_id,user_id,draw_id,tried_numbers,bought_numbers,amount_cents,status,error,provider,payment_id,reservation_id)
       values
         ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
      [
        autopay_id,
        user_id,
        draw_id,
        tried_numbers,
        bought_numbers,
        amount_cents,
        status,
        error,
        provider,
        payment_id,
        reservation_id,
      ]
    );
  } catch (e) {
    // Se existir UNIQUE(user_id, draw_id), fazemos UPDATE idempotente
    if (e?.code === "23505") {
      await client.query(
        `update public.autopay_runs
            set tried_numbers = $4,
                bought_numbers = $5,
                amount_cents = $6,
                status = $7,
                error = $8,
                provider = $9,
                payment_id = $10,
                reservation_id = $11,
                updated_at = now()
          where user_id = $2
            and draw_id = $3`,
        [
          autopay_id,
          user_id,
          draw_id,
          tried_numbers,
          bought_numbers,
          amount_cents,
          status,
          error,
          provider,
          payment_id,
          reservation_id,
        ]
      );
      return;
    }
    throw e;
  }
}

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
 * Ensure números 00..99 existem para o draw
 * ------------------------------------------------------- */
async function ensureNumbersForDraw(client, draw_id) {
  try {
    const { rows } = await client.query(
      `select count(*)::int as c from public.numbers where draw_id=$1`,
      [draw_id]
    );
    const c = rows?.[0]?.c || 0;
    if (c >= 100) return;

    // se não tem nenhum, cria 100; se tem parcial, completa os faltantes
    if (c === 0) {
      await client.query(
        `insert into public.numbers(draw_id, n, status, reservation_id)
         select $1, gs::int2, 'available', null
           from generate_series(0,99) as gs`,
        [draw_id]
      );
      log("numbers populated for draw", { draw_id, count: 100 });
      return;
    }

    await client.query(
      `insert into public.numbers(draw_id, n, status, reservation_id)
       select $1, gs::int2, 'available', null
         from generate_series(0,99) as gs
        where not exists (
          select 1 from public.numbers n
           where n.draw_id=$1 and n.n = gs::int2
        )`,
      [draw_id]
    );
    warn("numbers table was incomplete; missing rows inserted", { draw_id, existing: c });
  } catch (e) {
    err("ensureNumbersForDraw failed", { draw_id, msg: e?.message, code: e?.code });
    throw e;
  }
}

/* ------------------------------------------------------- *
 * Reserva subset dos números desejados (TX curta)
 * - Reserva = cria row em reservations + marca numbers como reserved (bloqueante)
 * - Commit antes de chamada externa (Vindi)
 * ------------------------------------------------------- */
async function reserveNumbersForProfile(client, { draw_id, user_id, wants, ttlMin }) {
  const reservationId = crypto.randomUUID();
  const expiresAt = new Date(Date.now() + Math.max(1, Number(ttlMin || 5)) * 60 * 1000);

  await client.query("BEGIN");
  try {
    // lock nos números desejados
    const locked = await client.query(
      `select n, status, reservation_id
         from public.numbers
        where draw_id = $1
          and n = any($2::int2[])
        for update`,
      [draw_id, wants]
    );

    // expira reservas bloqueantes vencidas (somente para números envolvidos)
    for (const row of locked.rows) {
      if (String(row.status).toLowerCase() === "reserved" && row.reservation_id) {
        const rid = row.reservation_id;
        const rsv = await client.query(
          `select id, status, expires_at
             from public.reservations
            where id=$1
            for update`,
          [rid]
        );
        const r = rsv.rows[0];
        if (r) {
          const st = String(r.status || "").toLowerCase();
          const isBlocking = ["active", "pending", "reserved", ""].includes(st);
          const isExpired = r.expires_at && new Date(r.expires_at).getTime() <= Date.now();
          if (isBlocking && isExpired) {
            await client.query(`update public.reservations set status='expired' where id=$1`, [rid]);
            await client.query(
              `update public.numbers
                  set status='available',
                      reservation_id=null
                where draw_id=$1
                  and reservation_id=$2`,
              [draw_id, rid]
            );
          }
        }
      }
    }

    // revalida sob lock: escolhe subset disponível
    const after = await client.query(
      `select n, status
         from public.numbers
        where draw_id = $1
          and n = any($2::int2[])
        for update`,
      [draw_id, wants]
    );

    const reservedNumbers = after.rows
      .filter((r) => String(r.status).toLowerCase() === "available")
      .map((r) => Number(r.n))
      .sort((a, b) => a - b);

    if (!reservedNumbers.length) {
      await client.query("ROLLBACK");
      return { reservationId: null, reservedNumbers: [] };
    }

    // cria reserva como pending (bloqueia e expira, mas ainda não foi paga)
    await client.query(
      `insert into public.reservations (id, user_id, draw_id, numbers, status, created_at, expires_at)
       values ($1, $2, $3, $4::int2[], 'pending', now(), $5)`,
      [reservationId, user_id, draw_id, reservedNumbers, expiresAt]
    );

    // marca números como reserved e amarra na reserva (garante bloqueio)
    const upd = await client.query(
      `update public.numbers
          set status='reserved',
              reservation_id=$3
        where draw_id=$1
          and n = any($2::int2[])
          and status='available'`,
      [draw_id, reservedNumbers, reservationId]
    );

    if (upd.rowCount !== reservedNumbers.length) {
      await client.query("ROLLBACK");
      return { reservationId: null, reservedNumbers: [] };
    }

    await client.query("COMMIT");
    return { reservationId, reservedNumbers };
  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    throw e;
  }
}

async function cancelReservation(client, { draw_id, reservationId }) {
  await client.query("BEGIN");
  try {
    await client.query(
      `update public.reservations
          set status='expired',
              expires_at = now()
        where id=$1`,
      [reservationId]
    );
    await client.query(
      `update public.numbers
          set status='available',
              reservation_id=null
        where draw_id=$1
          and reservation_id=$2
          and status='reserved'`,
      [draw_id, reservationId]
    );
    await client.query("COMMIT");
  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    throw e;
  }
}

async function finalizePaidReservation(client, { draw_id, reservationId, user_id, numbers, amount_cents, provider, billId, chargeId }) {
  await client.query("BEGIN");
  try {
    const pay = await client.query(
      `insert into public.payments (user_id, draw_id, numbers, amount_cents, status, created_at, provider, vindi_bill_id, vindi_charge_id, vindi_status, paid_at)
       values ($1,$2,$3::int2[],$4,'approved', now(), $5, $6, $7, 'paid', now())
       returning id`,
      [user_id, draw_id, numbers, amount_cents, provider, billId, chargeId]
    );
    const paymentId = pay.rows[0].id;

    await client.query(
      `update public.reservations
          set status='paid',
              payment_id=$2,
              expires_at = now()
        where id=$1`,
      [reservationId, paymentId]
    );

    const upd = await client.query(
      `update public.numbers
          set status='sold'
        where draw_id=$1
          and n = any($2::int2[])
          and reservation_id=$3`,
      [draw_id, numbers, reservationId]
    );

    if (upd.rowCount !== numbers.length) {
      throw new Error(`numbers_update_mismatch expected=${numbers.length} updated=${upd.rowCount}`);
    }

    await client.query("COMMIT");
    return { paymentId };
  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch {}
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
    // lock de sessão para o draw (segura entre commits; evita concorrência do runner)
    await client.query(`select pg_advisory_lock(911002, $1)`, [draw_id]);

    // 2) Verifica modo Vindi (obrigatório)
    const vindiMode = !!process.env.VINDI_API_KEY;
    
    if (!vindiMode) {
      err("VINDI_API_KEY não configurada - autopay requer Vindi", {});
      return { ok: false, error: "vindi_not_configured" };
    }

    // 3) Validação do draw + ensure numbers 00..99
    await client.query("BEGIN");
    const d = await client.query(
      `select id, status, autopay_ran_at
         from public.draws
        where id=$1
        for update`,
      [draw_id]
    );
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
    await ensureNumbersForDraw(client, draw_id);
    await client.query("COMMIT");

    // 4) Perfis elegíveis (Vindi + active + números agregados)
    const { rows: profiles } = await client.query(
      `select
          ap.id as autopay_id,
          ap.user_id as user_id,
          ap.vindi_customer_id,
          ap.vindi_payment_profile_id,
          coalesce(array_agg(an.n order by an.n) filter (where an.n is not null), '{}') as numbers
        from public.autopay_profiles ap
        left join public.autopay_numbers an on an.autopay_id = ap.id
       where ap.active = true
         and ap.vindi_customer_id is not null
         and ap.vindi_payment_profile_id is not null
       group by ap.id, ap.user_id, ap.vindi_customer_id, ap.vindi_payment_profile_id`
    );
    log("eligible profiles (Vindi mode)", { count: profiles.length });

    // 5) Preço
    const price_cents = await getTicketPriceCents(client);

    const results = [];
    let totalReserved = 0;
    let chargedOk = 0;
    let chargedFail = 0;

    const ttlMin = Number(process.env.RESERVATION_TTL_MIN || 5);

    // 6) Loop usuários
    for (const p of profiles) {
      const user_id = p.user_id;
      const autopay_id = p.autopay_id;
      const wants = (p.numbers || []).map(Number).filter((n) => n >= 0 && n <= 99);
      log("USER begin", { user_id, autopay_id, wants, provider: "vindi" });

      if (!wants.length) {
        results.push({ user_id, status: "skipped", reason: "no_numbers" });
        continue;
      }

      // Idempotência por perfil: se já teve OK nesse draw, não reprocessa
      // eslint-disable-next-line no-await-in-loop
      const alreadyOk = await client.query(
        `select 1 from public.autopay_runs where autopay_id=$1 and draw_id=$2 and status='ok' limit 1`,
        [autopay_id, draw_id]
      );
      if (alreadyOk.rowCount) {
        results.push({ user_id, status: "skipped", reason: "already_processed" });
        continue;
      }

      // 6.1) Reserva subset (TX curta) - COMMIT antes da cobrança externa
      // eslint-disable-next-line no-await-in-loop
      const reserved = await reserveNumbersForProfile(client, { draw_id, user_id, wants, ttlMin });
      const reservedNumbers = reserved.reservedNumbers;
      const reservationId = reserved.reservationId;

      log("USER reserved numbers", { user_id, autopay_id, reservedNumbers, reservationId });

      if (!reservedNumbers.length || !reservationId) {
        // registra tentativa (sem reserva) e segue
        // eslint-disable-next-line no-await-in-loop
        await writeAutopayRun(client, {
          autopay_id,
          user_id,
          draw_id,
          tried_numbers: wants,
          status: "skipped",
          error: "none_available",
          provider: "vindi",
        });
        results.push({ user_id, status: "skipped", reason: "none_available" });
        continue;
      }

      totalReserved += reservedNumbers.length;
      const amount_cents = reservedNumbers.length * price_cents;

      // 6.2) Cobrança Vindi avulsa (fora da TX do banco)
      let charge;
      let provider = "vindi";
      let billId = null;
      let chargeId = null;

      try {
        const description = `Autopay draw ${draw_id} — ${reservedNumbers.length} números: ${reservedNumbers
          .map((n) => String(n).padStart(2, "0"))
          .join(", ")}`;
        
        // Idempotency key: "draw:{drawId}:user:{userId}"
        const idempotencyKey = `draw:${draw_id}:user:${user_id}`;
        
        // eslint-disable-next-line no-await-in-loop
        const bill = await createBill({
          customerId: p.vindi_customer_id,
          amount: amount_cents,
          description,
          metadata: { user_id, draw_id, numbers: reservedNumbers, autopay_id, reservation_id: reservationId },
          paymentProfileId: p.vindi_payment_profile_id,
          idempotencyKey,
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

        chargedFail++;

        // Se foi Vindi e falhou, tenta refund se já cobrou (best-effort)
        if (billId && chargeId) {
          try {
            // eslint-disable-next-line no-await-in-loop
            await refundCharge(chargeId, true);
            warn("Vindi: refund executado após falha", { user_id, billId, chargeId });
          } catch (refundErr) {
            err("Vindi: falha ao fazer refund", { user_id, billId, chargeId, msg: refundErr?.message });
          }
        }

        // libera reserva
        try {
          // eslint-disable-next-line no-await-in-loop
          await cancelReservation(client, { draw_id, reservationId });
        } catch (cancelErr) {
          err("falha ao cancelar reserva após charge fail", { user_id, reservationId, msg: cancelErr?.message });
        }

        // audita autopay_run
        // eslint-disable-next-line no-await-in-loop
        await writeAutopayRun(client, {
          autopay_id,
          user_id,
          draw_id,
          tried_numbers: wants,
          bought_numbers: reservedNumbers,
          amount_cents,
          status: "error",
          error: emsg,
          provider,
          reservation_id: reservationId,
        });

        err("falha ao cobrar Vindi", { user_id, provider, msg: emsg });
        results.push({ user_id, status: "error", error: "charge_failed", provider });
        continue;
      }

      if (!charge || String(charge.status).toLowerCase() !== "approved") {
        chargedFail++;
        // libera reserva e registra
        // eslint-disable-next-line no-await-in-loop
        await cancelReservation(client, { draw_id, reservationId });
        // eslint-disable-next-line no-await-in-loop
        await writeAutopayRun(client, {
          autopay_id,
          user_id,
          draw_id,
          tried_numbers: wants,
          bought_numbers: reservedNumbers,
          amount_cents,
          status: "error",
          error: "not_approved",
          provider,
          reservation_id: reservationId,
        });
        warn("pagamento não aprovado", { user_id, draw_id, provider });
        results.push({ user_id, status: "error", error: "not_approved", provider });
        continue;
      }

      // 6.3) Confirma (paid) + grava payment + audita autopay_runs (TX)
      try {
        // eslint-disable-next-line no-await-in-loop
        const fin = await finalizePaidReservation(client, {
          draw_id,
          reservationId,
          user_id,
          numbers: reservedNumbers,
          amount_cents,
          provider,
          billId,
          chargeId,
        });

        // eslint-disable-next-line no-await-in-loop
        await writeAutopayRun(client, {
          autopay_id,
          user_id,
          draw_id,
          tried_numbers: wants,
          bought_numbers: reservedNumbers,
          amount_cents,
          status: "ok",
          provider,
          payment_id: fin.paymentId,
          reservation_id: reservationId,
        });

        chargedOk++;
        log("autopay ok", { user_id, autopay_id, reservationId, payment_id: fin.paymentId, reservedNumbers, amount_cents, billId, chargeId });
        results.push({ user_id, status: "ok", numbers: reservedNumbers, amount_cents });
      } catch (e) {
        chargedFail++;
        const emsg = String(e?.message || e);
        err("finalize paid failed (refund+cancel)", { user_id, reservationId, msg: emsg });

        // refund best-effort
        if (chargeId) {
          try {
            // eslint-disable-next-line no-await-in-loop
            await refundCharge(chargeId, true);
            warn("Vindi: refund executado após falha de persistência", { user_id, billId, chargeId });
          } catch (refundErr) {
            err("Vindi: falha ao fazer refund após falha de persistência", { user_id, billId, chargeId, msg: refundErr?.message });
          }
        }

        // cancela reserva para liberar números
        // eslint-disable-next-line no-await-in-loop
        await cancelReservation(client, { draw_id, reservationId });

        // audita erro
        // eslint-disable-next-line no-await-in-loop
        await writeAutopayRun(client, {
          autopay_id,
          user_id,
          draw_id,
          tried_numbers: wants,
          bought_numbers: reservedNumbers,
          amount_cents,
          status: "error",
          error: emsg,
          provider,
          reservation_id: reservationId,
        });

        results.push({ user_id, status: "error", error: "persist_failed", provider });
      }
    }

    // 7) Marca draw como processado (após varrer todos)
    await client.query("BEGIN");
    await client.query(
      `update public.draws set autopay_ran_at = now() where id=$1`,
      [draw_id]
    );
    await client.query("COMMIT");

    log("RUN done", {
      draw_id,
      eligible: profiles.length,
      totalReserved,
      chargedOk,
      chargedFail,
    });

    return { ok: true, draw_id, results, price_cents };
  } catch (e) {
    err("RUN error", { msg: e?.message, code: e?.code });
    return { ok: false, error: "run_failed" };
  } finally {
    try {
      await client.query(`select pg_advisory_unlock(911002, $1)`, [draw_id]);
    } catch {}
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
