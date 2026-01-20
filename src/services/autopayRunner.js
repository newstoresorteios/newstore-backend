// backend/src/services/autopayRunner.js
import { getPool } from "../db.js";
// MP desabilitado para autopay (mantido apenas para compatibilidade de imports, não usado)
// import { mpChargeCard } from "./mercadopago.js";
import { createBill, chargeBill, refundCharge, getBill, cancelBill } from "./vindi.js";
import crypto from "node:crypto";

/* ------------------------------------------------------- *
 * Logging enxuto com contexto
 * ------------------------------------------------------- */
const LP = "[autopayRunner]";
const log  = (msg, extra = null) => console.log(`${LP} ${msg}`, extra ?? "");
const warn = (msg, extra = null) => console.warn(`${LP} ${msg}`, extra ?? "");
const err  = (msg, extra = null) => console.error(`${LP} ${msg}`, extra ?? "");

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/* ------------------------------------------------------- *
 * Autopay Runs (1 linha por attempt_trace_id) — com CASTS explícitos
 * ------------------------------------------------------- */
async function insertAutopayRunAttempt(client, run) {
  const {
    run_trace_id,
    attempt_trace_id,
    autopay_id,
    user_id,
    draw_id,
    tried_numbers,
    reservation_id = null,
    provider = "vindi",
    status,
    amount_cents = null,
    provider_status = null,
    provider_bill_id = null,
    provider_charge_id = null,
    provider_request = null,
    provider_response = null,
    error_message = null,
  } = run;

  const providerRequestJson = provider_request != null ? JSON.stringify(provider_request) : null;
  const providerResponseJson = provider_response != null ? JSON.stringify(provider_response) : null;

  await client.query(
    `insert into public.autopay_runs (
        run_trace_id, attempt_trace_id,
        autopay_id, user_id, draw_id,
        tried_numbers,
        reservation_id,
        provider, status, amount_cents,
        provider_status, provider_bill_id, provider_charge_id,
        provider_request, provider_response,
        error_message
      ) values (
        $1::uuid, $2::uuid,
        $3::uuid, $4::int4, $5::int4,
        $6::int2[],
        $7::uuid,
        $8::text, $9::text, $10::int4,
        $11::int4, $12::text, $13::text,
        $14::jsonb, $15::jsonb,
        $16::text
      )`,
    [
      run_trace_id,
      attempt_trace_id,
      autopay_id,
      user_id,
      draw_id,
      tried_numbers,
      reservation_id,
      provider,
      status,
      amount_cents,
      provider_status,
      provider_bill_id,
      provider_charge_id,
      providerRequestJson,
      providerResponseJson,
      error_message,
    ]
  );
}

async function updateAutopayRunAttempt(client, run) {
  const {
    attempt_trace_id,
    reservation_id = null,
    status,
    amount_cents = null,
    provider_status = null,
    provider_bill_id = null,
    provider_charge_id = null,
    provider_request = null,
    provider_response = null,
    error_message = null,
  } = run;

  const providerRequestJson = provider_request != null ? JSON.stringify(provider_request) : null;
  const providerResponseJson = provider_response != null ? JSON.stringify(provider_response) : null;

  await client.query(
    `update public.autopay_runs
        set reservation_id   = coalesce($2::uuid, reservation_id),
            status           = $3::text,
            amount_cents     = coalesce($4::int4, amount_cents),
            provider_status  = coalesce($5::int4, provider_status),
            provider_bill_id = coalesce($6::text, provider_bill_id),
            provider_charge_id = coalesce($7::text, provider_charge_id),
            provider_request = coalesce($8::jsonb, provider_request),
            provider_response = coalesce($9::jsonb, provider_response),
            error_message    = coalesce($10::text, error_message)
      where attempt_trace_id = $1::uuid`,
    [
      attempt_trace_id,
      reservation_id,
      status,
      amount_cents,
      provider_status,
      provider_bill_id,
      provider_charge_id,
      providerRequestJson,
      providerResponseJson,
      error_message,
    ]
  );
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

function buildAutopayPaymentId({ provider, billId, chargeId, draw_id, user_id }) {
  const p = String(provider || "").toLowerCase();
  if (p === "vindi") {
    if (billId != null && String(billId).trim()) return `autopay:vindi:bill:${String(billId).trim()}`;
    if (chargeId != null && String(chargeId).trim()) return `autopay:vindi:charge:${String(chargeId).trim()}`;
  }
  // fallback (não deveria acontecer, mas evita id null)
  return `autopay:draw:${String(draw_id)}:user:${String(user_id)}:ts:${Date.now()}`;
}

async function finalizePaidReservation(client, { draw_id, reservationId, user_id, numbers, amount_cents, provider, billId, chargeId, vindiPayload = null }) {
  await client.query("BEGIN");
  try {
    const paymentId = buildAutopayPaymentId({ provider, billId, chargeId, draw_id, user_id });
    log("finalizePaidReservation.start", {
      draw_id,
      reservationId,
      user_id,
      provider,
      paymentId,
      billId: billId != null ? String(billId) : null,
      chargeId: chargeId != null ? String(chargeId) : null,
      amount_cents,
      numbers_len: Array.isArray(numbers) ? numbers.length : null,
    });

    const vindiPayloadJson = vindiPayload ? JSON.stringify(vindiPayload) : null;

    // payments.id é NOT NULL (tipo text). Para autopay Vindi, usamos id determinístico e UPSERT para idempotência.
    const pay = await client.query(
      `insert into public.payments (
          id,
          user_id,
          draw_id,
          numbers,
          amount_cents,
          status,
          created_at,
          provider,
          vindi_bill_id,
          vindi_charge_id,
          vindi_status,
          paid_at,
          vindi_payload_json
        )
       values (
          $1,
          $2,
          $3,
          $4::int2[],
          $5,
          'approved',
          now(),
          $6,
          $7,
          $8,
          'paid',
          now(),
          $9::jsonb
       )
       on conflict (id) do update
          set user_id = excluded.user_id,
              draw_id = excluded.draw_id,
              numbers = excluded.numbers,
              amount_cents = excluded.amount_cents,
              status = 'approved',
              provider = excluded.provider,
              vindi_bill_id = excluded.vindi_bill_id,
              vindi_charge_id = excluded.vindi_charge_id,
              vindi_status = excluded.vindi_status,
              paid_at = excluded.paid_at,
              vindi_payload_json = coalesce(excluded.vindi_payload_json, public.payments.vindi_payload_json)
       returning id`,
      [
        paymentId,
        user_id,
        draw_id,
        numbers,
        amount_cents,
        provider,
        billId != null ? String(billId) : null,
        chargeId != null ? String(chargeId) : null,
        vindiPayloadJson,
      ]
    );
    log("finalizePaidReservation.payment_upsert_ok", { paymentId: pay.rows?.[0]?.id || paymentId });

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
export async function runAutopayForDraw(draw_id, { force = false } = {}) {
  const pool = await getPool();
  const client = await pool.connect();
  const runTraceId = crypto.randomUUID();
  log("RUN start", { runTraceId, draw_id });

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

    // 4) Scan de candidatos (inclui active=false) + números agregados
    // Regras de elegibilidade (em JS):
    // - hasVindi = vindi_customer_id && vindi_payment_profile_id
    // - eligible = hasVindi && active=true && preferred.length>0
    const { rows: scanned } = await client.query(
      `select
          ap.id as autopay_id,
          ap.user_id as user_id,
          ap.active as active,
          ap.vindi_customer_id,
          ap.vindi_payment_profile_id,
          coalesce(array_agg(an.n order by an.n) filter (where an.n is not null), '{}') as numbers
        from public.autopay_profiles ap
        left join public.autopay_numbers an on an.autopay_id = ap.id
       group by ap.id, ap.user_id, ap.active, ap.vindi_customer_id, ap.vindi_payment_profile_id`
    );

    let eligible = 0;
    let inactive = 0;
    let noNumbers = 0;
    let missingVindi = 0;

    const candidates = [];

    for (const p of scanned) {
      const hasVindi = !!(p.vindi_customer_id && p.vindi_payment_profile_id);
      const preferred = (p.numbers || []).map(Number).filter((n) => n >= 0 && n <= 99);

      if (!hasVindi) {
        missingVindi++;
        console.warn(`${LP} skip profile`, {
          runTraceId,
          autopay_id: p.autopay_id,
          user_id: p.user_id,
          active: !!p.active,
          preferred,
          reason: "missingVindi",
        });
        // opcional: registrar em autopay_runs
        // (opcional) não gravamos em autopay_runs aqui para evitar poluição fora do attempt
        continue;
      }

      // candidato = tem Vindi configurado
      candidates.push(p);

      if (!p.active) {
        inactive++;
        console.warn(`${LP} skip profile`, {
          runTraceId,
          autopay_id: p.autopay_id,
          user_id: p.user_id,
          active: false,
          preferred,
          reason: "inactive",
        });
        // opcional: registrar em autopay_runs
        // (opcional) não gravamos em autopay_runs aqui para evitar poluição fora do attempt
        continue;
      }

      if (!preferred.length) {
        noNumbers++;
        console.warn(`${LP} skip profile`, {
          runTraceId,
          autopay_id: p.autopay_id,
          user_id: p.user_id,
          active: true,
          preferred,
          reason: "noNumbers",
        });
        // opcional: registrar em autopay_runs
        // (opcional) não gravamos em autopay_runs aqui para evitar poluição fora do attempt
        continue;
      }

      eligible++;
    }

    log("scan candidates", {
      runTraceId,
      total: scanned.length,
      eligible,
      inactive,
      noNumbers,
      missingVindi,
    });

    // Perfis elegíveis para processamento (apenas active=true + hasVindi + preferred>0)
    const profiles = candidates
      .filter((p) => !!p.active)
      .map((p) => ({
        autopay_id: p.autopay_id,
        user_id: p.user_id,
        vindi_customer_id: p.vindi_customer_id,
        vindi_payment_profile_id: p.vindi_payment_profile_id,
        numbers: (p.numbers || []).map(Number).filter((n) => n >= 0 && n <= 99),
      }))
      .filter((p) => p.numbers.length > 0);

    // 5) Preço
    const price_cents = await getTicketPriceCents(client);

    const results = [];
    let totalReserved = 0;
    let chargedOk = 0;
    let chargedFail = 0;

    const ttlMin = Number(process.env.RESERVATION_TTL_MIN || 5);

    // 6) Loop usuários
    for (const p of profiles) {
      const attemptTraceId = crypto.randomUUID();
      const user_id = p.user_id;
      const autopay_id = p.autopay_id;
      const wants = (p.numbers || []).map(Number).filter((n) => n >= 0 && n <= 99);
      log("attempt start", {
        runTraceId,
        attemptTraceId,
        draw_id,
        autopay_id,
        user_id,
        preferred: wants,
        priceEach: price_cents,
        provider: "vindi",
      });

      // Auditoria: 1 registro por attempt (sempre)
      // eslint-disable-next-line no-await-in-loop
      await insertAutopayRunAttempt(client, {
        run_trace_id: runTraceId,
        attempt_trace_id: attemptTraceId,
        autopay_id,
        user_id,
        draw_id,
        tried_numbers: wants,
        reservation_id: null,
        provider: "vindi",
        status: "attempt",
        amount_cents: null,
        provider_status: null,
        provider_bill_id: null,
        provider_charge_id: null,
        provider_request: null,
        provider_response: null,
        error_message: null,
      });

      if (!wants.length) {
        results.push({ user_id, status: "skipped", reason: "no_numbers" });
        continue;
      }

      // Idempotência por perfil: se já teve OK nesse draw, não reprocessa
      // eslint-disable-next-line no-await-in-loop
      const alreadyOk = await client.query(
        `select 1 from public.autopay_runs where autopay_id=$1 and draw_id=$2 and status='charged_ok' limit 1`,
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

      log("numbers free/reserved", {
        runTraceId,
        attemptTraceId,
        draw_id,
        autopay_id,
        user_id,
        preferred: wants,
        free: reservedNumbers, // subset realmente reservado (livre no momento)
        reservationId,
      });

      if (!reservedNumbers.length || !reservationId) {
        // atualiza attempt: não reservou nada
        // eslint-disable-next-line no-await-in-loop
        await updateAutopayRunAttempt(client, {
          attempt_trace_id: attemptTraceId,
          status: "skipped_no_available",
          error_message: "none_available",
        });
        results.push({ user_id, status: "skipped", reason: "none_available" });
        continue;
      }

      totalReserved += reservedNumbers.length;
      const amount_cents = reservedNumbers.length * price_cents;

      // atualiza attempt: reservado
      // eslint-disable-next-line no-await-in-loop
      await updateAutopayRunAttempt(client, {
        attempt_trace_id: attemptTraceId,
        reservation_id: reservationId,
        status: "reserved",
        amount_cents,
      });

      // 6.2) Cobrança Vindi avulsa (fora da TX do banco)
      let charge;
      let provider = "vindi";
      let bill = null;
      let billId = null;
      let chargeId = null;
      let providerRequest = null;

      try {
        const description = `Autopay draw ${draw_id} — ${reservedNumbers.length} números: ${reservedNumbers
          .map((n) => String(n).padStart(2, "0"))
          .join(", ")}`;
        
        // Idempotency key: "draw:{drawId}:user:{userId}"
        const idempotencyKey = `autopay:draw:${draw_id}:user:${user_id}`;
        const amount_reais = Number((amount_cents / 100).toFixed(2));

        providerRequest = {
          endpoint: "/bills",
          customer_id: Number(p.vindi_customer_id),
          payment_profile_id: Number(p.vindi_payment_profile_id),
          code: idempotencyKey,
          amount_cents,
          amount_reais,
          quantity: reservedNumbers.length,
          numbers: reservedNumbers,
          reservation_id: reservationId,
          autopay_id,
          user_id,
          draw_id,
        };
        
        // eslint-disable-next-line no-await-in-loop
        bill = await createBill({
          customerId: p.vindi_customer_id,
          amount_cents_total: amount_cents,
          quantity: reservedNumbers.length,
          description,
          metadata: {
            user_id,
            draw_id,
            numbers: reservedNumbers,
            autopay_id,
            reservation_id: reservationId,
            amount_cents,
            amount_reais,
          },
          paymentProfileId: p.vindi_payment_profile_id,
          idempotencyKey,
          traceId: attemptTraceId,
        });

        billId = bill.billId;
        chargeId = bill.chargeId;

        // atualiza attempt: billed (salva req/res do provider)
        // eslint-disable-next-line no-await-in-loop
        await updateAutopayRunAttempt(client, {
          attempt_trace_id: attemptTraceId,
          status: "billed",
          provider_status: bill.httpStatus ?? null,
          provider_bill_id: billId,
          provider_charge_id: chargeId,
          provider_request: providerRequest,
          provider_response: bill.raw || null,
        });
        log("bill created", {
          runTraceId,
          attemptTraceId,
          draw_id,
          autopay_id,
          user_id,
          bill_id: billId,
          bill_status: bill.billStatus,
          charge_id: bill.chargeId,
          charge_status: bill.chargeStatus,
          last_transaction_status: bill.lastTransactionStatus,
          gateway_message: bill.gatewayMessage,
          amount_cents,
        });

        const norm = (v) => String(v || "").toLowerCase();
        const createdBillStatus = norm(bill.billStatus);
        const createdChargeStatus = norm(bill.chargeStatus);
        const createdLastTxStatus = norm(bill.lastTransactionStatus);

        // rejected => falha imediata
        if (createdLastTxStatus === "rejected") {
          throw new Error(`Vindi rejected: ${bill.gatewayMessage || "rejected"}`);
        }

        // Se a criação já veio com charge/last_transaction, NÃO chamar /bills/:id/charge automaticamente.
        // Só chama chargeBill quando não veio chargeId na criação.
        if (!chargeId) {
          // eslint-disable-next-line no-await-in-loop
          const chargeResult = await chargeBill(billId, { traceId: attemptTraceId });
          chargeId = chargeResult.chargeId;

          // eslint-disable-next-line no-await-in-loop
          await updateAutopayRunAttempt(client, {
            attempt_trace_id: attemptTraceId,
            status: "charged",
            provider_status: chargeResult.httpStatus ?? null,
            provider_charge_id: chargeId,
            provider_response: chargeResult.raw || null,
          });
        }

        const createdPaid =
          createdBillStatus === "paid" ||
          createdChargeStatus === "paid" ||
          createdLastTxStatus === "success" ||
          createdLastTxStatus === "authorized";

        if (createdPaid) {
          charge = { status: "approved", paymentId: chargeId || billId };
        } else {
          // 1 re-check curto
          // eslint-disable-next-line no-await-in-loop
          await sleep(1000);
          // eslint-disable-next-line no-await-in-loop
          const billInfo = await getBill(billId);
          const billStatus = String(billInfo?.status || "").toLowerCase();
          const charge0 = billInfo?.charges?.[0] || null;
          const chargeStatus = String(charge0?.status || "").toLowerCase();
          const lastTxStatus = String(charge0?.last_transaction?.status || "").toLowerCase();
          const gatewayMessage = charge0?.last_transaction?.gateway_message || null;
          const paid =
            !!charge0?.paid_at ||
            billStatus === "paid" ||
            chargeStatus === "paid" ||
            lastTxStatus === "success" ||
            lastTxStatus === "authorized";
          const rejected = lastTxStatus === "rejected";
          const pending =
            billStatus === "pending" ||
            billStatus === "processing" ||
            chargeStatus === "pending" ||
            lastTxStatus === "pending";

          if (rejected) {
            throw new Error(`Vindi rejected: ${gatewayMessage || "rejected"}`);
          }
          if (paid) {
            charge = { status: "approved", paymentId: chargeId || billId };
          } else if (pending) {
            // pendente: não confirma pagamento => tratar como falha controlada (não segurar números)
            throw new Error(`Pagamento pendente: ${billStatus || chargeStatus || lastTxStatus || "unknown"}`);
          } else {
            throw new Error(`Bill não paga: status=${billStatus || "unknown"}`);
          }
        }

        log("bill charged", {
          runTraceId,
          attemptTraceId,
          draw_id,
          autopay_id,
          user_id,
          bill_id: billId,
          charge_id: chargeId,
          bill_status: "paid",
        });
      } catch (e) {
        const emsg = String(e?.message || e);

        chargedFail++;
        const providerStatus = e?.provider_status ?? e?.status ?? null;
        const providerResp = e?.response ?? null;

        // Provider cleanup:
        // - Só faz REFUND se realmente houve pagamento confirmado
        // - Se NÃO foi pago: cancela a bill (não tenta refund)
        if (billId) {
          try {
            // eslint-disable-next-line no-await-in-loop
            const billInfo = await getBill(billId);
            const charge0 = billInfo?.charges?.[0] || null;
            const effectiveChargeId = chargeId || charge0?.id || null;
            const paid =
              !!charge0?.paid_at ||
              String(charge0?.status || "").toLowerCase() === "paid" ||
              String(charge0?.last_transaction?.status || "").toLowerCase() === "success" ||
              String(billInfo?.status || "").toLowerCase() === "paid";

            if (paid && effectiveChargeId) {
              // eslint-disable-next-line no-await-in-loop
              await refundCharge(effectiveChargeId, true);
              warn("Vindi: refund executado após falha", { user_id, billId, chargeId: effectiveChargeId });
            } else {
              // eslint-disable-next-line no-await-in-loop
              await cancelBill(billId, { traceId: attemptTraceId });
              warn("Vindi: bill cancelada após falha (sem refund)", { user_id, billId, chargeId: effectiveChargeId });
            }
          } catch (providerCleanupErr) {
            err("Vindi: falha no cleanup (cancel/refund)", {
              user_id,
              billId,
              chargeId,
              msg: providerCleanupErr?.message,
            });
          }
        }

        // libera reserva
        try {
          warn("rollback reservation", {
            runTraceId,
            attemptTraceId,
            draw_id,
            user_id,
            autopay_id,
            reservationId,
            reason: "charge_fail",
          });
          // eslint-disable-next-line no-await-in-loop
          await cancelReservation(client, { draw_id, reservationId });
          log("rollback reservation ok", { attemptTraceId, reservationId });
        } catch (cancelErr) {
          err("falha ao cancelar reserva após charge fail", { attemptTraceId, user_id, reservationId, msg: cancelErr?.message });
        }

        // audita attempt: charged_fail
        // eslint-disable-next-line no-await-in-loop
        await updateAutopayRunAttempt(client, {
          attempt_trace_id: attemptTraceId,
          status: "charged_fail",
          provider_status: providerStatus,
          provider_request: providerRequest,
          provider_response: providerResp,
          error_message: emsg,
        });

        err("falha ao cobrar Vindi", { user_id, provider, msg: emsg });
        results.push({ user_id, status: "error", error: "charge_failed", provider });
        continue;
      }

      if (!charge || String(charge.status).toLowerCase() !== "approved") {
        chargedFail++;
        // libera reserva e registra
        // eslint-disable-next-line no-await-in-loop
        try {
          warn("rollback reservation", {
            runTraceId,
            attemptTraceId,
            draw_id,
            user_id,
            autopay_id,
            reservationId,
            reason: "not_approved",
          });
          await cancelReservation(client, { draw_id, reservationId });
          log("rollback reservation ok", { attemptTraceId, reservationId });
        } catch (cancelErr) {
          err("falha ao cancelar reserva após not_approved", { attemptTraceId, user_id, reservationId, msg: cancelErr?.message });
        }
        // eslint-disable-next-line no-await-in-loop
        await updateAutopayRunAttempt(client, {
          attempt_trace_id: attemptTraceId,
          status: "charged_fail",
          error_message: "not_approved",
        });
        warn("pagamento não aprovado", { user_id, draw_id, provider });
        results.push({ user_id, status: "error", error: "not_approved", provider });
        continue;
      }

      // 6.3) Confirma (paid) + grava payment + audita autopay_runs (TX)
      try {
        log("reserving numbers", {
          runTraceId,
          attemptTraceId,
          draw_id,
          autopay_id,
          user_id,
          reserved: reservedNumbers,
          reservationId,
        });
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
          vindiPayload: {
            create_bill: bill?.raw ?? null,
            billId: billId != null ? String(billId) : null,
            chargeId: chargeId != null ? String(chargeId) : null,
            billStatus: bill?.billStatus ?? null,
            chargeStatus: bill?.chargeStatus ?? null,
            lastTransactionStatus: bill?.lastTransactionStatus ?? null,
            gatewayMessage: bill?.gatewayMessage ?? null,
          },
        });

        // eslint-disable-next-line no-await-in-loop
        await updateAutopayRunAttempt(client, {
          attempt_trace_id: attemptTraceId,
          status: "charged_ok",
          provider_bill_id: billId,
          provider_charge_id: chargeId,
          error_message: null,
        });

        chargedOk++;
        log("numbers sold", {
          runTraceId,
          attemptTraceId,
          draw_id,
          autopay_id,
          user_id,
          sold: reservedNumbers,
          reservationId,
          payment_id: fin.paymentId,
          amount_cents,
          bill_id: billId,
          charge_id: chargeId,
        });
        results.push({ user_id, status: "ok", numbers: reservedNumbers, amount_cents });
      } catch (e) {
        chargedFail++;
        const emsg = String(e?.message || e);
        err("finalize paid failed (refund+cancel)", {
          user_id,
          reservationId,
          name: e?.name || null,
          msg: emsg,
          stack: e?.stack || null,
          billId: billId != null ? String(billId) : null,
          chargeId: chargeId != null ? String(chargeId) : null,
        });

        // Provider cleanup best-effort (refund só se pago; senão cancela bill)
        if (billId || chargeId) {
          try {
            // eslint-disable-next-line no-await-in-loop
            const billInfo = billId ? await getBill(billId) : null;
            const charge0 = billInfo?.charges?.[0] || null;
            const effectiveChargeId = chargeId || charge0?.id || null;
            const paid =
              !!charge0?.paid_at ||
              String(charge0?.status || "").toLowerCase() === "paid" ||
              String(charge0?.last_transaction?.status || "").toLowerCase() === "success" ||
              String(billInfo?.status || "").toLowerCase() === "paid";

            if (paid && effectiveChargeId) {
              // eslint-disable-next-line no-await-in-loop
              await refundCharge(effectiveChargeId, true);
              warn("Vindi: refund executado após falha de persistência", { user_id, billId, chargeId: effectiveChargeId });
            } else if (billId) {
              // eslint-disable-next-line no-await-in-loop
              await cancelBill(billId, { traceId: attemptTraceId });
              warn("Vindi: bill cancelada após falha de persistência (sem refund)", { user_id, billId, chargeId: effectiveChargeId });
            }
          } catch (providerCleanupErr) {
            err("Vindi: falha no cleanup após persist_failed", { user_id, billId, chargeId, msg: providerCleanupErr?.message });
          }
        }

        // cancela reserva para liberar números
        try {
          warn("rollback reservation", {
            runTraceId,
            attemptTraceId,
            draw_id,
            user_id,
            autopay_id,
            reservationId,
            reason: "persist_failed",
          });
          // eslint-disable-next-line no-await-in-loop
          await cancelReservation(client, { draw_id, reservationId });
          log("rollback reservation ok", { attemptTraceId, reservationId });
        } catch (cancelErr) {
          err("falha ao cancelar reserva após persist_failed", { attemptTraceId, user_id, reservationId, msg: cancelErr?.message });
        }

        // audita erro
        // eslint-disable-next-line no-await-in-loop
        await updateAutopayRunAttempt(client, {
          attempt_trace_id: attemptTraceId,
          status: "charged_fail",
          error_message: emsg,
        });

        results.push({ user_id, status: "error", error: "persist_failed", provider });
      }
    }

    if (eligible > 0 || force) {
      await client.query("BEGIN");
      await client.query(
        `update public.draws set autopay_ran_at = now() where id=$1`,
        [draw_id]
      );
      await client.query("COMMIT");
    } else {
      warn("autopay_ran_at não atualizado (nenhum elegível)", { runTraceId, draw_id, eligible });
    }

    log("RUN done", {
      runTraceId,
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

    return await runAutopayForDraw(draw_id, { force });
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
