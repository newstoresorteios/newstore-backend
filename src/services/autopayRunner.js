// backend/src/services/autopayRunner.js
import { getPool } from "../db.js";
// MP desabilitado para autopay (mantido apenas para compatibilidade de imports, não usado)
// import { mpChargeCard } from "./mercadopago.js";
import { createBill, chargeBill, refundCharge, getBill, getPaymentProfile, getCustomerPaymentProfiles, cancelBill } from "./vindi.js";
import { creditCouponOnApprovedPayment } from "./couponBalance.js";
import { closeDrawIfSoldOut } from "./drawLifecycle.js";
import crypto from "node:crypto";

/* ------------------------------------------------------- *
 * Logging enxuto com contexto
 * ------------------------------------------------------- */
const LP = "[autopayRunner]";
const log  = (msg, extra = null) => console.log(`${LP} ${msg}`, extra ?? "");
const warn = (msg, extra = null) => console.warn(`${LP} ${msg}`, extra ?? "");
const err  = (msg, extra = null) => console.error(`${LP} ${msg}`, extra ?? "");

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const AUTOPAY_BASE_AMOUNT_COLUMNS = [
  "authorized_amount_cents",
  "max_authorized_amount_cents",
  "default_amount_cents",
  "amount_cents",
];

function toPositiveInt(value) {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : null;
}

function getDefaultAuthorizedBaseAmountCents() {
  return toPositiveInt(process.env.CAPTIVE_AUTOPAY_DEFAULT_AMOUNT_CENTS) || 5500;
}

function shouldRequireCaptivePreauth({ currentAmountCents, authorizedBaseAmountCents }) {
  const current = Number(currentAmountCents);
  const base = Number(authorizedBaseAmountCents);
  return Number.isFinite(current) && Number.isFinite(base) && current > base;
}

/* ------------------------------------------------------- *
 * Autopay Runs (1 linha por attempt_trace_id) — com CASTS explícitos
 * ------------------------------------------------------- */
function isAutopayRunsUserDrawUniqueViolation(error) {
  const constraint = String(error?.constraint || error?.constraint_name || "").toLowerCase();
  const message = String(error?.message || "").toLowerCase();
  return (
    String(error?.code) === "23505" &&
    (
      constraint === "autopay_runs_user_draw_unique" ||
      message.includes("autopay_runs_user_draw_unique")
    )
  );
}

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

  let inserted;
  try {
    inserted = await client.query(
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
        )
        returning id`,
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
  } catch (error) {
    if (isAutopayRunsUserDrawUniqueViolation(error)) {
      const schemaError = new Error("autopay_runs_schema_not_migrated");
      schemaError.code = "autopay_runs_schema_not_migrated";
      schemaError.reason = "autopay_runs_user_draw_unique";
      throw schemaError;
    }
    throw error;
  }

  const autopayRunId = inserted.rows?.[0]?.id ?? null;
  if (autopayRunId == null) {
    const insertError = new Error("autopay_run_insert_failed");
    insertError.code = "autopay_run_insert_failed";
    throw insertError;
  }
  return autopayRunId;
}

async function updateAutopayRunAttempt(client, run) {
  const {
    id = null,
    attempt_trace_id = null,
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
  const autopayRunId = id != null ? id : null;

  if (autopayRunId != null) {
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
        where id = $1`,
      [
        autopayRunId,
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
    return;
  }

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

async function getDrawTicketPriceCents(client, draw_id) {
  try {
    const { rows: cols } = await client.query(
      `select column_name
         from information_schema.columns
        where table_schema='public'
          and table_name='draws'
          and column_name in ('ticket_price_cents','price_cents','quota_price_cents','amount_cents')`
    );
    const columns = (cols || [])
      .map((row) => row.column_name)
      .filter((columnName) => ["ticket_price_cents", "price_cents", "quota_price_cents", "amount_cents"].includes(columnName));
    if (columns.length) {
      const r = await client.query(
        `select ${columns.map((columnName) => `"${columnName}"`).join(", ")}
           from public.draws
          where id=$1
          limit 1`,
        [draw_id]
      );
      const row = r.rows?.[0] || {};
      for (const columnName of ["ticket_price_cents", "price_cents", "quota_price_cents", "amount_cents"]) {
        if (columns.includes(columnName)) {
          const value = toPositiveInt(row[columnName]);
          if (value) return value;
        }
      }
    }
  } catch {}

  return getTicketPriceCents(client);
}

async function hasAutopayNumberActiveColumn(client) {
  const { rows } = await client.query(
    `select 1
       from information_schema.columns
      where table_schema='public'
        and table_name='autopay_numbers'
        and column_name='active'
      limit 1`
  );
  return rows.length > 0;
}

async function hasAutopayProfileAuthorizationModeColumn(client) {
  const { rows } = await client.query(
    `select 1
       from information_schema.columns
      where table_schema='public'
        and table_name='autopay_profiles'
        and column_name='authorization_mode'
      limit 1`
  );
  return rows.length > 0;
}

async function getAutopayProfileBaseAmountColumns(client) {
  const { rows } = await client.query(
    `select column_name
       from information_schema.columns
      where table_schema='public'
        and table_name='autopay_profiles'
        and column_name = any($1::text[])`,
    [AUTOPAY_BASE_AMOUNT_COLUMNS]
  );
  return new Set((rows || []).map((row) => row.column_name));
}

function buildAuthorizedBaseAmountSql(existingColumns) {
  const expressions = AUTOPAY_BASE_AMOUNT_COLUMNS
    .filter((columnName) => existingColumns.has(columnName))
    .map((columnName) => `max(ap.${columnName})`);
  return expressions.length ? `coalesce(${expressions.join(", ")})` : "null";
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

async function ensureCaptivePreauthReservationForCharge(client, { draw_id, user_id, captiveNumber, expiresAt }) {
  const expiry = expiresAt ? new Date(expiresAt) : null;
  if (!(expiry instanceof Date) || Number.isNaN(expiry.getTime())) {
    return { ok: false, code: "authorization_missing_expiry", status: "failed" };
  }
  if (expiry.getTime() <= Date.now()) {
    return { ok: false, code: "preauth_reservation_expired", status: "expired" };
  }

  await client.query("BEGIN");
  try {
    const numberResult = await client.query(
      `select n, status, reservation_id
         from public.numbers
        where draw_id=$1
          and n=$2
        for update`,
      [draw_id, captiveNumber]
    );
    const numberRow = numberResult.rows?.[0] || null;
    if (!numberRow) {
      await client.query("ROLLBACK");
      return { ok: false, code: "number_not_found", status: "failed" };
    }
    const numberStatus = String(numberRow.status || "").toLowerCase();
    if (numberStatus === "sold") {
      await client.query("ROLLBACK");
      return { ok: false, code: "number_not_available", status: "failed" };
    }

    if (numberStatus === "reserved" && numberRow.reservation_id) {
      const reservationResult = await client.query(
        `select id, user_id, numbers, status, expires_at
           from public.reservations
          where id=$1
          for update`,
        [numberRow.reservation_id]
      );
      const reservation = reservationResult.rows?.[0] || null;
      if (!reservation) {
        await client.query("ROLLBACK");
        return { ok: false, code: "preauth_reservation_not_available", status: "failed" };
      }

      const reservationStatus = String(reservation.status || "").toLowerCase();
      const isBlocking = ["pending", "active", "reserved", ""].includes(reservationStatus);
      const reservationExpiresAt = reservation.expires_at ? new Date(reservation.expires_at).getTime() : null;
      const reservationExpired = reservationExpiresAt && reservationExpiresAt <= Date.now();
      const belongsToAuthorization =
        Number(reservation.user_id) === Number(user_id) &&
        (reservation.numbers || []).map(Number).includes(Number(captiveNumber));

      if (isBlocking && !reservationExpired && belongsToAuthorization) {
        await client.query("COMMIT");
        return {
          ok: true,
          reservationId: reservation.id,
          reservedNumbers: [Number(captiveNumber)],
        };
      }

      if (isBlocking && reservationExpired) {
        await client.query(`update public.reservations set status='expired', expires_at=now() where id=$1`, [reservation.id]);
        await client.query(
          `update public.numbers
              set status='available',
                  reservation_id=null
            where draw_id=$1
              and n=$2
              and reservation_id=$3
              and status='reserved'`,
          [draw_id, captiveNumber, reservation.id]
        );
      } else {
        await client.query("ROLLBACK");
        return { ok: false, code: "number_not_available", status: "failed" };
      }
    } else if (numberStatus !== "available") {
      await client.query("ROLLBACK");
      return { ok: false, code: "number_not_available", status: "failed" };
    }

    const after = await client.query(
      `select n, status
         from public.numbers
        where draw_id=$1
          and n=$2
        for update`,
      [draw_id, captiveNumber]
    );
    const afterRow = after.rows?.[0] || null;
    if (!afterRow || String(afterRow.status || "").toLowerCase() !== "available") {
      await client.query("ROLLBACK");
      return { ok: false, code: "number_not_available", status: "failed" };
    }

    const reservationId = crypto.randomUUID();
    await client.query(
      `insert into public.reservations (id, user_id, draw_id, numbers, status, created_at, expires_at)
       values ($1, $2, $3, $4::int2[], 'pending', now(), $5)`,
      [reservationId, user_id, draw_id, [captiveNumber], expiry]
    );
    const updated = await client.query(
      `update public.numbers
          set status='reserved',
              reservation_id=$3
        where draw_id=$1
          and n=$2
          and status='available'`,
      [draw_id, captiveNumber, reservationId]
    );
    if (updated.rowCount !== 1) {
      await client.query("ROLLBACK");
      return { ok: false, code: "number_not_available", status: "failed" };
    }

    await client.query("COMMIT");
    return {
      ok: true,
      reservationId,
      reservedNumbers: [Number(captiveNumber)],
      legacy_reservation_created: true,
    };
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

    await closeDrawIfSoldOut(draw_id, client);

    await client.query("COMMIT");
    return { paymentId };
  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    throw e;
  }
}

async function finalizePaidReservationGroup(client, {
  draw_id,
  reservationIds,
  user_id,
  numbers,
  amount_cents,
  provider,
  billId,
  chargeId,
  vindiPayload = null,
  authorizationIds,
  attemptTraceId,
  providerRequest,
}) {
  await client.query("BEGIN");
  try {
    const paymentId = buildAutopayPaymentId({ provider, billId, chargeId, draw_id, user_id });
    const vindiPayloadJson = vindiPayload ? JSON.stringify(vindiPayload) : null;
    await client.query(
      `INSERT INTO public.payments (
          id, user_id, draw_id, numbers, amount_cents, status, created_at,
          provider, vindi_bill_id, vindi_charge_id, vindi_status, paid_at, vindi_payload_json
        ) VALUES (
          $1, $2, $3, $4::int2[], $5, 'approved', now(),
          $6, $7, $8, 'paid', now(), $9::jsonb
        )
        ON CONFLICT (id) DO UPDATE
          SET user_id = EXCLUDED.user_id,
              draw_id = EXCLUDED.draw_id,
              numbers = EXCLUDED.numbers,
              amount_cents = EXCLUDED.amount_cents,
              status = 'approved',
              provider = EXCLUDED.provider,
              vindi_bill_id = EXCLUDED.vindi_bill_id,
              vindi_charge_id = EXCLUDED.vindi_charge_id,
              vindi_status = EXCLUDED.vindi_status,
              paid_at = EXCLUDED.paid_at,
              vindi_payload_json = COALESCE(EXCLUDED.vindi_payload_json, public.payments.vindi_payload_json)`,
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
    const reservationUpdate = await client.query(
      `UPDATE public.reservations
          SET status = 'paid',
              payment_id = $2,
              expires_at = now()
        WHERE id = ANY($1::uuid[])
          AND draw_id = $3
          AND user_id = $4`,
      [reservationIds, paymentId, draw_id, user_id]
    );
    if (reservationUpdate.rowCount !== reservationIds.length) {
      throw new Error(`reservations_update_mismatch expected=${reservationIds.length} updated=${reservationUpdate.rowCount}`);
    }
    const numberUpdate = await client.query(
      `UPDATE public.numbers
          SET status = 'sold'
        WHERE draw_id = $1
          AND n = ANY($2::int2[])
          AND reservation_id = ANY($3::uuid[])`,
      [draw_id, numbers, reservationIds]
    );
    if (numberUpdate.rowCount !== numbers.length) {
      throw new Error(`numbers_update_mismatch expected=${numbers.length} updated=${numberUpdate.rowCount}`);
    }
    const authorizationUpdate = await client.query(
      `UPDATE public.autopay_draw_authorizations
          SET status = 'charged',
              charged_at = COALESCE(charged_at, now()),
              updated_at = now()
        WHERE id = ANY($1::uuid[])
          AND draw_id = $2
          AND user_id = $3
          AND status = 'authorized'
          AND charged_at IS NULL
        RETURNING id`,
      [authorizationIds, draw_id, user_id]
    );
    if (authorizationUpdate.rowCount !== authorizationIds.length) {
      throw new Error(
        `authorizations_update_mismatch expected=${authorizationIds.length} updated=${authorizationUpdate.rowCount}`
      );
    }
    await updateAutopayRunAttempt(client, {
      attempt_trace_id: attemptTraceId,
      status: "charged_ok",
      provider_bill_id: billId,
      provider_charge_id: chargeId,
      provider_request: providerRequest,
      error_message: null,
    });
    await closeDrawIfSoldOut(draw_id, client);
    await client.query("COMMIT");
    return { paymentId };
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    throw error;
  }
}

function isVindiPaymentApproved({ bill, billInfo, chargeId }) {
  const norm = (value) => String(value || "").toLowerCase();
  const billStatus = norm(bill?.billStatus || billInfo?.status);
  const charge0 = billInfo?.charges?.[0] || null;
  const chargeStatus = norm(bill?.chargeStatus || charge0?.status);
  const lastTxStatus = norm(bill?.lastTransactionStatus || charge0?.last_transaction?.status);
  return Boolean(
    charge0?.paid_at ||
    billStatus === "paid" ||
    chargeStatus === "paid" ||
    lastTxStatus === "success" ||
    lastTxStatus === "authorized"
  );
}

async function loadAuthorizationForCharge(client, authorizationId, lock = false) {
  const result = await client.query(
    `SELECT ada.*,
            ap.id AS profile_id,
            ap.active AS profile_active,
            ap.vindi_customer_id,
            ap.vindi_payment_profile_id
       FROM public.autopay_draw_authorizations ada
       LEFT JOIN public.autopay_profiles ap ON ap.id = ada.autopay_profile_id
      WHERE ada.id = $1
      ${lock ? "FOR UPDATE OF ada" : ""}
      LIMIT 1`,
    [authorizationId]
  );
  return result.rows?.[0] || null;
}

async function markCaptivePreauthFailedOutsideTransaction(pool, authorizationId) {
  const id = String(authorizationId || "").trim();
  if (!id) return null;

  const updated = await pool.query(
    `UPDATE public.autopay_draw_authorizations
        SET status = 'failed',
            updated_at = now()
      WHERE id = $1
        AND status = 'authorized'
      RETURNING *`,
    [id]
  );
  return updated.rows?.[0] || null;
}

async function chargeAuthorizedCaptivePreauthGroup({
  drawId,
  userId,
  expectedAuthorizationIds,
  authorizationSource,
  authorizedByAdminId,
} = {}) {
  const normalizedDrawId = Number(drawId);
  const normalizedUserId = Number(userId);
  const normalizedAuthorizationIds = Array.from(
    new Set(
      Array.isArray(expectedAuthorizationIds)
        ? expectedAuthorizationIds.map((id) => String(id).trim())
        : []
    )
  ).sort();
  const context = {
    drawId: normalizedDrawId,
    userId: normalizedUserId,
    authorizationIds: normalizedAuthorizationIds,
    captiveNumbers: [],
    totalAmountCents: null,
    autopayProfileId: null,
    customerId: null,
    paymentProfileId: null,
    financialStage: "preflight",
  };
  const runTraceId = crypto.randomUUID();
  const attemptTraceId = crypto.randomUUID();
  let pool = null;
  let client = null;
  let lockKey = null;
  let bill = null;
  let billId = null;
  let chargeId = null;
  let providerRequest = null;
  let group = null;
  let selectedAuthorizations = [];
  let financialStage = "preflight";
  let paymentProfileLookupStarted = false;
  let paymentProfileResolved = false;
  let attemptPersisted = false;
  let autopayRunId = null;
  let billRequestStarted = false;
  let billResponseReceived = false;

  try {
    const missingFields = [];
    if (!Number.isInteger(normalizedDrawId) || normalizedDrawId <= 0) missingFields.push("drawId");
    if (!Number.isInteger(normalizedUserId) || normalizedUserId <= 0) missingFields.push("userId");
    if (!normalizedAuthorizationIds.length) missingFields.push("expectedAuthorizationIds");
    if (normalizedAuthorizationIds.some((value) => !UUID_RE.test(value))) {
      missingFields.push("expectedAuthorizationIds.uuid");
    }
    if (missingFields.length) {
      const contractError = new Error("runner_context_invalid");
      contractError.code = "payment_preflight_contract_invalid";
      contractError.reason = "runner_context_invalid";
      contractError.missingFields = missingFields;
      throw contractError;
    }
    const isAdminAuthorization = authorizationSource === "admin" && Boolean(toPositiveInt(authorizedByAdminId));
    const isExpiryAutoAuthorization = authorizationSource === "system" && !authorizedByAdminId;
    if (!isAdminAuthorization && !isExpiryAutoAuthorization) {
      const contractError = new Error("runner_context_invalid");
      contractError.code = "payment_preflight_contract_invalid";
      contractError.reason = "runner_context_invalid";
      contractError.missingFields = [
        ...(["admin", "system"].includes(authorizationSource) ? [] : ["authorizationSource"]),
        ...(authorizationSource !== "admin" || toPositiveInt(authorizedByAdminId) ? [] : ["authorizedByAdminId"]),
      ];
      throw contractError;
    }

    pool = await getPool();
    client = await pool.connect();
    const draw_id = context.drawId;
    const user_id = context.userId;
    const expectedIds = context.authorizationIds;
    const idempotencyPrefix = isExpiryAutoAuthorization
      ? "captive-preauth-expiry"
      : "captive-preauth-admin";
    lockKey = `${idempotencyPrefix}:${draw_id}:${user_id}:${expectedIds.join(",")}`;
    await client.query("SELECT pg_advisory_lock(hashtext($1))", [lockKey]);

    await client.query("BEGIN");
    const authResult = await client.query(
      `SELECT ada.*,
              ap.id AS profile_id,
              ap.user_id AS profile_user_id,
              ap.active AS profile_active,
              ap.vindi_customer_id,
              ap.vindi_payment_profile_id
         FROM public.autopay_draw_authorizations ada
         JOIN public.autopay_profiles ap ON ap.id = ada.autopay_profile_id
        WHERE ada.draw_id = $1
          AND ada.user_id = $2
          AND ada.id = ANY($3::uuid[])
        ORDER BY ada.id
        FOR UPDATE OF ada, ap`,
      [draw_id, user_id, expectedIds]
    );
    selectedAuthorizations = authResult.rows || [];
    const authorizations = selectedAuthorizations;
    const authorizationIds = authorizations.map((item) => String(item.id));

    const actualIds = [...authorizationIds].sort();
    if (
      expectedIds.length !== actualIds.length ||
      expectedIds.some((value, index) => value !== actualIds[index])
    ) {
      await client.query("ROLLBACK");
      return { ok: false, code: "group_changed", status: "failed", charged: false, definitive: true, retryable: false };
    }
    if (!authorizations.length || authorizations.some((item) => String(item.status || "").toLowerCase() !== "authorized")) {
      await client.query("ROLLBACK");
      return { ok: false, code: "group_requires_review", status: "failed", charged: false, definitive: true, retryable: false };
    }
    const autopayProfileIds = new Set(authorizations.map((item) => String(item.autopay_profile_id || "")));
    const joinedProfileIds = new Set(authorizations.map((item) => String(item.profile_id || "")));
    const profileMismatch =
      autopayProfileIds.size !== 1 ||
      joinedProfileIds.size !== 1 ||
      [...autopayProfileIds][0] !== [...joinedProfileIds][0] ||
      authorizations.some((item) => Number(item.profile_user_id) !== user_id);
    if (profileMismatch) {
      await client.query("ROLLBACK");
      return { ok: false, code: "captive_payment_profile_mismatch", status: "failed", charged: false, definitive: true };
    }
    if (authorizations.some((item) => (
      !item.profile_active ||
      !item.vindi_customer_id ||
      !item.vindi_payment_profile_id
    ))) {
      await client.query("ROLLBACK");
      return { ok: false, code: "payment_method_unavailable", status: "failed", charged: false, definitive: true };
    }
    const customerIds = new Set(authorizations.map((item) => String(item.vindi_customer_id)));
    const paymentProfileIds = new Set(authorizations.map((item) => String(item.vindi_payment_profile_id)));
    if (customerIds.size !== 1 || paymentProfileIds.size !== 1) {
      await client.query("ROLLBACK");
      return { ok: false, code: "captive_payment_profile_mismatch", status: "failed", charged: false, definitive: true };
    }

    const autopay_id = authorizations[0].autopay_profile_id || authorizations[0].profile_id;
    const captiveNumbers = authorizations.map((item) => Number(item.captive_number)).sort((a, b) => a - b);
    const totalAmountCents = authorizations.reduce((total, item) => total + Number(item.amount_cents), 0);
    context.authorizationIds = authorizationIds;
    context.captiveNumbers = captiveNumbers;
    context.totalAmountCents = totalAmountCents;
    context.autopayProfileId = String(autopay_id);
    context.customerId = String(authorizations[0].vindi_customer_id);
    context.paymentProfileId = String(authorizations[0].vindi_payment_profile_id);
    const officialAmountCents = await getDrawTicketPriceCents(client, draw_id);
    if (
      authorizations.some((item) => !Number.isInteger(Number(item.amount_cents)) || Number(item.amount_cents) <= 0) ||
      authorizations.some((item) => Number(item.amount_cents) !== Number(officialAmountCents)) ||
      !Number.isInteger(totalAmountCents) ||
      totalAmountCents <= 0
    ) {
      await client.query("ROLLBACK");
      return { ok: false, code: "authorization_amount_mismatch", status: "failed", charged: false, definitive: true };
    }

    financialStage = "existing_payment_check";
    context.financialStage = financialStage;
    providerRequest = {
      stage: financialStage,
      authorization_ids: context.authorizationIds,
      numbers: context.captiveNumbers,
      amount_cents: context.totalAmountCents,
      customer_id: context.customerId,
      payment_profile_id: context.paymentProfileId,
      idempotency_key: lockKey,
    };
    const existingPayment = await client.query(
      `SELECT 1
         FROM public.payments
        WHERE draw_id = $1
          AND user_id = $2
          AND numbers && $3::int[]
          AND (
            lower(status) IN ('approved', 'paid', 'pago')
            OR lower(coalesce(vindi_status, '')) IN ('approved', 'paid', 'pago', 'success', 'successful')
          )
        LIMIT 1`,
      [draw_id, user_id, captiveNumbers]
    );
    if (existingPayment.rowCount) {
      await client.query("ROLLBACK");
      return {
        ok: false,
        code: "group_already_partially_or_fully_charged",
        status: "failed",
        charged: false,
        definitive: true,
        retryable: false,
      };
    }
    const inProgressRun = await client.query(
      `SELECT 1
         FROM public.autopay_runs
        WHERE draw_id = $1
          AND user_id = $2
          AND lower(coalesce(status, '')) IN ('attempt', 'reserved', 'billed', 'charged')
        LIMIT 1`,
      [draw_id, user_id]
    );
    if (inProgressRun.rowCount) {
      await client.query("ROLLBACK");
      return { ok: false, code: "payment_in_progress", status: "failed", charged: false, definitive: true, retryable: false };
    }

    const existingRun = await client.query(
      `SELECT provider_bill_id, provider_charge_id
         FROM public.autopay_runs
        WHERE draw_id = $1
          AND user_id = $2
          AND status = 'charged_ok'
          AND provider_request->>'idempotency_key' = $3
        ORDER BY created_at DESC
        LIMIT 1`,
      [draw_id, user_id, lockKey]
    );
    if (existingRun.rowCount) {
      await client.query(
        `UPDATE public.autopay_draw_authorizations
            SET status = 'charged',
                charged_at = COALESCE(charged_at, now()),
                updated_at = now()
          WHERE id = ANY($1::uuid[])
            AND status IN ('authorized', 'charged')`,
        [authorizationIds]
      );
      await client.query("COMMIT");
      return {
        ok: true,
        code: "already_charged",
        status: "charged",
        charged: true,
        provider_bill_id: existingRun.rows[0].provider_bill_id || null,
        provider_charge_id: existingRun.rows[0].provider_charge_id || null,
      };
    }

    const reservationResult = await client.query(
      `SELECT n.n, n.reservation_id, r.status, r.expires_at, r.user_id, r.numbers
         FROM public.numbers n
         JOIN public.reservations r ON r.id = n.reservation_id
        WHERE n.draw_id = $1
          AND n.n = ANY($2::smallint[])
        ORDER BY n.n
        FOR UPDATE OF n, r`,
      [draw_id, captiveNumbers]
    );
    const reservedRows = reservationResult.rows || [];
    const reservationsValid = reservedRows.length === captiveNumbers.length && reservedRows.every((item) => {
      const status = String(item.status || "").toLowerCase();
      const expiresAt = item.expires_at ? new Date(item.expires_at).getTime() : null;
      return ["pending", "active", "reserved", ""].includes(status) &&
        (!expiresAt || expiresAt > Date.now()) &&
        Number(item.user_id) === user_id &&
        (item.numbers || []).map(Number).includes(Number(item.n));
    });
    if (!reservationsValid) {
      await client.query("ROLLBACK");
      return { ok: false, code: "preauth_reservation_not_available", status: "failed", charged: false, definitive: true };
    }
    const reservationIds = [...new Set(reservedRows.map((item) => String(item.reservation_id)))];
    group = {
      draw_id,
      user_id,
      authorizationIds,
      captiveNumbers,
      reservationIds,
      totalAmountCents,
      unitAmountCents: Number(authorizations[0].amount_cents),
      customerId: String(authorizations[0].vindi_customer_id),
      paymentProfileId: String(authorizations[0].vindi_payment_profile_id),
      autopay_id,
      failureCode: null,
      failureReason: null,
      definitivePaymentFailure: false,
    };
    providerRequest = {
      stage: "preflight",
      idempotency_key: lockKey,
      authorization_ids: authorizationIds,
      numbers: captiveNumbers,
      amount_cents: totalAmountCents,
      customer_id: group.customerId,
      payment_profile_id: group.paymentProfileId,
    };
    await client.query("COMMIT");
    financialStage = "attempt_persisting";
    autopayRunId = await insertAutopayRunAttempt(pool, {
      run_trace_id: runTraceId,
      attempt_trace_id: attemptTraceId,
      autopay_id,
      user_id,
      draw_id,
      tried_numbers: captiveNumbers,
      reservation_id: reservationIds[0] || null,
      provider: "vindi",
      status: "preflight",
      amount_cents: totalAmountCents,
      provider_request: providerRequest,
    });
    attemptPersisted = true;
    financialStage = "attempt_persisted";

    let paymentProfile = null;
    paymentProfileLookupStarted = true;
    financialStage = "payment_profile_lookup";
    providerRequest = { ...providerRequest, stage: financialStage };
    await updateAutopayRunAttempt(pool, {
      id: autopayRunId,
      attempt_trace_id: attemptTraceId,
      status: "payment_profile_lookup",
      provider_request: providerRequest,
    });
    try {
      paymentProfile = await getPaymentProfile(group.paymentProfileId);
      if (!paymentProfile || !paymentProfile.id) {
        group.failureCode = "payment_method_unavailable";
        group.failureReason = "payment_profile_response_invalid";
        group.definitivePaymentFailure = true;
        throw new Error(group.failureReason);
      }

      const resolvedProfileId = String(paymentProfile.id);
      if (resolvedProfileId !== String(group.paymentProfileId)) {
        group.failureCode = "captive_payment_profile_mismatch";
        group.failureReason = "payment_profile_response_invalid";
        group.definitivePaymentFailure = true;
        throw new Error(group.failureReason);
      }

      const directCustomerId = paymentProfile?.customer?.id ?? paymentProfile?.customer_id ?? null;
      if (directCustomerId != null) {
        if (String(directCustomerId) !== String(group.customerId)) {
          group.failureCode = "captive_payment_profile_mismatch";
          group.failureReason = "payment_profile_customer_mismatch";
          group.definitivePaymentFailure = true;
          throw new Error(group.failureReason);
        }
      } else {
        const customerProfiles = await getCustomerPaymentProfiles(group.customerId);
        const ownedProfile = customerProfiles.find(
          (item) => String(item?.id || "") === String(group.paymentProfileId)
        );
        if (!ownedProfile) {
          group.failureCode = "captive_payment_profile_mismatch";
          group.failureReason = "payment_profile_customer_mismatch";
          group.definitivePaymentFailure = true;
          throw new Error(group.failureReason);
        }
        paymentProfile = { ...ownedProfile, ...paymentProfile };
      }

      const paymentMethodCode = String(
        paymentProfile?.payment_method?.code || paymentProfile?.payment_method_code || ""
      ).toLowerCase();
      const paymentProfileStatus = String(paymentProfile?.status || "").toLowerCase();
      if (
        paymentProfile?.active === false ||
        ["inactive", "deleted", "removed", "canceled", "cancelled"].includes(paymentProfileStatus)
      ) {
        group.failureCode = "payment_method_unavailable";
        group.failureReason = "payment_profile_inactive";
        group.definitivePaymentFailure = true;
        throw new Error(group.failureReason);
      }
      if (paymentMethodCode && paymentMethodCode !== "credit_card") {
        group.failureCode = "payment_method_unavailable";
        group.failureReason = "payment_method_not_credit_card";
        group.definitivePaymentFailure = true;
        throw new Error(group.failureReason);
      }

      paymentProfileResolved = true;
    } catch (profileError) {
      const profileStatus = Number(profileError?.status || profileError?.provider_status);
      if (!group.failureCode && [404, 422].includes(profileStatus)) {
        group.failureCode = "payment_method_unavailable";
        group.failureReason = "payment_profile_not_found";
        group.definitivePaymentFailure = true;
      } else if (!group.failureCode && (!profileStatus || profileStatus >= 500)) {
        group.failureCode = "payment_provider_unavailable";
        group.failureReason = "payment_profile_lookup_failed";
        group.definitivePaymentFailure = true;
      }
      throw profileError;
    }
    financialStage = "payment_profile_resolved";
    providerRequest = { ...providerRequest, stage: financialStage };
    await updateAutopayRunAttempt(pool, {
      id: autopayRunId,
      attempt_trace_id: attemptTraceId,
      status: "payment_profile_resolved",
      provider_request: providerRequest,
    });
    log("admin_captive_payment_profile_resolved", {
      event: "admin_captive_payment_profile_resolved",
      draw_id,
      user_id,
      authorization_ids: authorizationIds,
      captive_numbers: captiveNumbers,
      autopay_profile_id: autopay_id,
      has_vindi_customer: true,
      has_payment_profile: true,
    });
    log("admin_captive_payment_attempt", {
      event: "admin_captive_payment_attempt",
      draw_id,
      user_id,
      authorization_ids: authorizationIds,
      captive_numbers: captiveNumbers,
      total_amount_cents: totalAmountCents,
      idempotency_key: lockKey,
      provider_bill_id: null,
      provider_charge_id: null,
      provider_status: "starting",
    });
    const description = `Autopay preauth draw ${draw_id} - numeros ${captiveNumbers.join(",")}`;
    bill = await createBill({
      customerId: group.customerId,
      amount_cents_total: totalAmountCents,
      quantity: 1,
      description,
      metadata: {
        admin_group_key: lockKey,
        authorization_id: authorizationIds[0],
        authorization_ids: authorizationIds,
        user_id,
        draw_id,
        numbers: captiveNumbers,
        quantity: captiveNumbers.length,
        amount_cents: totalAmountCents,
      },
      paymentProfileId: group.paymentProfileId,
      idempotencyKey: lockKey,
      traceId: attemptTraceId,
      onRequestStarted: async () => {
        financialStage = "bill_request_started";
        providerRequest = { ...providerRequest, stage: financialStage };
        await updateAutopayRunAttempt(pool, {
          id: autopayRunId,
          attempt_trace_id: attemptTraceId,
          status: "bill_request_started",
          provider_request: providerRequest,
        });
        billRequestStarted = true;
      },
    });
    billResponseReceived = true;
    financialStage = "bill_response_received";
    billId = bill.billId;
    chargeId = bill.chargeId;
    await updateAutopayRunAttempt(client, {
      id: autopayRunId,
      attempt_trace_id: attemptTraceId,
      status: "billed",
      provider_status: bill.httpStatus ?? null,
      provider_bill_id: billId,
      provider_charge_id: chargeId,
      provider_request: providerRequest,
      provider_response: bill.raw || null,
    });
    log("admin_captive_payment_attempt", {
      event: "admin_captive_payment_attempt",
      draw_id,
      user_id,
      authorization_ids: authorizationIds,
      captive_numbers: captiveNumbers,
      total_amount_cents: totalAmountCents,
      idempotency_key: lockKey,
      provider_bill_id: billId != null ? String(billId) : null,
      provider_charge_id: chargeId != null ? String(chargeId) : null,
      provider_status: bill.lastTransactionStatus || bill.chargeStatus || bill.billStatus || bill.httpStatus || null,
    });
    const norm = (value) => String(value || "").toLowerCase();
    if (norm(bill.lastTransactionStatus) === "rejected") {
      group.definitivePaymentFailure = true;
      throw new Error(`Vindi rejected: ${bill.gatewayMessage || "rejected"}`);
    }
    if (!chargeId) {
      const chargeResult = await chargeBill(billId, { traceId: attemptTraceId });
      chargeId = chargeResult.chargeId;
      await updateAutopayRunAttempt(client, {
        id: autopayRunId,
        attempt_trace_id: attemptTraceId,
        status: "charged",
        provider_status: chargeResult.httpStatus ?? null,
        provider_charge_id: chargeId,
        provider_response: chargeResult.raw || null,
      });
    }
    await sleep(1000);
    const billInfo = await getBill(billId);
    if (!isVindiPaymentApproved({ bill, billInfo, chargeId })) {
      const charge0 = billInfo?.charges?.[0] || null;
      const providerStatuses = [
        billInfo?.status,
        bill?.billStatus,
        charge0?.status,
        bill?.chargeStatus,
        charge0?.last_transaction?.status,
        bill?.lastTransactionStatus,
      ].map(norm).filter(Boolean);
      group.definitivePaymentFailure = providerStatuses.some((status) =>
        ["rejected", "failed", "declined", "canceled", "cancelled", "not_paid"].includes(status)
      );
      throw new Error("payment_not_approved");
    }
    financialStage = "bill_confirmed";
    const fin = await finalizePaidReservationGroup(client, {
      draw_id,
      reservationIds,
      user_id,
      numbers: captiveNumbers,
      amount_cents: totalAmountCents,
      provider: "vindi",
      billId,
      chargeId,
      vindiPayload: {
        billId: billId != null ? String(billId) : null,
        chargeId: chargeId != null ? String(chargeId) : null,
        admin_group_key: lockKey,
        authorization_ids: authorizationIds,
        numbers: captiveNumbers,
      },
      authorizationIds,
      attemptTraceId,
      providerRequest,
    });
    try {
      await creditCouponOnApprovedPayment(fin.paymentId, {
        channel: "VINDI",
        source: "reconcile_sync",
        runTraceId,
        meta: { pricing_source: "autopay_draw_authorizations.amount_cents", autopay: true, captive_preauth: true },
        pgClient: client,
      });
    } catch (couponError) {
      err("admin_captive_coupon_credit_failed", {
        event: "admin_captive_coupon_credit_failed",
        draw_id,
        user_id,
        authorization_ids: authorizationIds,
        captive_numbers: captiveNumbers,
        payment_id: fin.paymentId,
        reason: couponError?.message || "coupon_credit_failed",
      });
    }
    financialStage = "finalized";
    return {
      ok: true,
      code: "charged_success",
      status: "charged",
      charged: true,
      payment_id: fin.paymentId,
      provider_bill_id: billId != null ? String(billId) : null,
      provider_charge_id: chargeId != null ? String(chargeId) : null,
    };
  } catch (error) {
    if (client) {
      try {
        await client.query("ROLLBACK");
      } catch {}
    }
    console.error("[autopayRunner] admin_captive_prebill_exception", {
      stage: financialStage,
      error_name: error?.name ?? null,
      error_message: error?.message ?? null,
      error_code: error?.code ?? null,
      stack_head: String(error?.stack || "")
        .split("\n")
        .slice(0, 6)
        .join("\n"),
      draw_id: context.drawId,
      user_id: context.userId,
      authorization_ids: context.authorizationIds,
      captive_numbers: context.captiveNumbers,
      total_amount_cents: context.totalAmountCents,
      autopay_profile_id: context.autopayProfileId,
      bill_request_started: Boolean(billRequestStarted),
      missing_fields: error?.missingFields || [],
    });
    if (error?.code === "payment_preflight_contract_invalid") {
      return {
        ok: false,
        code: "payment_preflight_contract_invalid",
        status: "failed",
        charged: false,
        definitive: true,
        retryable: false,
        financial_stage: financialStage,
        bill_request_started: false,
        reason: "runner_context_invalid",
        provider_bill_id: null,
        provider_charge_id: null,
      };
    }
    if (error?.code === "autopay_runs_schema_not_migrated") {
      console.error("[autopayRunner] autopay_runs_schema_not_migrated", {
        draw_id: context.drawId,
        user_id: context.userId,
        authorization_ids: context.authorizationIds,
      });
      return {
        ok: false,
        code: "autopay_runs_schema_not_migrated",
        status: "failed",
        charged: false,
        definitive: true,
        retryable: false,
        financial_stage: financialStage,
        bill_request_started: false,
        reason: "autopay_runs_user_draw_unique",
        provider_bill_id: null,
        provider_charge_id: null,
      };
    }
    if (billRequestStarted && billId) {
      try {
        const billInfo = await getBill(billId);
        const charge0 = billInfo?.charges?.[0] || null;
        const effectiveChargeId = chargeId || charge0?.id || null;
        const paid =
          !!charge0?.paid_at ||
          String(charge0?.status || "").toLowerCase() === "paid" ||
          String(charge0?.last_transaction?.status || "").toLowerCase() === "success" ||
          String(billInfo?.status || "").toLowerCase() === "paid";
        if (paid && effectiveChargeId) await refundCharge(effectiveChargeId, true);
        else await cancelBill(billId, { traceId: attemptTraceId });
      } catch {}
    }

    const providerStatusValue = Number(bill?.httpStatus || error?.provider_status || error?.status);
    const providerStatus = Number.isInteger(providerStatusValue) ? providerStatusValue : null;
    const preBillFailure = billRequestStarted !== true;
    let definitive = group?.definitivePaymentFailure === true;
    let failureCode = group?.failureCode || null;
    let failureReason = group?.failureReason || null;
    let retryable = false;
    let runStatus = "preflight_failed";

    if (preBillFailure) {
      definitive = true;
      if (!failureCode && financialStage === "attempt_persisting") {
        failureCode = "payment_preflight_internal_error";
        failureReason = "preflight_internal_error";
      } else if (!failureCode && paymentProfileLookupStarted && !paymentProfileResolved) {
        failureCode = [404, 422].includes(providerStatus)
          ? "payment_method_unavailable"
          : "payment_provider_unavailable";
        failureReason = [404, 422].includes(providerStatus)
          ? "payment_profile_not_found"
          : "payment_profile_lookup_failed";
      } else if (!failureCode) {
        failureCode = "payment_preflight_internal_error";
        failureReason = "preflight_internal_error";
      }
      retryable = failureCode === "payment_provider_unavailable";
      runStatus = failureCode === "payment_method_unavailable"
        ? "payment_method_unavailable"
        : failureCode === "payment_provider_unavailable"
          ? "provider_unavailable"
          : "preflight_failed";
    } else {
      billResponseReceived = billResponseReceived || providerStatus != null;
      const providerUnavailable = [401, 403].includes(providerStatus);
      const explicitProviderRejection =
        group?.definitivePaymentFailure === true ||
        [400, 404, 422].includes(providerStatus);
      definitive = providerUnavailable || explicitProviderRejection;
      failureCode = failureCode || (
        providerUnavailable
          ? "payment_provider_unavailable"
          : explicitProviderRejection
            ? "payment_failed"
            : "payment_result_unknown"
      );
      retryable = providerUnavailable;
      runStatus = providerUnavailable
        ? "provider_unavailable"
        : explicitProviderRejection
          ? "charged_fail"
          : "result_unknown";
    }

    providerRequest = {
      ...(providerRequest || {}),
      stage: financialStage,
    };
    const technicalErrorMessage = `${financialStage}: ${String(
      error?.message || failureReason || failureCode
    )}`.replace(/\s+/g, " ").slice(0, 180);
    if (attemptPersisted) {
      try {
        await updateAutopayRunAttempt(pool, {
          id: autopayRunId,
          attempt_trace_id: attemptTraceId,
          status: runStatus,
          provider_status: providerStatus,
          provider_bill_id: billId,
          provider_charge_id: chargeId,
          provider_request: providerRequest,
          provider_response: error?.response || null,
          error_message: technicalErrorMessage,
        });
      } catch (runUpdateError) {
        err("admin_captive_payment_attempt_persist_failed", {
          event: "admin_captive_payment_attempt_persist_failed",
          stage: financialStage,
          draw_id: context.drawId,
          user_id: context.userId,
          authorization_ids: context.authorizationIds,
          captive_numbers: context.captiveNumbers,
          reason: runUpdateError?.message || "autopay_run_update_failed",
        });
      }
    } else {
      try {
        autopayRunId = await insertAutopayRunAttempt(pool, {
          run_trace_id: runTraceId,
          attempt_trace_id: attemptTraceId,
          autopay_id: context.autopayProfileId,
          user_id: context.userId,
          draw_id: context.drawId,
          tried_numbers: context.captiveNumbers,
          reservation_id: group?.reservationIds?.[0] || null,
          provider: "vindi",
          status: runStatus,
          amount_cents: context.totalAmountCents,
          provider_status: providerStatus,
          provider_request: providerRequest,
          error_message: technicalErrorMessage,
        });
        attemptPersisted = true;
      } catch (runInsertError) {
        if (runInsertError?.code === "autopay_runs_schema_not_migrated") {
          console.error("[autopayRunner] autopay_runs_schema_not_migrated", {
            draw_id: context.drawId,
            user_id: context.userId,
            authorization_ids: context.authorizationIds,
          });
          return {
            ok: false,
            code: "autopay_runs_schema_not_migrated",
            status: "failed",
            charged: false,
            definitive: true,
            retryable: false,
            financial_stage: financialStage,
            bill_request_started: false,
            reason: "autopay_runs_user_draw_unique",
            provider_bill_id: null,
            provider_charge_id: null,
          };
        }
        err("admin_captive_payment_attempt_insert_failed", {
          event: "admin_captive_payment_attempt_insert_failed",
          stage: financialStage,
          draw_id: context.drawId,
          user_id: context.userId,
          authorization_ids: context.authorizationIds,
          captive_numbers: context.captiveNumbers,
          reason: runInsertError?.message || "autopay_run_insert_failed",
        });
      }
    }

    if (preBillFailure) {
      warn("admin_captive_prebill_failure", {
        event: "admin_captive_prebill_failure",
        stage: financialStage,
        draw_id: context.drawId,
        user_id: context.userId,
        authorization_ids: context.authorizationIds,
        captive_numbers: context.captiveNumbers,
        total_amount_cents: context.totalAmountCents,
        autopay_profile_id: context.autopayProfileId,
        has_customer_id: Boolean(context.customerId),
        has_payment_profile_id: Boolean(context.paymentProfileId),
        bill_request_started: false,
        error_code: failureCode,
        reason: failureReason,
        provider_status: providerStatus,
      });
    } else {
      log("admin_captive_payment_attempt", {
        event: "admin_captive_payment_attempt",
        stage: financialStage,
        draw_id: context.drawId,
        user_id: context.userId,
        authorization_ids: context.authorizationIds,
        captive_numbers: context.captiveNumbers,
        total_amount_cents: context.totalAmountCents,
        idempotency_key: lockKey,
        provider_bill_id: billId != null ? String(billId) : null,
        provider_charge_id: chargeId != null ? String(chargeId) : null,
        provider_status: definitive ? "failed" : "unknown",
      });
    }
    return {
      ok: false,
      code: failureCode,
      status: definitive ? "failed" : "authorized",
      charged: false,
      definitive,
      retryable,
      financial_stage: financialStage,
      bill_request_started: billRequestStarted,
      reason: failureReason,
      provider_bill_id: billId != null ? String(billId) : null,
      provider_charge_id: chargeId != null ? String(chargeId) : null,
    };
  } finally {
    if (lockKey && client) {
      try {
        await client.query("SELECT pg_advisory_unlock(hashtext($1))", [lockKey]);
      } catch {}
    }
    client?.release();
  }
}

export async function chargeAuthorizedCaptivePreauth(authorizationOrOptions, legacyOptions = {}) {
  const namedOptions =
    authorizationOrOptions && typeof authorizationOrOptions === "object" && !Array.isArray(authorizationOrOptions)
      ? authorizationOrOptions
      : legacyOptions;
  if (namedOptions.adminGroup === true || namedOptions.expiryGroup === true) {
    return chargeAuthorizedCaptivePreauthGroup({
      drawId: namedOptions.drawId,
      userId: namedOptions.userId,
      expectedAuthorizationIds: namedOptions.expectedAuthorizationIds,
      authorizationSource: namedOptions.authorizationSource,
      authorizedByAdminId: namedOptions.authorizedByAdminId,
    });
  }
  const authorizationId = namedOptions === authorizationOrOptions
    ? namedOptions.authorizationId
    : authorizationOrOptions;
  const options = namedOptions;
  const pool = await getPool();
  const client = await pool.connect();
  const runTraceId = crypto.randomUUID();
  const attemptTraceId = crypto.randomUUID();
  const id = String(authorizationId || "").trim();
  let reservationId = null;
  let bill = null;
  let billId = null;
  let chargeId = null;
  let providerRequest = null;
  const chargeContext = {
    draw_id: null,
    user_id: null,
    captive_number: null,
    amount_cents: null,
    expires_at: null,
  };

  try {
    await client.query("select pg_advisory_lock(hashtext($1))", [`captive-preauth:${id}`]);

    await client.query("BEGIN");
    const auth = await loadAuthorizationForCharge(client, id, true);
    if (!auth) {
      await client.query("COMMIT");
      return { ok: false, code: "authorization_not_found", status: "not_found" };
    }

    const status = String(auth.status || "").toLowerCase();
    if (status === "charged") {
      await client.query("COMMIT");
      return { ok: true, code: "already_charged", status: "charged", charged: true, authorization: auth };
    }
    if (status !== "authorized") {
      await client.query("ROLLBACK");
      return { ok: false, code: `authorization_${status || "invalid"}`, status };
    }

    const authorizedAt = auth.authorized_at ? new Date(auth.authorized_at).getTime() : null;
    const expiresAt = auth.expires_at ? new Date(auth.expires_at).getTime() : null;
    if (expiresAt && (!authorizedAt || authorizedAt > expiresAt) && expiresAt <= Date.now()) {
      await client.query(
        `UPDATE public.autopay_draw_authorizations
            SET status = 'expired',
                expired_at = COALESCE(expired_at, now()),
                updated_at = now()
          WHERE id = $1
            AND status = 'authorized'
          RETURNING *`,
        [id]
      );
      await client.query("COMMIT");
      return { ok: false, code: "authorization_expired", status: "expired" };
    }

    const draw_id = Number(auth.draw_id);
    const user_id = Number(auth.user_id);
    const captiveNumber = Number(auth.captive_number);
    const amount_cents = Number(auth.amount_cents);
    const autopay_id = auth.autopay_profile_id || auth.profile_id;
    chargeContext.draw_id = draw_id;
    chargeContext.user_id = user_id;
    chargeContext.captive_number = captiveNumber;
    chargeContext.amount_cents = amount_cents;
    chargeContext.expires_at = auth.expires_at || null;
    if (
      !Number.isInteger(draw_id) ||
      !Number.isInteger(user_id) ||
      !Number.isInteger(captiveNumber) ||
      !Number.isInteger(amount_cents) ||
      amount_cents <= 0 ||
      !autopay_id ||
      !auth.vindi_customer_id ||
      !auth.vindi_payment_profile_id
    ) {
      await client.query("ROLLBACK");
      await markCaptivePreauthFailedOutsideTransaction(pool, id);
      err("charge_authorized_failed", {
        authorization_id: id,
        draw_id,
        user_id,
        captive_number: captiveNumber,
        amount_cents,
        status: "failed",
        error_code: "authorization_charge_not_configured",
      });
      return { ok: false, code: "authorization_charge_not_configured", status: "failed" };
    }

    log("charge_authorized_started", {
      authorization_id: id,
      draw_id,
      user_id,
      captive_number: captiveNumber,
      amount_cents,
      status,
    });

    await client.query("COMMIT");

    const alreadyOk = await client.query(
      `select 1
         from public.autopay_runs
        where draw_id = $1
          and user_id = $2
          and status = 'charged_ok'
          and provider_request->>'authorization_id' = $3
        limit 1`,
      [draw_id, user_id, id]
    );
    if (alreadyOk.rowCount) {
      const updated = await client.query(
        `UPDATE public.autopay_draw_authorizations
            SET status = 'charged',
                charged_at = COALESCE(charged_at, now()),
                updated_at = now()
          WHERE id = $1
            AND status IN ('authorized', 'charged')
          RETURNING *`,
        [id]
      );
      return {
        ok: true,
        code: "already_charged",
        status: "charged",
        charged: true,
        authorization: updated.rows?.[0] || auth,
      };
    }

    await insertAutopayRunAttempt(client, {
      run_trace_id: runTraceId,
      attempt_trace_id: attemptTraceId,
      autopay_id,
      user_id,
      draw_id,
      tried_numbers: [captiveNumber],
      reservation_id: null,
      provider: "vindi",
      status: "attempt",
      amount_cents: null,
    });

    const reserved = await ensureCaptivePreauthReservationForCharge(client, {
      draw_id,
      user_id,
      captiveNumber,
      expiresAt: auth.expires_at,
    });
    reservationId = reserved.reservationId;
    const reservedNumbers = reserved.reservedNumbers || [];
    if (!reserved.ok || !reservationId || !reservedNumbers.includes(captiveNumber)) {
      await updateAutopayRunAttempt(client, {
        attempt_trace_id: attemptTraceId,
        status: "skipped_no_available",
        error_message: reserved.code || "preauth_reservation_not_available",
      });
      if (reserved.status === "expired") {
        await pool.query(
          `UPDATE public.autopay_draw_authorizations
              SET status = 'expired',
                  expired_at = COALESCE(expired_at, now()),
                  updated_at = now()
            WHERE id = $1
              AND status = 'authorized'`,
          [id]
        );
      } else {
        await markCaptivePreauthFailedOutsideTransaction(pool, id);
      }
      err("charge_authorized_failed", {
        authorization_id: id,
        draw_id,
        user_id,
        captive_number: captiveNumber,
        amount_cents,
        status: reserved.status || "failed",
        error_code: reserved.code || "preauth_reservation_not_available",
      });
      return {
        ok: false,
        code: reserved.code || "preauth_reservation_not_available",
        status: reserved.status || "failed",
      };
    }

    await updateAutopayRunAttempt(client, {
      attempt_trace_id: attemptTraceId,
      reservation_id: reservationId,
      status: "reserved",
      amount_cents,
    });

    const idempotencyKey = `captive-preauth:${id}`;
    const amount_reais = Number((amount_cents / 100).toFixed(2));
    const description = `Autopay preauth draw ${draw_id} - numero ${String(captiveNumber).padStart(2, "0")}`;
    providerRequest = {
      endpoint: "/bills",
      customer_id: Number(auth.vindi_customer_id),
      payment_profile_id: Number(auth.vindi_payment_profile_id),
      code: idempotencyKey,
      amount_cents,
      amount_reais,
      quantity: 1,
      numbers: [captiveNumber],
      reservation_id: reservationId,
      autopay_id,
      user_id,
      draw_id,
      authorization_id: id,
    };

    bill = await createBill({
      customerId: auth.vindi_customer_id,
      amount_cents_total: amount_cents,
      quantity: 1,
      description,
      metadata: {
        user_id,
        draw_id,
        numbers: [captiveNumber],
        autopay_id,
        reservation_id: reservationId,
        authorization_id: id,
        amount_cents,
        amount_reais,
      },
      paymentProfileId: auth.vindi_payment_profile_id,
      idempotencyKey,
      traceId: attemptTraceId,
    });

    billId = bill.billId;
    chargeId = bill.chargeId;
    await updateAutopayRunAttempt(client, {
      attempt_trace_id: attemptTraceId,
      status: "billed",
      provider_status: bill.httpStatus ?? null,
      provider_bill_id: billId,
      provider_charge_id: chargeId,
      provider_request: providerRequest,
      provider_response: bill.raw || null,
    });

    const norm = (value) => String(value || "").toLowerCase();
    if (norm(bill.lastTransactionStatus) === "rejected") {
      throw new Error(`Vindi rejected: ${bill.gatewayMessage || "rejected"}`);
    }
    if (!chargeId) {
      const chargeResult = await chargeBill(billId, { traceId: attemptTraceId });
      chargeId = chargeResult.chargeId;
      await updateAutopayRunAttempt(client, {
        attempt_trace_id: attemptTraceId,
        status: "charged",
        provider_status: chargeResult.httpStatus ?? null,
        provider_charge_id: chargeId,
        provider_response: chargeResult.raw || null,
      });
    }

    await sleep(1000);
    const billInfo = await getBill(billId);
    if (!isVindiPaymentApproved({ bill, billInfo, chargeId })) {
      throw new Error("payment_not_approved");
    }

    const fin = await finalizePaidReservation(client, {
      draw_id,
      reservationId,
      user_id,
      numbers: [captiveNumber],
      amount_cents,
      provider: "vindi",
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
        authorization_id: id,
      },
    });

    await updateAutopayRunAttempt(client, {
      attempt_trace_id: attemptTraceId,
      status: "charged_ok",
      provider_bill_id: billId,
      provider_charge_id: chargeId,
      error_message: null,
    });

    await creditCouponOnApprovedPayment(fin.paymentId, {
      channel: "VINDI",
      source: "reconcile_sync",
      runTraceId,
      meta: { pricing_source: "autopay_draw_authorizations.amount_cents", autopay: true, captive_preauth: true },
      pgClient: client,
    });

    const updated = await client.query(
      `UPDATE public.autopay_draw_authorizations
          SET status = 'charged',
              charged_at = COALESCE(charged_at, now()),
              updated_at = now()
        WHERE id = $1
          AND status = 'authorized'
        RETURNING *`,
      [id]
    );

    log("charge_authorized_success", {
      authorization_id: id,
      draw_id,
      user_id,
      captive_number: captiveNumber,
      amount_cents,
      status: "charged",
    });
    return {
      ok: true,
      code: "charged_success",
      status: "charged",
      charged: true,
      payment_id: fin.paymentId,
      authorization: updated.rows?.[0] || auth,
    };
  } catch (e) {
    const errorCode = e?.code || e?.status || e?.message || "payment_failed";
    try {
      if (client) await client.query("ROLLBACK");
    } catch {}

    if (billId) {
      try {
        const billInfo = await getBill(billId);
        const charge0 = billInfo?.charges?.[0] || null;
        const effectiveChargeId = chargeId || charge0?.id || null;
        const paid =
          !!charge0?.paid_at ||
          String(charge0?.status || "").toLowerCase() === "paid" ||
          String(charge0?.last_transaction?.status || "").toLowerCase() === "success" ||
          String(billInfo?.status || "").toLowerCase() === "paid";
        if (paid && effectiveChargeId) {
          await refundCharge(effectiveChargeId, true);
        } else {
          await cancelBill(billId, { traceId: attemptTraceId });
        }
      } catch {}
    }

    const authExpiresAt = chargeContext.expires_at ? new Date(chargeContext.expires_at).getTime() : null;
    const keepReservationForRetry = Boolean(reservationId && authExpiresAt && authExpiresAt > Date.now());
    if (reservationId && !keepReservationForRetry) {
      try {
        await cancelReservation(client, { draw_id: chargeContext.draw_id, reservationId });
      } catch {}
    }

    try {
      await updateAutopayRunAttempt(client, {
        attempt_trace_id: attemptTraceId,
        status: "charged_fail",
        provider_request: providerRequest,
        provider_response: e?.response || null,
        error_message: String(errorCode).slice(0, 180),
      });
    } catch {}

    try {
      await markCaptivePreauthFailedOutsideTransaction(pool, id);
    } catch {}

    err("charge_authorized_failed", {
      authorization_id: id,
      draw_id: chargeContext.draw_id,
      user_id: chargeContext.user_id,
      captive_number: chargeContext.captive_number,
      amount_cents: chargeContext.amount_cents,
      status: "failed",
      error_code: String(errorCode).slice(0, 80),
      reservation_id: reservationId || null,
      reservation_kept_for_retry: keepReservationForRetry,
    });
    return { ok: false, code: "payment_failed", status: "failed", charged: false };
  } finally {
    try {
      await client.query("select pg_advisory_unlock(hashtext($1))", [`captive-preauth:${id}`]);
    } catch {}
    client.release();
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

    const hasNumberActive = await hasAutopayNumberActiveColumn(client);
    const hasAuthorizationMode = await hasAutopayProfileAuthorizationModeColumn(client);
    const baseAmountColumns = await getAutopayProfileBaseAmountColumns(client);
    const authorizedBaseAmountSql = buildAuthorizedBaseAmountSql(baseAmountColumns);
    const price_cents = await getDrawTicketPriceCents(client, draw_id);
    const defaultAmountCents = getDefaultAuthorizedBaseAmountCents();
    const requiresCaptivePreauthByAmount = shouldRequireCaptivePreauth({
      currentAmountCents: price_cents,
      authorizedBaseAmountCents: defaultAmountCents,
    });

    if (requiresCaptivePreauthByAmount) {
      warn("direct_autopay_skipped_requires_captive_preauth", {
        runTraceId,
        draw_id,
        price_cents,
        default_amount_cents: defaultAmountCents,
        reason: "draw_amount_above_default",
      });
      await client.query("BEGIN");
      await client.query(
        `update public.draws set autopay_ran_at = now() where id=$1`,
        [draw_id]
      );
      await client.query("COMMIT");
      return {
        ok: true,
        draw_id,
        skipped: true,
        reason: "requires_captive_preauth_amount_increased",
        results: [],
        price_cents,
        default_amount_cents: defaultAmountCents,
      };
    }

    // 4) Scan de candidatos (inclui active=false) + números agregados
    // Regras de elegibilidade (em JS):
    // - hasVindi = vindi_customer_id && vindi_payment_profile_id
    // - eligible = hasVindi && active=true && authorization_mode=false && preferred.length>0
    // - se autopay_numbers.active existir, números pausados ficam fora de preferred
    const { rows: scanned } = await client.query(
      `select
          ap.id as autopay_id,
          ap.user_id as user_id,
          ap.active as active,
          ${hasAuthorizationMode ? "coalesce(ap.authorization_mode, false)" : "false"} as authorization_mode,
          ${authorizedBaseAmountSql} as authorized_base_amount_cents,
          ap.vindi_customer_id,
          ap.vindi_payment_profile_id,
          coalesce(array_agg(an.n order by an.n) filter (where an.n is not null ${hasNumberActive ? "and an.active = true" : ""}), '{}') as numbers
        from public.autopay_profiles ap
        left join public.autopay_numbers an on an.autopay_id = ap.id
       group by ap.id, ap.user_id, ap.active, ${hasAuthorizationMode ? "ap.authorization_mode," : ""} ap.vindi_customer_id, ap.vindi_payment_profile_id`
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

      if (p.authorization_mode === true) {
        const authorizedBaseAmountCents =
          toPositiveInt(p.authorized_base_amount_cents) || getDefaultAuthorizedBaseAmountCents();
        if (shouldRequireCaptivePreauth({
          currentAmountCents: price_cents,
          authorizedBaseAmountCents,
        })) {
          inactive++;
          console.warn("[autopay] skipped_authorization_mode", {
            runTraceId,
            autopay_id: p.autopay_id,
            user_id: p.user_id,
            active: true,
            preferred,
            reason: "requires_preauth_amount_increased",
            current_amount_cents: price_cents,
            authorized_base_amount_cents: authorizedBaseAmountCents,
          });
          continue;
        }
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

      candidates.push(p);
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

        // Crédito de saldo (idempotente) após payment ficar approved
        // eslint-disable-next-line no-await-in-loop
        const creditRes = await creditCouponOnApprovedPayment(fin.paymentId, {
          channel: "VINDI",
          source: "reconcile_sync",
          runTraceId,
          meta: { pricing_source: "public.app_config.ticket_price_cents", autopay: true },
          pgClient: client,
        });
        if (creditRes?.ok === false || ["error", "not_supported", "invalid_amount"].includes(String(creditRes?.action || ""))) {
          warn("coupon credit failed", {
            paymentId: fin.paymentId,
            action: creditRes?.action || null,
            reason: creditRes?.reason || null,
            user_id: creditRes?.user_id ?? null,
            status: creditRes?.status ?? null,
            errCode: creditRes?.errCode ?? null,
            errMsg: creditRes?.errMsg ?? null,
          });
        }

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
