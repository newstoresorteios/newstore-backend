// src/services/couponLedger.js
//
// FASE 5 — decisao de negocio: o saldo de NSCreditos da Loja de Premios
// passa a SER o cupom individual do usuario. Nao existem mais duas
// carteiras concorrentes.
//
//   users.coupon_value_cents   -> saldo atual (centavos de R$)
//   coupon_balance_history     -> ledger canonico (o MESMO ledger do cupom
//                                  legado -- nao um segundo historico)
//
// nscredit_wallets/nscredit_transactions ficam como estrutura legada,
// intocada, sem uso na Loja a partir daqui. Ver src/services/nscreditWallet.js.
//
// Escala: NAO ha conversao. coupon_value_cents continua em centavos de R$
// no banco; a Loja EXIBE esse mesmo numero dividido por 100 como
// "NSCreditos" (ver formatCouponAsNsCredits). O valor armazenado nunca muda
// de forma.

import { query as defaultQuery, getPool } from "../db.js";

export class CouponLedgerError extends Error {
  constructor(code, { status = 400, details = null } = {}) {
    super(code);
    this.name = "CouponLedgerError";
    this.code = code;
    this.status = status;
    this.details = details;
  }
}

class IdempotencyRace extends Error {
  constructor(key) {
    super("idempotency_race");
    this.name = "IdempotencyRace";
    this.key = key;
  }
}

function resolveDeps(deps = {}) {
  return {
    query: deps.query || defaultQuery,
    withTransaction: deps.withTransaction || defaultWithTransaction,
  };
}

async function defaultWithTransaction(fn) {
  const pool = await getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const out = await fn(client);
    await client.query("COMMIT");
    return out;
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    client.release();
  }
}

export function parseUserId(value) {
  const n = Number(value);
  if (!Number.isSafeInteger(n) || n <= 0) throw new CouponLedgerError("invalid_user_id", { status: 400 });
  return n;
}

/** Centavos: inteiro positivo. A Loja exibe /100, mas o valor trafegado aqui e sempre em centavos. */
export function parseCentsAmount(value) {
  const n = Number(value);
  if (!Number.isSafeInteger(n) || n <= 0) throw new CouponLedgerError("invalid_amount", { status: 400 });
  return n;
}

/**
 * 38100 (cents) -> 381 NSCreditos (BRL inteiros, sem casas decimais na UI,
 * igual ao criterio ja usado pelo cupom legado em telas administrativas).
 * Mantido separado do valor armazenado: NUNCA persistir o resultado desta
 * funcao, ela e so para exibicao.
 */
export function formatCouponAsNsCredits(cents) {
  const n = Number(cents);
  if (!Number.isFinite(n)) return 0;
  return Math.trunc(n) / 100;
}

/**
 * Saldo factual do usuario, decidido pela MESMA fonte que o cupom legado:
 * users.coupon_value_cents + a view canonica de expiracao
 * (public.user_coupon_balance_expiry, migration 027). Nao reimplementa a
 * regra de expiracao em JS -- consulta a view para nunca divergir dela.
 */
export async function getCouponBalance(userId, deps = {}) {
  const d = resolveDeps(deps);
  const id = parseUserId(userId);

  const u = await d.query(
    `select coalesce(coupon_value_cents,0)::int as balance_cents,
            coupon_code, tray_coupon_id, coupon_expires_at
       from public.users
      where id = $1`,
    [id]
  );
  if (!u.rows.length) throw new CouponLedgerError("user_not_found", { status: 404 });
  const row = u.rows[0];

  let isExpired = false;
  let expiresAt = row.coupon_expires_at || null;
  if (row.balance_cents > 0) {
    const v = await d.query(
      `select expires_at, is_expired from public.user_coupon_balance_expiry where user_id = $1`,
      [id]
    );
    if (v.rows.length) {
      isExpired = !!v.rows[0].is_expired;
      expiresAt = v.rows[0].expires_at || expiresAt;
    }
  }

  return {
    balance_cents: row.balance_cents,
    coupon_code: row.coupon_code || null,
    tray_coupon_id: row.tray_coupon_id || null,
    expires_at: expiresAt,
    is_expired: isExpired,
  };
}

async function findByIdempotencyKey(client, key) {
  if (!key) return null;
  const { rows } = await client.query(
    `select * from public.coupon_balance_history where idempotency_key = $1 limit 1`,
    [key]
  );
  return rows[0] || null;
}

function mapEntry(row) {
  return {
    id: row.id,
    event_type: row.event_type,
    delta_cents: Number(row.delta_cents),
    balance_before_cents: Number(row.balance_before_cents),
    balance_after_cents: Number(row.balance_after_cents),
    channel: row.channel || null,
    redemption_id: row.redemption_id || null,
    created_at: row.created_at,
  };
}

/**
 * Lancamento atomico no ledger canonico do cupom.
 *
 * operation: 'credit' | 'debit'
 * eventType: texto livre do dominio (ex.: REDEMPTION_DEBIT,
 *   REDEMPTION_COMPENSATION, ADMIN_BALANCE_ADJUSTMENT).
 *
 * Fluxo dentro de UMA transacao:
 *   1. se ja existe lancamento com a mesma idempotency_key -> devolve o original
 *   2. SELECT users ... FOR UPDATE (serializa concorrencia no mesmo usuario)
 *   3. debito: recusa se o cupom estiver expirado ou se faltar saldo
 *   4. UPDATE users.coupon_value_cents
 *   5. INSERT coupon_balance_history
 */
export async function applyCouponLedgerEntry(
  { userId, operation, amountCents, eventType, redemptionId = null, idempotencyKey = null, meta = null, channel = null } = {},
  deps = {}
) {
  const d = resolveDeps(deps);
  const id = parseUserId(userId);
  const value = parseCentsAmount(amountCents);

  if (operation !== "credit" && operation !== "debit") {
    throw new CouponLedgerError("invalid_operation", { status: 400 });
  }
  const type = String(eventType || "").trim();
  if (!type) throw new CouponLedgerError("event_type_required", { status: 400 });

  const key = idempotencyKey == null ? null : String(idempotencyKey).trim() || null;

  try {
    return await run();
  } catch (e) {
    if (e instanceof IdempotencyRace) {
      const { rows } = await d.query(
        `select * from public.coupon_balance_history where idempotency_key = $1 limit 1`,
        [e.key]
      );
      if (rows[0]) return { replayed: true, balance_cents: Number(rows[0].balance_after_cents), entry: mapEntry(rows[0]) };
      throw new CouponLedgerError("idempotency_conflict", { status: 409 });
    }
    throw e;
  }

  async function run() {
    return d.withTransaction(async (client) => {
      const replay = await findByIdempotencyKey(client, key);
      if (replay) return { replayed: true, balance_cents: Number(replay.balance_after_cents), entry: mapEntry(replay) };

      const locked = await client.query(
        `select id, coalesce(coupon_value_cents,0)::int as balance_cents, coupon_expires_at
           from public.users where id = $1 for update`,
        [id]
      );
      if (!locked.rows.length) throw new CouponLedgerError("user_not_found", { status: 404 });
      const balanceBefore = locked.rows[0].balance_cents;

      if (operation === "debit") {
        const expiresAt = locked.rows[0].coupon_expires_at;
        // Expiracao explicita em users.coupon_expires_at: checagem direta e
        // suficiente aqui (mesma regra de prioridade da view canonica quando
        // o campo esta preenchido). Sem expiracao explicita, o saldo e valido.
        if (expiresAt && new Date(expiresAt).getTime() < Date.now()) {
          throw new CouponLedgerError("coupon_expired", { status: 409, details: { expires_at: expiresAt } });
        }
        if (value > balanceBefore) {
          throw new CouponLedgerError("insufficient_balance", { status: 409, details: { balance_cents: balanceBefore, requested_cents: value } });
        }
      }

      const balanceAfter = operation === "credit" ? balanceBefore + value : balanceBefore - value;
      if (!Number.isSafeInteger(balanceAfter) || balanceAfter < 0) {
        throw new CouponLedgerError("balance_out_of_range", { status: 409 });
      }

      await client.query(
        `update public.users set coupon_value_cents = $2, coupon_updated_at = now() where id = $1`,
        [id, balanceAfter]
      );

      const deltaCents = operation === "credit" ? value : -value;
      const metaJson = JSON.stringify(meta && typeof meta === "object" ? meta : {});

      let inserted;
      try {
        inserted = await client.query(
          `insert into public.coupon_balance_history
             (user_id, delta_cents, balance_before_cents, balance_after_cents,
              event_type, channel, redemption_id, idempotency_key, meta, event_occurred_at)
           values ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,now())
           returning *`,
          [id, deltaCents, balanceBefore, balanceAfter, type, channel, redemptionId, key, metaJson]
        );
      } catch (e) {
        if (e?.code === "23505" && key) throw new IdempotencyRace(key);
        throw e;
      }

      return { replayed: false, balance_cents: balanceAfter, entry: mapEntry(inserted.rows[0]) };
    });
  }
}
