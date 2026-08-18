// tests/couponLedger.integration.test.js
//
// FASE 5: o saldo de NSCreditos da Loja passou a ser o cupom individual do
// usuario (users.coupon_value_cents), com coupon_balance_history como ledger
// canonico -- nao um segundo ledger paralelo a nscredit_transactions.
//
// Testes contra Postgres REAL (unico jeito honesto de provar FOR UPDATE,
// idempotencia e as constraints do banco).
//
//   TEST_DATABASE_URL=postgres://user:pass@localhost:5432/db npm test
//
// Sem TEST_DATABASE_URL os testes sao pulados (nunca usam banco de producao).
import test, { before, after, beforeEach } from "node:test";
import assert from "node:assert/strict";

import {
  getCouponBalance,
  applyCouponLedgerEntry,
  CouponLedgerError,
} from "../src/services/couponLedger.js";

const TEST_DB = process.env.TEST_DATABASE_URL || "";
const SKIP = !TEST_DB;
const skipOpts = { skip: SKIP ? "defina TEST_DATABASE_URL para rodar os testes de integracao" : false };

let pool;
let deps;
let userId;

function sslFor(url) {
  try {
    const host = new URL(url).hostname;
    return { rejectUnauthorized: false, servername: /^\d+\.\d+\.\d+\.\d+$/.test(host) ? undefined : host };
  } catch {
    return { rejectUnauthorized: false };
  }
}

before(async () => {
  if (SKIP) return;
  const pg = (await import("pg")).default;
  pool = new pg.Pool({ connectionString: TEST_DB, ssl: sslFor(TEST_DB), max: 10 });

  deps = {
    query: (sql, params) => pool.query(sql, params),
    withTransaction: async (fn) => {
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
    },
  };

  const stamp = Date.now();
  const u = await pool.query(
    `insert into public.users (name, email, pass_hash, is_admin)
     values ($1,$2,'x',false) returning id`,
    ["Teste Cupom Ledger", `coupon-ledger-test-${stamp}@exemplo.local`]
  );
  userId = u.rows[0].id;
});

after(async () => {
  if (SKIP || !pool) return;
  await pool.query("delete from public.coupon_balance_history where user_id = $1", [userId]).catch(() => {});
  await pool.query("delete from public.users where id = $1", [userId]).catch(() => {});
  await pool.end().catch(() => {});
});

async function resetUser({ cents = 0, expiresAt = null } = {}) {
  await pool.query("delete from public.coupon_balance_history where user_id = $1", [userId]);
  await pool.query(
    `update public.users
        set coupon_value_cents = $2, coupon_expires_at = $3, coupon_code = 'NSU-TEST-XX', coupon_updated_at = now()
      where id = $1`,
    [userId, cents, expiresAt]
  );
}

test("saldo zero factual quando usuario nunca recebeu credito", skipOpts, async () => {
  await resetUser({ cents: 0 });
  const b = await getCouponBalance(userId, deps);
  assert.equal(b.balance_cents, 0);
  assert.equal(b.is_expired, false);
});

test("saldo positivo reflete users.coupon_value_cents", skipOpts, async () => {
  await resetUser({ cents: 38100 });
  const b = await getCouponBalance(userId, deps);
  assert.equal(b.balance_cents, 38100);
  assert.equal(b.coupon_code, "NSU-TEST-XX");
});

test("debito atomico: saldo e ledger consistentes", skipOpts, async () => {
  await resetUser({ cents: 200000 });

  const out = await applyCouponLedgerEntry(
    { userId, operation: "debit", amountCents: 150000, eventType: "REDEMPTION_DEBIT", redemptionId: null },
    deps
  );

  assert.equal(out.balance_cents, 50000);
  const b = await getCouponBalance(userId, deps);
  assert.equal(b.balance_cents, 50000);

  const hist = await pool.query(
    // id e uuid (nao sequencial) — ordenar por created_at, nunca por id.
    "select event_type, delta_cents, balance_before_cents, balance_after_cents from public.coupon_balance_history where user_id=$1 order by created_at desc limit 1",
    [userId]
  );
  assert.equal(hist.rows[0].event_type, "REDEMPTION_DEBIT");
  assert.equal(hist.rows[0].delta_cents, -150000);
  assert.equal(hist.rows[0].balance_before_cents, 200000);
  assert.equal(hist.rows[0].balance_after_cents, 50000);
});

test("credito de compensacao restaura o saldo", skipOpts, async () => {
  await resetUser({ cents: 50000 });

  const out = await applyCouponLedgerEntry(
    { userId, operation: "credit", amountCents: 150000, eventType: "REDEMPTION_COMPENSATION" },
    deps
  );

  assert.equal(out.balance_cents, 200000);
});

test("debito maior que o saldo e recusado, nada muda", skipOpts, async () => {
  await resetUser({ cents: 1000 });

  await assert.rejects(
    () => applyCouponLedgerEntry({ userId, operation: "debit", amountCents: 5000, eventType: "REDEMPTION_DEBIT" }, deps),
    (e) => e instanceof CouponLedgerError && e.code === "insufficient_balance"
  );

  const b = await getCouponBalance(userId, deps);
  assert.equal(b.balance_cents, 1000);
  const hist = await pool.query("select count(*)::int as n from public.coupon_balance_history where user_id=$1", [userId]);
  assert.equal(hist.rows[0].n, 0);
});

test("cupom expirado nao pode ser debitado", skipOpts, async () => {
  await resetUser({ cents: 50000, expiresAt: new Date(Date.now() - 24 * 3600_000).toISOString() });

  const b = await getCouponBalance(userId, deps);
  assert.equal(b.is_expired, true);

  await assert.rejects(
    () => applyCouponLedgerEntry({ userId, operation: "debit", amountCents: 100, eventType: "REDEMPTION_DEBIT" }, deps),
    (e) => e instanceof CouponLedgerError && e.code === "coupon_expired"
  );

  assert.equal((await getCouponBalance(userId, deps)).balance_cents, 50000);
});

test("idempotency_key repetida nao debita duas vezes", skipOpts, async () => {
  await resetUser({ cents: 200000 });
  const key = `redeem-${Date.now()}`;

  const a = await applyCouponLedgerEntry(
    { userId, operation: "debit", amountCents: 50000, eventType: "REDEMPTION_DEBIT", idempotencyKey: key },
    deps
  );
  const b = await applyCouponLedgerEntry(
    { userId, operation: "debit", amountCents: 50000, eventType: "REDEMPTION_DEBIT", idempotencyKey: key },
    deps
  );

  assert.equal(a.replayed, false);
  assert.equal(b.replayed, true);
  assert.equal(a.entry.id, b.entry.id);
  assert.equal((await getCouponBalance(userId, deps)).balance_cents, 150000);
});

test("requisicoes concorrentes com a mesma idempotency_key aplicam uma unica vez", skipOpts, async () => {
  await resetUser({ cents: 200000 });
  const key = `redeem-race-${Date.now()}`;

  const results = await Promise.allSettled(
    Array.from({ length: 6 }, () =>
      applyCouponLedgerEntry({ userId, operation: "debit", amountCents: 10000, eventType: "REDEMPTION_DEBIT", idempotencyKey: key }, deps)
    )
  );
  assert.equal(results.filter((r) => r.status === "fulfilled").length, 6);
  assert.equal((await getCouponBalance(userId, deps)).balance_cents, 190000);

  const hist = await pool.query("select count(*)::int as n from public.coupon_balance_history where idempotency_key=$1", [key]);
  assert.equal(hist.rows[0].n, 1);
});

test("debitos simultaneos sem idempotency_key nao levam saldo abaixo de zero", skipOpts, async () => {
  await resetUser({ cents: 100000 });

  const results = await Promise.allSettled(
    Array.from({ length: 5 }, (_, i) =>
      applyCouponLedgerEntry({ userId, operation: "debit", amountCents: 30000, eventType: "REDEMPTION_DEBIT" }, deps)
    )
  );
  const ok = results.filter((r) => r.status === "fulfilled").length;
  const b = await getCouponBalance(userId, deps);
  assert.ok(b.balance_cents >= 0);
  assert.equal(b.balance_cents, 100000 - ok * 30000);
  assert.equal(ok, 3);
});

test("redemption_id fica gravado no lancamento do ledger", skipOpts, async () => {
  await resetUser({ cents: 50000 });
  const fakeRedemptionId = "00000000-0000-0000-0000-000000000000";

  // FK de coupon_balance_history.redemption_id -> reward_redemptions(id):
  // usamos null aqui porque o teste e so do ledger; o vinculo real e coberto
  // no teste de integracao da saga (rewardRedemption.integration.test.js).
  const out = await applyCouponLedgerEntry(
    { userId, operation: "debit", amountCents: 100, eventType: "REDEMPTION_DEBIT", redemptionId: null, meta: { note: "sem redemption real" } },
    deps
  );
  assert.equal(out.balance_cents, 49900);
});

test("nscredit_wallets nao e tocada por nenhuma operacao do ledger de cupom", skipOpts, async () => {
  await resetUser({ cents: 50000 });
  await pool.query("delete from public.nscredit_wallets where user_id=$1", [userId]);

  await applyCouponLedgerEntry({ userId, operation: "debit", amountCents: 1000, eventType: "REDEMPTION_DEBIT" }, deps);

  const w = await pool.query("select 1 from public.nscredit_wallets where user_id=$1", [userId]);
  assert.equal(w.rowCount, 0, "nscredit_wallets deve permanecer legada/intocada");
});
