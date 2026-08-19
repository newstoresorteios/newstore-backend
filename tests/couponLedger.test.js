// tests/couponLedger.test.js
//
// Regras e integridade da transacao do ledger canonico do cupom, com banco
// falso em memoria. Concorrencia/locking real ficam em
// couponLedger.integration.test.js (Postgres real).
import test from "node:test";
import assert from "node:assert/strict";

import {
  getCouponBalance,
  applyCouponLedgerEntry,
  parseCentsAmount,
  parseUserId,
  formatCouponAsNsCredits,
  CouponLedgerError,
} from "../src/services/couponLedger.js";

function makeDb({ users = [{ id: 1, coupon_value_cents: 0, coupon_code: "NSU-0001-AB", tray_coupon_id: "77", coupon_expires_at: null }], history = [], expiryView = {} } = {}) {
  const state = {
    users: users.map((u) => ({ ...u })),
    history: [...history],
    nextId: 1 + history.length,
  };

  function run(sql, params = []) {
    const s = String(sql).toLowerCase();

    if (/from public\.users/.test(s) && /where\s+id\s*=\s*\$1/.test(s) && !/for update/.test(s)) {
      const u = state.users.find((x) => String(x.id) === String(params[0]));
      const row = u ? { ...u, balance_cents: u.coupon_value_cents ?? 0 } : null;
      return { rows: row ? [row] : [], rowCount: row ? 1 : 0 };
    }

    if (/from public\.users/.test(s) && /for update/.test(s)) {
      const u = state.users.find((x) => String(x.id) === String(params[0]));
      const row = u ? { id: u.id, balance_cents: u.coupon_value_cents ?? 0, coupon_expires_at: u.coupon_expires_at ?? null } : null;
      return { rows: row ? [row] : [], rowCount: row ? 1 : 0 };
    }

    if (/from public\.user_coupon_balance_expiry/.test(s)) {
      const v = expiryView[String(params[0])];
      return v ? { rows: [v], rowCount: 1 } : { rows: [], rowCount: 0 };
    }

    if (/from public\.coupon_balance_history where idempotency_key/.test(s)) {
      const hit = state.history.find((h) => h.idempotency_key && h.idempotency_key === params[0]);
      return { rows: hit ? [hit] : [], rowCount: hit ? 1 : 0 };
    }

    if (/^update public\.users set coupon_value_cents/.test(s)) {
      const [id, next] = params;
      const u = state.users.find((x) => String(x.id) === String(id));
      u.coupon_value_cents = next;
      return { rows: [], rowCount: 1 };
    }

    if (/^insert into public\.coupon_balance_history/.test(s)) {
      const [user_id, delta_cents, balance_before_cents, balance_after_cents, event_type, channel, redemption_id, idempotency_key, meta] = params;
      if (idempotency_key && state.history.some((h) => h.idempotency_key === idempotency_key)) {
        throw Object.assign(new Error("duplicate key"), { code: "23505" });
      }
      const row = {
        id: state.nextId++,
        user_id,
        delta_cents,
        balance_before_cents,
        balance_after_cents,
        event_type,
        channel,
        redemption_id,
        idempotency_key,
        meta,
        created_at: new Date().toISOString(),
      };
      state.history.push(row);
      return { rows: [row], rowCount: 1 };
    }

    throw new Error(`SQL nao mapeado no fake db: ${sql}`);
  }

  const deps = {
    query: async (sql, params) => run(sql, params),
    withTransaction: async (fn) => fn({ query: async (sql, params) => run(sql, params) }),
  };

  return { state, deps };
}

/* ─────────────────────────── Validacao ─────────────────────────── */

test("parseCentsAmount recusa zero, negativo e nao-inteiro", () => {
  assert.throws(() => parseCentsAmount(0), CouponLedgerError);
  assert.throws(() => parseCentsAmount(-10), CouponLedgerError);
  assert.throws(() => parseCentsAmount(1.5), CouponLedgerError);
  assert.equal(parseCentsAmount(150000), 150000);
});

test("parseUserId recusa invalido", () => {
  assert.throws(() => parseUserId(0), CouponLedgerError);
  assert.throws(() => parseUserId("abc"), CouponLedgerError);
  assert.equal(parseUserId("42"), 42);
});

test("formatCouponAsNsCredits divide por 100 sem alterar o valor armazenado", () => {
  assert.equal(formatCouponAsNsCredits(38100), 381);
  assert.equal(formatCouponAsNsCredits(200000), 2000);
  assert.equal(formatCouponAsNsCredits(0), 0);
  assert.equal(formatCouponAsNsCredits(null), 0);
});

/* ─────────────────────────── Saldo ─────────────────────────── */

test("saldo zero factual", async () => {
  const { deps } = makeDb();
  const b = await getCouponBalance(1, deps);
  assert.equal(b.balance_cents, 0);
  assert.equal(b.is_expired, false);
});

test("saldo positivo consulta a view canonica de expiracao", async () => {
  const { deps } = makeDb({
    users: [{ id: 1, coupon_value_cents: 38100, coupon_code: "NSU-0001-AB", tray_coupon_id: "77", coupon_expires_at: null }],
    expiryView: { 1: { expires_at: "2027-01-18T00:00:00.000Z", is_expired: false } },
  });
  const b = await getCouponBalance(1, deps);
  assert.equal(b.balance_cents, 38100);
  assert.equal(b.is_expired, false);
  assert.equal(b.expires_at, "2027-01-18T00:00:00.000Z");
});

test("usuario inexistente falha alto, nunca devolve saldo 0 fantasma", async () => {
  const { deps } = makeDb({ users: [] });
  await assert.rejects(() => getCouponBalance(999, deps), (e) => e.code === "user_not_found");
});

/* ─────────────────────────── Debito/credito ─────────────────────────── */

test("debito reduz saldo e grava delta negativo no ledger", async () => {
  const { deps, state } = makeDb({ users: [{ id: 1, coupon_value_cents: 200000, coupon_expires_at: null }] });

  const out = await applyCouponLedgerEntry({ userId: 1, operation: "debit", amountCents: 150000, eventType: "REDEMPTION_DEBIT" }, deps);

  assert.equal(out.balance_cents, 50000);
  assert.equal(state.history[0].delta_cents, -150000);
  assert.equal(state.history[0].balance_before_cents, 200000);
  assert.equal(state.history[0].balance_after_cents, 50000);
  assert.equal(state.history[0].event_type, "REDEMPTION_DEBIT");
});

test("credito aumenta saldo (compensacao)", async () => {
  const { deps } = makeDb({ users: [{ id: 1, coupon_value_cents: 50000, coupon_expires_at: null }] });
  const out = await applyCouponLedgerEntry({ userId: 1, operation: "credit", amountCents: 150000, eventType: "REDEMPTION_COMPENSATION" }, deps);
  assert.equal(out.balance_cents, 200000);
});

test("debito maior que o saldo e recusado", async () => {
  const { deps, state } = makeDb({ users: [{ id: 1, coupon_value_cents: 1000, coupon_expires_at: null }] });
  await assert.rejects(
    () => applyCouponLedgerEntry({ userId: 1, operation: "debit", amountCents: 5000, eventType: "REDEMPTION_DEBIT" }, deps),
    (e) => e instanceof CouponLedgerError && e.code === "insufficient_balance"
  );
  assert.equal(state.history.length, 0);
  assert.equal(state.users[0].coupon_value_cents, 1000);
});

test("cupom expirado bloqueia debito mesmo com saldo suficiente", async () => {
  const past = new Date(Date.now() - 3600_000).toISOString();
  const { deps, state } = makeDb({ users: [{ id: 1, coupon_value_cents: 50000, coupon_expires_at: past }] });

  await assert.rejects(
    () => applyCouponLedgerEntry({ userId: 1, operation: "debit", amountCents: 100, eventType: "REDEMPTION_DEBIT" }, deps),
    (e) => e instanceof CouponLedgerError && e.code === "coupon_expired"
  );
  assert.equal(state.history.length, 0);
});

test("cupom expirado nao bloqueia credito (compensacao sempre pode devolver)", async () => {
  const past = new Date(Date.now() - 3600_000).toISOString();
  const { deps } = makeDb({ users: [{ id: 1, coupon_value_cents: 0, coupon_expires_at: past }] });
  const out = await applyCouponLedgerEntry({ userId: 1, operation: "credit", amountCents: 1000, eventType: "REDEMPTION_COMPENSATION" }, deps);
  assert.equal(out.balance_cents, 1000);
});

test("usuario inexistente nao gera lancamento", async () => {
  const { deps, state } = makeDb({ users: [] });
  await assert.rejects(
    () => applyCouponLedgerEntry({ userId: 999, operation: "credit", amountCents: 100, eventType: "REDEMPTION_COMPENSATION" }, deps),
    (e) => e.code === "user_not_found"
  );
  assert.equal(state.history.length, 0);
});

/* ─────────────────────────── Idempotencia ─────────────────────────── */

test("idempotency_key repetida devolve o lancamento original sem reaplicar", async () => {
  const { deps, state } = makeDb({ users: [{ id: 1, coupon_value_cents: 200000, coupon_expires_at: null }] });
  const key = "redeem-abc";

  const a = await applyCouponLedgerEntry({ userId: 1, operation: "debit", amountCents: 50000, eventType: "REDEMPTION_DEBIT", idempotencyKey: key }, deps);
  const b = await applyCouponLedgerEntry({ userId: 1, operation: "debit", amountCents: 50000, eventType: "REDEMPTION_DEBIT", idempotencyKey: key }, deps);

  assert.equal(a.replayed, false);
  assert.equal(b.replayed, true);
  assert.equal(a.entry.id, b.entry.id);
  assert.equal(state.history.length, 1);
  assert.equal(state.users[0].coupon_value_cents, 150000);
});

test("redemption_id fica gravado no lancamento", async () => {
  const { deps, state } = makeDb({ users: [{ id: 1, coupon_value_cents: 50000, coupon_expires_at: null }] });
  await applyCouponLedgerEntry(
    { userId: 1, operation: "debit", amountCents: 1000, eventType: "REDEMPTION_DEBIT", redemptionId: "aaaa-bbbb" },
    deps
  );
  assert.equal(state.history[0].redemption_id, "aaaa-bbbb");
});

test("event_type vazio e recusado", async () => {
  const { deps } = makeDb();
  await assert.rejects(
    () => applyCouponLedgerEntry({ userId: 1, operation: "debit", amountCents: 100, eventType: "" }, deps),
    (e) => e.code === "event_type_required"
  );
});

test("operation invalida e recusada", async () => {
  const { deps } = makeDb();
  await assert.rejects(
    () => applyCouponLedgerEntry({ userId: 1, operation: "set", amountCents: 100, eventType: "X" }, deps),
    (e) => e.code === "invalid_operation"
  );
});
