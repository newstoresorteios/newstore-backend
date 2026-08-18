// tests/couponAdmin.test.js
// Administracao do saldo de NSCreditos (FASE 5: cupom individual do usuario).
import test from "node:test";
import assert from "node:assert/strict";

import {
  searchUsersForCouponAdmin,
  getUserCouponDetail,
  applyAdminCouponAdjustment,
} from "../src/services/couponAdmin.js";
import { CouponLedgerError } from "../src/services/couponLedger.js";

function makeDb({ users = [{ id: 1, name: "Joao", email: "joao@x.com", coupon_value_cents: 38100, coupon_code: "NSU-0001-AB", coupon_expires_at: null }], history = [] } = {}) {
  const state = { users: users.map((u) => ({ ...u })), history: [...history], nextId: 1 + history.length };

  function run(sql, params = []) {
    const s = String(sql).toLowerCase();

    if (/select u\.id/.test(s) && /from public\.users u/.test(s)) {
      const term = params[0] ? String(params[0]).replace(/%/g, "").toLowerCase() : null;
      const filtered = term
        ? state.users.filter((u) => u.name.toLowerCase().includes(term) || u.email.toLowerCase().includes(term))
        : state.users;
      return { rows: filtered.map((u) => ({ ...u, balance_cents: u.coupon_value_cents, total: filtered.length })) };
    }

    if (/select id, name, email from public\.users/.test(s)) {
      const u = state.users.find((x) => String(x.id) === String(params[0]));
      return { rows: u ? [u] : [] };
    }

    if (/coupon_value_cents/.test(s) && /from public\.users/.test(s) && !/for update/.test(s) && /coupon_code/.test(s)) {
      const u = state.users.find((x) => String(x.id) === String(params[0]));
      return { rows: u ? [{ balance_cents: u.coupon_value_cents, coupon_code: u.coupon_code, tray_coupon_id: null, coupon_expires_at: u.coupon_expires_at }] : [] };
    }

    if (/from public\.users/.test(s) && /for update/.test(s)) {
      const u = state.users.find((x) => String(x.id) === String(params[0]));
      return { rows: u ? [{ id: u.id, balance_cents: u.coupon_value_cents, coupon_expires_at: u.coupon_expires_at }] : [] };
    }

    if (/from public\.user_coupon_balance_expiry/.test(s)) {
      const u = state.users.find((x) => String(x.id) === String(params[0]));
      return { rows: u ? [{ expires_at: u.coupon_expires_at, is_expired: false }] : [] };
    }

    if (/from public\.coupon_balance_history where idempotency_key/.test(s)) {
      const hit = state.history.find((h) => h.idempotency_key && h.idempotency_key === params[0]);
      return { rows: hit ? [hit] : [] };
    }

    if (/select id, event_type, delta_cents.*from public\.coupon_balance_history\s+where user_id/is.test(sql)) {
      const rows = state.history.filter((h) => String(h.user_id) === String(params[0])).slice(0, params[1]);
      return { rows };
    }

    if (/select count\(\*\) as total from public\.coupon_balance_history/.test(s)) {
      const n = state.history.filter((h) => String(h.user_id) === String(params[0])).length;
      return { rows: [{ total: n }] };
    }

    if (/^update public\.users set coupon_value_cents/.test(s)) {
      const [id, next] = params;
      state.users.find((u) => String(u.id) === String(id)).coupon_value_cents = next;
      return { rows: [], rowCount: 1 };
    }

    if (/^insert into public\.coupon_balance_history/.test(s)) {
      const [user_id, delta_cents, balance_before_cents, balance_after_cents, event_type, channel, redemption_id, idempotency_key, meta] = params;
      if (idempotency_key && state.history.some((h) => h.idempotency_key === idempotency_key)) {
        throw Object.assign(new Error("dup"), { code: "23505" });
      }
      const row = { id: state.nextId++, user_id, delta_cents, balance_before_cents, balance_after_cents, event_type, channel, redemption_id, idempotency_key, meta, created_at: new Date().toISOString() };
      state.history.push(row);
      return { rows: [row] };
    }

    throw new Error(`SQL nao mapeado: ${sql}`);
  }

  const deps = {
    query: async (sql, params) => run(sql, params),
    withTransaction: async (fn) => fn({ query: async (sql, params) => run(sql, params) }),
  };
  return { state, deps };
}

test("busca devolve saldo em centavos e em NSCreditos por usuario", async () => {
  const { deps } = makeDb();
  const out = await searchUsersForCouponAdmin({ q: "joao" }, deps);
  assert.equal(out.items.length, 1);
  assert.equal(out.items[0].balance_cents, 38100);
  assert.equal(out.items[0].balance, 381);
  assert.equal(out.items[0].coupon_code, "NSU-0001-AB");
});

test("historico do detalhe usa o contrato ja publicado do admin (operation/amount/balance_before/balance_after/source_type/reason)", async () => {
  const { deps } = makeDb({
    users: [{ id: 1, name: "Joao", email: "joao@x.com", coupon_value_cents: 48100, coupon_code: "NSU-0001-AB", coupon_expires_at: null }],
    history: [
      {
        id: 1,
        user_id: 1,
        delta_cents: 10000,
        balance_before_cents: 38100,
        balance_after_cents: 48100,
        event_type: "ADMIN_BALANCE_ADJUSTMENT",
        channel: "ADMIN",
        redemption_id: null,
        meta: JSON.stringify({ reason: "Bonus de aniversario", admin_user_id: 9 }),
        created_at: new Date().toISOString(),
      },
    ],
  });

  const out = await getUserCouponDetail(1, { page: 1, limit: 10 }, deps);
  const t = out.transactions[0];
  assert.equal(t.operation, "credit");
  assert.equal(t.amount, 100);
  assert.equal(t.balance_before, 381);
  assert.equal(t.balance_after, 481);
  assert.equal(t.source_type, "ADMIN");
  assert.equal(t.reason, "Bonus de aniversario");
});

test("detalhe devolve saldo em NSCreditos + historico", async () => {
  const { deps } = makeDb();
  const out = await getUserCouponDetail(1, { page: 1, limit: 10 }, deps);
  assert.equal(out.wallet.balance, 381);
  assert.equal(out.wallet.balance_cents, 38100);
  assert.equal(out.user.id, 1);
});

test("detalhe de usuario inexistente falha 404", async () => {
  const { deps } = makeDb({ users: [] });
  await assert.rejects(() => getUserCouponDetail(999, {}, deps), (e) => e.status === 404);
});

test("ajuste credito exige admin autenticado", async () => {
  const { deps } = makeDb();
  await assert.rejects(
    () => applyAdminCouponAdjustment({ userId: 1, operation: "credit", amount: 100, reason: "x", adminUserId: null }, deps),
    (e) => e instanceof CouponLedgerError && e.code === "admin_required"
  );
});

test("ajuste exige motivo", async () => {
  const { deps } = makeDb();
  await assert.rejects(
    () => applyAdminCouponAdjustment({ userId: 1, operation: "credit", amount: 100, reason: "  ", adminUserId: 9 }, deps),
    (e) => e.code === "reason_required"
  );
});

test("credito converte NSCreditos para centavos ao gravar", async () => {
  const { deps, state } = makeDb();
  const out = await applyAdminCouponAdjustment({ userId: 1, operation: "credit", amount: 100, reason: "Bonus", adminUserId: 9 }, deps);

  assert.equal(out.balance, 481); // 381 + 100
  assert.equal(out.balance_cents, 48100);
  assert.equal(state.history[0].delta_cents, 10000); // 100 NSCreditos = 10000 cents
  assert.equal(state.history[0].event_type, "ADMIN_BALANCE_ADJUSTMENT");
  const meta = JSON.parse(state.history[0].meta);
  assert.equal(meta.reason, "Bonus");
  assert.equal(meta.admin_user_id, 9);
});

test("debito maior que o saldo e recusado", async () => {
  const { deps } = makeDb({ users: [{ id: 1, name: "Joao", email: "joao@x.com", coupon_value_cents: 1000, coupon_code: null, coupon_expires_at: null }] });
  await assert.rejects(
    () => applyAdminCouponAdjustment({ userId: 1, operation: "debit", amount: 50, reason: "x", adminUserId: 9 }, deps),
    (e) => e.code === "insufficient_balance"
  );
});

test("idempotency_key repetida nao credita duas vezes", async () => {
  const { deps } = makeDb();
  const key = "adm-1";
  const a = await applyAdminCouponAdjustment({ userId: 1, operation: "credit", amount: 50, reason: "x", adminUserId: 9, idempotencyKey: key }, deps);
  const b = await applyAdminCouponAdjustment({ userId: 1, operation: "credit", amount: 50, reason: "x", adminUserId: 9, idempotencyKey: key }, deps);
  assert.equal(a.replayed, false);
  assert.equal(b.replayed, true);
  assert.equal(b.balance, a.balance);
});
