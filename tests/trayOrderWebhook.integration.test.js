// tests/trayOrderWebhook.integration.test.js
//
// Reconciliacao de gasto direto na Tray contra Postgres REAL — precisa do
// banco de verdade para provar a idempotencia via UNIQUE de
// coupon_balance_history.idempotency_key (o mesmo mecanismo da saga de
// resgate, reusado aqui).
//
//   TEST_DATABASE_URL=postgres://... npm test
//
// Sem TEST_DATABASE_URL os testes sao pulados (nunca usam banco de producao).
import test, { before, after, beforeEach } from "node:test";
import assert from "node:assert/strict";

import { handleTrayOrderWebhook } from "../src/services/trayOrderWebhook.js";
import { getCouponBalance, applyCouponLedgerEntry } from "../src/services/couponLedger.js";

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
  userId = (await pool.query(
    `insert into public.users (name, email, pass_hash, is_admin, coupon_code) values ($1,$2,'x',false,$3) returning id`,
    ["Webhook Test", `webhook-test-${stamp}@exemplo.local`, `NSU-WEBHOOK-${stamp}`]
  )).rows[0].id;
});

after(async () => {
  if (SKIP || !pool) return;
  await pool.query("delete from public.coupon_balance_history where user_id=$1", [userId]).catch(() => {});
  await pool.query("delete from public.users where id=$1", [userId]).catch(() => {});
  await pool.end().catch(() => {});
});

beforeEach(async () => {
  if (SKIP) return;
  await pool.query("delete from public.coupon_balance_history where user_id=$1", [userId]);
  await pool.query("update public.users set coupon_value_cents=0 where id=$1", [userId]);
});

async function couponCodeFor(uid) {
  const { rows } = await pool.query("select coupon_code from public.users where id=$1", [uid]);
  return rows[0].coupon_code;
}

test("gasto direto confirmado debita EXATAMENTE o discount (integral == saldo neste caso) e grava DIRECT_TRAY_SPEND", skipOpts, async () => {
  await applyCouponLedgerEntry({ userId, operation: "credit", amountCents: 50000, eventType: "ADMIN_BALANCE_ADJUSTMENT" }, deps);
  const code = await couponCodeFor(userId);

  const out = await handleTrayOrderWebhook(
    { seller_id: "1", scope_id: "9001", scope_name: "order", act: "insert" },
    { query: deps.query, withTransaction: deps.withTransaction, getTrayOrderFull: async () => ({ couponCode: code, discount: 500, discountCents: 50000 }) }
  );

  assert.equal(out.handled, true);
  assert.equal(out.balance_cents, 0);

  const balance = await getCouponBalance(userId, deps);
  assert.equal(balance.balance_cents, 0);

  const { rows } = await pool.query(
    "select event_type, delta_cents from public.coupon_balance_history where user_id=$1 and event_type='DIRECT_TRAY_SPEND'",
    [userId]
  );
  assert.equal(rows.length, 1);
  assert.equal(rows[0].delta_cents, -50000);
});

test("gasto PARCIAL: discount menor que o saldo debita so o valor exato, nunca zera o resto", skipOpts, async () => {
  await applyCouponLedgerEntry({ userId, operation: "credit", amountCents: 38100, eventType: "ADMIN_BALANCE_ADJUSTMENT" }, deps);
  const code = await couponCodeFor(userId);

  const out = await handleTrayOrderWebhook(
    { seller_id: "1", scope_id: "9005", scope_name: "order", act: "insert" },
    { query: deps.query, withTransaction: deps.withTransaction, getTrayOrderFull: async () => ({ couponCode: code, discount: 50, discountCents: 5000 }) }
  );

  assert.equal(out.handled, true);
  assert.equal(out.balance_cents, 33100, "38100 - 5000 = 33100, nunca zero");

  const balance = await getCouponBalance(userId, deps);
  assert.equal(balance.balance_cents, 33100);

  const { rows } = await pool.query(
    "select delta_cents from public.coupon_balance_history where user_id=$1 and event_type='DIRECT_TRAY_SPEND'",
    [userId]
  );
  assert.equal(rows.length, 1);
  assert.equal(rows[0].delta_cents, -5000);
});

test("dois gastos parciais em pedidos diferentes debitam cada um o seu valor exato", skipOpts, async () => {
  await applyCouponLedgerEntry({ userId, operation: "credit", amountCents: 38100, eventType: "ADMIN_BALANCE_ADJUSTMENT" }, deps);
  const code = await couponCodeFor(userId);
  const webhookDeps = { query: deps.query, withTransaction: deps.withTransaction };

  const a = await handleTrayOrderWebhook(
    { seller_id: "1", scope_id: "9006", scope_name: "order", act: "insert" },
    { ...webhookDeps, getTrayOrderFull: async () => ({ couponCode: code, discount: 50, discountCents: 5000 }) }
  );
  assert.equal(a.handled, true);
  assert.equal(a.balance_cents, 33100);

  const b = await handleTrayOrderWebhook(
    { seller_id: "1", scope_id: "9007", scope_name: "order", act: "insert" },
    { ...webhookDeps, getTrayOrderFull: async () => ({ couponCode: code, discount: 31, discountCents: 3100 }) }
  );
  assert.equal(b.handled, true);
  assert.equal(b.balance_cents, 30000);

  assert.equal((await getCouponBalance(userId, deps)).balance_cents, 30000);
});

test("discount MAIOR que o saldo local: ledger recusa (insufficient_balance), nunca mascara com Math.max, nao debita nada", skipOpts, async () => {
  await applyCouponLedgerEntry({ userId, operation: "credit", amountCents: 3000, eventType: "ADMIN_BALANCE_ADJUSTMENT" }, deps);
  const code = await couponCodeFor(userId);

  const out = await handleTrayOrderWebhook(
    { seller_id: "1", scope_id: "9008", scope_name: "order", act: "insert" },
    { query: deps.query, withTransaction: deps.withTransaction, getTrayOrderFull: async () => ({ couponCode: code, discount: 50, discountCents: 5000 }) }
  );

  assert.equal(out.handled, false);
  assert.equal(out.reason, "balance_changed_concurrently");

  // Saldo nunca e tocado quando ha inconsistencia -- fica exatamente como estava.
  assert.equal((await getCouponBalance(userId, deps)).balance_cents, 3000);
  const { rows } = await pool.query(
    "select count(*)::int as n from public.coupon_balance_history where user_id=$1 and event_type='DIRECT_TRAY_SPEND'",
    [userId]
  );
  assert.equal(rows[0].n, 0, "nenhum lancamento gravado quando a inconsistencia e detectada");
});

test("mesmo tray_order_id entregue duas vezes (retry do webhook) nao debita duas vezes", skipOpts, async () => {
  await applyCouponLedgerEntry({ userId, operation: "credit", amountCents: 30000, eventType: "ADMIN_BALANCE_ADJUSTMENT" }, deps);
  const code = await couponCodeFor(userId);
  const webhookDeps = { query: deps.query, withTransaction: deps.withTransaction, getTrayOrderFull: async () => ({ couponCode: code, discount: 300, discountCents: 30000 }) };
  const payload = { seller_id: "1", scope_id: "9002", scope_name: "order", act: "insert" };

  const a = await handleTrayOrderWebhook(payload, webhookDeps);
  const b = await handleTrayOrderWebhook(payload, webhookDeps);

  assert.equal(a.handled, true);
  assert.equal(a.replayed, false);
  // segunda entrega: saldo ja esta zerado, entao cai em balance_already_zero
  // (no-op seguro) em vez de tentar debitar de novo -- mesmo resultado
  // pratico do replay, sem depender de uma segunda leitura de saldo positivo.
  assert.equal(b.handled, false);
  assert.equal(b.reason, "balance_already_zero");

  const balance = await getCouponBalance(userId, deps);
  assert.equal(balance.balance_cents, 0);

  const { rows } = await pool.query(
    "select count(*)::int as n from public.coupon_balance_history where user_id=$1 and event_type='DIRECT_TRAY_SPEND'",
    [userId]
  );
  assert.equal(rows[0].n, 1, "nunca mais que um lancamento, mesmo com entrega duplicada");
});

test("retry chega DEPOIS de um credito legitimo novo: idempotency_key protege, nao debita o credito novo", skipOpts, async () => {
  await applyCouponLedgerEntry({ userId, operation: "credit", amountCents: 20000, eventType: "ADMIN_BALANCE_ADJUSTMENT" }, deps);
  const code = await couponCodeFor(userId);
  const webhookDeps = { query: deps.query, withTransaction: deps.withTransaction, getTrayOrderFull: async () => ({ couponCode: code, discount: 200, discountCents: 20000 }) };
  const payload = { seller_id: "1", scope_id: "9004", scope_name: "order", act: "insert" };

  const a = await handleTrayOrderWebhook(payload, webhookDeps);
  assert.equal(a.handled, true);
  assert.equal((await getCouponBalance(userId, deps)).balance_cents, 0);

  // Credito legitimo NOVO chega depois (ex.: compra de ticket aprovada) —
  // nao pode ser confundido com o gasto direto ja reconciliado.
  await applyCouponLedgerEntry({ userId, operation: "credit", amountCents: 15000, eventType: "ADMIN_BALANCE_ADJUSTMENT" }, deps);

  // A Tray reenvia a MESMA notificacao (retry documentado com backoff).
  const b = await handleTrayOrderWebhook(payload, webhookDeps);
  assert.equal(b.handled, true);
  assert.equal(b.replayed, true, "mesma idempotency_key -> replay, nunca um novo debito");

  // O credito novo tem que continuar intacto -- o replay nao pode zera-lo.
  assert.equal((await getCouponBalance(userId, deps)).balance_cents, 15000);

  const { rows } = await pool.query(
    "select count(*)::int as n from public.coupon_balance_history where user_id=$1 and event_type='DIRECT_TRAY_SPEND'",
    [userId]
  );
  assert.equal(rows[0].n, 1, "o retry nunca cria um segundo lancamento de gasto direto");
});

test("saldo ja alterado por outra operacao concorrente (ex.: resgate) nao quebra o webhook", skipOpts, async () => {
  await applyCouponLedgerEntry({ userId, operation: "credit", amountCents: 10000, eventType: "ADMIN_BALANCE_ADJUSTMENT" }, deps);
  const code = await couponCodeFor(userId);

  // Simula: entre a leitura do saldo (feita dentro do handler) e o debito,
  // outra operacao ja reduziu o saldo para um valor MENOR que o lido —
  // aqui simplificado debitando o saldo TODO antes de chamar o webhook,
  // que le o saldo (ja zerado) e cai no early-return de saldo zero.
  await applyCouponLedgerEntry({ userId, operation: "debit", amountCents: 10000, eventType: "REDEMPTION_DEBIT" }, deps);

  const out = await handleTrayOrderWebhook(
    { seller_id: "1", scope_id: "9003", scope_name: "order", act: "insert" },
    { query: deps.query, withTransaction: deps.withTransaction, getTrayOrderFull: async () => ({ couponCode: code, discount: 100, discountCents: 10000 }) }
  );

  assert.equal(out.handled, false);
  assert.equal(out.reason, "balance_already_zero");
});
