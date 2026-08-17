// tests/nscreditWallet.integration.test.js
//
// Testes da carteira contra um PostgreSQL REAL — e o unico jeito honesto de
// provar locking, concorrencia, idempotencia e as constraints do banco.
//
// Rode apontando para um banco de DESENVOLVIMENTO/TESTE com a migration 021:
//   TEST_DATABASE_URL=postgres://user:pass@localhost:5432/db npm test
//
// Sem TEST_DATABASE_URL os testes sao pulados (nunca usam banco de producao).
import test, { before, after } from "node:test";
import assert from "node:assert/strict";

import {
  applyTransaction,
  applyAdminAdjustment,
  getBalance,
  getTransactionHistory,
  NsCreditError,
} from "../src/services/nscreditWallet.js";

const TEST_DB = process.env.TEST_DATABASE_URL || "";
const SKIP = !TEST_DB;
const skipOpts = { skip: SKIP ? "defina TEST_DATABASE_URL para rodar os testes de integracao" : false };

let pool;
let deps;
let userId;
let adminId;

function sslFor(url) {
  try {
    const host = new URL(url).hostname;
    // Host local costuma usar certificado self-signed; SNI nao aceita IP.
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
    ["Teste NSCreditos", `nscredit-test-${stamp}@exemplo.local`]
  );
  userId = u.rows[0].id;

  const a = await pool.query(
    `insert into public.users (name, email, pass_hash, is_admin)
     values ($1,$2,'x',true) returning id`,
    ["Admin NSCreditos", `nscredit-admin-${stamp}@exemplo.local`]
  );
  adminId = a.rows[0].id;
});

after(async () => {
  if (SKIP || !pool) return;
  // ON DELETE CASCADE limpa wallet e ledger junto.
  await pool.query("delete from public.users where id = any($1::int[])", [[userId, adminId]]).catch(() => {});
  await pool.end().catch(() => {});
});

const admin = () => ({ userId, adminUserId: adminId, reason: "Teste automatizado" });

async function resetWallet(balance = 0) {
  await pool.query("delete from public.nscredit_transactions where user_id = $1", [userId]);
  await pool.query("delete from public.nscredit_wallets where user_id = $1", [userId]);
  if (balance > 0) {
    await pool.query("insert into public.nscredit_wallets (user_id, balance) values ($1,$2)", [userId, balance]);
  }
}

/* ─────────────────────────── Fluxo do criterio de aceite ─────────────────────────── */

test("fluxo completo: 0 -> +10000 -> -1550 = 8450 com ledger auditavel", skipOpts, async () => {
  await resetWallet(0);

  assert.deepEqual(await getBalance(userId, deps), { balance: 0 });

  const c = await applyAdminAdjustment({ ...admin(), operation: "credit", amount: 10000, reason: "Credito inicial" }, deps);
  assert.equal(c.balance, 10000);

  const d = await applyAdminAdjustment({ ...admin(), operation: "debit", amount: 1550, reason: "Ajuste administrativo" }, deps);
  assert.equal(d.balance, 8450);

  assert.deepEqual(await getBalance(userId, deps), { balance: 8450 });

  const hist = await getTransactionHistory(userId, { page: 1, limit: 20 }, deps);
  assert.equal(hist.paging.total, 2);
  assert.equal(hist.items[0].operation, "debit");
  assert.equal(hist.items[0].balance_before, 10000);
  assert.equal(hist.items[0].balance_after, 8450);
  assert.equal(hist.items[1].operation, "credit");
  assert.equal(hist.items[1].balance_before, 0);
  assert.equal(hist.items[1].balance_after, 10000);

  // A soma do ledger tem que bater com o saldo materializado.
  const soma = await pool.query(
    `select coalesce(sum(case when operation='credit' then amount else -amount end),0)::bigint as total
       from public.nscredit_transactions where user_id = $1`,
    [userId]
  );
  assert.equal(Number(soma.rows[0].total), 8450);
});

test("leitura nao cria wallet no banco real", skipOpts, async () => {
  await resetWallet(0);
  await pool.query("delete from public.nscredit_wallets where user_id = $1", [userId]);

  await getBalance(userId, deps);

  const w = await pool.query("select 1 from public.nscredit_wallets where user_id = $1", [userId]);
  assert.equal(w.rowCount, 0, "consultar saldo nao pode inserir carteira");
});

/* ─────────────────────────── Concorrencia ─────────────────────────── */

test("operacoes simultaneas nao causam lost update", skipOpts, async () => {
  await resetWallet(1000);

  const [a, b] = await Promise.allSettled([
    applyAdminAdjustment({ ...admin(), operation: "credit", amount: 500, reason: "Concorrente A" }, deps),
    applyAdminAdjustment({ ...admin(), operation: "debit", amount: 300, reason: "Concorrente B" }, deps),
  ]);

  assert.equal(a.status, "fulfilled", `A falhou: ${a.reason?.message}`);
  assert.equal(b.status, "fulfilled", `B falhou: ${b.reason?.message}`);

  const { balance } = await getBalance(userId, deps);
  assert.equal(balance, 1200, `esperado 1200 (1000 +500 -300), veio ${balance}`);

  // O encadeamento do ledger tem que ser consistente, em qualquer ordem.
  const hist = await getTransactionHistory(userId, { page: 1, limit: 20 }, deps);
  assert.equal(hist.items.length, 2);
  const ordenado = [...hist.items].sort((x, y) => x.id - y.id);
  assert.equal(ordenado[0].balance_before, 1000);
  assert.equal(ordenado[1].balance_before, ordenado[0].balance_after, "uma operacao viu o saldo da outra");
  assert.equal(ordenado[1].balance_after, 1200);
});

test("muitas operacoes simultaneas mantem o saldo exato", skipOpts, async () => {
  await resetWallet(0);

  const N = 12;
  const results = await Promise.allSettled(
    Array.from({ length: N }, (_, i) =>
      applyAdminAdjustment({ ...admin(), operation: "credit", amount: 100, reason: `Lote ${i}` }, deps)
    )
  );
  assert.equal(results.filter((r) => r.status === "fulfilled").length, N);

  const { balance } = await getBalance(userId, deps);
  assert.equal(balance, N * 100);

  const hist = await getTransactionHistory(userId, { page: 1, limit: 100 }, deps);
  assert.equal(hist.paging.total, N);

  // Nenhum balance_after repetido: prova que ninguem leu saldo desatualizado.
  const finais = new Set(hist.items.map((t) => t.balance_after));
  assert.equal(finais.size, N, "houve leitura de saldo desatualizado");
});

test("debitos simultaneos nao levam o saldo abaixo de zero", skipOpts, async () => {
  await resetWallet(1000);

  const results = await Promise.allSettled(
    Array.from({ length: 5 }, (_, i) =>
      applyAdminAdjustment({ ...admin(), operation: "debit", amount: 300, reason: `Debito ${i}` }, deps)
    )
  );

  const ok = results.filter((r) => r.status === "fulfilled").length;
  const { balance } = await getBalance(userId, deps);

  assert.ok(balance >= 0, "saldo nunca pode ficar negativo");
  assert.equal(balance, 1000 - ok * 300);
  assert.equal(ok, 3, "so cabem 3 debitos de 300 em um saldo de 1000");

  const rejeitados = results.filter((r) => r.status === "rejected");
  rejeitados.forEach((r) => assert.equal(r.reason.code, "insufficient_balance"));
});

/* ─────────────────────────── Idempotencia ─────────────────────────── */

test("requisicoes simultaneas com a mesma idempotency_key aplicam uma unica vez", skipOpts, async () => {
  await resetWallet(1000);
  const key = `idem-${Date.now()}`;

  const results = await Promise.allSettled(
    Array.from({ length: 6 }, () =>
      applyAdminAdjustment({ ...admin(), operation: "credit", amount: 500, reason: "Duplo clique", idempotencyKey: key }, deps)
    )
  );

  const ok = results.filter((r) => r.status === "fulfilled");
  assert.equal(ok.length, 6, "todas devem responder com sucesso");

  const aplicadas = ok.filter((r) => r.value.replayed === false);
  assert.equal(aplicadas.length, 1, "apenas uma pode ter aplicado de fato");

  const { balance } = await getBalance(userId, deps);
  assert.equal(balance, 1500, `o valor so pode ter entrado uma vez, veio ${balance}`);

  const hist = await getTransactionHistory(userId, { page: 1, limit: 20 }, deps);
  assert.equal(hist.paging.total, 1, "uma unica linha no ledger");

  // Todas devolvem a MESMA movimentacao.
  const ids = new Set(ok.map((r) => r.value.transaction.id));
  assert.equal(ids.size, 1);
});

test("reenvio sequencial com a mesma chave devolve a original sem reaplicar", skipOpts, async () => {
  await resetWallet(0);
  const key = `idem-seq-${Date.now()}`;

  const a = await applyAdminAdjustment({ ...admin(), operation: "credit", amount: 2000, reason: "Bonificacao", idempotencyKey: key }, deps);
  const b = await applyAdminAdjustment({ ...admin(), operation: "credit", amount: 2000, reason: "Bonificacao", idempotencyKey: key }, deps);

  assert.equal(a.replayed, false);
  assert.equal(b.replayed, true);
  assert.equal(a.transaction.id, b.transaction.id);
  assert.equal((await getBalance(userId, deps)).balance, 2000);
});

/* ─────────────────────────── Constraints do banco ─────────────────────────── */

test("o banco recusa saldo negativo mesmo por escrita direta", skipOpts, async () => {
  await resetWallet(100);
  await assert.rejects(
    () => pool.query("update public.nscredit_wallets set balance = -1 where user_id = $1", [userId]),
    (e) => {
      assert.equal(e.code, "23514");
      return true;
    }
  );
});

test("o banco recusa ledger com aritmetica inconsistente", skipOpts, async () => {
  await assert.rejects(
    () =>
      pool.query(
        `insert into public.nscredit_transactions
           (user_id, operation, amount, balance_before, balance_after, source_type, reason, created_by)
         values ($1,'credit',100,0,999,'admin','fraude',$2)`,
        [userId, adminId]
      ),
    (e) => {
      assert.equal(e.code, "23514");
      return true;
    }
  );
});

test("o banco recusa movimentacao admin sem motivo", skipOpts, async () => {
  await assert.rejects(
    () =>
      pool.query(
        `insert into public.nscredit_transactions
           (user_id, operation, amount, balance_before, balance_after, source_type, reason, created_by)
         values ($1,'credit',100,0,100,'admin','   ',$2)`,
        [userId, adminId]
      ),
    (e) => {
      assert.equal(e.code, "23514");
      return true;
    }
  );
});

test("debito insuficiente nao deixa nada no banco", skipOpts, async () => {
  await resetWallet(500);

  await assert.rejects(
    () => applyAdminAdjustment({ ...admin(), operation: "debit", amount: 1000, reason: "Nao deve passar" }, deps),
    (e) => e instanceof NsCreditError && e.code === "insufficient_balance"
  );

  assert.equal((await getBalance(userId, deps)).balance, 500);
  const hist = await getTransactionHistory(userId, { page: 1, limit: 20 }, deps);
  assert.equal(hist.paging.total, 0);
});

test("usuario inexistente nao gera wallet nem ledger", skipOpts, async () => {
  await assert.rejects(
    () => applyAdminAdjustment({ userId: 2147483000, adminUserId: adminId, operation: "credit", amount: 100, reason: "x" }, deps),
    (e) => e.code === "user_not_found"
  );

  const w = await pool.query("select 1 from public.nscredit_wallets where user_id = $1", [2147483000]);
  assert.equal(w.rowCount, 0);
});

test("origem futura (redemption) grava sem admin responsavel", skipOpts, async () => {
  await resetWallet(5000);

  const out = await applyTransaction(
    { userId, operation: "debit", amount: 1200, sourceType: "redemption", sourceId: "pedido-teste-1" },
    deps
  );

  assert.equal(out.balance, 3800);
  const r = await pool.query(
    "select source_type, source_id, created_by, reason from public.nscredit_transactions where user_id = $1 order by id desc limit 1",
    [userId]
  );
  assert.equal(r.rows[0].source_type, "redemption");
  assert.equal(r.rows[0].source_id, "pedido-teste-1");
  assert.equal(r.rows[0].created_by, null);
});
