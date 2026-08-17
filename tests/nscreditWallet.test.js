// tests/nscreditWallet.test.js
//
// Carteira de NSCreditos — regras, ledger e integridade da transacao.
// Concorrencia real fica em nscreditWallet.integration.test.js.
import test from "node:test";
import assert from "node:assert/strict";

import {
  MAX_NSCREDIT_AMOUNT,
  NsCreditError,
  parseNsCreditAmount,
  parseOperation,
  parseReason,
  parseUserId,
  getBalance,
  getTransactionHistory,
  applyTransaction,
  applyAdminAdjustment,
  searchUsersForAdmin,
  getAdminWalletDetail,
} from "../src/services/nscreditWallet.js";

/**
 * Banco falso em memoria que reproduz o comportamento relevante:
 * wallets, ledger, unique de idempotency_key e rollback.
 */
function makeDb({ users = [{ id: 123, name: "Joao Pedro", email: "joao@exemplo.com" }], wallets = {}, transactions = [] } = {}) {
  const state = {
    users,
    wallets: { ...wallets },
    transactions: [...transactions],
    sql: [],
    txCount: 0,
    commits: 0,
    rollbacks: 0,
    nextId: 1 + transactions.length,
    failOnLedgerInsert: false,
    failOnWalletUpdate: false,
  };

  function run(sql, params = []) {
    state.sql.push({ sql, params });
    const s = String(sql).toLowerCase();

    if (/from public\.users\b/.test(s) && /where\s+id\s*=/.test(s)) {
      const u = state.users.find((x) => String(x.id) === String(params[0]));
      return { rows: u ? [u] : [], rowCount: u ? 1 : 0 };
    }

    if (/from public\.nscredit_transactions/.test(s) && /idempotency_key\s*=/.test(s)) {
      const hit = state.transactions.find((t) => t.idempotency_key && t.idempotency_key === params[0]);
      return { rows: hit ? [hit] : [], rowCount: hit ? 1 : 0 };
    }

    if (/insert into public\.nscredit_wallets/.test(s)) {
      const uid = String(params[0]);
      if (state.wallets[uid] === undefined) state.wallets[uid] = 0;
      return { rows: [], rowCount: 0 };
    }

    if (/select\s+balance/.test(s) && /nscredit_wallets/.test(s)) {
      const uid = String(params[0]);
      const bal = state.wallets[uid];
      // bigint chega como STRING no driver pg
      return bal === undefined ? { rows: [], rowCount: 0 } : { rows: [{ balance: String(bal) }], rowCount: 1 };
    }

    if (/update public\.nscredit_wallets/.test(s)) {
      if (state.failOnWalletUpdate) throw Object.assign(new Error("wallet update falhou"), { code: "XX000" });
      const next = Number(params[0]);
      const uid = String(params[1]);
      if (next < 0) throw Object.assign(new Error("check violation"), { code: "23514" });
      state.wallets[uid] = next;
      return { rows: [], rowCount: 1 };
    }

    if (/insert into public\.nscredit_transactions/.test(s)) {
      if (state.failOnLedgerInsert) throw Object.assign(new Error("ledger insert falhou"), { code: "XX000" });
      const [user_id, operation, amount, balance_before, balance_after, source_type, source_id, reason, created_by, idempotency_key] = params;
      if (idempotency_key && state.transactions.some((t) => t.idempotency_key === idempotency_key)) {
        throw Object.assign(new Error("duplicate key"), { code: "23505" });
      }
      const row = {
        id: state.nextId++,
        user_id: Number(user_id),
        operation,
        amount: String(amount),
        balance_before: String(balance_before),
        balance_after: String(balance_after),
        source_type,
        source_id: source_id ?? null,
        reason: reason ?? null,
        created_by: created_by ?? null,
        idempotency_key: idempotency_key ?? null,
        created_at: new Date("2026-08-16T12:00:00.000Z"),
      };
      state.transactions.push(row);
      return { rows: [row], rowCount: 1 };
    }

    if (/from public\.nscredit_transactions/.test(s) && /count\(\*\)/.test(s)) {
      const uid = String(params[0]);
      return { rows: [{ total: String(state.transactions.filter((t) => String(t.user_id) === uid).length) }], rowCount: 1 };
    }

    if (/from public\.nscredit_transactions/.test(s)) {
      const uid = String(params[0]);
      const rows = state.transactions.filter((t) => String(t.user_id) === uid).sort((a, b) => b.id - a.id);
      return { rows, rowCount: rows.length };
    }

    if (/from public\.users/.test(s)) {
      const rows = state.users.map((u) => ({
        ...u,
        balance: String(state.wallets[String(u.id)] ?? 0),
        total: String(state.users.length),
      }));
      return { rows, rowCount: rows.length };
    }

    return { rows: [], rowCount: 0 };
  }

  const deps = {
    query: async (sql, params) => run(sql, params),
    withTransaction: async (fn) => {
      state.txCount += 1;
      const snapshot = { wallets: { ...state.wallets }, transactions: [...state.transactions] };
      try {
        const out = await fn({ query: async (sql, params) => run(sql, params) });
        state.commits += 1;
        return out;
      } catch (e) {
        // ROLLBACK: desfaz tudo que a transacao alterou.
        state.wallets = snapshot.wallets;
        state.transactions = snapshot.transactions;
        state.rollbacks += 1;
        throw e;
      }
    },
  };

  return { state, deps };
}

const ADMIN = { userId: 123, reason: "Bonificacao administrativa", adminUserId: 7 };

/* ─────────────────────────── Validacao ─────────────────────────── */

test("amount aceita inteiros positivos", () => {
  for (const v of [1, 100, 5000, 100000, "1000"]) {
    assert.equal(parseNsCreditAmount(v), Number(v));
  }
});

test("amount rejeita zero, negativo, decimal e nao-numerico", () => {
  for (const bad of [0, -1, 1.5, "1000abc", NaN, null, undefined, "", true, false, [], {}, Infinity, "1.5", "-5"]) {
    assert.throws(
      () => parseNsCreditAmount(bad),
      (e) => {
        assert.ok(e instanceof NsCreditError, `esperado NsCreditError para ${JSON.stringify(bad)}`);
        assert.equal(e.status, 400);
        assert.equal(e.code, "invalid_amount");
        return true;
      },
      `deveria rejeitar ${JSON.stringify(bad)}`
    );
  }
});

test("amount acima da faixa segura do JavaScript e rejeitado", () => {
  assert.equal(parseNsCreditAmount(MAX_NSCREDIT_AMOUNT), MAX_NSCREDIT_AMOUNT);
  assert.equal(MAX_NSCREDIT_AMOUNT, Number.MAX_SAFE_INTEGER);

  for (const bad of [Number.MAX_SAFE_INTEGER + 1, "9007199254740992", "99999999999999999999"]) {
    assert.throws(() => parseNsCreditAmount(bad), (e) => {
      assert.equal(e.code, "amount_out_of_range");
      assert.equal(e.status, 400);
      return true;
    }, `deveria rejeitar ${bad}`);
  }
});

test("operation aceita apenas credit e debit", () => {
  assert.equal(parseOperation("credit"), "credit");
  assert.equal(parseOperation("debit"), "debit");
  for (const bad of ["CREDIT", "set", "balance", "", null, 1]) {
    assert.throws(() => parseOperation(bad), (e) => {
      assert.equal(e.code, "invalid_operation");
      return true;
    });
  }
});

test("reason e obrigatorio, sanitizado e limitado", () => {
  assert.equal(parseReason("  Bonificacao  "), "Bonificacao");
  assert.equal(parseReason("a".repeat(300)).length, 300);

  for (const bad of ["", "   ", null, undefined, 123, {}]) {
    assert.throws(() => parseReason(bad), (e) => {
      assert.equal(e.code, "reason_required");
      assert.equal(e.status, 400);
      return true;
    });
  }
  assert.throws(() => parseReason("a".repeat(301)), (e) => {
    assert.equal(e.code, "reason_too_long");
    return true;
  });
});

test("userId invalido e rejeitado", () => {
  assert.equal(parseUserId("123"), 123);
  for (const bad of [0, -1, 1.5, "abc", null, undefined]) {
    assert.throws(() => parseUserId(bad), (e) => {
      assert.equal(e.code, "invalid_user_id");
      return true;
    });
  }
});

/* ─────────────────────────── Leitura ─────────────────────────── */

test("usuario sem wallet retorna saldo 0", async () => {
  const { deps } = makeDb();
  assert.deepEqual(await getBalance(123, deps), { balance: 0 });
});

test("leitura NAO cria wallet", async () => {
  const { state, deps } = makeDb();
  await getBalance(123, deps);

  assert.equal(state.wallets["123"], undefined, "consultar saldo nao pode inserir wallet");
  assert.ok(
    !state.sql.some((q) => /insert into public\.nscredit_wallets/i.test(q.sql)),
    "nenhum INSERT de wallet pode acontecer na leitura"
  );
  assert.equal(state.txCount, 0, "leitura nao abre transacao");
});

test("usuario com wallet retorna o saldo factual", async () => {
  const { deps } = makeDb({ wallets: { 123: 8450 } });
  assert.deepEqual(await getBalance(123, deps), { balance: 8450 });
});

/* ─────────────────────────── Credito / Debito ─────────────────────────── */

test("primeiro credito cria a wallet", async () => {
  const { state, deps } = makeDb();

  const out = await applyAdminAdjustment({ ...ADMIN, operation: "credit", amount: 10000 }, deps);

  assert.equal(state.wallets["123"], 10000);
  assert.equal(out.balance, 10000);
  assert.equal(out.replayed, false);
  assert.ok(state.sql.some((q) => /insert into public\.nscredit_wallets/i.test(q.sql)));
});

test("credito aumenta o saldo", async () => {
  const { state, deps } = makeDb({ wallets: { 123: 8450 } });
  const out = await applyAdminAdjustment({ ...ADMIN, operation: "credit", amount: 2000 }, deps);
  assert.equal(out.balance, 10450);
  assert.equal(state.wallets["123"], 10450);
});

test("debito reduz o saldo", async () => {
  const { state, deps } = makeDb({ wallets: { 123: 9450 } });
  const out = await applyAdminAdjustment({ ...ADMIN, operation: "debit", amount: 1000, reason: "Correcao" }, deps);
  assert.equal(out.balance, 8450);
  assert.equal(state.wallets["123"], 8450);
});

test("saldo nunca fica negativo e nada e gravado", async () => {
  const { state, deps } = makeDb({ wallets: { 123: 500 } });

  await assert.rejects(
    () => applyAdminAdjustment({ ...ADMIN, operation: "debit", amount: 1000, reason: "Correcao" }, deps),
    (e) => {
      assert.equal(e.code, "insufficient_balance");
      assert.equal(e.status, 409);
      assert.deepEqual(e.details, { balance: 500, requested: 1000 });
      return true;
    }
  );

  assert.equal(state.wallets["123"], 500, "saldo deve permanecer intacto");
  assert.equal(state.transactions.length, 0, "nenhuma linha no ledger");
});

test("debito exatamente igual ao saldo e permitido e zera a carteira", async () => {
  const { state, deps } = makeDb({ wallets: { 123: 500 } });
  const out = await applyAdminAdjustment({ ...ADMIN, operation: "debit", amount: 500, reason: "Zerar" }, deps);
  assert.equal(out.balance, 0);
  assert.equal(state.wallets["123"], 0);
});

test("usuario inexistente e rejeitado antes de qualquer escrita", async () => {
  const { state, deps } = makeDb();

  await assert.rejects(
    () => applyAdminAdjustment({ ...ADMIN, userId: 999, operation: "credit", amount: 100 }, deps),
    (e) => {
      assert.equal(e.code, "user_not_found");
      assert.equal(e.status, 404);
      return true;
    }
  );

  assert.equal(state.transactions.length, 0);
  assert.deepEqual(state.wallets, {});
});

/* ─────────────────────────── Ledger ─────────────────────────── */

test("credito grava o ledger com saldo antes e depois corretos", async () => {
  const { state, deps } = makeDb();

  await applyAdminAdjustment({ ...ADMIN, operation: "credit", amount: 10000, reason: "Credito inicial" }, deps);

  assert.equal(state.transactions.length, 1);
  const t = state.transactions[0];
  assert.equal(t.user_id, 123);
  assert.equal(t.operation, "credit");
  assert.equal(t.amount, "10000");
  assert.equal(t.balance_before, "0");
  assert.equal(t.balance_after, "10000");
  assert.equal(t.source_type, "admin");
  assert.equal(t.reason, "Credito inicial");
  assert.equal(t.created_by, 7);
});

test("debito grava o ledger com saldo antes e depois corretos", async () => {
  const { state, deps } = makeDb({ wallets: { 123: 10000 } });

  await applyAdminAdjustment({ ...ADMIN, operation: "debit", amount: 1550, reason: "Ajuste administrativo" }, deps);

  const t = state.transactions[0];
  assert.equal(t.operation, "debit");
  assert.equal(t.amount, "1550");
  assert.equal(t.balance_before, "10000");
  assert.equal(t.balance_after, "8450");
  assert.equal(t.created_by, 7);
});

test("sequencia de movimentacoes mantem o encadeamento do ledger", async () => {
  const { state, deps } = makeDb();

  await applyAdminAdjustment({ ...ADMIN, operation: "credit", amount: 10000, reason: "Credito inicial" }, deps);
  await applyAdminAdjustment({ ...ADMIN, operation: "debit", amount: 1550, reason: "Ajuste" }, deps);

  const [t1, t2] = state.transactions;
  assert.equal(t1.balance_after, "10000");
  assert.equal(t2.balance_before, "10000", "o saldo anterior tem que ser o posterior da movimentacao anterior");
  assert.equal(t2.balance_after, "8450");
  assert.equal(state.wallets["123"], 8450);
});

test("created_by NUNCA vem do payload — so do admin autenticado", async () => {
  const { state, deps } = makeDb();

  await applyAdminAdjustment(
    { ...ADMIN, operation: "credit", amount: 100, created_by: 1, createdBy: 1, adminUserId: 7 },
    deps
  );

  assert.equal(state.transactions[0].created_by, 7);
});

test("movimentacao administrativa exige admin autenticado", async () => {
  const { deps } = makeDb();
  await assert.rejects(
    () => applyAdminAdjustment({ ...ADMIN, adminUserId: null, operation: "credit", amount: 100 }, deps),
    (e) => {
      assert.equal(e.code, "admin_required");
      assert.equal(e.status, 403);
      return true;
    }
  );
});

/* ─────────────────────────── Atomicidade ─────────────────────────── */

test("tudo acontece dentro de UMA transacao com lock da carteira", async () => {
  const { state, deps } = makeDb({ wallets: { 123: 1000 } });

  await applyAdminAdjustment({ ...ADMIN, operation: "credit", amount: 500 }, deps);

  assert.equal(state.txCount, 1);
  assert.equal(state.commits, 1);
  assert.ok(
    state.sql.some((q) => /select\s+balance[\s\S]*for update/i.test(q.sql)),
    "a leitura do saldo precisa usar SELECT ... FOR UPDATE"
  );
});

test("falha no INSERT do ledger desfaz o saldo (rollback)", async () => {
  const { state, deps } = makeDb({ wallets: { 123: 1000 } });
  state.failOnLedgerInsert = true;

  await assert.rejects(() => applyAdminAdjustment({ ...ADMIN, operation: "credit", amount: 500 }, deps));

  assert.equal(state.wallets["123"], 1000, "saldo nao pode ter sido alterado");
  assert.equal(state.transactions.length, 0);
  assert.equal(state.commits, 0);
  assert.equal(state.rollbacks, 1);
});

test("falha no UPDATE da wallet nao deixa ledger orfao", async () => {
  const { state, deps } = makeDb({ wallets: { 123: 1000 } });
  state.failOnWalletUpdate = true;

  await assert.rejects(() => applyAdminAdjustment({ ...ADMIN, operation: "credit", amount: 500 }, deps));

  assert.equal(state.transactions.length, 0, "nenhum ledger sem saldo correspondente");
  assert.equal(state.wallets["123"], 1000);
  assert.equal(state.rollbacks, 1);
});

/* ─────────────────────────── Idempotencia ─────────────────────────── */

test("mesma idempotency_key nao aplica duas vezes", async () => {
  const { state, deps } = makeDb({ wallets: { 123: 1000 } });
  const key = "11111111-2222-3333-4444-555555555555";

  const a = await applyAdminAdjustment({ ...ADMIN, operation: "credit", amount: 500, idempotencyKey: key }, deps);
  const b = await applyAdminAdjustment({ ...ADMIN, operation: "credit", amount: 500, idempotencyKey: key }, deps);

  assert.equal(a.replayed, false);
  assert.equal(b.replayed, true, "a segunda chamada e uma repeticao");
  assert.equal(a.transaction.id, b.transaction.id, "devolve a movimentacao original");
  assert.equal(state.wallets["123"], 1500, "o valor so pode ter sido aplicado uma vez");
  assert.equal(state.transactions.length, 1);
});

test("chaves diferentes aplicam normalmente", async () => {
  const { state, deps } = makeDb({ wallets: { 123: 1000 } });
  await applyAdminAdjustment({ ...ADMIN, operation: "credit", amount: 500, idempotencyKey: "chave-a" }, deps);
  await applyAdminAdjustment({ ...ADMIN, operation: "credit", amount: 500, idempotencyKey: "chave-b" }, deps);
  assert.equal(state.wallets["123"], 2000);
  assert.equal(state.transactions.length, 2);
});

test("corrida na mesma chave nao aplica o valor duas vezes e devolve a original", async () => {
  const key = "chave-corrida";

  // A requisicao vencedora ja gravou: saldo 1000 -> 1500 e ledger id 99.
  const vencedora = {
    id: 99,
    user_id: 123,
    operation: "credit",
    amount: "500",
    balance_before: "1000",
    balance_after: "1500",
    source_type: "admin",
    source_id: null,
    reason: "Bonificacao administrativa",
    created_by: 7,
    idempotency_key: key,
    created_at: new Date("2026-08-16T12:00:00.000Z"),
  };

  const { state, deps } = makeDb({ wallets: { 123: 1500 }, transactions: [vencedora] });

  // Dentro da transacao perdedora, a checagem previa AINDA nao enxerga a
  // movimentacao vencedora — e por isso ela segue e bate no unique no INSERT.
  const runInTx = deps.withTransaction;
  deps.withTransaction = async (fn) =>
    runInTx(async (client) => {
      const original = client.query;
      return fn({
        query: async (sql, params) => {
          const s = String(sql).toLowerCase();
          if (/from public\.nscredit_transactions/.test(s) && /idempotency_key\s*=/.test(s)) {
            return { rows: [], rowCount: 0 };
          }
          return original(sql, params);
        },
      });
    });

  const out = await applyAdminAdjustment({ ...ADMIN, operation: "credit", amount: 500, idempotencyKey: key }, deps);

  assert.equal(out.replayed, true);
  assert.equal(out.transaction.id, 99, "devolve a movimentacao vencedora");
  assert.equal(state.rollbacks, 1, "a transacao perdedora precisa ter sido revertida");
  assert.equal(state.wallets["123"], 1500, "o saldo NAO pode ser aplicado de novo");
  assert.equal(state.transactions.length, 1, "nenhuma linha duplicada no ledger");
});

/* ─────────────────────────── Fronteira para o futuro ─────────────────────────── */

test("o service aceita outras origens alem de admin (sem expor endpoint)", async () => {
  const { state, deps } = makeDb({ wallets: { 123: 10000 } });

  const out = await applyTransaction(
    {
      userId: 123,
      operation: "debit",
      amount: 5000,
      sourceType: "redemption",
      sourceId: "order-1",
      reason: "Resgate de premio",
      createdBy: null,
    },
    deps
  );

  assert.equal(out.balance, 5000);
  const t = state.transactions[0];
  assert.equal(t.source_type, "redemption");
  assert.equal(t.source_id, "order-1");
  assert.equal(t.created_by, null, "origem automatica nao tem admin responsavel");
});

test("origem nao-admin nao exige reason, mas admin exige", async () => {
  const { deps } = makeDb({ wallets: { 123: 10000 } });

  await applyTransaction(
    { userId: 123, operation: "debit", amount: 100, sourceType: "redemption", sourceId: "o1" },
    deps
  );

  await assert.rejects(
    () => applyAdminAdjustment({ userId: 123, adminUserId: 7, operation: "credit", amount: 100, reason: "" }, deps),
    (e) => {
      assert.equal(e.code, "reason_required");
      return true;
    }
  );
});

/* ─────────────────────────── Historico e busca ─────────────────────────── */

test("historico e paginado e vem do mais recente para o mais antigo", async () => {
  const { deps } = makeDb();
  await applyAdminAdjustment({ ...ADMIN, operation: "credit", amount: 10000, reason: "Credito inicial" }, deps);
  await applyAdminAdjustment({ ...ADMIN, operation: "debit", amount: 1550, reason: "Ajuste" }, deps);

  const out = await getTransactionHistory(123, { page: 1, limit: 20 }, deps);

  assert.equal(out.items.length, 2);
  assert.equal(out.items[0].operation, "debit", "mais recente primeiro");
  assert.equal(out.items[0].amount, 1550);
  assert.equal(out.items[0].balance_after, 8450);
  assert.equal(out.paging.page, 1);
  assert.equal(out.paging.total, 2);
});

test("historico limita o page size", async () => {
  const { state, deps } = makeDb();
  await getTransactionHistory(123, { page: 1, limit: 9999 }, deps);

  const select = state.sql.find((q) => /from public\.nscredit_transactions/i.test(q.sql) && /limit/i.test(q.sql));
  assert.ok(select, "esperado SELECT paginado");
  assert.ok(select.params.includes(100), `limite deveria ser travado em 100, params: ${JSON.stringify(select.params)}`);
});

test("historico nao expoe idempotency_key nem dados do admin", async () => {
  const { deps } = makeDb();
  await applyAdminAdjustment({ ...ADMIN, operation: "credit", amount: 100, idempotencyKey: "segredo" }, deps);

  const out = await getTransactionHistory(123, { page: 1, limit: 20 }, deps);
  assert.equal(out.items[0].idempotency_key, undefined);
  assert.equal(out.items[0].created_by, undefined);
});

test("busca administrativa e paginada e traz o saldo", async () => {
  const { state, deps } = makeDb({ wallets: { 123: 8450 } });

  const out = await searchUsersForAdmin({ q: "joao", page: 1, limit: 20 }, deps);

  assert.equal(out.items[0].id, 123);
  assert.equal(out.items[0].balance, 8450);
  assert.equal(out.paging.limit, 20);
  const sql = state.sql[state.sql.length - 1];
  assert.ok(/limit/i.test(sql.sql) && /offset/i.test(sql.sql), "a busca precisa ser paginada");
});

test("busca administrativa nao interpola SQL", async () => {
  const { state, deps } = makeDb();
  await searchUsersForAdmin({ q: "'; drop table users; --", page: 1, limit: 20 }, deps);

  const sql = state.sql[state.sql.length - 1];
  assert.ok(!sql.sql.toLowerCase().includes("drop table"), "o termo de busca nao pode entrar no SQL");
  assert.ok(sql.params.some((p) => String(p).includes("drop table")), "o termo tem que ir como parametro");
});

test("detalhe administrativo devolve usuario, saldo e historico", async () => {
  const { deps } = makeDb({ wallets: { 123: 8450 } });
  await applyAdminAdjustment({ ...ADMIN, operation: "credit", amount: 100, reason: "x" }, deps);

  const out = await getAdminWalletDetail(123, { page: 1, limit: 20 }, deps);

  assert.equal(out.user.id, 123);
  assert.equal(out.user.email, "joao@exemplo.com");
  assert.ok(out.wallet.balance >= 0);
  assert.ok(Array.isArray(out.transactions));
});

test("detalhe de usuario inexistente devolve 404", async () => {
  const { deps } = makeDb();
  await assert.rejects(() => getAdminWalletDetail(999, { page: 1, limit: 20 }, deps), (e) => {
    assert.equal(e.status, 404);
    assert.equal(e.code, "user_not_found");
    return true;
  });
});

/* ─────────────────────────── BIGINT ─────────────────────────── */

test("saldo bigint fora da faixa segura vira erro explicito, nao numero errado", async () => {
  const { deps } = makeDb({ wallets: { 123: "9007199254740993" } });

  await assert.rejects(() => getBalance(123, deps), (e) => {
    assert.equal(e.code, "balance_out_of_range");
    assert.equal(e.status, 500);
    return true;
  });
});
