// tests/trayCustomerResolver.test.js
// Unidade: logica de resolucao com deps mockados (sem Postgres real).
// A prova de que o lock realmente serializa concorrencia esta em
// trayCustomerResolver.integration.test.js (Postgres real, pg_advisory_lock).
import test from "node:test";
import assert from "node:assert/strict";

import { resolveTrayCustomerId, TrayCustomerProfileIncompleteError } from "../src/services/trayCustomerResolver.js";
import { TrayCatalogError } from "../src/services/trayCatalogClient.js";

function makeQuery(initialUsers) {
  const users = new Map(initialUsers.map((u) => [u.id, { ...u }]));
  return {
    users,
    query: async (sql, params) => {
      const s = String(sql).toLowerCase();
      if (/^select tray_customer_id from public\.users where id = \$1/.test(s)) {
        const u = users.get(params[0]);
        return { rows: u ? [{ tray_customer_id: u.tray_customer_id }] : [] };
      }
      if (/^update public\.users set tray_customer_id = \$2 where id = \$1 and tray_customer_id is null/.test(s)) {
        const u = users.get(params[0]);
        if (u && !u.tray_customer_id) u.tray_customer_id = params[1];
        return { rows: [] };
      }
      throw new Error(`SQL nao mapeado: ${sql}`);
    },
  };
}

function fakePool() {
  return {
    connect: async () => ({
      query: async () => ({ rows: [] }), // pg_advisory_lock/unlock no-op
      release: () => {},
    }),
  };
}

const PROFILE_COMPLETE = { name: "Joao Pedro", email: "joao@exemplo.com", birthDate: "1990-05-20", phone: null };
const PROFILE_INCOMPLETE = { name: "Joao Pedro", email: "joao@exemplo.com", birthDate: null, phone: null };

test("usa tray_customer_id ja cacheado, nunca chama a Tray", async () => {
  const { query, users } = makeQuery([{ id: 1, tray_customer_id: "777" }]);
  let calledFind = false;
  const out = await resolveTrayCustomerId(1, PROFILE_COMPLETE, {
    query,
    getPool: async () => fakePool(),
    findTrayCustomerByEmail: async () => { calledFind = true; return null; },
    createTrayCustomer: async () => { throw new Error("nao deveria criar"); },
  });
  assert.equal(out, "777");
  assert.equal(calledFind, false);
  assert.equal(users.get(1).tray_customer_id, "777");
});

test("encontrado por e-mail: persiste e usa, nunca cria", async () => {
  const { query, users } = makeQuery([{ id: 1, tray_customer_id: null }]);
  let calledCreate = false;
  const out = await resolveTrayCustomerId(1, PROFILE_COMPLETE, {
    query,
    getPool: async () => fakePool(),
    findTrayCustomerByEmail: async () => ({ id: "888", name: "Joao", email: "joao@exemplo.com" }),
    createTrayCustomer: async () => { calledCreate = true; throw new Error("nao deveria criar"); },
  });
  assert.equal(out, "888");
  assert.equal(calledCreate, false);
  assert.equal(users.get(1).tray_customer_id, "888");
});

test("nao encontrado e perfil incompleto: bloqueio isolado, nunca cria", async () => {
  const { query } = makeQuery([{ id: 1, tray_customer_id: null }]);
  let calledCreate = false;
  await assert.rejects(
    () =>
      resolveTrayCustomerId(1, PROFILE_INCOMPLETE, {
        query,
        getPool: async () => fakePool(),
        findTrayCustomerByEmail: async () => null,
        createTrayCustomer: async () => { calledCreate = true; },
      }),
    (e) => e instanceof TrayCustomerProfileIncompleteError && e.missingFields.includes("birth_date")
  );
  assert.equal(calledCreate, false);
});

test("nao encontrado e perfil completo: cria e persiste", async () => {
  const { query, users } = makeQuery([{ id: 1, tray_customer_id: null }]);
  const out = await resolveTrayCustomerId(1, PROFILE_COMPLETE, {
    query,
    getPool: async () => fakePool(),
    findTrayCustomerByEmail: async () => null,
    createTrayCustomer: async (profile) => {
      assert.equal(profile.email, "joao@exemplo.com");
      assert.equal(profile.birthDate, "1990-05-20");
      return { id: "999" };
    },
  });
  assert.equal(out, "999");
  assert.equal(users.get(1).tray_customer_id, "999");
});

test("recheca DENTRO do lock: se o lock demorou a liberar porque outra tentativa ja tinha resolvido, nao cria de novo", async () => {
  // Modela a corrida REAL: pg_advisory_lock so retorna DEPOIS que a
  // tentativa concorrente (que segurava o lock) terminou e liberou --
  // e ela ja tera persistido o tray_customer_id antes de soltar o lock.
  // A checagem fora do lock (findTrayCustomerByEmail) nao pode ver isso
  // ainda, mas a RECHECAGEM logo apos o lock ser concedido tem que ver.
  const { query, users } = makeQuery([{ id: 1, tray_customer_id: null }]);
  let calledCreate = false;
  const lockedPool = {
    connect: async () => ({
      query: async (sql) => {
        if (String(sql).includes("pg_advisory_lock")) {
          // Concedido so agora -- simula que a corrida concorrente ja
          // persistiu enquanto esperavamos.
          users.get(1).tray_customer_id = "concorrente-venceu";
        }
        return { rows: [] };
      },
      release: () => {},
    }),
  };
  const out = await resolveTrayCustomerId(1, PROFILE_COMPLETE, {
    query,
    getPool: async () => lockedPool,
    findTrayCustomerByEmail: async () => null, // nunca achou por e-mail, nos dois processos
    createTrayCustomer: async () => { calledCreate = true; return { id: "nunca-deveria-existir" }; },
  });
  assert.equal(out, "concorrente-venceu");
  assert.equal(calledCreate, false);
});

test("erro tray_customer_ambiguous propaga sem tentar criar", async () => {
  const { query } = makeQuery([{ id: 1, tray_customer_id: null }]);
  let calledCreate = false;
  await assert.rejects(
    () =>
      resolveTrayCustomerId(1, PROFILE_COMPLETE, {
        query,
        getPool: async () => fakePool(),
        findTrayCustomerByEmail: async () => { throw new TrayCatalogError("tray_customer_ambiguous", { status: 409 }); },
        createTrayCustomer: async () => { calledCreate = true; },
      }),
    (e) => e.code === "tray_customer_ambiguous"
  );
  assert.equal(calledCreate, false);
});
