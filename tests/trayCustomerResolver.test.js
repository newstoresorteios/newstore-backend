// tests/trayCustomerResolver.test.js
// Unidade: logica de resolucao com deps mockados (sem Postgres real).
// A prova de que o lock realmente serializa concorrencia esta em
// trayCustomerResolver.integration.test.js (Postgres real, pg_advisory_lock).
import test from "node:test";
import assert from "node:assert/strict";

import {
  resolveTrayCustomerId,
  reconcileCustomerMatches,
  TrayCustomerProfileIncompleteError,
  TrayCustomerIdentityConflictError,
} from "../src/services/trayCustomerResolver.js";
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

const VALID_CPF = "11144477735";
const OTHER_CPF = "52998224725";

const PROFILE_COMPLETE = { name: "Joao Pedro", email: "joao@exemplo.com", birthDate: "1990-05-20", cpf: VALID_CPF, phone: null };
const PROFILE_NO_BIRTH_DATE = { name: "Joao Pedro", email: "joao@exemplo.com", birthDate: null, cpf: VALID_CPF, phone: null };
const PROFILE_NO_CPF = { name: "Joao Pedro", email: "joao@exemplo.com", birthDate: "1990-05-20", cpf: null, phone: null };

test("usa tray_customer_id ja cacheado, nunca chama a Tray", async () => {
  const { query, users } = makeQuery([{ id: 1, tray_customer_id: "777" }]);
  let calledFind = false;
  const out = await resolveTrayCustomerId(1, PROFILE_COMPLETE, {
    query,
    getPool: async () => fakePool(),
    findTrayCustomerByEmail: async () => { calledFind = true; return null; },
    findTrayCustomerByCpf: async () => { calledFind = true; return null; },
    createTrayCustomer: async () => { throw new Error("nao deveria criar"); },
  });
  assert.equal(out, "777");
  assert.equal(calledFind, false);
  assert.equal(users.get(1).tray_customer_id, "777");
});

/* ─────────────────────────── reconcileCustomerMatches (casos A-F) ─────────────────────────── */

test("caso F: nenhum sinal encontra nada -- segue pra criacao", () => {
  const out = reconcileCustomerMatches({ emailResult: null, cpfResult: null, profileEmail: "joao@exemplo.com" });
  assert.deepEqual(out, { type: "create" });
});

test("caso A/C: e-mail e cpf confirmam o MESMO Customer -- mapeia", () => {
  const out = reconcileCustomerMatches({
    emailResult: { id: "1", email: "joao@exemplo.com", cpf: VALID_CPF },
    cpfResult: { id: "1", email: "joao@exemplo.com", cpf: VALID_CPF },
    profileEmail: "joao@exemplo.com",
  });
  assert.deepEqual(out, { type: "map", id: "1" });
});

test("caso E: e-mail e cpf apontam pra Customers DIFERENTES -- bloqueio, nunca escolhe", () => {
  const out = reconcileCustomerMatches({
    emailResult: { id: "1", email: "joao@exemplo.com", cpf: null },
    cpfResult: { id: "2", email: "outro@exemplo.com", cpf: VALID_CPF },
    profileEmail: "joao@exemplo.com",
  });
  assert.equal(out.type, "conflict");
  assert.equal(out.reason, "email_and_cpf_different_customers");
  assert.deepEqual(out.details, { emailCustomerId: "1", cpfCustomerId: "2" });
});

test("caso B: so e-mail bate, Customer sem cpf cadastrado -- mapeia sem sobrescrever", () => {
  const out = reconcileCustomerMatches({
    emailResult: { id: "1", email: "joao@exemplo.com", cpf: null },
    cpfResult: null,
    profileEmail: "joao@exemplo.com",
  });
  assert.deepEqual(out, { type: "map", id: "1" });
});

test("so e-mail bate mas o Customer tem OUTRO cpf cadastrado -- conflito, nunca mapeia as cegas", () => {
  const out = reconcileCustomerMatches({
    emailResult: { id: "1", email: "joao@exemplo.com", cpf: OTHER_CPF },
    cpfResult: null,
    profileEmail: "joao@exemplo.com",
  });
  assert.equal(out.type, "conflict");
  assert.equal(out.reason, "email_match_cpf_mismatch");
  assert.deepEqual(out.details, { customerId: "1" });
});

test("caso C: so cpf bate, mesmo e-mail -- mapeia", () => {
  const out = reconcileCustomerMatches({
    emailResult: null,
    cpfResult: { id: "1", email: "joao@exemplo.com", cpf: VALID_CPF },
    profileEmail: "joao@exemplo.com",
  });
  assert.deepEqual(out, { type: "map", id: "1" });
});

test("cpf bate, Customer sem e-mail cadastrado -- nenhum dado conflitante, mapeia", () => {
  const out = reconcileCustomerMatches({
    emailResult: null,
    cpfResult: { id: "1", email: null, cpf: VALID_CPF },
    profileEmail: "joao@exemplo.com",
  });
  assert.deepEqual(out, { type: "map", id: "1" });
});

test("caso D: cpf bate mas o e-mail e DIFERENTE -- nunca linka automaticamente", () => {
  const out = reconcileCustomerMatches({
    emailResult: null,
    cpfResult: { id: "1", email: "outro@exemplo.com", cpf: VALID_CPF },
    profileEmail: "joao@exemplo.com",
  });
  assert.equal(out.type, "conflict");
  assert.equal(out.reason, "cpf_match_email_mismatch");
  assert.deepEqual(out.details, { customerId: "1" });
});

/* ─────────────────────────── resolveTrayCustomerId fim a fim ─────────────────────────── */

test("encontrado por e-mail e cpf (mesmo Customer): persiste e usa, nunca cria", async () => {
  const { query, users } = makeQuery([{ id: 1, tray_customer_id: null }]);
  let calledCreate = false;
  const out = await resolveTrayCustomerId(1, PROFILE_COMPLETE, {
    query,
    getPool: async () => fakePool(),
    findTrayCustomerByEmail: async () => ({ id: "888", name: "Joao", email: "joao@exemplo.com", cpf: VALID_CPF }),
    findTrayCustomerByCpf: async () => ({ id: "888", name: "Joao", email: "joao@exemplo.com", cpf: VALID_CPF }),
    createTrayCustomer: async () => { calledCreate = true; throw new Error("nao deveria criar"); },
  });
  assert.equal(out, "888");
  assert.equal(calledCreate, false);
  assert.equal(users.get(1).tray_customer_id, "888");
});

test("e-mail e cpf apontam pra Customers diferentes: bloqueio isolado, nunca cria", async () => {
  const { query } = makeQuery([{ id: 1, tray_customer_id: null }]);
  let calledCreate = false;
  await assert.rejects(
    () =>
      resolveTrayCustomerId(1, PROFILE_COMPLETE, {
        query,
        getPool: async () => fakePool(),
        findTrayCustomerByEmail: async () => ({ id: "1", email: "joao@exemplo.com", cpf: null }),
        findTrayCustomerByCpf: async () => ({ id: "2", email: "outro@exemplo.com", cpf: VALID_CPF }),
        createTrayCustomer: async () => { calledCreate = true; },
      }),
    (e) => e instanceof TrayCustomerIdentityConflictError && e.code === "tray_customer_identity_conflict" && e.reason === "email_and_cpf_different_customers"
  );
  assert.equal(calledCreate, false);
});

test("nao encontrado e perfil sem birth_date: bloqueio isolado, nunca cria", async () => {
  const { query } = makeQuery([{ id: 1, tray_customer_id: null }]);
  let calledCreate = false;
  await assert.rejects(
    () =>
      resolveTrayCustomerId(1, PROFILE_NO_BIRTH_DATE, {
        query,
        getPool: async () => fakePool(),
        findTrayCustomerByEmail: async () => null,
        findTrayCustomerByCpf: async () => null,
        createTrayCustomer: async () => { calledCreate = true; },
      }),
    (e) => e instanceof TrayCustomerProfileIncompleteError && e.missingFields.includes("birth_date") && !e.missingFields.includes("cpf")
  );
  assert.equal(calledCreate, false);
});

test("nao encontrado e perfil sem cpf: bloqueio isolado, nunca cria", async () => {
  const { query } = makeQuery([{ id: 1, tray_customer_id: null }]);
  let calledCreate = false;
  let calledCpfLookup = false;
  await assert.rejects(
    () =>
      resolveTrayCustomerId(1, PROFILE_NO_CPF, {
        query,
        getPool: async () => fakePool(),
        findTrayCustomerByEmail: async () => null,
        findTrayCustomerByCpf: async () => { calledCpfLookup = true; return null; },
        createTrayCustomer: async () => { calledCreate = true; },
      }),
    (e) => e instanceof TrayCustomerProfileIncompleteError && e.missingFields.includes("cpf") && !e.missingFields.includes("birth_date")
  );
  assert.equal(calledCreate, false);
  // cpf ausente -- nunca chama a busca por cpf com valor vazio.
  assert.equal(calledCpfLookup, false);
});

test("nao encontrado e perfil completo: cria (com cpf) e persiste", async () => {
  const { query, users } = makeQuery([{ id: 1, tray_customer_id: null }]);
  const out = await resolveTrayCustomerId(1, PROFILE_COMPLETE, {
    query,
    getPool: async () => fakePool(),
    findTrayCustomerByEmail: async () => null,
    findTrayCustomerByCpf: async () => null,
    createTrayCustomer: async (profile) => {
      assert.equal(profile.email, "joao@exemplo.com");
      assert.equal(profile.birthDate, "1990-05-20");
      assert.equal(profile.cpf, VALID_CPF);
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
  // A checagem fora do lock nao pode ver isso ainda, mas a RECHECAGEM
  // logo apos o lock ser concedido tem que ver.
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
    findTrayCustomerByEmail: async () => null,
    findTrayCustomerByCpf: async () => null,
    createTrayCustomer: async () => { calledCreate = true; return { id: "nunca-deveria-existir" }; },
  });
  assert.equal(out, "concorrente-venceu");
  assert.equal(calledCreate, false);
});

test("erro tray_customer_ambiguous (e-mail) propaga sem tentar criar", async () => {
  const { query } = makeQuery([{ id: 1, tray_customer_id: null }]);
  let calledCreate = false;
  await assert.rejects(
    () =>
      resolveTrayCustomerId(1, PROFILE_COMPLETE, {
        query,
        getPool: async () => fakePool(),
        findTrayCustomerByEmail: async () => { throw new TrayCatalogError("tray_customer_ambiguous", { status: 409 }); },
        findTrayCustomerByCpf: async () => null,
        createTrayCustomer: async () => { calledCreate = true; },
      }),
    (e) => e.code === "tray_customer_ambiguous"
  );
  assert.equal(calledCreate, false);
});

test("erro tray_customer_ambiguous (cpf) propaga sem tentar criar", async () => {
  const { query } = makeQuery([{ id: 1, tray_customer_id: null }]);
  let calledCreate = false;
  await assert.rejects(
    () =>
      resolveTrayCustomerId(1, PROFILE_COMPLETE, {
        query,
        getPool: async () => fakePool(),
        findTrayCustomerByEmail: async () => null,
        findTrayCustomerByCpf: async () => { throw new TrayCatalogError("tray_customer_ambiguous", { status: 409 }); },
        createTrayCustomer: async () => { calledCreate = true; },
      }),
    (e) => e.code === "tray_customer_ambiguous"
  );
  assert.equal(calledCreate, false);
});
