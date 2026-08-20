// tests/trayCustomerResolver.integration.test.js
//
// Prova real (Postgres de verdade) de que pg_advisory_lock realmente
// serializa duas tentativas concorrentes de resolveTrayCustomerId para o
// MESMO usuario -- nunca cria dois Customer Tray para o mesmo user
// (item 14 do pedido).
//
//   TEST_DATABASE_URL=postgres://... npm test
//
// Sem TEST_DATABASE_URL os testes sao pulados (nunca usam banco de producao).
import test, { before, after, beforeEach } from "node:test";
import assert from "node:assert/strict";

import { resolveTrayCustomerId } from "../src/services/trayCustomerResolver.js";

const TEST_DB = process.env.TEST_DATABASE_URL || "";
const SKIP = !TEST_DB;
const skipOpts = { skip: SKIP ? "defina TEST_DATABASE_URL para rodar os testes de integracao" : false };

let pool;
let userId;

function sslFor(url) {
  try {
    const host = new URL(url).hostname;
    return { rejectUnauthorized: false, servername: /^\d+\.\d+\.\d+\.\d+$/.test(host) ? undefined : host };
  } catch {
    return { rejectUnauthorized: false };
  }
}

const VALID_CPF = "11144477735";

before(async () => {
  if (SKIP) return;
  const pg = (await import("pg")).default;
  pool = new pg.Pool({ connectionString: TEST_DB, ssl: sslFor(TEST_DB), max: 10 });

  // cpf NAO e gravado no seed -- resolveTrayCustomerId nunca le users.cpf
  // (so tray_customer_id, pra cache); o cpf do PERFIL usado na resolucao
  // vem do objeto passado em cada teste, nunca da linha em si.
  const stamp = Date.now();
  userId = (await pool.query(
    `insert into public.users (name, email, pass_hash, is_admin, birth_date) values ($1,$2,'x',false,'1990-01-01') returning id`,
    ["Resolver Test", `resolver-test-${stamp}@exemplo.local`]
  )).rows[0].id;
});

after(async () => {
  if (SKIP || !pool) return;
  await pool.query("delete from public.users where id=$1", [userId]).catch(() => {});
  await pool.end().catch(() => {});
});

beforeEach(async () => {
  if (SKIP) return;
  await pool.query("update public.users set tray_customer_id = null where id=$1", [userId]);
});

test("dois resolveTrayCustomerId concorrentes para o mesmo usuario: so um cria, o outro reusa", skipOpts, async () => {
  const deps = { query: (sql, params) => pool.query(sql, params), getPool: async () => pool };
  const profile = { name: "Resolver Test", email: `x@x.com`, birthDate: "1990-01-01", cpf: VALID_CPF, phone: null };

  let createCalls = 0;
  const createTrayCustomer = async (p) => {
    createCalls++;
    assert.equal(p.cpf, VALID_CPF, "cpf precisa chegar na criacao real");
    // Atraso real para dar chance de uma corrida genuina acontecer se o
    // lock nao estiver funcionando.
    await new Promise((r) => setTimeout(r, 150));
    return { id: "TRAY-CUST-REAL-1" };
  };
  const findTrayCustomerByEmail = async () => null; // nunca existe na Tray nos dois processos
  const findTrayCustomerByCpf = async () => null;

  const [a, b] = await Promise.all([
    resolveTrayCustomerId(userId, profile, { ...deps, createTrayCustomer, findTrayCustomerByEmail, findTrayCustomerByCpf }),
    resolveTrayCustomerId(userId, profile, { ...deps, createTrayCustomer, findTrayCustomerByEmail, findTrayCustomerByCpf }),
  ]);

  assert.equal(a, "TRAY-CUST-REAL-1");
  assert.equal(b, "TRAY-CUST-REAL-1");
  assert.equal(createCalls, 1, "exatamente uma criacao real, mesmo com duas tentativas concorrentes");

  const { rows } = await pool.query("select tray_customer_id from public.users where id=$1", [userId]);
  assert.equal(rows[0].tray_customer_id, "TRAY-CUST-REAL-1");
});

test("cinco resolveTrayCustomerId concorrentes: ainda assim so uma criacao (com cpf)", skipOpts, async () => {
  const deps = { query: (sql, params) => pool.query(sql, params), getPool: async () => pool };
  const profile = { name: "Resolver Test", email: `x@x.com`, birthDate: "1990-01-01", cpf: VALID_CPF, phone: null };

  let createCalls = 0;
  const createTrayCustomer = async (p) => {
    createCalls++;
    assert.equal(p.cpf, VALID_CPF);
    await new Promise((r) => setTimeout(r, 80));
    return { id: "TRAY-CUST-REAL-2" };
  };
  const findTrayCustomerByEmail = async () => null;
  const findTrayCustomerByCpf = async () => null;

  const results = await Promise.all(
    Array.from({ length: 5 }, () =>
      resolveTrayCustomerId(userId, profile, { ...deps, createTrayCustomer, findTrayCustomerByEmail, findTrayCustomerByCpf })
    )
  );

  assert.ok(results.every((id) => id === "TRAY-CUST-REAL-2"));
  assert.equal(createCalls, 1);
});
