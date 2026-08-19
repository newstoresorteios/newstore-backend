// tests/trayCustomerClient.test.js
import test from "node:test";
import assert from "node:assert/strict";

import { findTrayCustomerByEmail, createTrayCustomer } from "../src/services/trayCustomerClient.js";
import { TrayCatalogError } from "../src/services/trayCatalogClient.js";

const API_BASE = "https://www.exemplo-loja.com.br/web_api";
const TOKEN = "token-abc";

function makeResponse(body) {
  return {
    ok: true,
    status: 200,
    headers: { get: (h) => (String(h).toLowerCase() === "content-type" ? "application/json" : null) },
    json: async () => body,
    text: async () => JSON.stringify(body),
  };
}

function makeDeps(body) {
  const calls = [];
  return {
    calls,
    deps: {
      getToken: async () => TOKEN,
      getApiBase: async () => API_BASE,
      fetchImpl: async (url, options) => {
        calls.push({ url: String(url), method: options?.method || "GET" });
        return makeResponse(body);
      },
    },
  };
}

test("email vazio nunca chega a rede", async () => {
  const { calls, deps } = makeDeps({ Customers: [] });
  await assert.rejects(() => findTrayCustomerByEmail("", { deps }), (e) => e instanceof TrayCatalogError && e.code === "email_missing");
  assert.equal(calls.length, 0);
});

test("nenhum cliente encontrado devolve null, nunca inventa", async () => {
  const { deps } = makeDeps({ Customers: [] });
  const out = await findTrayCustomerByEmail("ninguem@exemplo.com", { deps });
  assert.equal(out, null);
});

test("cliente encontrado devolve id/name/email normalizados", async () => {
  const { calls, deps } = makeDeps({ Customers: [{ Customer: { id: 555, name: "Joao Pedro", email: "Joao@Exemplo.com" } }] });
  const out = await findTrayCustomerByEmail("joao@exemplo.com", { deps });

  assert.equal(out.id, "555");
  assert.equal(out.name, "Joao Pedro");
  assert.equal(out.email, "joao@exemplo.com");
  assert.equal(calls[0].method, "GET");
  assert.ok(calls[0].url.includes("email=joao%40exemplo.com") || calls[0].url.includes("email=joao@exemplo.com"));
});

test("nao usa cegamente o primeiro resultado se o e-mail nao bate exatamente", async () => {
  const { deps } = makeDeps({ Customers: [{ Customer: { id: 1, email: "outro@exemplo.com" } }] });
  const out = await findTrayCustomerByEmail("joao@exemplo.com", { deps });
  assert.equal(out, null);
});

test("resposta em formato inesperado falha alto, nunca finge sucesso", async () => {
  const { deps } = makeDeps({ unexpected: true });
  await assert.rejects(() => findTrayCustomerByEmail("joao@exemplo.com", { deps }), (e) => e.code === "tray_invalid_response");
});

test("mais de um cliente com o mesmo e-mail exato: nunca escolhe arbitrariamente", async () => {
  const { deps } = makeDeps({
    Customers: [
      { Customer: { id: 1, email: "joao@exemplo.com" } },
      { Customer: { id: 2, email: "joao@exemplo.com" } },
    ],
  });
  await assert.rejects(
    () => findTrayCustomerByEmail("joao@exemplo.com", { deps }),
    (e) => e instanceof TrayCatalogError && e.code === "tray_customer_ambiguous"
  );
});

/* ─────────────────────────── createTrayCustomer ─────────────────────────── */

function makeMutationDeps(handler) {
  const calls = [];
  return {
    calls,
    deps: {
      getToken: async () => TOKEN,
      getApiBase: async () => API_BASE,
      fetchImpl: async (url, options) => {
        calls.push({ url: String(url), method: options?.method, body: options?.body ? JSON.parse(options.body) : null });
        return handler(String(url), options);
      },
    },
  };
}

function mutationResponse({ status = 200, body = {} } = {}) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: (h) => (String(h).toLowerCase() === "content-type" ? "application/json" : null) },
    json: async () => body,
    text: async () => JSON.stringify(body),
  };
}

test("createTrayCustomer sem name/email/birth_date nunca chega a rede", async () => {
  const { calls, deps } = makeMutationDeps(() => mutationResponse({}));
  await assert.rejects(() => createTrayCustomer({ name: "", email: "a@x.com", birthDate: "1990-01-01" }, { deps }), (e) => e.code === "customer_name_missing");
  await assert.rejects(() => createTrayCustomer({ name: "Joao", email: "", birthDate: "1990-01-01" }, { deps }), (e) => e.code === "customer_email_missing");
  await assert.rejects(() => createTrayCustomer({ name: "Joao", email: "a@x.com", birthDate: "" }, { deps }), (e) => e.code === "customer_birth_date_missing");
  await assert.rejects(() => createTrayCustomer({ name: "Joao", email: "a@x.com", birthDate: "01/01/1990" }, { deps }), (e) => e.code === "customer_birth_date_missing");
  assert.equal(calls.length, 0);
});

test("createTrayCustomer envia so name/email/birth_date (+phone se houver), nunca cpf/rg/gender", async () => {
  const { calls, deps } = makeMutationDeps(() => mutationResponse({ status: 201, body: { id: 999 } }));
  const out = await createTrayCustomer({ name: "Joao Pedro", email: "Joao@Exemplo.com", birthDate: "1990-05-20", phone: "11999999999" }, { deps });

  assert.equal(out.id, "999");
  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "POST");
  assert.ok(calls[0].url.includes("/customers?access_token=token-abc"));

  const customer = calls[0].body.Customer;
  assert.deepEqual(customer, {
    name: "Joao Pedro",
    email: "joao@exemplo.com",
    birth_date: "1990-05-20",
    phone: "11999999999",
  });
  assert.equal("cpf" in customer, false);
  assert.equal("rg" in customer, false);
  assert.equal("gender" in customer, false);
});

test("createTrayCustomer omite phone quando ausente", async () => {
  const { calls, deps } = makeMutationDeps(() => mutationResponse({ status: 201, body: { id: 1 } }));
  await createTrayCustomer({ name: "Joao", email: "j@x.com", birthDate: "1990-01-01" }, { deps });
  assert.equal("phone" in calls[0].body.Customer, false);
});

test("createTrayCustomer sem id na resposta falha alto", async () => {
  const { deps } = makeMutationDeps(() => mutationResponse({ status: 200, body: { ok: true } }));
  await assert.rejects(
    () => createTrayCustomer({ name: "Joao", email: "j@x.com", birthDate: "1990-01-01" }, { deps }),
    (e) => e.code === "tray_customer_id_missing"
  );
});
