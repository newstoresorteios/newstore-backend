// tests/trayCustomerClient.test.js
import test from "node:test";
import assert from "node:assert/strict";

import { findTrayCustomerByEmail } from "../src/services/trayCustomerClient.js";
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
