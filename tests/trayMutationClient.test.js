// tests/trayMutationClient.test.js
import test from "node:test";
import assert from "node:assert/strict";

import { trayMutationRequest } from "../src/services/trayMutationClient.js";
import { TrayCatalogError } from "../src/services/trayCatalogClient.js";

const API_BASE = "https://www.exemplo-loja.com.br/web_api";
const TOKEN = "token-abc";

function makeResponse({ status = 200, body = {} } = {}) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: (h) => (String(h).toLowerCase() === "content-type" ? "application/json" : null) },
    json: async () => body,
    text: async () => JSON.stringify(body),
  };
}

function makeDeps(handler) {
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

test("operacao fora da allow-list nunca chega a rede", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({}));
  await assert.rejects(
    () => trayMutationRequest("SOME_RANDOM_OPERATION", "POST", "/whatever", {}, { deps }),
    (e) => e instanceof TrayCatalogError && e.code === "tray_mutation_not_allowed"
  );
  assert.equal(calls.length, 0);
});

test("metodo errado para uma operacao autorizada tambem e recusado", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({}));
  await assert.rejects(
    () => trayMutationRequest("TRAY_ORDER_CREATE", "DELETE", "/orders", {}, { deps }),
    (e) => e.code === "tray_mutation_not_allowed"
  );
  assert.equal(calls.length, 0);
});

test("operacao autorizada emite POST com body e token corretos", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 201, body: { id: 999 } }));
  const out = await trayMutationRequest("TRAY_ORDER_CREATE", "POST", "/orders", { Order: { customer_id: 1 } }, { deps });

  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "POST");
  assert.deepEqual(calls[0].body, { Order: { customer_id: 1 } });
  assert.ok(calls[0].url.includes("/orders?access_token=token-abc"));
  assert.deepEqual(out, { id: 999 });
});

test("HTTP 400 vira tray_request_invalid com o corpo da Tray preservado", async () => {
  const { deps } = makeDeps(() => makeResponse({ status: 400, body: { message: "customer_id obrigatorio" } }));
  await assert.rejects(
    () => trayMutationRequest("TRAY_ORDER_CREATE", "POST", "/orders", {}, { deps }),
    (e) => e.code === "tray_request_invalid" && e.publicDetails?.tray_body?.message === "customer_id obrigatorio"
  );
});

test("HTTP 401/403 vira tray_auth_failed", async () => {
  const { deps } = makeDeps(() => makeResponse({ status: 401 }));
  await assert.rejects(
    () => trayMutationRequest("TRAY_ORDER_CREATE", "POST", "/orders", {}, { deps }),
    (e) => e.code === "tray_auth_failed"
  );
});

test("HTTP 5xx vira tray_unavailable", async () => {
  const { deps } = makeDeps(() => makeResponse({ status: 502 }));
  await assert.rejects(
    () => trayMutationRequest("TRAY_ORDER_CREATE", "POST", "/orders", {}, { deps }),
    (e) => e.code === "tray_unavailable"
  );
});

test("timeout vira tray_timeout", async () => {
  const { deps } = makeDeps(() => new Promise((_resolve, reject) => {
    setTimeout(() => reject(Object.assign(new Error("aborted"), { name: "AbortError" })), 5);
  }));
  await assert.rejects(
    () => trayMutationRequest("TRAY_ORDER_CREATE", "POST", "/orders", {}, { deps, timeoutMs: 1 }),
    (e) => e.code === "tray_timeout"
  );
});
