// tests/trayPaymentAllowlist.test.js
//
// A allow-list de mutacoes Tray e a unica porta de escrita. Este arquivo
// prova que a abertura para Payment de resgate e CIRURGICA:
//   - somente TRAY_REDEMPTION_PAYMENT_CREATE + POST passa;
//   - /payments NAO foi liberado genericamente;
//   - PUT e DELETE em Payment continuam proibidos, para sempre.
import test from "node:test";
import assert from "node:assert/strict";

import { trayMutationRequest } from "../src/services/trayMutationClient.js";
import { assertAllowedTrayMutation, TrayCatalogError } from "../src/services/trayCatalogClient.js";

function makeResponse({ status = 200, body = {} } = {}) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: (h) => (String(h).toLowerCase() === "content-type" ? "application/json" : null) },
    json: async () => body,
    text: async () => JSON.stringify(body),
  };
}

function makeDeps(handler = () => makeResponse({ status: 201, body: { id: "77001" } })) {
  const calls = [];
  return {
    calls,
    deps: {
      getToken: async () => "token-abc",
      getApiBase: async () => "https://www.exemplo-loja.com.br/web_api",
      fetchImpl: async (url, options) => {
        calls.push({ url: String(url), method: String(options?.method || "GET").toUpperCase() });
        return handler(String(url), options);
      },
    },
  };
}

const PAYMENT_BODY = {
  payment: { order_id: "25999", method: "NSCréditos", value: "489.99", date: "2026-09-09", note: "LOJA_NS_REDEMPTION:x" },
};

test("nenhuma operacao NAO nomeada consegue postar em /payments", async () => {
  const { calls, deps } = makeDeps();

  for (const operation of ["TRAY_PAYMENT_CREATE", "TRAY_ORDER_PAYMENT_CREATE", "PAYMENTS", "TRAY_ORDER_CREATE"]) {
    // eslint-disable-next-line no-await-in-loop
    await assert.rejects(
      () => trayMutationRequest(operation, "POST", "/payments", PAYMENT_BODY, { deps }),
      (e) => e instanceof TrayCatalogError && e.code === "tray_mutation_not_allowed",
      `${operation} POST /payments deveria ser recusado`
    );
  }

  assert.equal(calls.length, 0, "nenhuma dessas recusas chegou a rede");
});

test("TRAY_REDEMPTION_PAYMENT_CREATE autoriza POST — a unica mutacao nova", async () => {
  const { calls, deps } = makeDeps();

  await trayMutationRequest("TRAY_REDEMPTION_PAYMENT_CREATE", "POST", "/payments", PAYMENT_BODY, { deps });

  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "POST");
  assert.match(calls[0].url, /\/payments\?access_token=/);
});

test("PUT em Payment continua proibido", async () => {
  const { calls, deps } = makeDeps();

  for (const operation of ["TRAY_REDEMPTION_PAYMENT_CREATE", "TRAY_PAYMENT_UPDATE", "TRAY_REDEMPTION_PAYMENT_UPDATE"]) {
    // eslint-disable-next-line no-await-in-loop
    await assert.rejects(
      () => trayMutationRequest(operation, "PUT", "/payments/77001", PAYMENT_BODY, { deps }),
      (e) => e instanceof TrayCatalogError && e.code === "tray_mutation_not_allowed",
      `${operation} PUT deveria ser recusado`
    );
  }

  assert.equal(calls.length, 0);
});

test("DELETE em Payment continua proibido", async () => {
  const { calls, deps } = makeDeps();

  for (const operation of ["TRAY_REDEMPTION_PAYMENT_CREATE", "TRAY_PAYMENT_DELETE"]) {
    // eslint-disable-next-line no-await-in-loop
    await assert.rejects(
      () => trayMutationRequest(operation, "DELETE", "/payments/77001", null, { deps }),
      (e) => e instanceof TrayCatalogError && e.code === "tray_mutation_not_allowed",
      `${operation} DELETE deveria ser recusado`
    );
  }

  assert.equal(calls.length, 0);
});

test("PATCH em Payment continua proibido", async () => {
  const { calls, deps } = makeDeps();

  await assert.rejects(
    () => trayMutationRequest("TRAY_REDEMPTION_PAYMENT_CREATE", "PATCH", "/payments/77001", PAYMENT_BODY, { deps }),
    (e) => e instanceof TrayCatalogError && e.code === "tray_mutation_not_allowed"
  );

  assert.equal(calls.length, 0);
});

test("a guarda de allow-list, isolada, so aceita o par (operacao, metodo) exato", () => {
  assert.equal(assertAllowedTrayMutation("TRAY_REDEMPTION_PAYMENT_CREATE", "POST"), "POST");

  for (const method of ["PUT", "PATCH", "DELETE", "GET"]) {
    assert.throws(
      () => assertAllowedTrayMutation("TRAY_REDEMPTION_PAYMENT_CREATE", method),
      (e) => e instanceof TrayCatalogError && e.code === "tray_mutation_not_allowed",
      `TRAY_REDEMPTION_PAYMENT_CREATE ${method} deveria ser recusado`
    );
  }
});

test("as mutacoes ja existentes seguem intactas e nao ganharam metodos novos", () => {
  assert.equal(assertAllowedTrayMutation("TRAY_ORDER_CREATE", "POST"), "POST");
  assert.equal(assertAllowedTrayMutation("TRAY_ORDER_STATUS_UPDATE", "PUT"), "PUT");
  assert.equal(assertAllowedTrayMutation("TRAY_CUSTOMER_CREATE", "POST"), "POST");

  for (const [operation, method] of [
    ["TRAY_ORDER_CREATE", "DELETE"],
    ["TRAY_ORDER_STATUS_UPDATE", "POST"],
    ["TRAY_CUSTOMER_CREATE", "PUT"],
  ]) {
    assert.throws(
      () => assertAllowedTrayMutation(operation, method),
      (e) => e.code === "tray_mutation_not_allowed"
    );
  }
});
