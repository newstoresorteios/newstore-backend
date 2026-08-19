// tests/trayOrderClient.test.js
import test from "node:test";
import assert from "node:assert/strict";

import { createTrayOrder } from "../src/services/trayOrderClient.js";
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

test("customer_id ausente/invalido nunca chega a rede", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({}));
  await assert.rejects(
    () => createTrayOrder({ customerId: null, items: [{ trayProductId: "1", quantity: 1 }], notes: "x" }, { deps }),
    (e) => e instanceof TrayCatalogError && e.code === "customer_id_invalid"
  );
  assert.equal(calls.length, 0);
});

test("carrinho vazio nunca chega a rede", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({}));
  await assert.rejects(
    () => createTrayOrder({ customerId: 10, items: [], notes: "x" }, { deps }),
    (e) => e.code === "order_items_empty"
  );
  assert.equal(calls.length, 0);
});

test("item sem product_id/variant valido nunca chega a rede", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({}));
  await assert.rejects(
    () => createTrayOrder({ customerId: 10, items: [{ trayProductId: null, quantity: 1 }], notes: "x" }, { deps }),
    (e) => e.code === "order_item_product_id_invalid"
  );
  assert.equal(calls.length, 0);
});

test("quantidade invalida nunca chega a rede", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({}));
  await assert.rejects(
    () => createTrayOrder({ customerId: 10, items: [{ trayProductId: "1", quantity: 0 }], notes: "x" }, { deps }),
    (e) => e.code === "order_item_quantity_invalid"
  );
  assert.equal(calls.length, 0);
});

test("pedido valido envia Order com customer_id/products/notes, sem payment_method/shipping", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 201, body: { id: 4321 } }));
  const out = await createTrayOrder(
    {
      customerId: "10",
      items: [
        { trayProductId: "111", quantity: 2 },
        { trayProductId: "222", trayVariantId: "333", quantity: 1 },
      ],
      notes: "Resgate Loja NS / redemption_id=abc / coupon_code=XYZ",
    },
    { deps }
  );

  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "POST");
  assert.ok(calls[0].url.includes("/orders?access_token=token-abc"));

  const order = calls[0].body.Order;
  assert.equal(order.customer_id, 10);
  assert.deepEqual(order.products, [
    { product_id: 111, quantity: 2 },
    { product_id: 333, quantity: 1 }, // variante prevalece sobre o produto pai
  ]);
  assert.equal(order.notes, "Resgate Loja NS / redemption_id=abc / coupon_code=XYZ");
  assert.equal("payment_method" in order, false);
  assert.equal("shipping_method" in order, false);
  assert.equal("shipping_cost" in order, false);

  assert.deepEqual(out, { orderId: "4321", raw: { id: 4321 } });
});

test("resposta sem id identificavel falha alto (nunca finge sucesso)", async () => {
  const { deps } = makeDeps(() => makeResponse({ status: 200, body: { status: "ok" } }));
  await assert.rejects(
    () => createTrayOrder({ customerId: 10, items: [{ trayProductId: "1", quantity: 1 }], notes: "x" }, { deps }),
    (e) => e.code === "tray_order_id_missing"
  );
});

test("id aninhado em Order.id tambem e reconhecido", async () => {
  const { deps } = makeDeps(() => makeResponse({ status: 200, body: { Order: { id: 77 } } }));
  const out = await createTrayOrder({ customerId: 10, items: [{ trayProductId: "1", quantity: 1 }], notes: "x" }, { deps });
  assert.equal(out.orderId, "77");
});

test("400 da Tray propaga tray_request_invalid com corpo preservado", async () => {
  const { deps } = makeDeps(() => makeResponse({ status: 400, body: { message: "customer_id invalido" } }));
  await assert.rejects(
    () => createTrayOrder({ customerId: 10, items: [{ trayProductId: "1", quantity: 1 }], notes: "x" }, { deps }),
    (e) => e.code === "tray_request_invalid" && e.publicDetails?.tray_body?.message === "customer_id invalido"
  );
});
