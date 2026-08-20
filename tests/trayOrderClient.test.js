// tests/trayOrderClient.test.js
import test from "node:test";
import assert from "node:assert/strict";

import { createTrayOrder, getTrayOrderFull, parseMoneyStringToCents } from "../src/services/trayOrderClient.js";
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
    { product_id: 222, variant_id: 333, quantity: 1 }, // product_id e variant_id sao campos SEPARADOS
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

/* ─────────────────────────── parseMoneyStringToCents ─────────────────────────── */

test("parseMoneyStringToCents: casos comuns, sem multiplicacao de float", () => {
  assert.equal(parseMoneyStringToCents("50.00"), 5000);
  assert.equal(parseMoneyStringToCents("50"), 5000);
  assert.equal(parseMoneyStringToCents("381.00"), 38100);
  assert.equal(parseMoneyStringToCents("0.01"), 1);
  assert.equal(parseMoneyStringToCents("0.1"), 10);
  assert.equal(parseMoneyStringToCents("1234,50"), 123450); // vírgula BR
  assert.equal(parseMoneyStringToCents("0"), 0);
  assert.equal(parseMoneyStringToCents("0.00"), 0);
});

test("parseMoneyStringToCents: formato invalido devolve null, nunca 0 silencioso", () => {
  assert.equal(parseMoneyStringToCents("abc"), null);
  assert.equal(parseMoneyStringToCents(""), null);
  assert.equal(parseMoneyStringToCents(null), null);
  assert.equal(parseMoneyStringToCents(undefined), null);
  assert.equal(parseMoneyStringToCents("1.234"), null); // 3 casas decimais nao e formato monetario valido
});

/* ─────────────────────────── getTrayOrderFull ─────────────────────────── */

test("getTrayOrderFull extrai coupon_code/discount/discountCents do pedido real", async () => {
  const { deps } = makeDeps(() => makeResponse({ status: 200, body: { Order: { id: 1, coupon_code: "NSU-0418-Q4", discount: "50.00" } } }));
  const out = await getTrayOrderFull("1", { deps });
  assert.equal(out.couponCode, "NSU-0418-Q4");
  assert.equal(out.discount, 50);
  assert.equal(out.discountCents, 5000);
});

test("getTrayOrderFull sem desconto: discount=0, discountCents=0, coupon_code null se ausente", async () => {
  const { deps } = makeDeps(() => makeResponse({ status: 200, body: { Order: { id: 1 } } }));
  const out = await getTrayOrderFull("1", { deps });
  assert.equal(out.couponCode, null);
  assert.equal(out.discount, 0);
  assert.equal(out.discountCents, 0);
});

test("getTrayOrderFull sem Order na resposta falha alto", async () => {
  const { deps } = makeDeps(() => makeResponse({ status: 200, body: {} }));
  await assert.rejects(() => getTrayOrderFull("1", { deps }), (e) => e.code === "tray_invalid_response");
});
