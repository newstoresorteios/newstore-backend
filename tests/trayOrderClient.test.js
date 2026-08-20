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

const ADDRESS = {
  street: "Rua Um",
  number: "100",
  complement: "Ap 2",
  neighborhood: "Centro",
  city: "Conselheiro Mairinck",
  state: "PR",
  zipcode: "86480-000",
  country: "BR",
};

test("pedido valido envia Order com customer_id/ProductsSold/CustomerAddress/notes, sem payment_method/shipping", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 201, body: { id: 4321 } }));
  const out = await createTrayOrder(
    {
      customerId: "10",
      items: [
        { trayProductId: "111", quantity: 2 },
        { trayProductId: "222", trayVariantId: "333", quantity: 1 },
      ],
      notes: "Resgate Loja NS / redemption_id=abc / coupon_code=XYZ",
      address: ADDRESS,
    },
    { deps }
  );

  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "POST");
  assert.ok(calls[0].url.includes("/orders?access_token=token-abc"));

  const order = calls[0].body.Order;
  assert.equal(order.customer_id, 10);
  // Container de itens e `ProductsSold` (prova real M7: `products` faz a Tray
  // responder 400 "Pedido nao tem produtos.").
  assert.equal("products" in order, false);
  assert.deepEqual(order.ProductsSold, [
    { product_id: 111, quantity: 2 },
    { product_id: 222, variant_id: 333, quantity: 1 }, // product_id e variant_id sao campos SEPARADOS
  ]);
  // Estrutura oficial: o endereco vive em Order.Customer.CustomerAddress[],
  // NUNCA em Order.CustomerAddress (posicao testada e recusada pela Tray).
  assert.equal("CustomerAddress" in order, false);
  assert.equal("ProductsSold" in order.Customer, false);
  assert.ok(Array.isArray(order.Customer.CustomerAddress));
  assert.equal(order.Customer.CustomerAddress.length, 1);
  assert.deepEqual(order.Customer.CustomerAddress[0], {
    address: "Rua Um",
    number: "100",
    complement: "Ap 2",
    neighborhood: "Centro",
    city: "Conselheiro Mairinck",
    state: "PR",
    zip_code: "86480000",
    country: "BRA", // ISO-3 no boundary da Tray
    type: "1",
  });
  assert.equal(order.notes, "Resgate Loja NS / redemption_id=abc / coupon_code=XYZ");
  assert.equal("payment_method" in order, false);
  assert.equal("shipping_method" in order, false);
  assert.equal("shipping_cost" in order, false);

  assert.deepEqual(out, { orderId: "4321", raw: { id: 4321 } });
});

test("resposta sem id identificavel falha alto (nunca finge sucesso)", async () => {
  const { deps } = makeDeps(() => makeResponse({ status: 200, body: { status: "ok" } }));
  await assert.rejects(
    () => createTrayOrder({ customerId: 10, items: [{ trayProductId: "1", quantity: 1 }], notes: "x", address: ADDRESS }, { deps }),
    (e) => e.code === "tray_order_id_missing"
  );
});

test("id aninhado em Order.id tambem e reconhecido", async () => {
  const { deps } = makeDeps(() => makeResponse({ status: 200, body: { Order: { id: 77 } } }));
  const out = await createTrayOrder({ customerId: 10, items: [{ trayProductId: "1", quantity: 1 }], notes: "x", address: ADDRESS }, { deps });
  assert.equal(out.orderId, "77");
});

test("400 da Tray propaga tray_request_invalid com corpo preservado", async () => {
  const { deps } = makeDeps(() => makeResponse({ status: 400, body: { message: "customer_id invalido" } }));
  await assert.rejects(
    () => createTrayOrder({ customerId: 10, items: [{ trayProductId: "1", quantity: 1 }], notes: "x", address: ADDRESS }, { deps }),
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

test("endereco incompleto nunca chega a rede (Tray exige CustomerAddress)", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 201, body: { id: 1 } }));
  await assert.rejects(
    () =>
      createTrayOrder(
        {
          customerId: 10,
          items: [{ trayProductId: "1", quantity: 1 }],
          notes: "x",
          address: { ...ADDRESS, city: "", zipcode: "" },
        },
        { deps }
      ),
    (e) => e.code === "order_address_incomplete"
  );
  assert.equal(calls.length, 0);
});

test("endereco ausente nunca chega a rede", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 201, body: { id: 1 } }));
  await assert.rejects(
    () => createTrayOrder({ customerId: 10, items: [{ trayProductId: "1", quantity: 1 }], notes: "x" }, { deps }),
    (e) => e.code === "order_address_incomplete"
  );
  assert.equal(calls.length, 0);
});

test("country e normalizado para ISO-3 (BRA) so no boundary da Tray", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 201, body: { id: 9 } }));
  for (const raw of ["BR", "Brasil", "BRASIL", "brazil"]) {
    calls.length = 0;
    await createTrayOrder(
      {
        customerId: 10,
        items: [{ trayProductId: "1", quantity: 1 }],
        notes: "x",
        address: { ...ADDRESS, country: raw },
      },
      { deps }
    );
    assert.equal(calls[0].body.Order.Customer.CustomerAddress[0].country, "BRA", `country=${raw}`);
  }
});

test("payload do pedido nunca carrega password/secret/token do usuario", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 201, body: { id: 9 } }));
  await createTrayOrder(
    {
      customerId: 10,
      customer: { name: "jpjp", email: "jp@newstore.com", cpf: "10425415902" },
      items: [{ trayProductId: "1", quantity: 1 }],
      notes: "LOJA NS",
      address: ADDRESS,
    },
    { deps }
  );
  const serialized = JSON.stringify(calls[0].body);
  for (const forbidden of ["password", "pass_hash", "authorization", "refresh_token", "access_token", "DATABASE_URL", "coupon_value_cents"]) {
    assert.equal(serialized.toLowerCase().includes(forbidden.toLowerCase()), false, `vazou ${forbidden}`);
  }
});

test("Order carrega os campos obrigatorios da Loja NS (decisao de produto)", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 201, body: { id: 555 } }));
  await createTrayOrder(
    {
      customerId: 24858,
      customer: { name: "jpjp", email: "jp@newstore.com", cpf: "10425415902" },
      items: [{ trayProductId: "14518", quantity: 1 }],
      notes: "LOJA NS / redemption_id=abc",
      address: ADDRESS,
    },
    { deps }
  );

  const order = calls[0].body.Order;
  assert.equal(order.point_sale, "LOJA NS");
  assert.equal(order.shipment, "A DEFINIR PELA TRAY");
  assert.equal(order.shipment_value, "0.00");
  assert.equal(order.payment_form, "NSCréditos");

  // Limites documentados: point_sale 45, shipment 100, payment_form 50.
  assert.ok(order.point_sale.length <= 45);
  assert.ok(order.shipment.length <= 100);
  assert.ok(order.payment_form.length <= 50);

  // Estrutura que ja avancou na API real permanece.
  assert.ok(Array.isArray(order.Customer.CustomerAddress));
  assert.ok(Array.isArray(order.ProductsSold));

  // Nunca um meio de pagamento ficticio.
  const serialized = JSON.stringify(order).toLowerCase();
  for (const fake of ["pix", "boleto", "cartao", "cartão", "credit_card", "dinheiro"]) {
    assert.equal(serialized.includes(fake), false, `meio de pagamento ficticio: ${fake}`);
  }

  // Nao adicionamos preventivamente o que a Tray nunca pediu.
  assert.equal("partner_id" in order, false);
  assert.equal("session_id" in order, false);
  assert.equal("price" in order.ProductsSold[0], false);
  assert.equal("original_price" in order.ProductsSold[0], false);
});

test("notes do pedido nunca carrega PII", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 201, body: { id: 556 } }));
  await createTrayOrder(
    {
      customerId: 24858,
      customer: { name: "jpjp", email: "jp@newstore.com", cpf: "10425415902" },
      items: [{ trayProductId: "14518", quantity: 1 }],
      notes: "LOJA NS / redemption_id=abc / coupon_code=NSU-0418-Q4",
      address: ADDRESS,
    },
    { deps }
  );
  const notes = String(calls[0].body.Order.notes);
  for (const pii of ["10425415902", "jp@newstore.com", "43998640480", "86480"]) {
    assert.equal(notes.includes(pii), false, `PII em notes: ${pii}`);
  }
});
