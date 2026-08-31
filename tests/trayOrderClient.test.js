// tests/trayOrderClient.test.js
import test from "node:test";
import assert from "node:assert/strict";

import {
  createTrayOrder,
  getTrayOrderFull,
  resolveTrayOperationalStatus,
  updateTrayOrderStatus,
  advanceTrayOrderToOperationalStatus,
  parseMoneyStringToCents,
  buildTraySessionId,
  normalizeTrayBirthDate,
  normalizeTrayCountry,
} from "../src/services/trayOrderClient.js";
import { TrayCatalogError } from "../src/services/trayCatalogClient.js";
import { normalizeTrayMoney } from "../src/services/trayOrderClient.js";

const TRAY_CUSTOMER = {
  id: "24858",
  type: "0",
  name: "Cliente Tray",
  cpf: "10425415902",
  email: "tray@example.com",   // divergente do e-mail NewStore de proposito
  birth_date: "2007-09-25",
  phone: "43998640480",
  cellphone: null,
  rg: null,
  gender: null,
};

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

/** Qualquer um dos caminhos documentados de listagem de status. */
function isStatusListing(url) {
  return url.includes("/order_status") || url.includes("/orders/statuses");
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

test("lookup escolhe o ID factual do status A ENVIAR retornado pela Tray", async () => {
  const { calls, deps } = makeDeps(() =>
    makeResponse({
      body: {
        paging: { total: 3, page: 1, limit: 50, maxLimit: 50 },
        OrderStatuses: [
          { OrderStatus: { id: "16", status: "AGUARDANDO PAGAMENTO", show_backoffice: "1" } },
          { OrderStatus: { id: "27", status: "  A ENVIAR  ", show_backoffice: "1" } },
          { OrderStatus: { id: "99", status: "A ENVIAR VIP", show_backoffice: "1" } },
        ],
      },
    })
  );

  const target = await resolveTrayOperationalStatus({ deps, cache: false });

  assert.deepEqual(target, { id: "27", status: "A ENVIAR" });
  assert.equal(calls.length, 1);
  assert.match(calls[0].url, /\/order_status\?/);
  assert.equal(calls[0].method, "GET");
});

test("lookup aceita o rotulo em 'name' (formato documentado da listagem de status)", async () => {
  const { calls, deps } = makeDeps(() =>
    makeResponse({
      body: {
        OrderStatuses: [
          { OrderStatus: { id: "16", name: "AGUARDANDO PAGAMENTO", type: "open" } },
          { OrderStatus: { id: "27", name: "A ENVIAR", type: "open" } },
        ],
      },
    })
  );

  const target = await resolveTrayOperationalStatus({ deps, cache: false });

  assert.deepEqual(target, { id: "27", status: "A ENVIAR" });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "GET");
});

test("lookup tenta o segundo caminho documentado quando o primeiro responde 404", async () => {
  const { calls, deps } = makeDeps((url) => {
    if (url.includes("/order_status")) return makeResponse({ status: 404, body: { message: "Not Found" } });
    return makeResponse({ body: { OrderStatuses: [{ OrderStatus: { id: "27", status: "A ENVIAR" } }] } });
  });

  const target = await resolveTrayOperationalStatus({ deps, cache: false });

  assert.equal(target.id, "27");
  assert.equal(calls.length, 2);
  for (const call of calls) assert.equal(call.method, "GET", "descoberta de status e sempre read-only");
});

test("PUT de status envia somente Order.status_id para o pedido especifico", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ body: { message: "Saved", id: "5555", code: 200 } }));

  await updateTrayOrderStatus({ orderId: "5555", statusId: "27" }, { deps });

  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "PUT");
  assert.match(calls[0].url, /\/orders\/5555\?access_token=/);
  assert.deepEqual(calls[0].body, { Order: { status_id: "27" } });
});

test("avanço operacional faz lookup, PUT e confirma o OrderStatus via GET simples", async () => {
  const { calls, deps } = makeDeps((url, options) => {
    if (isStatusListing(url)) {
      return makeResponse({
        body: { OrderStatuses: [{ OrderStatus: { id: "27", status: "A ENVIAR", show_backoffice: "1" } }] },
      });
    }
    if (options?.method === "PUT") {
      return makeResponse({ body: { message: "Saved", id: "5555", code: 200 } });
    }
    return makeResponse({
      body: {
        Order: {
          id: "5555",
          status: "A ENVIAR",
          has_payment: "0",
          OrderStatus: { id: "27", status: "A ENVIAR", type: "open" },
        },
      },
    });
  });

  const result = await advanceTrayOrderToOperationalStatus(
    { orderId: "5555" },
    { deps, cache: false }
  );

  assert.equal(result.targetStatus.id, "27");
  assert.equal(result.order.OrderStatus.id, "27");
  assert.equal(result.hasPayment, "0");
  assert.deepEqual(calls.map((call) => call.method), ["GET", "PUT", "GET"]);
  assert.equal(calls.some((call) => call.url.includes("/full")), false);
});

test("PUT com timeout: reconcilia por GET e trata status ja aplicado como sucesso", async () => {
  const { calls, deps } = makeDeps((url, options) => {
    if (isStatusListing(url)) {
      return makeResponse({ body: { OrderStatuses: [{ OrderStatus: { id: "27", status: "A ENVIAR" } }] } });
    }
    if (options?.method === "PUT") {
      const abort = new Error("aborted");
      abort.name = "AbortError";
      throw abort;
    }
    return makeResponse({
      body: { Order: { id: "5555", has_payment: "0", OrderStatus: { id: "27", status: "A ENVIAR" } } },
    });
  });

  const result = await advanceTrayOrderToOperationalStatus({ orderId: "5555" }, { deps, cache: false });

  assert.equal(result.targetStatus.id, "27");
  assert.equal(calls.filter((call) => call.method === "PUT").length, 1, "nunca repete a mutation as cegas");
  assert.deepEqual(calls.map((call) => call.method), ["GET", "PUT", "GET"]);
});

test("PUT com timeout e status ainda antigo: nao repete o PUT e exige reconciliacao", async () => {
  const { calls, deps } = makeDeps((url, options) => {
    if (isStatusListing(url)) {
      return makeResponse({ body: { OrderStatuses: [{ OrderStatus: { id: "27", status: "A ENVIAR" } }] } });
    }
    if (options?.method === "PUT") {
      const abort = new Error("aborted");
      abort.name = "AbortError";
      throw abort;
    }
    return makeResponse({
      body: { Order: { id: "5555", has_payment: "0", OrderStatus: { id: "16", status: "AGUARDANDO PAGAMENTO" } } },
    });
  });

  await assert.rejects(
    () => advanceTrayOrderToOperationalStatus({ orderId: "5555" }, { deps, cache: false }),
    (e) => e instanceof TrayCatalogError && e.code === "tray_order_status_unconfirmed"
  );

  assert.equal(calls.filter((call) => call.method === "PUT").length, 1);
});

test("PUT com timeout e GET indisponivel: falha para reconciliacao sem novo PUT", async () => {
  const { calls, deps } = makeDeps((url, options) => {
    if (isStatusListing(url)) {
      return makeResponse({ body: { OrderStatuses: [{ OrderStatus: { id: "27", status: "A ENVIAR" } }] } });
    }
    if (options?.method === "PUT") {
      const abort = new Error("aborted");
      abort.name = "AbortError";
      throw abort;
    }
    return makeResponse({ status: 503, body: { message: "unavailable" } });
  });

  await assert.rejects(
    () => advanceTrayOrderToOperationalStatus({ orderId: "5555" }, { deps, cache: false }),
    (e) => e instanceof TrayCatalogError && e.code === "tray_order_status_unconfirmed"
  );

  assert.equal(calls.filter((call) => call.method === "PUT").length, 1);
});

test("status operacional inexistente falha fechado ANTES de qualquer mutation", async () => {
  const { calls, deps } = makeDeps(() =>
    makeResponse({
      body: {
        OrderStatuses: [
          { OrderStatus: { id: "16", status: "AGUARDANDO PAGAMENTO" } },
          { OrderStatus: { id: "21", status: "CANCELADO" } },
        ],
      },
    })
  );

  await assert.rejects(
    () => advanceTrayOrderToOperationalStatus({ orderId: "5555" }, { deps, cache: false }),
    (e) => e instanceof TrayCatalogError && e.code === "tray_operational_status_not_found"
  );

  assert.deepEqual(calls.map((call) => call.method), ["GET"], "nenhum PUT com status arbitrario");
});

test("o avanco de status nunca chama a API de pagamentos da Tray", async () => {
  const { calls, deps } = makeDeps((url, options) => {
    if (isStatusListing(url)) {
      return makeResponse({ body: { OrderStatuses: [{ OrderStatus: { id: "27", status: "A ENVIAR" } }] } });
    }
    if (options?.method === "PUT") return makeResponse({ body: { message: "Saved", id: "5555", code: 200 } });
    return makeResponse({
      body: { Order: { id: "5555", has_payment: "0", OrderStatus: { id: "27", status: "A ENVIAR" } } },
    });
  });

  await advanceTrayOrderToOperationalStatus({ orderId: "5555" }, { deps, cache: false });

  assert.equal(calls.some((call) => call.url.includes("/payments")), false, "zero chamadas a /payments");
  assert.equal(calls.some((call) => call.body && "Payment" in call.body), false);
});

/* ─────────────────────────── validacoes pre-rede ─────────────────────────── */

test("customer_id ausente/invalido nunca chega a rede", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({}));
  await assert.rejects(
    () => createTrayOrder({ customerId: null, items: [{ trayProductId: "1", quantity: 1, trayPrice: "9.99" }], notes: "x" }, { deps }),
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

test("item sem product_id valido nunca chega a rede", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({}));
  await assert.rejects(
    () => createTrayOrder({ customerId: 10, items: [{ trayProductId: null, quantity: 1, trayPrice: "9.99" }], notes: "x" }, { deps }),
    (e) => e.code === "order_item_product_id_invalid"
  );
  assert.equal(calls.length, 0);
});

test("quantidade invalida nunca chega a rede", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({}));
  await assert.rejects(
    () => createTrayOrder({ customerId: 10, items: [{ trayProductId: "1", quantity: 0, trayPrice: "9.99" }], notes: "x" }, { deps }),
    (e) => e.code === "order_item_quantity_invalid"
  );
  assert.equal(calls.length, 0);
});

/* ─────────── contrato: pedido REFERENCIA cliente existente ─────────── */

test("identidade vem da Tray, endereco vem da NewStore, sem customer_id duplicado", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 201, body: { id: 4321 } }));
  const out = await createTrayOrder(
    {
      customerId: "24858",
      items: [
        { trayProductId: "111", quantity: 2, trayPrice: "10.00" },
        { trayProductId: "222", trayVariantId: "333", quantity: 1, trayPrice: "20.50" },
      ],
      customer: TRAY_CUSTOMER,
      notes: "LOJA NS / redemption_id=abc",
      address: ADDRESS,
      sessionId: buildTraySessionId("2612b836-a0bd-4e74-b952-7620c8564e7c"),
    },
    { deps }
  );

  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "POST");
  assert.ok(calls[0].url.includes("/orders?access_token=token-abc"));

  const order = calls[0].body.Order;

  // Um unico modelo de identidade: Order.Customer completo, sem customer_id.
  assert.equal("customer_id" in order, false);
  assert.equal("CustomerAddress" in order, false, "a Tray nao le CustomerAddress no nivel do Order");

  // IDENTIDADE = TRAY (nunca remontada com dados da NewStore)
  assert.equal(order.Customer.email, "tray@example.com");
  assert.equal(order.Customer.cpf, "10425415902");
  assert.equal(order.Customer.birth_date, "2007-09-25");
  assert.equal(order.Customer.name, "Cliente Tray");
  assert.equal(order.Customer.type, "0");
  assert.equal(order.Customer.phone, "43998640480");
  // campos opcionais que a Tray nao tem nunca sao inventados
  assert.equal("rg" in order.Customer, false);
  assert.equal("gender" in order.Customer, false);

  // ENDERECO = NEWSTORE
  assert.equal(order.Customer.CustomerAddress[0].city, "Conselheiro Mairinck");

  assert.deepEqual(out, { orderId: "4321", raw: { id: 4321 } });
});

test("itens vao em ProductsSold (nunca products) com product_id/variant_id separados", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 201, body: { id: 1 } }));
  await createTrayOrder(
    {
      customerId: 24858,
      items: [
        { trayProductId: "111", quantity: 2, trayPrice: "10.00" },
        { trayProductId: "222", trayVariantId: "333", quantity: 1, trayPrice: "20.50" },
      ],
      notes: "x",
      address: ADDRESS,
      customer: TRAY_CUSTOMER,
    },
    { deps }
  );
  const order = calls[0].body.Order;
  assert.equal("products" in order, false);
  assert.deepEqual(order.ProductsSold, [
    { product_id: 111, quantity: 2, price: "10.00", original_price: "10.00" },
    { product_id: 222, quantity: 1, price: "20.50", original_price: "20.50", variant_id: 333 },
  ]);
});

test("Order carrega os campos obrigatorios da Loja NS (decisao de produto)", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 201, body: { id: 555 } }));
  await createTrayOrder(
    { customerId: 24858, items: [{ trayProductId: "14518", quantity: 1, trayPrice: "299.99" }], notes: "LOJA NS", sessionId: "abc123", address: ADDRESS, customer: TRAY_CUSTOMER },
    { deps }
  );

  const order = calls[0].body.Order;
  assert.equal(order.point_sale, "LOJA NS");
  assert.equal(order.shipment, "PENDENTE TRAY");
  assert.equal(order.shipment_value, "0.00");
  assert.equal(order.payment_form, "NSCréditos");
  assert.equal(order.session_id, "abc123");

  // limites documentados
  assert.ok(order.point_sale.length <= 45);
  assert.ok(order.shipment.length <= 100);
  assert.ok(order.payment_form.length <= 50);

  // nunca um meio de pagamento ficticio, nunca campos preventivos
  const serialized = JSON.stringify(order).toLowerCase();
  for (const fake of ["pix", "boleto", "cartao", "cartão", "credit_card", "dinheiro"]) {
    assert.equal(serialized.includes(fake), false, `meio de pagamento ficticio: ${fake}`);
  }
  assert.equal("partner_id" in order, false);
  assert.equal("MarketplaceOrder" in order, false);
});

test("identificacao do resgate vai em notes E store_note, sem PII", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 201, body: { id: 556 } }));
  await createTrayOrder(
    {
      customerId: 24858,
      items: [{ trayProductId: "14518", quantity: 1, trayPrice: "299.99" }],
      notes: "LOJA NS / redemption_id=abc / coupon_code=NSU-0418-Q4",
      address: ADDRESS,
      customer: TRAY_CUSTOMER,
    },
    { deps }
  );
  const order = calls[0].body.Order;
  assert.ok(String(order.notes).includes("LOJA NS"));
  assert.equal(order.store_note, order.notes);
  for (const pii of ["10425415902", "jp@newstore.com", "43998640480", "86480"]) {
    assert.equal(String(order.store_note).includes(pii), false, `PII em store_note: ${pii}`);
  }
});

test("payload do pedido nunca carrega password/secret/token", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 201, body: { id: 9 } }));
  await createTrayOrder({ customerId: 24858, items: [{ trayProductId: "1", quantity: 1, trayPrice: "9.99" }], notes: "LOJA NS", address: ADDRESS, customer: TRAY_CUSTOMER }, { deps });
  const serialized = JSON.stringify(calls[0].body).toLowerCase();
  for (const forbidden of ["password", "pass_hash", "authorization", "refresh_token", "access_token", "database_url", "coupon_value_cents"]) {
    assert.equal(serialized.includes(forbidden), false, `vazou ${forbidden}`);
  }
});

/* ─────────────────────────── session_id ─────────────────────────── */

test("session_id e estavel e derivado do redemption, nunca aleatorio", () => {
  const rid = "2612b836-a0bd-4e74-b952-7620c8564e7c";
  assert.equal(buildTraySessionId(rid), buildTraySessionId(rid));
  assert.ok(buildTraySessionId(rid).length > 0 && buildTraySessionId(rid).length <= 26);
  assert.match(buildTraySessionId(rid), /^[a-zA-Z0-9]+$/);
  assert.notEqual(buildTraySessionId(rid), buildTraySessionId("11111111-2222-3333-4444-555555555555"));
  assert.equal(buildTraySessionId(null), "");
});

/* ───────────────── helpers preservados (perfil/endereco) ───────────────── */

test("normalizeTrayBirthDate: YYYY-MM-DD, sem deslocamento de fuso", () => {
  assert.equal(normalizeTrayBirthDate("2007-09-25"), "2007-09-25");
  assert.equal(normalizeTrayBirthDate("2007-09-25T03:00:00.000Z"), "2007-09-25");
  assert.equal(normalizeTrayBirthDate(new Date(Date.UTC(2007, 8, 25))), "2007-09-25");
  assert.equal(normalizeTrayBirthDate("25/09/2007"), "", "formato nao reconhecido nunca vira data inventada");
  assert.equal(normalizeTrayBirthDate(null), "");
});

test("normalizeTrayCountry: Brasil vira ISO-3 BRA", () => {
  for (const raw of ["BR", "Brasil", "BRASIL", "brazil", "bra"]) {
    assert.equal(normalizeTrayCountry(raw), "BRA", `country=${raw}`);
  }
  assert.equal(normalizeTrayCountry(""), "");
});

/* ─────────────────────────── respostas da Tray ─────────────────────────── */

test("resposta sem id identificavel falha alto (nunca finge sucesso)", async () => {
  const { deps } = makeDeps(() => makeResponse({ status: 200, body: { status: "ok" } }));
  await assert.rejects(
    () => createTrayOrder({ customerId: 24858, items: [{ trayProductId: "1", quantity: 1, trayPrice: "9.99" }], notes: "x", address: ADDRESS, customer: TRAY_CUSTOMER }, { deps }),
    (e) => e.code === "tray_order_id_missing"
  );
});

test("id aninhado em Order.id tambem e reconhecido", async () => {
  const { deps } = makeDeps(() => makeResponse({ status: 200, body: { Order: { id: 77 } } }));
  const out = await createTrayOrder({ customerId: 24858, items: [{ trayProductId: "1", quantity: 1, trayPrice: "9.99" }], notes: "x", address: ADDRESS, customer: TRAY_CUSTOMER }, { deps });
  assert.equal(out.orderId, "77");
});

test("400 da Tray propaga tray_request_invalid com corpo preservado", async () => {
  const { deps } = makeDeps(() => makeResponse({ status: 400, body: { message: "customer_id invalido" } }));
  await assert.rejects(
    () => createTrayOrder({ customerId: 24858, items: [{ trayProductId: "1", quantity: 1, trayPrice: "9.99" }], notes: "x", address: ADDRESS, customer: TRAY_CUSTOMER }, { deps }),
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
  assert.equal(parseMoneyStringToCents("1.234"), null);
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

test("endereco do resgate vai em Order.Customer.CustomerAddress[] normalizado", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 201, body: { id: 1 } }));
  await createTrayOrder(
    { customerId: 24858, items: [{ trayProductId: "14518", quantity: 1, trayPrice: "299.99" }], notes: "LOJA NS", address: ADDRESS, customer: TRAY_CUSTOMER },
    { deps }
  );
  const list = calls[0].body.Order.Customer.CustomerAddress;
  assert.ok(Array.isArray(list));
  assert.equal(list.length, 1);
  assert.deepEqual(list[0], {
    address: "Rua Um",
    number: "100",
    complement: "Ap 2",
    neighborhood: "Centro",
    city: "Conselheiro Mairinck",
    state: "PR",
    zip_code: "86480000", // so digitos
    country: "BRA", // ISO-3 no boundary
    type: "1", // entrega
  });
});

test("endereco incompleto/ausente nunca chega a rede (nunca inventa endereco)", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 201, body: { id: 1 } }));
  await assert.rejects(
    () =>
      createTrayOrder(
        { customerId: 24858, items: [{ trayProductId: "1", quantity: 1, trayPrice: "9.99" }], notes: "x", customer: TRAY_CUSTOMER, address: { ...ADDRESS, city: "", zipcode: "" } },
        { deps }
      ),
    (e) => e.code === "order_address_incomplete"
  );
  await assert.rejects(
    () => createTrayOrder({ customerId: 24858, items: [{ trayProductId: "1", quantity: 1, trayPrice: "9.99" }], notes: "x", customer: TRAY_CUSTOMER }, { deps }),
    (e) => e.code === "order_address_incomplete"
  );
  assert.equal(calls.length, 0);
});

test("preco monetario da Tray e obrigatorio e nunca inventado", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 201, body: { id: 1 } }));
  // sem trayPrice -> falha ANTES da rede
  await assert.rejects(
    () =>
      createTrayOrder(
        { customerId: 24858, items: [{ trayProductId: "14518", quantity: 1 }], notes: "x", address: ADDRESS, customer: TRAY_CUSTOMER },
        { deps }
      ),
    (e) => e.code === "order_item_price_invalid"
  );
  // preco malformado tambem
  await assert.rejects(
    () =>
      createTrayOrder(
        { customerId: 24858, items: [{ trayProductId: "14518", quantity: 1, trayPrice: "abc" }], notes: "x", address: ADDRESS, customer: TRAY_CUSTOMER },
        { deps }
      ),
    (e) => e.code === "order_item_price_invalid"
  );
  assert.equal(calls.length, 0);
});

test("normalizeTrayMoney: formato monetario valido, sem float ingenuo", () => {
  assert.equal(normalizeTrayMoney("299.99"), "299.99");
  assert.equal(normalizeTrayMoney("299,99"), "299.99");
  assert.equal(normalizeTrayMoney("300"), "300.00");
  assert.equal(normalizeTrayMoney("0.00"), "0.00");
  assert.equal(normalizeTrayMoney("abc"), "");
  assert.equal(normalizeTrayMoney(""), "");
  assert.equal(normalizeTrayMoney(null), "");
  assert.equal(normalizeTrayMoney("-5.00"), "");
  assert.equal(normalizeTrayMoney("1.234"), "");
});
