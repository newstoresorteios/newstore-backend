// tests/trayPaymentClient.test.js
//
// Contrato do Payment REAL do resgate na Tray (POST /payments).
//
// Regra de negocio (supera a decisao de 31/08 "has_payment pode continuar 0"):
// todo resgate NOVO concluido com NSCreditos precisa de um Payment real no
// pedido Tray. O VALOR do Payment e SEMPRE o valor monetario factual do
// pedido (Order.total) — NUNCA os NSCreditos, que sao ledger interno.
import test from "node:test";
import assert from "node:assert/strict";

import {
  TRAY_REDEMPTION_PAYMENT_METHOD,
  buildRedemptionPaymentMarker,
  normalizeTrayPayment,
  listTrayPaymentsByOrder,
  findExistingRedemptionPayment,
  createTrayRedemptionPayment,
  ensureTrayRedemptionPayment,
  resolveTrayPaymentDate,
} from "../src/services/trayPaymentClient.js";
import { TrayCatalogError } from "../src/services/trayCatalogClient.js";

const API_BASE = "https://www.exemplo-loja.com.br/web_api";
const TOKEN = "token-abc";
const REDEMPTION_ID = "11111111-2222-3333-4444-555555555555";
const ORDER_ID = "25999";
const ORDER_TOTAL = "489.99";
const MARKER = `LOJA_NS_REDEMPTION:${REDEMPTION_ID}`;

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
        calls.push({
          url: String(url),
          method: String(options?.method || "GET").toUpperCase(),
          body: options?.body ? JSON.parse(options.body) : null,
        });
        return handler(String(url), options);
      },
    },
  };
}

/** Payment como a Tray devolve na listagem do pedido. */
function trayPaymentRow({ id = "77001", value = ORDER_TOTAL, note = MARKER, orderId = ORDER_ID } = {}) {
  return {
    Payment: {
      id,
      order_id: orderId,
      method: TRAY_REDEMPTION_PAYMENT_METHOD,
      value,
      date: "2026-09-09",
      note,
    },
  };
}

function abortError() {
  const e = new Error("aborted");
  e.name = "AbortError";
  return e;
}

/* ─────────────────────────── marker deterministico ─────────────────────────── */

test("o marker do Payment e deterministico e derivado do redemption_id", () => {
  assert.equal(buildRedemptionPaymentMarker(REDEMPTION_ID), MARKER);
  assert.equal(buildRedemptionPaymentMarker(REDEMPTION_ID), buildRedemptionPaymentMarker(REDEMPTION_ID));
  assert.notEqual(buildRedemptionPaymentMarker("outro-resgate"), MARKER);
});

test("redemption_id vazio nunca produz marker (nao existe Payment sem resgate)", () => {
  assert.throws(
    () => buildRedemptionPaymentMarker(""),
    (e) => e instanceof TrayCatalogError && e.code === "redemption_id_missing"
  );
});

/* ─────────────────────────── leitura ─────────────────────────── */

test("listTrayPaymentsByOrder le por GET filtrando pelo pedido, sem mutation", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ body: { Payments: [trayPaymentRow()] } }));

  const payments = await listTrayPaymentsByOrder(ORDER_ID, { deps });

  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "GET");
  assert.match(calls[0].url, /\/payments\?/);
  assert.match(calls[0].url, /order_id=25999/);
  assert.equal(payments.length, 1);
  assert.equal(payments[0].note, MARKER);
  assert.equal(payments[0].value, ORDER_TOTAL);
  assert.equal(payments[0].orderId, ORDER_ID);
});

test("normalizeTrayPayment aceita o envelope { Payment: {...} } e o objeto cru", () => {
  const wrapped = normalizeTrayPayment(trayPaymentRow());
  const bare = normalizeTrayPayment(trayPaymentRow().Payment);
  assert.equal(wrapped.id, "77001");
  assert.deepEqual(wrapped, bare);
});

test("listagem em formato desconhecido falha fechado — nunca vira 'nao existe Payment'", async () => {
  const { deps } = makeDeps(() => makeResponse({ body: { unexpected: "shape" } }));

  await assert.rejects(
    () => listTrayPaymentsByOrder(ORDER_ID, { deps }),
    (e) => e instanceof TrayCatalogError && e.code === "tray_payment_list_invalid"
  );
});

/* ─────────────────────────── correspondencia do Payment ─────────────────────────── */

test("encontra o Payment do resgate pelo marker + order_id + valor esperado", () => {
  const payments = [
    normalizeTrayPayment(trayPaymentRow({ id: "1", note: "outro pagamento" })),
    normalizeTrayPayment(trayPaymentRow({ id: "2" })),
  ];

  const found = findExistingRedemptionPayment({
    payments,
    orderId: ORDER_ID,
    redemptionId: REDEMPTION_ID,
    expectedValue: ORDER_TOTAL,
  });

  assert.equal(found.id, "2");
});

test("Payment de OUTRO pedido com o mesmo marker nao e reutilizado", () => {
  const payments = [normalizeTrayPayment(trayPaymentRow({ orderId: "99999" }))];

  const found = findExistingRedemptionPayment({
    payments,
    orderId: ORDER_ID,
    redemptionId: REDEMPTION_ID,
    expectedValue: ORDER_TOTAL,
  });

  assert.equal(found, null);
});

test("marker correto com VALOR divergente falha fechado — nunca aceita silenciosamente", () => {
  const payments = [normalizeTrayPayment(trayPaymentRow({ value: "10.00" }))];

  assert.throws(
    () =>
      findExistingRedemptionPayment({
        payments,
        orderId: ORDER_ID,
        redemptionId: REDEMPTION_ID,
        expectedValue: ORDER_TOTAL,
      }),
    (e) => e instanceof TrayCatalogError && e.code === "tray_payment_value_mismatch"
  );
});

test("valor equivalente com zeros a mais ('489.9900') e o mesmo dinheiro", () => {
  const payments = [normalizeTrayPayment(trayPaymentRow({ value: "489.9900" }))];

  const found = findExistingRedemptionPayment({
    payments,
    orderId: ORDER_ID,
    redemptionId: REDEMPTION_ID,
    expectedValue: ORDER_TOTAL,
  });

  assert.equal(found.id, "77001");
});

/* ─────────────────────────── criacao ─────────────────────────── */

test("POST /payments envia exatamente o payload oficial da Tray", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 201, body: { id: "77001" } }));

  await createTrayRedemptionPayment(
    { orderId: ORDER_ID, redemptionId: REDEMPTION_ID, value: ORDER_TOTAL, date: "2026-09-09" },
    { deps }
  );

  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "POST");
  assert.match(calls[0].url, /\/payments\?access_token=/);
  // ENVELOPE `Payment` MAIUSCULO — provado contra a loja real (smoke
  // 2026-09-09, pedido 25894): enviando `{ payment: {...} }` a Tray responde
  // 400 com causes.Payment.{value,method,order_id} = "Este campo nao pode ser
  // deixado em branco", ou seja, ela simplesmente nao enxerga o objeto. Mesmo
  // padrao ja conhecido em `Order`/`ProductsSold`.
  assert.deepEqual(calls[0].body, {
    Payment: {
      order_id: ORDER_ID,
      method: "NSCréditos",
      value: ORDER_TOTAL,
      date: "2026-09-09",
      note: MARKER,
    },
  });
  assert.equal("payment" in calls[0].body, false, "chave minuscula e recusada pela Tray");
});

test("o metodo do Payment representa NSCreditos — nunca pix/boleto/cartao", () => {
  assert.equal(TRAY_REDEMPTION_PAYMENT_METHOD, "NSCréditos");
  assert.doesNotMatch(TRAY_REDEMPTION_PAYMENT_METHOD, /pix|boleto|cart|credit card/i);
});

test("valor ausente/invalido nunca chega a rede", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({}));

  for (const value of [null, "", "abc", "-1.00"]) {
    // eslint-disable-next-line no-await-in-loop
    await assert.rejects(
      () => createTrayRedemptionPayment({ orderId: ORDER_ID, redemptionId: REDEMPTION_ID, value, date: "2026-09-09" }, { deps }),
      (e) => e instanceof TrayCatalogError && e.code === "tray_payment_value_invalid",
      `value=${JSON.stringify(value)} deveria ser recusado antes da rede`
    );
  }
  assert.equal(calls.length, 0);
});

test("a data do Payment sai no formato YYYY-MM-DD do fuso da loja", () => {
  // 2026-09-10T01:30:00Z e ainda 09/09 em Sao Paulo (UTC-3).
  assert.equal(resolveTrayPaymentDate(new Date("2026-09-10T01:30:00Z")), "2026-09-09");
  assert.match(resolveTrayPaymentDate(new Date("2026-09-09T12:00:00Z")), /^\d{4}-\d{2}-\d{2}$/);
});

/* ─────────────────────────── ensure: idempotencia ─────────────────────────── */

test("Payment ja existente com marker correto: zero POST", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ body: { Payments: [trayPaymentRow()] } }));

  const out = await ensureTrayRedemptionPayment(
    { orderId: ORDER_ID, redemptionId: REDEMPTION_ID, value: ORDER_TOTAL, date: "2026-09-09" },
    { deps }
  );

  assert.equal(out.created, false);
  assert.equal(out.payment.id, "77001");
  assert.equal(calls.filter((c) => c.method === "POST").length, 0, "reutiliza, nunca duplica");
});

test("sem Payment do resgate: cria UM e confirma por GET", async () => {
  let created = false;
  const { calls, deps } = makeDeps((url, options) => {
    if (String(options?.method || "GET").toUpperCase() === "POST") {
      created = true;
      return makeResponse({ status: 201, body: { id: "77001" } });
    }
    return makeResponse({ body: { Payments: created ? [trayPaymentRow()] : [] } });
  });

  const out = await ensureTrayRedemptionPayment(
    { orderId: ORDER_ID, redemptionId: REDEMPTION_ID, value: ORDER_TOTAL, date: "2026-09-09" },
    { deps }
  );

  assert.equal(out.created, true);
  assert.equal(out.payment.note, MARKER);
  assert.equal(calls.filter((c) => c.method === "POST").length, 1);
  assert.deepEqual(calls.map((c) => c.method), ["GET", "POST", "GET"]);
});

test("chamadas repetidas do ensure nao criam um segundo Payment", async () => {
  let stored = [];
  const { calls, deps } = makeDeps((url, options) => {
    if (String(options?.method || "GET").toUpperCase() === "POST") {
      stored = [trayPaymentRow()];
      return makeResponse({ status: 201, body: { id: "77001" } });
    }
    return makeResponse({ body: { Payments: stored } });
  });

  const args = { orderId: ORDER_ID, redemptionId: REDEMPTION_ID, value: ORDER_TOTAL, date: "2026-09-09" };
  await ensureTrayRedemptionPayment(args, { deps });
  await ensureTrayRedemptionPayment(args, { deps });
  await ensureTrayRedemptionPayment(args, { deps });

  assert.equal(calls.filter((c) => c.method === "POST").length, 1, "exatamente um POST em tres execucoes");
});

/* ─────────────────────────── ensure: timeout / resultado ambiguo ─────────────────────────── */

test("timeout no POST + Payment encontrado no GET: sucesso com exatamente um POST", async () => {
  const { calls, deps } = makeDeps((url, options) => {
    if (String(options?.method || "GET").toUpperCase() === "POST") throw abortError();
    // O primeiro GET nao acha; depois do POST ambiguo a Tray ja tem o Payment.
    const posted = calls.some((c) => c.method === "POST");
    return makeResponse({ body: { Payments: posted ? [trayPaymentRow()] : [] } });
  });

  const out = await ensureTrayRedemptionPayment(
    { orderId: ORDER_ID, redemptionId: REDEMPTION_ID, value: ORDER_TOTAL, date: "2026-09-09" },
    { deps }
  );

  assert.equal(out.created, true);
  assert.equal(out.reconciled, true);
  assert.equal(out.payment.id, "77001");
  assert.equal(calls.filter((c) => c.method === "POST").length, 1, "nunca repete o POST as cegas");
});

test("timeout no POST + Payment NAO encontrado: tray_payment_unconfirmed sem retry cego", async () => {
  const { calls, deps } = makeDeps((url, options) => {
    if (String(options?.method || "GET").toUpperCase() === "POST") throw abortError();
    return makeResponse({ body: { Payments: [] } });
  });

  await assert.rejects(
    () =>
      ensureTrayRedemptionPayment(
        { orderId: ORDER_ID, redemptionId: REDEMPTION_ID, value: ORDER_TOTAL, date: "2026-09-09" },
        { deps }
      ),
    (e) => e instanceof TrayCatalogError && e.code === "tray_payment_unconfirmed"
  );

  assert.equal(calls.filter((c) => c.method === "POST").length, 1, "exatamente um POST, nenhum retry");
});

test("POST aceito mas o GET nao confirma: tray_payment_unconfirmed, sem segundo POST", async () => {
  const { calls, deps } = makeDeps((url, options) => {
    if (String(options?.method || "GET").toUpperCase() === "POST") {
      return makeResponse({ status: 201, body: { id: "77001" } });
    }
    return makeResponse({ body: { Payments: [] } });
  });

  await assert.rejects(
    () =>
      ensureTrayRedemptionPayment(
        { orderId: ORDER_ID, redemptionId: REDEMPTION_ID, value: ORDER_TOTAL, date: "2026-09-09" },
        { deps }
      ),
    (e) => e instanceof TrayCatalogError && e.code === "tray_payment_unconfirmed"
  );

  assert.equal(calls.filter((c) => c.method === "POST").length, 1);
});

test("erro deterministico do POST (400) sobe como esta — nunca vira 'unconfirmed'", async () => {
  const { calls, deps } = makeDeps((url, options) => {
    if (String(options?.method || "GET").toUpperCase() === "POST") {
      return makeResponse({ status: 400, body: { message: "invalido" } });
    }
    return makeResponse({ body: { Payments: [] } });
  });

  await assert.rejects(
    () =>
      ensureTrayRedemptionPayment(
        { orderId: ORDER_ID, redemptionId: REDEMPTION_ID, value: ORDER_TOTAL, date: "2026-09-09" },
        { deps }
      ),
    (e) => e instanceof TrayCatalogError && e.code === "tray_request_invalid"
  );

  assert.equal(calls.filter((c) => c.method === "POST").length, 1);
});

/* ─────────────────────────── o valor NUNCA vem dos NSCreditos ─────────────────────────── */

test("o Payment carrega Order.total, jamais os NSCreditos do resgate", async () => {
  const { calls, deps } = makeDeps((url, options) => {
    if (String(options?.method || "GET").toUpperCase() === "POST") return makeResponse({ status: 201, body: { id: "77001" } });
    const posted = calls.some((c) => c.method === "POST");
    return makeResponse({ body: { Payments: posted ? [trayPaymentRow()] : [] } });
  });

  // O resgate custou 5000 NSCreditos; o pedido Tray vale R$ 489,99.
  await ensureTrayRedemptionPayment(
    { orderId: ORDER_ID, redemptionId: REDEMPTION_ID, value: ORDER_TOTAL, date: "2026-09-09" },
    { deps }
  );

  const post = calls.find((c) => c.method === "POST");
  assert.equal(post.body.Payment.value, "489.99");
  const serialized = JSON.stringify(calls.map((c) => c.body));
  assert.doesNotMatch(serialized, /5000/, "NSCreditos nunca aparecem no payload do Payment");
});

/* ─────────────── contrato REAL da loja (auditoria 2026-09-09) ─────────────── */
//
// Corpo factual de GET /payments?order_id=... capturado contra a loja real,
// somente leitura. O Payment da Tray NAO tem campo `method`: o rotulo vem em
// `payment_place` e o identificador em `payment_method_id`. A fixture acima
// (que usa `method`) descrevia um contrato que a Tray nao devolve.
const REAL_TRAY_PAYMENTS_BODY = {
  paging: { total: 1, page: 1, offset: 0, limit: 30, maxLimit: 50 },
  sort: [{ id: "asc" }],
  availableFilters: ["id", "order_id", "note"],
  appliedFilters: { "Payment.order_id": "25999" },
  Payments: [
    {
      Payment: {
        created: "2026-09-09 10:52:46",
        modified: "2026-09-09 10:52:46",
        id: "16704",
        order_id: "25999",
        payment_method_id: "10547",
        payment_place: "NSCréditos",
        value: "489.99",
        date: "2026-09-09",
        note: MARKER,
      },
    },
  ],
};

test("o Payment real da Tray traz payment_place/payment_method_id, nao `method`", async () => {
  const { deps } = makeDeps(() => makeResponse({ body: REAL_TRAY_PAYMENTS_BODY }));

  const [payment] = await listTrayPaymentsByOrder(ORDER_ID, { deps });

  assert.equal(payment.id, "16704");
  assert.equal(payment.orderId, ORDER_ID);
  assert.equal(payment.value, "489.99");
  assert.equal(payment.date, "2026-09-09");
  assert.equal(payment.note, MARKER);
  // O rotulo factual do meio de pagamento vem de `payment_place`.
  assert.equal(payment.method, "NSCréditos");
  assert.equal(payment.paymentMethodId, "10547");
});

test("Payment sem `method` ainda casa pelo marker + order_id + valor", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ body: REAL_TRAY_PAYMENTS_BODY }));

  const out = await ensureTrayRedemptionPayment(
    { orderId: ORDER_ID, redemptionId: REDEMPTION_ID, value: ORDER_TOTAL, date: "2026-09-09" },
    { deps }
  );

  assert.equal(out.created, false, "reutiliza o Payment real, sem criar outro");
  assert.equal(calls.filter((c) => c.method === "POST").length, 0);
});

test("a listagem pede o limite maximo da Tray (maxLimit 50), nao o default 30", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ body: REAL_TRAY_PAYMENTS_BODY }));

  await listTrayPaymentsByOrder(ORDER_ID, { deps });

  assert.match(calls[0].url, /limit=50/);
});
