// tests/trayRedemptionSettlement.test.js
//
// Liquidacao do resgate na Tray, ponta a ponta no nivel HTTP:
//
//   GET /orders/:id            -> Order.total FACTUAL (nunca NSCreditos)
//   ensureTrayRedemptionPayment (GET /payments [+ POST /payments + GET])
//   GET /orders/:id            -> EXIGE has_payment === "1"
//   status operacional         -> PUT so quando ainda NAO esta "A ENVIAR"
//   confirmacao final          -> has_payment === "1" + status esperado
//
// Supera a decisao de 31/08: `has_payment` 0 NAO e mais aceitavel para um
// resgate novo — sem Payment real o resgate nao pode virar `confirmed`.
import test from "node:test";
import assert from "node:assert/strict";

import { settleTrayRedemptionOrder } from "../src/services/trayRedemptionOrder.js";
import { TrayCatalogError } from "../src/services/trayCatalogClient.js";

const API_BASE = "https://www.exemplo-loja.com.br/web_api";
const ORDER_ID = "25999";
const REDEMPTION_ID = "11111111-2222-3333-4444-555555555555";
const MARKER = `LOJA_NS_REDEMPTION:${REDEMPTION_ID}`;
const ORDER_TOTAL = "489.99";

function makeResponse({ status = 200, body = {} } = {}) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: (h) => (String(h).toLowerCase() === "content-type" ? "application/json" : null) },
    json: async () => body,
    text: async () => JSON.stringify(body),
  };
}

function isStatusListing(url) {
  return url.includes("/order_status") || url.includes("/orders/statuses");
}
function isPayments(url) {
  return url.includes("/payments");
}

/**
 * Tray simulada: guarda o Payment criado e reflete has_payment/status como a
 * loja real faria. `startStatus` controla o cenario de status.
 */
function makeTray({ startStatus = "AGUARDANDO PAGAMENTO", startStatusId = "16", total = ORDER_TOTAL, paymentsSeed = [] } = {}) {
  const calls = [];
  const state = { payments: [...paymentsSeed], status: startStatus, statusId: startStatusId };

  const deps = {
    getToken: async () => "token-abc",
    getApiBase: async () => API_BASE,
    fetchImpl: async (url, options) => {
      const u = String(url);
      const method = String(options?.method || "GET").toUpperCase();
      const body = options?.body ? JSON.parse(options.body) : null;
      calls.push({ url: u, method, body });

      if (isPayments(u)) {
        if (method === "POST") {
          state.payments.push({
            Payment: {
              id: String(77000 + state.payments.length + 1),
              order_id: body.Payment.order_id,
              method: body.Payment.method,
              value: body.Payment.value,
              date: body.Payment.date,
              note: body.Payment.note,
            },
          });
          return makeResponse({ status: 201, body: { id: "77001" } });
        }
        return makeResponse({ body: { Payments: state.payments } });
      }

      if (isStatusListing(u)) {
        return makeResponse({
          body: {
            OrderStatuses: [
              { OrderStatus: { id: "16", status: "AGUARDANDO PAGAMENTO" } },
              { OrderStatus: { id: "27", status: "A ENVIAR" } },
            ],
          },
        });
      }

      if (method === "PUT") {
        state.statusId = String(body.Order.status_id);
        state.status = state.statusId === "27" ? "A ENVIAR" : state.status;
        return makeResponse({ body: { message: "Saved", id: ORDER_ID, code: 200 } });
      }

      return makeResponse({
        body: {
          Order: {
            id: ORDER_ID,
            total,
            // A Tray so marca has_payment quando existe Payment de verdade.
            has_payment: state.payments.length ? "1" : "0",
            status: state.status,
            OrderStatus: { id: state.statusId, status: state.status },
          },
        },
      });
    },
  };

  return { calls, state, deps };
}

const OPTS = (deps) => ({ deps, cache: false });

/* ─────────────────────────── caminho feliz ─────────────────────────── */

test("cria o Payment com Order.total e so entao avanca o pedido para A ENVIAR", async () => {
  const { calls, deps } = makeTray();

  const out = await settleTrayRedemptionOrder({ orderId: ORDER_ID, redemptionId: REDEMPTION_ID }, OPTS(deps));

  assert.equal(out.hasPayment, "1");
  assert.equal(out.targetStatus.status, "A ENVIAR");
  assert.equal(out.payment.value, ORDER_TOTAL);
  assert.equal(out.payment.note, MARKER);

  const post = calls.find((c) => c.method === "POST");
  assert.deepEqual(post.body.Payment.order_id, ORDER_ID);
  assert.equal(post.body.Payment.method, "NSCréditos");
  assert.equal(post.body.Payment.value, ORDER_TOTAL);
  assert.match(post.body.Payment.date, /^\d{4}-\d{2}-\d{2}$/);
  assert.equal(post.body.Payment.note, MARKER);

  // O Payment nasce ANTES de qualquer mudanca de status.
  const postIndex = calls.findIndex((c) => c.method === "POST");
  const putIndex = calls.findIndex((c) => c.method === "PUT");
  assert.ok(postIndex >= 0 && putIndex > postIndex, "PUT de status vem depois do Payment");
});

test("o valor do Payment vem do pedido Tray, nunca dos NSCreditos do resgate", async () => {
  const { calls, deps } = makeTray({ total: "1234.50" });

  await settleTrayRedemptionOrder(
    // O resgate custou 5000 NSCreditos — irrelevante para a Tray.
    { orderId: ORDER_ID, redemptionId: REDEMPTION_ID, creditsAmount: 5000 },
    OPTS(deps)
  );

  const post = calls.find((c) => c.method === "POST");
  assert.equal(post.body.Payment.value, "1234.50");
  assert.doesNotMatch(JSON.stringify(calls.map((c) => c.body)), /5000/);
});

/* ─────────────────────────── gate do has_payment ─────────────────────────── */

test("has_payment continuando '0' impede a confirmacao do resgate", async () => {
  // Tray patologica: aceita o Payment, devolve na listagem, mas nunca
  // marca has_payment no pedido.
  const calls = [];
  const payments = [];
  const deps = {
    getToken: async () => "token-abc",
    getApiBase: async () => API_BASE,
    fetchImpl: async (url, options) => {
      const u = String(url);
      const method = String(options?.method || "GET").toUpperCase();
      const body = options?.body ? JSON.parse(options.body) : null;
      calls.push({ url: u, method });
      if (isPayments(u)) {
        if (method === "POST") {
          payments.push({ Payment: { id: "77001", order_id: ORDER_ID, method: "NSCréditos", value: ORDER_TOTAL, date: "2026-09-09", note: body.Payment.note } });
          return makeResponse({ status: 201, body: { id: "77001" } });
        }
        return makeResponse({ body: { Payments: payments } });
      }
      if (isStatusListing(u)) return makeResponse({ body: { OrderStatuses: [{ OrderStatus: { id: "27", status: "A ENVIAR" } }] } });
      return makeResponse({
        body: { Order: { id: ORDER_ID, total: ORDER_TOTAL, has_payment: "0", status: "AGUARDANDO PAGAMENTO", OrderStatus: { id: "16", status: "AGUARDANDO PAGAMENTO" } } },
      });
    },
  };

  await assert.rejects(
    () => settleTrayRedemptionOrder({ orderId: ORDER_ID, redemptionId: REDEMPTION_ID }, OPTS(deps)),
    (e) => e instanceof TrayCatalogError && e.code === "tray_payment_not_reflected"
  );

  assert.equal(calls.filter((c) => c.method === "PUT").length, 0, "nunca avanca status sem pagamento refletido");
});

test("has_payment '1' libera a verificacao de status", async () => {
  const { deps } = makeTray();
  const out = await settleTrayRedemptionOrder({ orderId: ORDER_ID, redemptionId: REDEMPTION_ID }, OPTS(deps));
  assert.equal(out.hasPayment, "1");
  assert.equal(out.order.OrderStatus.id, "27");
});

/* ─────────────────────────── status: sem PUT redundante ─────────────────────────── */

test("pedido ja em A ENVIAR: nenhum PUT de status e emitido", async () => {
  const { calls, deps } = makeTray({ startStatus: "A ENVIAR", startStatusId: "27" });

  const out = await settleTrayRedemptionOrder({ orderId: ORDER_ID, redemptionId: REDEMPTION_ID }, OPTS(deps));

  assert.equal(out.hasPayment, "1");
  assert.equal(out.targetStatus.status, "A ENVIAR");
  assert.equal(calls.filter((c) => c.method === "PUT").length, 0, "zero PUT redundante");
  assert.equal(calls.some((c) => isStatusListing(c.url)), false, "nem o lookup de status foi necessario");
});

test("pedido ainda nao em A ENVIAR: reutiliza a resolucao dinamica existente (nunca ID fixo)", async () => {
  const { calls, deps } = makeTray();

  await settleTrayRedemptionOrder({ orderId: ORDER_ID, redemptionId: REDEMPTION_ID }, OPTS(deps));

  assert.ok(calls.some((c) => isStatusListing(c.url)), "resolveu o status pela listagem factual");
  const put = calls.find((c) => c.method === "PUT");
  assert.deepEqual(put.body, { Order: { status_id: "27" } });
  assert.equal(calls.filter((c) => c.method === "PUT").length, 1);
});

test("A ENVIAR inexistente nesta conta: falha fechada sem PUT arbitrario", async () => {
  const calls = [];
  const payments = [];
  const deps = {
    getToken: async () => "token-abc",
    getApiBase: async () => API_BASE,
    fetchImpl: async (url, options) => {
      const u = String(url);
      const method = String(options?.method || "GET").toUpperCase();
      const body = options?.body ? JSON.parse(options.body) : null;
      calls.push({ url: u, method });
      if (isPayments(u)) {
        if (method === "POST") {
          payments.push({ Payment: { id: "77001", order_id: ORDER_ID, method: "NSCréditos", value: ORDER_TOTAL, date: "2026-09-09", note: body.Payment.note } });
          return makeResponse({ status: 201, body: { id: "77001" } });
        }
        return makeResponse({ body: { Payments: payments } });
      }
      if (isStatusListing(u)) {
        return makeResponse({ body: { OrderStatuses: [{ OrderStatus: { id: "16", status: "AGUARDANDO PAGAMENTO" } }] } });
      }
      return makeResponse({
        body: { Order: { id: ORDER_ID, total: ORDER_TOTAL, has_payment: payments.length ? "1" : "0", status: "AGUARDANDO PAGAMENTO", OrderStatus: { id: "16", status: "AGUARDANDO PAGAMENTO" } } },
      });
    },
  };

  await assert.rejects(
    () => settleTrayRedemptionOrder({ orderId: ORDER_ID, redemptionId: REDEMPTION_ID }, OPTS(deps)),
    (e) => e instanceof TrayCatalogError && e.code === "tray_operational_status_not_found"
  );

  assert.equal(calls.filter((c) => c.method === "PUT").length, 0);
});

/* ─────────────────────────── Order.total ─────────────────────────── */

test("Order.total ausente/invalido falha ANTES de qualquer Payment", async () => {
  for (const total of [null, "", "gratis"]) {
    const { calls, deps } = makeTray({ total });
    // eslint-disable-next-line no-await-in-loop
    await assert.rejects(
      () => settleTrayRedemptionOrder({ orderId: ORDER_ID, redemptionId: REDEMPTION_ID }, OPTS(deps)),
      (e) => e instanceof TrayCatalogError && e.code === "tray_order_total_invalid",
      `total=${JSON.stringify(total)} deveria falhar fechado`
    );
    assert.equal(calls.filter((c) => c.method === "POST").length, 0, "nenhum Payment criado sem total factual");
  }
});

/* ─────────────────────────── idempotencia da liquidacao ─────────────────────────── */

test("liquidar o mesmo resgate duas vezes nao cria um segundo Payment", async () => {
  const { calls, deps } = makeTray();

  await settleTrayRedemptionOrder({ orderId: ORDER_ID, redemptionId: REDEMPTION_ID }, OPTS(deps));
  await settleTrayRedemptionOrder({ orderId: ORDER_ID, redemptionId: REDEMPTION_ID }, OPTS(deps));

  assert.equal(calls.filter((c) => c.method === "POST").length, 1, "exatamente um Payment");
  assert.equal(calls.filter((c) => c.method === "PUT").length, 1, "e exatamente um PUT de status");
});

test("Payment pre-existente do resgate com valor divergente nao e aceito", async () => {
  const { calls, deps } = makeTray({
    paymentsSeed: [
      { Payment: { id: "70000", order_id: ORDER_ID, method: "NSCréditos", value: "10.00", date: "2026-09-01", note: MARKER } },
    ],
  });

  await assert.rejects(
    () => settleTrayRedemptionOrder({ orderId: ORDER_ID, redemptionId: REDEMPTION_ID }, OPTS(deps)),
    (e) => e instanceof TrayCatalogError && e.code === "tray_payment_value_mismatch"
  );

  assert.equal(calls.filter((c) => c.method === "POST").length, 0, "nunca cria um segundo Payment por cima");
  assert.equal(calls.filter((c) => c.method === "PUT").length, 0);
});

/* ─────────────────────────── nenhuma mutacao fora do contrato ─────────────────────────── */

test("a liquidacao so emite GET, um POST /payments e no maximo um PUT /orders/:id", async () => {
  const { calls, deps } = makeTray();

  await settleTrayRedemptionOrder({ orderId: ORDER_ID, redemptionId: REDEMPTION_ID }, OPTS(deps));

  const mutations = calls.filter((c) => c.method !== "GET");
  assert.deepEqual(
    mutations.map((c) => `${c.method} ${c.url.replace(/\?.*$/, "").replace(API_BASE, "")}`),
    ["POST /payments", `PUT /orders/${ORDER_ID}`]
  );
  assert.equal(calls.some((c) => c.method === "DELETE" || c.method === "PATCH"), false);
});
