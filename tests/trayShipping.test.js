// tests/trayShipping.test.js
// Cotacao de frete Tray: SOMENTE LEITURA, nunca cobra nada.
import test from "node:test";
import assert from "node:assert/strict";

import { fetchShippingCotation } from "../src/services/trayShipping.js";
import { TrayCatalogError } from "../src/services/trayCatalogClient.js";

const API_BASE = "https://www.exemplo-loja.com.br/web_api";
const TOKEN = "token-abc";

function makeResponse({ status = 200, body = {} } = {}) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: (name) => (String(name).toLowerCase() === "content-type" ? "application/json" : null) },
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
        calls.push({ url: String(url), method: options?.method || "GET" });
        return handler(String(url));
      },
    },
  };
}

test("zipcode invalido nunca chega a rede", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ body: { Shipping: [] } }));
  await assert.rejects(
    () => fetchShippingCotation({ zipcode: "123", items: [{ trayProductId: "1", priceCents: 1000, quantity: 1 }] }, { deps }),
    (e) => e instanceof TrayCatalogError && e.code === "invalid_zipcode"
  );
  assert.equal(calls.length, 0);
});

test("carrinho sem itens nunca chega a rede", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ body: { Shipping: [] } }));
  await assert.rejects(
    () => fetchShippingCotation({ zipcode: "01304001", items: [] }, { deps }),
    (e) => e.code === "shipping_cotation_no_items"
  );
  assert.equal(calls.length, 0);
});

test("monta querystring products[n][...] a partir dos itens, so GET", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ body: { Shipping: [] } }));
  await fetchShippingCotation(
    { zipcode: "01304-001", items: [{ trayProductId: "900001", priceCents: 459900, quantity: 2 }] },
    { deps }
  );

  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "GET");
  const url = new URL(calls[0].url);
  assert.equal(url.searchParams.get("zipcode"), "01304001");
  assert.equal(url.searchParams.get("products[0][product_id]"), "900001");
  assert.equal(url.searchParams.get("products[0][price]"), "4599.00");
  assert.equal(url.searchParams.get("products[0][quantity]"), "2");
});

test("normaliza a resposta da Tray para price_cents e prazo em dias", async () => {
  const { deps } = makeDeps(() =>
    makeResponse({
      body: {
        Shipping: [
          { id: "1", name: "PAC", price: "25.90", delivery_time: "8", delivery_time_text: "8 dias uteis" },
          { id: "2", name: "SEDEX", price: "45.50", delivery_time: "3", delivery_time_text: "3 dias uteis" },
        ],
      },
    })
  );

  const out = await fetchShippingCotation({ zipcode: "01304001", items: [{ trayProductId: "1", priceCents: 1000, quantity: 1 }] }, { deps });

  assert.equal(out.length, 2);
  assert.deepEqual(out[0], { id: "1", name: "PAC", price_cents: 2590, delivery_time_days: 8, delivery_time_text: "8 dias uteis" });
  assert.equal(out[1].price_cents, 4550);
});

test("array Shipping vazio e resposta valida (indisponibilidade regional), nao erro", async () => {
  const { deps } = makeDeps(() => makeResponse({ body: { Shipping: [] } }));
  const out = await fetchShippingCotation({ zipcode: "01304001", items: [{ trayProductId: "1", priceCents: 1000, quantity: 1 }] }, { deps });
  assert.deepEqual(out, []);
});

test("usa o preco REAL da Tray (priceCents), nunca o preco em NSCreditos", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ body: { Shipping: [] } }));
  // priceCents aqui simula o preco Tray (ex.: R$ 2.500,00), bem diferente do
  // preco em NSCreditos do produto (ex.: 1500) — a funcao nao sabe nem deve
  // saber sobre NSCreditos, so recebe centavos e converte para BRL.
  await fetchShippingCotation({ zipcode: "01304001", items: [{ trayProductId: "1", priceCents: 250000, quantity: 1 }] }, { deps });
  const url = new URL(calls[0].url);
  assert.equal(url.searchParams.get("products[0][price]"), "2500.00");
});
