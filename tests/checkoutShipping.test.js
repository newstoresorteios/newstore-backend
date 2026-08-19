// tests/checkoutShipping.test.js
import test from "node:test";
import assert from "node:assert/strict";

import { getShippingOptionsForCart, CheckoutError } from "../src/services/checkoutShipping.js";

const ADDRESS_ROW = {
  id: "addr-1", user_id: 1, recipient_name: "Joao", zipcode: "01304001", street: "Rua Augusta",
  number: "123", complement: null, neighborhood: "Consolacao", city: "Sao Paulo", state: "SP",
  country: "BR", is_default: true, created_at: new Date().toISOString(),
};

function makeQuery({ cart = { id: "cart-1", user_id: 1, status: "active" }, items = [] } = {}) {
  return async (sql, params) => {
    const s = String(sql).toLowerCase();
    if (/from public\.user_addresses where id = \$1 and user_id = \$2/.test(s)) {
      return { rows: String(params[0]) === ADDRESS_ROW.id && params[1] === 1 ? [ADDRESS_ROW] : [] };
    }
    if (/from public\.reward_carts where user_id/.test(s)) {
      return { rows: cart && cart.user_id === params[0] ? [cart] : [] };
    }
    if (/from public\.reward_cart_items i/.test(s)) {
      return { rows: items };
    }
    throw new Error(`SQL nao mapeado: ${sql}`);
  };
}

function itemRow({ id = "item-1", tray_product_id = "900001", tray_variant_id = null, quantity = 1, nscredits_price = 1500 } = {}) {
  return {
    id, reward_product_id: "rp-1", tray_product_id, tray_variant_id, quantity,
    nscredits_unit_price_snapshot: nscredits_price, current_nscredits_price: nscredits_price,
    is_published: true, product_has_variation: false, current_name: "Kit Relogio",
    variant_name_snapshot: null, image_url_snapshot: null, created_at: new Date().toISOString(),
  };
}

const SIMPLE_PRODUCT = { tray_product_id: "900001", tray_price: 2500, variants: [] };

function baseDeps({ query, getCatalogProduct, shippingBody = { Shipping: [{ id: "1", name: "PAC", price: "25.90", delivery_time: "8", delivery_time_text: "8 dias" }] } } = {}) {
  return {
    query,
    getCatalogProduct: getCatalogProduct || (async () => SIMPLE_PRODUCT),
    getToken: async () => "token",
    getApiBase: async () => "https://loja.exemplo.com.br/web_api",
    fetchImpl: async () => ({
      ok: true, status: 200,
      headers: { get: (h) => (String(h).toLowerCase() === "content-type" ? "application/json" : null) },
      json: async () => shippingBody,
      text: async () => JSON.stringify(shippingBody),
    }),
  };
}

test("endereco de outro usuario nao e encontrado", async () => {
  const deps = baseDeps({ query: makeQuery({ items: [itemRow()] }) });
  await assert.rejects(
    () => getShippingOptionsForCart(2, ADDRESS_ROW.id, deps),
    (e) => e instanceof CheckoutError && e.code === "address_not_found"
  );
});

test("carrinho vazio nao cotaciona frete", async () => {
  const deps = baseDeps({ query: makeQuery({ items: [] }) });
  await assert.rejects(
    () => getShippingOptionsForCart(1, ADDRESS_ROW.id, deps),
    (e) => e.code === "cart_empty"
  );
});

test("usa o preco REAL da Tray no item, nao o preco em NSCreditos", async () => {
  const deps = baseDeps({ query: makeQuery({ items: [itemRow({ nscredits_price: 1500 })] }) });
  const out = await getShippingOptionsForCart(1, ADDRESS_ROW.id, deps);

  assert.equal(out.items[0].tray_price_cents, 250000); // R$2500,00 do catalogo Tray, nao 1500
  assert.equal(out.options.length, 1);
  assert.equal(out.options[0].name, "PAC");
  assert.equal(out.options[0].price_cents, 2590);
});

test("produto com variacao usa o preco da variante quando existe", async () => {
  const productWithVariant = {
    tray_product_id: "900002",
    tray_price: 100,
    variants: [{ variant_id: "v1", price: 199.9 }],
  };
  const deps = baseDeps({
    query: makeQuery({ items: [itemRow({ tray_product_id: "900002", tray_variant_id: "v1" })] }),
    getCatalogProduct: async () => productWithVariant,
  });
  const out = await getShippingOptionsForCart(1, ADDRESS_ROW.id, deps);
  assert.equal(out.items[0].tray_price_cents, 19990);
});

test("falha da Tray no catalogo vira erro tipado, nao explode cru", async () => {
  const deps = baseDeps({
    query: makeQuery({ items: [itemRow()] }),
    getCatalogProduct: async () => { throw new Error("boom"); },
  });
  await assert.rejects(() => getShippingOptionsForCart(1, ADDRESS_ROW.id, deps), (e) => e.code === "tray_unavailable");
});

test("array Shipping vazio nao e erro — so nenhuma opcao disponivel", async () => {
  const deps = baseDeps({ query: makeQuery({ items: [itemRow()] }), shippingBody: { Shipping: [] } });
  const out = await getShippingOptionsForCart(1, ADDRESS_ROW.id, deps);
  assert.deepEqual(out.options, []);
});
