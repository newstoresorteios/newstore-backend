// tests/trayRedemptionOrder.test.js
//
// Identidade canonica: quando ja existe um Customer Tray, a identidade do
// Order.Customer vem da PROPRIA Tray. O CPF dos dois lados tem que bater --
// divergencia significa mapeamento errado e nao pode virar pedido.
import test from "node:test";
import assert from "node:assert/strict";

import { createTrayRedemptionOrder } from "../src/services/trayRedemptionOrder.js";

const ADDRESS = {
  street: "Rua Um",
  number: "1",
  neighborhood: "Centro",
  city: "Conselheiro Mairinck",
  state: "PR",
  zipcode: "86480000",
  country: "BR",
};

const BASE_PARAMS = {
  userId: 418,
  redemptionId: "2612b836-a0bd-4e74-b952-7620c8564e7c",
  items: [{ tray_product_id: "14518", tray_variant_id: null, quantity: 1 }],
  address: ADDRESS,
  couponSnapshot: { coupon_code: "NSU-0418-Q4", tray_coupon_id: "2618" },
};

/** options com o cache de tray_customer_id ja resolvido (sem tocar DB real). */
function makeOptions(trayCustomer, onPost) {
  const calls = [];
  return {
    calls,
    options: {
      // resolveTrayCustomerId le o cache por aqui -> curto-circuito, zero Tray
      query: async () => ({ rows: [{ tray_customer_id: "24858" }] }),
      deps: {
        getToken: async () => "token",
        getApiBase: async () => "https://exemplo.invalid/web_api",
        fetchImpl: async (url, opts) => {
          const method = String(opts?.method || "GET").toUpperCase();
          calls.push({ url: String(url), method });
          if (method === "POST") {
            onPost?.(JSON.parse(opts.body));
            return {
              ok: true,
              status: 201,
              headers: { get: () => "application/json" },
              json: async () => ({ id: 5555 }),
              text: async () => "{}",
            };
          }
          return {
            ok: true,
            status: 200,
            headers: { get: () => "application/json" },
            json: async () => ({ Customer: trayCustomer }),
            text: async () => "{}",
          };
        },
      },
    },
  };
}

test("CPF da Tray diferente do CPF local bloqueia ANTES de qualquer POST", async () => {
  const { calls, options } = makeOptions({
    id: 24858,
    cpf: "99999999999", // divergente
    name: "X",
    email: "x@y.com",
    birth_date: "2000-01-01",
    type: "0",
  });

  await assert.rejects(
    () =>
      createTrayRedemptionOrder(
        { ...BASE_PARAMS, userProfile: { name: "n", email: "e@f.com", cpf: "10425415902", birthDate: "2007-09-25", phone: "1" } },
        options
      ),
    (e) => e.code === "tray_customer_identity_conflict" && e.reason === "tray_customer_cpf_mismatch"
  );
  assert.equal(calls.filter((c) => c.method === "POST").length, 0, "nenhum POST com identidade divergente");
});

test("identidade do pedido vem da Tray; endereco vem da NewStore", async () => {
  let posted = null;
  const { options } = makeOptions(
    {
      id: 24858,
      cpf: "10425415902",
      name: "Cliente Tray",
      email: "tray@example.com", // divergente do e-mail NewStore, de proposito
      birth_date: "2007-09-25",
      phone: "43998640480",
      type: "0",
    },
    (body) => {
      posted = body;
    }
  );

  const out = await createTrayRedemptionOrder(
    {
      ...BASE_PARAMS,
      userProfile: { name: "Nome NewStore", email: "newstore@example.com", cpf: "10425415902", birthDate: "2007-09-25", phone: "999" },
    },
    options
  );

  assert.equal(out.orderId, "5555");

  const C = posted.Order.Customer;
  // identidade: TRAY
  assert.equal(C.email, "tray@example.com");
  assert.notEqual(C.email, "newstore@example.com");
  assert.equal(C.name, "Cliente Tray");
  assert.equal(C.cpf, "10425415902");
  assert.equal(C.birth_date, "2007-09-25");
  assert.equal(C.phone, "43998640480");
  // endereco: NEWSTORE
  assert.equal(C.CustomerAddress[0].city, "Conselheiro Mairinck");
  assert.equal(C.CustomerAddress[0].zip_code, "86480000");
  // um unico modelo de identidade
  assert.equal("customer_id" in posted.Order, false);
  assert.ok(Array.isArray(posted.Order.ProductsSold));
});
