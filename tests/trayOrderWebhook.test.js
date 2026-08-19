// tests/trayOrderWebhook.test.js
// Unidade: parsing do payload e desvios que nunca tocam o banco.
import test from "node:test";
import assert from "node:assert/strict";

import { parseOrderWebhookPayload, handleTrayOrderWebhook, TrayWebhookError } from "../src/services/trayOrderWebhook.js";

test("payload sem campo obrigatorio e rejeitado", () => {
  assert.throws(
    () => parseOrderWebhookPayload({ seller_id: "1", scope_id: "2", scope_name: "order" /* falta act */ }),
    (e) => e instanceof TrayWebhookError && e.code === "webhook_field_missing"
  );
});

test("payload completo e parseado corretamente", () => {
  const out = parseOrderWebhookPayload({ seller_id: "391250", scope_id: "4375797", scope_name: "order", act: "update", app_code: "718" });
  assert.deepEqual(out, { sellerId: "391250", scopeId: "4375797", scopeName: "order", act: "update" });
});

test("escopo diferente de order nunca consulta a Tray nem o banco", async () => {
  let queryCalled = false;
  let getOrderCalled = false;
  const out = await handleTrayOrderWebhook(
    { seller_id: "1", scope_id: "2", scope_name: "product", act: "update" },
    { query: async () => { queryCalled = true; }, getTrayOrderFull: async () => { getOrderCalled = true; } }
  );
  assert.equal(out.handled, false);
  assert.equal(out.reason, "scope_not_order");
  assert.equal(queryCalled, false);
  assert.equal(getOrderCalled, false);
});

test("seller_id configurado e diferente do recebido e rejeitado antes de qualquer chamada", async () => {
  let called = false;
  await assert.rejects(
    () =>
      handleTrayOrderWebhook(
        { seller_id: "999", scope_id: "2", scope_name: "order", act: "update" },
        { expectedSellerId: "391250", getTrayOrderFull: async () => { called = true; } }
      ),
    (e) => e instanceof TrayWebhookError && e.code === "webhook_seller_mismatch"
  );
  assert.equal(called, false);
});

test("sem seller_id configurado, nao bloqueia (best-effort documentado)", async () => {
  const out = await handleTrayOrderWebhook(
    { seller_id: "999", scope_id: "2", scope_name: "order", act: "update" },
    { getTrayOrderFull: async () => ({ couponCode: null, discount: 0 }) }
  );
  assert.equal(out.handled, false);
  assert.equal(out.reason, "no_coupon_discount_applied");
});

test("pedido sem coupon_code ou com discount=0 nunca toca o banco", async () => {
  let queryCalled = false;
  const out = await handleTrayOrderWebhook(
    { seller_id: "1", scope_id: "2", scope_name: "order", act: "insert" },
    { query: async () => { queryCalled = true; }, getTrayOrderFull: async () => ({ couponCode: "NSU-0001-AB", discount: 0 }) }
  );
  assert.equal(out.handled, false);
  assert.equal(out.reason, "no_coupon_discount_applied");
  assert.equal(queryCalled, false, "discount=0 nunca busca usuario nem debita");
});

test("coupon_code que nao pertence a nenhum usuario nosso e ignorado", async () => {
  const out = await handleTrayOrderWebhook(
    { seller_id: "1", scope_id: "2", scope_name: "order", act: "insert" },
    {
      query: async () => ({ rows: [] }),
      getTrayOrderFull: async () => ({ couponCode: "OUTRO-CUPOM-GENERICO", discount: 50 }),
    }
  );
  assert.equal(out.handled, false);
  assert.equal(out.reason, "coupon_code_not_ours");
});

test("saldo local ja zerado: no-op, nao tenta debitar", async () => {
  const out = await handleTrayOrderWebhook(
    { seller_id: "1", scope_id: "2", scope_name: "order", act: "insert" },
    {
      query: async () => ({ rows: [{ id: 42, coupon_value_cents: 0 }] }),
      getTrayOrderFull: async () => ({ couponCode: "NSU-0042-AB", discount: 100 }),
    }
  );
  assert.equal(out.handled, false);
  assert.equal(out.reason, "balance_already_zero");
});
