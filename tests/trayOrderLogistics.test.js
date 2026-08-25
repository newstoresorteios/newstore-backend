// tests/trayOrderLogistics.test.js
//
// Normalizacao READ-ONLY da logistica do pedido Tray.
//
// Todas as fixtures abaixo usam a FORMA REAL do payload observado numa
// auditoria read-only da loja (150 pedidos pela listagem + 6 detalhes de
// pedidos ENVIADO/FINALIZADO). Os IDs sao ficticios de proposito: nenhum
// teste depende de um pedido especifico de producao.
import test from "node:test";
import assert from "node:assert/strict";

import {
  normalizeTrayLogistics,
  buildCustomerTrayStatus,
  buildAdminTrayOrderView,
  safeTrackingUrl,
  TRAY_LOGISTICS_PHASES,
} from "../src/services/trayOrderLogistics.js";

/** Pedido recem-criado pela Loja NS: existe na Tray, sem nenhuma logistica. */
function pedidoRecebido(overrides = {}) {
  return {
    id: 90001,
    status: "AGUARDANDO PAGAMENTO",
    OrderStatus: { type: "open", status: "AGUARDANDO PAGAMENTO", display_name: "" },
    date: "2026-08-24",
    modified: "2026-08-24 22:23:57",
    point_sale: "LOJA NS",
    payment_method: "NSCréditos",
    shipment: "PENDENTE TRAY",
    shipment_value: "0.00",
    shipment_integrator: "",
    shipment_date: "",
    sending_code: "",
    sending_date: "0000-00-00",
    tracking_url: "",
    has_shipment: "0",
    is_traceable: "0",
    delivered: "",
    delivered_status: "",
    delivery_date: "",
    delivery_time: "",
    estimated_delivery_date: "2026-08-24",
    total: "489.99",
    store_note: "Resgate Loja NS / redemption_id=00000000-0000-0000-0000-000000000001",
    customer_note: "",
    customer_id: 8095,
    ...overrides,
  };
}

/** Pedido realmente postado: forma exata dos pedidos ENVIADO reais. */
function pedidoEnviado(overrides = {}) {
  return {
    id: 90002,
    status: "ENVIADO",
    OrderStatus: { type: "open", status: "ENVIADO", display_name: "" },
    date: "2026-05-27",
    modified: "2026-05-28 15:14:29",
    point_sale: "LOJA VIRTUAL",
    shipment: "Sedex",
    shipment_value: "51.48",
    shipment_integrator: "Correios",
    shipment_date: "2026-05-28",
    sending_code: "AD507735291BR",
    sending_date: "2026-05-28",
    tracking_url: "https://www.exemplo-loja.com.br/rastreio?cod_acesso=A4400C4741",
    has_shipment: "1",
    is_traceable: "1",
    delivered: "",
    delivered_status: "",
    delivery_date: "",
    delivery_time: "14",
    estimated_delivery_date: "2026-06-17",
    total: "551.47",
    ...overrides,
  };
}

/* ─────────────── Sem logistica: fallback honesto ─────────────── */

test("pedido sem envio: recebido pela Tray, aguardando logistica", () => {
  const out = normalizeTrayLogistics(pedidoRecebido());

  assert.equal(out.phase, TRAY_LOGISTICS_PHASES.RECEIVED);
  assert.equal(out.label, "Pedido recebido pela Tray");
  assert.match(out.hint, /Aguardando atualiza/i);
});

test('status "AGUARDANDO PAGAMENTO" nunca vira status logistico', () => {
  const out = normalizeTrayLogistics(pedidoRecebido());
  const serialized = JSON.stringify(out);

  assert.equal(out.phase, "received");
  assert.ok(!/aguardando envio/i.test(serialized));
  assert.ok(!/separa[cç]/i.test(out.label));
  assert.ok(!/AGUARDANDO PAGAMENTO/i.test(serialized), "status comercial cru nunca entra na logistica");
});

test("campo logistico ausente e OMITIDO, nunca devolvido como null", () => {
  const out = normalizeTrayLogistics(pedidoRecebido());

  for (const key of ["carrier", "tracking_code", "tracking_url", "shipped_at", "estimated_delivery_at"]) {
    assert.ok(!(key in out), `${key} nao deveria existir sem evidencia`);
  }
});

test('"PENDENTE TRAY" nao e forma de envio real e nao e exibido', () => {
  const out = normalizeTrayLogistics(pedidoRecebido());
  assert.equal(out.shipment_method, undefined);
});

test("previsao de entrega nao aparece sem envio (evita prazo inventado)", () => {
  // A Tray devolve estimated_delivery_date em 100% dos pedidos, inclusive
  // igual a data do pedido quando nada foi enviado.
  const out = normalizeTrayLogistics(pedidoRecebido({ estimated_delivery_date: "2026-08-24" }));
  assert.equal(out.estimated_delivery_at, undefined);
});

/* ─────────────── Enviado: so com evidencia factual ─────────────── */

test("pedido postado vira fase enviado com os dados reais", () => {
  const out = normalizeTrayLogistics(pedidoEnviado());

  assert.equal(out.phase, TRAY_LOGISTICS_PHASES.SHIPPED);
  assert.equal(out.label, "Pedido enviado");
  assert.equal(out.shipment_method, "Sedex");
  assert.equal(out.carrier, "Correios");
  assert.equal(out.tracking_code, "AD507735291BR");
  assert.equal(out.tracking_url, "https://www.exemplo-loja.com.br/rastreio?cod_acesso=A4400C4741");
  assert.equal(out.shipped_at, "2026-05-28");
  assert.equal(out.is_traceable, true);
  assert.equal(out.estimated_delivery_at, "2026-06-17");
  assert.equal(out.updated_at, "2026-05-28 15:14:29");
});

test("codigo de rastreio sozinho ja e evidencia de envio", () => {
  const out = normalizeTrayLogistics(
    pedidoRecebido({ sending_code: "LW067310786US", has_shipment: "0" })
  );
  assert.equal(out.phase, "shipped");
  assert.equal(out.tracking_code, "LW067310786US");
});

test("previsao anterior a data do pedido e descartada", () => {
  const out = normalizeTrayLogistics(pedidoEnviado({ estimated_delivery_date: "2026-05-01" }));
  assert.equal(out.phase, "shipped");
  assert.equal(out.estimated_delivery_at, undefined);
});

/* ─────────────── Entrega: sem evidencia, sem fase ─────────────── */

test("FINALIZADO NAO e prova de entrega", () => {
  // Nos 6 pedidos reais inspecionados (inclusive FINALIZADO), delivered,
  // delivered_status e delivery_date vieram TODOS vazios.
  const out = normalizeTrayLogistics(
    pedidoEnviado({ status: "FINALIZADO", OrderStatus: { type: "closed" }, delivery_time: "31" })
  );

  assert.equal(out.phase, "shipped");
  assert.ok(!/entregue/i.test(out.label));
  assert.equal(out.delivered_at, undefined);
});

test("delivery_time e prazo em dias, nunca horario de entrega", () => {
  const out = normalizeTrayLogistics(pedidoEnviado({ delivery_time: "31" }));
  assert.ok(!JSON.stringify(out).includes("31"), "delivery_time nao pode virar dado de entrega");
});

/* ─────────────── Cancelado ─────────────── */

test("pedido cancelado pela Tray e reportado como cancelado", () => {
  for (const fixture of [
    pedidoRecebido({ status: "CANCELADO", OrderStatus: { type: "canceled" } }),
    pedidoRecebido({ status: "CANCELADO AUT", OrderStatus: { type: "canceled" } }),
  ]) {
    const out = normalizeTrayLogistics(fixture);
    assert.equal(out.phase, TRAY_LOGISTICS_PHASES.CANCELED);
    assert.equal(out.label, "Pedido cancelado");
  }
});

/* ─────────────── URL de rastreio ─────────────── */

test("so aceita URL http(s) devolvida pela propria Tray", () => {
  assert.equal(safeTrackingUrl("https://exemplo.com/x"), "https://exemplo.com/x");
  assert.equal(safeTrackingUrl("http://exemplo.com/x"), "http://exemplo.com/x");
  assert.equal(safeTrackingUrl("javascript:alert(1)"), null);
  assert.equal(safeTrackingUrl("data:text/html,<script>"), null);
  assert.equal(safeTrackingUrl(""), null);
  assert.equal(safeTrackingUrl(null), null);
  assert.equal(safeTrackingUrl("nao-e-url"), null);
});

test("URL perigosa no payload nunca chega ao DTO", () => {
  const out = normalizeTrayLogistics(pedidoEnviado({ tracking_url: "javascript:alert(1)" }));
  assert.equal(out.tracking_url, undefined);
});

/* ─────────────── DTO do cliente: whitelist ─────────────── */

test("DTO do cliente nao expoe resposta crua, store_note nem PII", () => {
  const out = buildCustomerTrayStatus(
    pedidoEnviado({
      store_note: "Resgate Loja NS / redemption_id=00000000-0000-0000-0000-000000000002",
      customer_note: "anotacao interna",
      customer_id: 8095,
      Customer: { cpf: "12345678901", email: "cliente@exemplo.com", name: "Fulano" },
      CustomerAddress: { street: "Rua X", zip_code: "01001000" },
      ProductsSold: [{ product_id: 1 }],
      Payment: [{ id: 1 }],
      access_code: "E5FA7DE34178B1E",
    })
  );

  const serialized = JSON.stringify(out);
  assert.equal(out.available, true);
  assert.deepEqual(Object.keys(out).sort(), ["available", "logistics", "order"]);
  assert.deepEqual(Object.keys(out.order), ["id"]);

  for (const forbidden of [
    "12345678901",
    "cliente@exemplo.com",
    "Fulano",
    "Rua X",
    "01001000",
    "store_note",
    "redemption_id",
    "anotacao interna",
    "access_code",
    "E5FA7DE34178B1E",
    "ProductsSold",
    "Payment",
    "customer_id",
  ]) {
    assert.ok(!serialized.includes(forbidden), `DTO do cliente jamais pode conter ${forbidden}`);
  }
});

test("DTO do cliente nunca mostra o status comercial cru da Tray", () => {
  const out = buildCustomerTrayStatus(pedidoRecebido());
  assert.ok(!JSON.stringify(out).includes("AGUARDANDO PAGAMENTO"));
  assert.equal(out.order.status, undefined);
});

test("payload irreconhecivel devolve null em vez de estrutura vazia", () => {
  assert.equal(normalizeTrayLogistics(null), null);
  assert.equal(normalizeTrayLogistics("texto"), null);
  assert.equal(buildCustomerTrayStatus(null), null);
});

/* ─────────────── Visao do admin ─────────────── */

test("admin ve o status comercial cru e a mesma normalizacao logistica", () => {
  const out = buildAdminTrayOrderView(pedidoEnviado());

  assert.equal(out.tray_order_id, "90002");
  assert.equal(out.status, "ENVIADO");
  assert.equal(out.status_type, "open");
  assert.equal(out.shipment_value, "51.48");
  assert.equal(out.logistics.phase, "shipped");
  assert.equal(out.logistics.tracking_code, "AD507735291BR");
});

test("admin tambem nao recebe store_note, Customer nem itens do pedido", () => {
  const serialized = JSON.stringify(
    buildAdminTrayOrderView(
      pedidoEnviado({
        store_note: "Resgate Loja NS / redemption_id=xyz",
        Customer: { cpf: "12345678901" },
        ProductsSold: [{ product_id: 1 }],
      })
    )
  );
  assert.ok(!serialized.includes("12345678901"));
  assert.ok(!serialized.includes("redemption_id=xyz"));
  assert.ok(!serialized.includes("ProductsSold"));
});
