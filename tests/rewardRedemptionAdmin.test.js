// tests/rewardRedemptionAdmin.test.js
//
// Read model administrativo do resgate — testes sem banco.
//
// O que EXIGE Postgres real (agregacao de relatorio, exclusao de
// compensados, clientes unicos, paginacao real) esta em
// tests/rewardRedemptionAdmin.integration.test.js, na mesma convencao ja
// usada por couponLedger.integration.test.js: roda com TEST_DATABASE_URL,
// pulado sem ela — nunca contra banco de producao.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import {
  REDEMPTION_STATUS_CATALOG,
  REDEMPTION_STATUSES,
  describeRedemptionStatus,
  summarizeRedemptionLedger,
  buildAdminEventMeta,
  mapAddressSnapshot,
  mapTrayOrderForAdmin,
  parseDateBoundary,
  listAdminRedemptions,
  getRedemptionReportMetrics,
  getRedemptionReport,
  getAdminRedemptionDetail,
  getAdminRedemptionTrayOrder,
  getTrayOrderWebhookEvidence,
} from "../src/services/rewardRedemptionAdmin.js";

const REDEMPTION_ID = "11111111-2222-3333-4444-555555555555";

/** Fake de `query` que grava cada SQL executado e responde por padrao. */
function recordingQuery(responder = () => ({ rows: [] })) {
  const calls = [];
  const query = async (sql, params) => {
    calls.push({ sql: String(sql).replace(/\s+/g, " ").trim(), params: params || [] });
    return responder(String(sql), params || []) || { rows: [] };
  };
  return { calls, query };
}

/* ─────────────────── Status: a lista REAL, nunca inventada ─────────────────── */

test("o catalogo de status cobre exatamente o CHECK factual do banco", () => {
  const migration = fileURLToPath(new URL("../src/migrations/034_reward_redemptions_profile_ambiguous.sql", import.meta.url));
  const sql = readFileSync(migration, "utf8");
  const checkBlock = sql.slice(sql.lastIndexOf("CHECK (status IN ("));
  const fromDb = [...checkBlock.matchAll(/'([a-z_]+)'/g)].map((m) => m[1]);

  assert.ok(fromDb.length >= 12, "migration deveria listar os status no CHECK");
  assert.deepEqual([...REDEMPTION_STATUSES].sort(), [...new Set(fromDb)].sort());
});

test("cada status tem rotulo administrativo proprio, nunca o codigo cru", () => {
  for (const entry of REDEMPTION_STATUS_CATALOG) {
    assert.ok(entry.label && entry.label !== entry.status, `status ${entry.status} precisa de rotulo`);
    assert.ok(entry.description, `status ${entry.status} precisa de descricao`);
    assert.ok(["success", "warning", "error", "info", "neutral"].includes(entry.severity));
  }
});

test("somente confirmed conta como sucesso do resgate", () => {
  const success = REDEMPTION_STATUS_CATALOG.filter((s) => s.is_success).map((s) => s.status);
  assert.deepEqual(success, ["confirmed"]);
});

test("compensados e falhas nunca ficam com credito comprometido", () => {
  for (const status of ["compensated", "failed", "blocked_tray_profile_incomplete", "blocked_tray_customer_ambiguous"]) {
    assert.equal(describeRedemptionStatus(status).credits_committed, false, status);
  }
  // Ambiguidade: credito SEGUE debitado ate conferencia manual.
  assert.equal(describeRedemptionStatus("reconciliation_required").credits_committed, true);
  assert.equal(describeRedemptionStatus("reconciliation_required").is_compensated, false);
});

test("status desconhecido nao ganha rotulo inventado", () => {
  assert.equal(describeRedemptionStatus("qualquer_coisa"), null);
});

/* ─────────────────────────── Ledger ─────────────────────────── */

function ledgerEntry(deltaCents, before, after, eventType) {
  return {
    id: 1,
    event_type: eventType,
    operation: deltaCents >= 0 ? "credit" : "debit",
    delta_cents: deltaCents,
    delta: deltaCents / 100,
    balance_before_cents: before,
    balance_after_cents: after,
    balance_before: before / 100,
    balance_after: after / 100,
    created_at: "2026-08-20T10:00:00.000Z",
  };
}

test("confirmado: espera debito e nenhuma compensacao", () => {
  const out = summarizeRedemptionLedger([ledgerEntry(-3000000, 5000000, 2000000, "REDEMPTION_DEBIT")], "confirmed");
  assert.equal(out.debit_cents, 3000000);
  assert.equal(out.compensation_cents, 0);
  assert.equal(out.expectation, "debit_only");
  assert.equal(out.matches_expectation, true);
  assert.equal(out.balance_before_cents, 5000000);
  assert.equal(out.balance_after_cents, 2000000);
});

test("compensado: espera debito + compensacao de mesmo valor, efeito liquido zero", () => {
  const out = summarizeRedemptionLedger(
    [
      ledgerEntry(-3000000, 5000000, 2000000, "REDEMPTION_DEBIT"),
      ledgerEntry(3000000, 2000000, 5000000, "REDEMPTION_COMPENSATION"),
    ],
    "compensated"
  );
  assert.equal(out.debit_cents, 3000000);
  assert.equal(out.compensation_cents, 3000000);
  assert.equal(out.net_cents, 0);
  assert.equal(out.expectation, "debit_and_compensation");
  assert.equal(out.matches_expectation, true);
});

test("confirmado com compensacao inesperada e sinalizado como divergente", () => {
  const out = summarizeRedemptionLedger(
    [
      ledgerEntry(-3000000, 5000000, 2000000, "REDEMPTION_DEBIT"),
      ledgerEntry(3000000, 2000000, 5000000, "REDEMPTION_COMPENSATION"),
    ],
    "confirmed"
  );
  assert.equal(out.matches_expectation, false);
});

test("falhou sem lancamento nenhum e o esperado", () => {
  const out = summarizeRedemptionLedger([], "failed");
  assert.equal(out.expectation, "no_movement");
  assert.equal(out.matches_expectation, true);
  assert.equal(out.balance_before_cents, null);
});

/* ─────────────────── Metadata dos eventos: whitelist ─────────────────── */

test("meta do evento nunca vaza token, senha, cookie ou URL de banco", () => {
  const out = buildAdminEventMeta({
    http_status: 400,
    tray_error_code: "tray_request_failed",
    access_token: "abc123",
    refresh_token: "def456",
    authorization: "Bearer xyz",
    password: "hunter2",
    cookie: "session=1",
    database_url: "postgres://user:pass@host/db",
    tray_body: { message: "Erro de validacao", causes: { Customer: { cpf: "Está em uso em outro cadastro." } } },
  });

  const serialized = JSON.stringify(out);
  assert.equal(out.http_status, 400);
  assert.equal(out.tray_error_code, "tray_request_failed");
  assert.ok(out.tray_messages.some((m) => /Está em uso em outro cadastro/.test(m)));
  for (const forbidden of ["abc123", "def456", "Bearer xyz", "hunter2", "session=1", "postgres://"]) {
    assert.ok(!serialized.includes(forbidden), `meta jamais pode conter ${forbidden}`);
  }
  assert.ok(!/access_token|refresh_token|password|cookie|database_url/i.test(serialized));
});

test("meta com CPF no corpo da Tray e redigido na leitura", () => {
  const out = buildAdminEventMeta({ tray_body: { Customer: { cpf: "12345678901" } } });
  assert.ok(!JSON.stringify(out).includes("12345678901"));
});

test("meta vazio vira null em vez de objeto ruidoso", () => {
  assert.equal(buildAdminEventMeta({}), null);
  assert.equal(buildAdminEventMeta(null), null);
  assert.equal(buildAdminEventMeta("nao-e-json"), null);
});

test("meta aceita jsonb devolvido como string", () => {
  const out = buildAdminEventMeta(JSON.stringify({ http_status: 502 }));
  assert.equal(out.http_status, 502);
});

/* ─────────────────────────── Endereco ─────────────────────────── */

test("endereco usa o snapshot do resgate e so os campos da operacao", () => {
  const out = mapAddressSnapshot({
    id: "addr-1",
    recipient_name: "Fulano de Tal",
    zipcode: "01001000",
    street: "Praca da Se",
    number: "10",
    complement: null,
    neighborhood: "Se",
    city: "Sao Paulo",
    state: "SP",
    country: "BR",
    is_default: true,
    created_at: "2026-01-01T00:00:00.000Z",
  });

  assert.deepEqual(Object.keys(out).sort(), [
    "city",
    "complement",
    "country",
    "neighborhood",
    "number",
    "recipient_name",
    "state",
    "street",
    "zipcode",
  ]);
  assert.equal(out.city, "Sao Paulo");
  assert.equal(out.complement, null);
});

test("resgate sem snapshot de endereco devolve null, nunca campos vazios", () => {
  assert.equal(mapAddressSnapshot(null), null);
  assert.equal(mapAddressSnapshot({}), null);
});

/* ─────────────────────────── Pedido Tray ─────────────────────────── */

test("pedido Tray expoe somente campos uteis e nunca inventa rastreio", () => {
  const out = mapTrayOrderForAdmin({
    id: 25626,
    status: "A enviar",
    payment_method: "NSCréditos",
    point_sale: "LOJA NS",
    shipment: "PENDENTE TRAY",
    shipment_value: "0.00",
    total: "299.90",
    date: "2026-08-21 10:00:00",
    modified: "2026-08-21 11:00:00",
    Customer: { cpf: "12345678901", email: "cliente@exemplo.com" },
  });

  assert.equal(out.tray_order_id, "25626");
  assert.equal(out.status, "A enviar");
  assert.equal(out.point_sale, "LOJA NS");
  assert.equal(out.total, "299.90");
  assert.equal(out.tracking, null);
  assert.ok(!JSON.stringify(out).includes("12345678901"));
  assert.ok(!JSON.stringify(out).includes("cliente@exemplo.com"));
});

test("rastreio aparece somente quando a Tray realmente devolve", () => {
  const out = mapTrayOrderForAdmin({ id: 1, tracking_code: "BR123456789BR" });
  assert.deepEqual(out.tracking, { code: "BR123456789BR", url: null, carrier: null });
});

/* ─────────────────────────── Periodo ─────────────────────────── */

test("filtro de periodo aceita dia inteiro e ISO, e ignora lixo", () => {
  assert.equal(parseDateBoundary("2026-08-01"), "2026-08-01T00:00:00.000Z");
  assert.equal(parseDateBoundary("2026-08-01", { endOfDay: true }), "2026-08-02T00:00:00.000Z");
  assert.equal(parseDateBoundary("2026-08-01T12:00:00.000Z"), "2026-08-01T12:00:00.000Z");
  assert.equal(parseDateBoundary("ontem"), null);
  assert.equal(parseDateBoundary(""), null);
});

/* ─────────────────────────── Listagem ─────────────────────────── */

function listResponder(rows) {
  return (sql) => {
    if (/count\(\*\)::int as total\b/.test(sql) && /from public\.reward_redemptions/.test(sql)) {
      return { rows: [{ total: rows.length }] };
    }
    if (/reward_redemption_items/.test(sql)) {
      return { rows: rows.map((r) => ({ redemption_id: r.id, item_count: 2 })) };
    }
    return { rows };
  };
}

const LIST_ROW = {
  id: REDEMPTION_ID,
  status: "confirmed",
  credits_amount: "30000",
  tray_order_id: "25626",
  created_at: new Date("2026-08-21T10:00:00.000Z"),
  updated_at: new Date("2026-08-21T10:05:00.000Z"),
  user_id: 42,
  user_name: "Fulano de Tal",
  user_email: "fulano@exemplo.com",
};

test("listagem devolve so o necessario e nunca CPF, telefone ou endereco", async () => {
  const { query } = recordingQuery(listResponder([LIST_ROW]));
  const out = await listAdminRedemptions({ page: 1, limit: 20 }, { query });

  assert.deepEqual(out.items[0], {
    id: REDEMPTION_ID,
    status: "confirmed",
    user: { id: 42, name: "Fulano de Tal", email: "fulano@exemplo.com" },
    credits_amount: 30000,
    item_count: 2,
    tray_order_id: "25626",
    created_at: "2026-08-21T10:00:00.000Z",
    updated_at: "2026-08-21T10:05:00.000Z",
  });
  assert.ok(!/cpf|zipcode|street|phone/i.test(JSON.stringify(out.items)));
});

test("listagem devolve paginacao real com total_pages", async () => {
  const { query } = recordingQuery((sql) => {
    if (/count\(\*\)::int as total\b/.test(sql)) return { rows: [{ total: 47 }] };
    if (/reward_redemption_items/.test(sql)) return { rows: [] };
    return { rows: [LIST_ROW] };
  });
  const out = await listAdminRedemptions({ page: 2, limit: 20 }, { query });
  assert.deepEqual(out.paging, { page: 2, limit: 20, total: 47, total_pages: 3 });
});

test("paginacao acontece no banco (LIMIT/OFFSET), nunca em memoria", async () => {
  const { calls, query } = recordingQuery(listResponder([LIST_ROW]));
  await listAdminRedemptions({ page: 3, limit: 10 }, { query });

  const page = calls.find((c) => /order by r\.created_at desc/.test(c.sql));
  assert.match(page.sql, /limit \$\d+ offset \$\d+/);
  assert.deepEqual(page.params.slice(-2), [10, 20]);
});

test("listagem nao faz N+1: 3 consultas fixas, nenhuma por linha", async () => {
  const rows = Array.from({ length: 20 }, (_, i) => ({ ...LIST_ROW, id: `${i}`.padStart(8, "0") + "-2222-3333-4444-555555555555" }));
  const { calls, query } = recordingQuery(listResponder(rows));
  await listAdminRedemptions({ page: 1, limit: 20 }, { query });

  assert.equal(calls.length, 3, "pagina + total + contagem de itens agregada");
  const itemsCall = calls.find((c) => /reward_redemption_items/.test(c.sql));
  assert.match(itemsCall.sql, /where redemption_id = any\(\$1::uuid\[\]\) group by redemption_id/);
});

test("listagem nunca chama a Tray", async () => {
  let trayCalled = false;
  const { query } = recordingQuery(listResponder([LIST_ROW]));
  await listAdminRedemptions({ page: 1, limit: 20 }, { query, getTrayOrderFull: async () => { trayCalled = true; return {}; } });
  assert.equal(trayCalled, false);
});

test("filtro de status so aceita status factuais", async () => {
  const { calls, query } = recordingQuery(listResponder([]));
  await listAdminRedemptions({ status: "confirmed,inventado,compensated" }, { query });

  const page = calls.find((c) => /order by r\.created_at desc/.test(c.sql));
  assert.match(page.sql, /r\.status = any\(\$1::text\[\]\)/);
  assert.deepEqual(page.params[0], ["confirmed", "compensated"]);
});

test("busca cobre resgate, pedido Tray, id de usuario, nome e e-mail — nunca CPF", async () => {
  const { calls, query } = recordingQuery(listResponder([]));
  await listAdminRedemptions({ q: "25626" }, { query });
  const numeric = calls.find((c) => /order by r\.created_at desc/.test(c.sql));
  assert.match(numeric.sql, /u\.name ILIKE/);
  assert.match(numeric.sql, /u\.email ILIKE/);
  assert.match(numeric.sql, /u\.id = \$/);
  assert.match(numeric.sql, /r\.tray_order_id = \$/);
  assert.ok(!/cpf/i.test(numeric.sql), "a busca administrativa nunca toca em CPF");

  const { calls: uuidCalls, query: uuidQuery } = recordingQuery(listResponder([]));
  await listAdminRedemptions({ q: REDEMPTION_ID }, { query: uuidQuery });
  const byId = uuidCalls.find((c) => /order by r\.created_at desc/.test(c.sql));
  assert.match(byId.sql, /r\.id = \$\d+::uuid/);
});

test("filtro de periodo vira recorte no banco", async () => {
  const { calls, query } = recordingQuery(listResponder([]));
  await listAdminRedemptions({ from: "2026-08-01", to: "2026-08-31" }, { query });
  const page = calls.find((c) => /order by r\.created_at desc/.test(c.sql));
  assert.match(page.sql, /r\.created_at >= \$\d+/);
  assert.match(page.sql, /r\.created_at < \$\d+/);
  assert.ok(page.params.includes("2026-08-01T00:00:00.000Z"));
  assert.ok(page.params.includes("2026-09-01T00:00:00.000Z"));
});

/* ─────────────────────────── Detalhe ─────────────────────────── */

const DETAIL_HEAD = {
  id: REDEMPTION_ID,
  status: "compensated",
  credits_amount: "30000",
  coupon_value_before_cents: 5000000,
  coupon_value_after_cents: 5000000,
  coupon_code_snapshot: "NSU-0418-Q4",
  failure_reason: "tray_order_failed",
  tray_order_id: null,
  shipping_snapshot: null,
  address_snapshot: { recipient_name: "Fulano", zipcode: "01001000", street: "Praca da Se", number: "10", neighborhood: "Se", city: "Sao Paulo", state: "SP", country: "BR" },
  created_at: new Date("2026-08-21T10:00:00.000Z"),
  updated_at: new Date("2026-08-21T10:05:00.000Z"),
  user_id: 42,
  user_name: "Fulano de Tal",
  user_email: "fulano@exemplo.com",
};

function detailResponder() {
  return (sql) => {
    if (/from public\.reward_redemptions r join public\.users/.test(sql.replace(/\s+/g, " "))) return { rows: [DETAIL_HEAD] };
    if (/reward_redemption_items/.test(sql)) {
      return {
        rows: [
          {
            id: "item-1",
            reward_product_id: "rp-1",
            tray_product_id: "900010",
            tray_variant_id: "77",
            product_name_snapshot: "Kit Relogio",
            variant_name_snapshot: "Azul",
            image_url_snapshot: "https://cdn/1.jpg",
            quantity: 2,
            nscredits_unit_price_snapshot: "15000",
            nscredits_total_snapshot: "30000",
          },
        ],
      };
    }
    if (/reward_redemption_events/.test(sql)) {
      return {
        rows: [
          { id: 1, from_status: null, to_status: "processing", reason: null, meta: {}, created_at: new Date("2026-08-21T10:00:00.000Z") },
          { id: 2, from_status: "processing", to_status: "credits_reserved", reason: null, meta: {}, created_at: new Date("2026-08-21T10:01:00.000Z") },
          { id: 3, from_status: "tray_order_pending", to_status: "compensated", reason: "tray_order_failed", meta: { http_status: 400, access_token: "segredo" }, created_at: new Date("2026-08-21T10:02:00.000Z") },
        ],
      };
    }
    if (/coupon_balance_history/.test(sql)) {
      return {
        rows: [
          { id: 1, event_type: "REDEMPTION_DEBIT", delta_cents: -3000000, balance_before_cents: 5000000, balance_after_cents: 2000000, created_at: new Date("2026-08-21T10:01:00.000Z") },
          { id: 2, event_type: "REDEMPTION_COMPENSATION", delta_cents: 3000000, balance_before_cents: 2000000, balance_after_cents: 5000000, created_at: new Date("2026-08-21T10:02:00.000Z") },
        ],
      };
    }
    return { rows: [] };
  };
}

test("detalhe traz resgate, cliente, itens, endereco, timeline e ledger", async () => {
  const { calls, query } = recordingQuery(detailResponder());
  const out = await getAdminRedemptionDetail(REDEMPTION_ID, { query });

  assert.equal(out.redemption.id, REDEMPTION_ID);
  assert.equal(out.redemption.status_info.label, "Compensado");
  assert.deepEqual(out.user, { id: 42, name: "Fulano de Tal", email: "fulano@exemplo.com" });

  assert.equal(out.items.length, 1);
  assert.equal(out.items[0].product_name, "Kit Relogio");
  assert.equal(out.items[0].tray_product_id, "900010");
  assert.equal(out.items[0].tray_variant_id, "77");
  assert.equal(out.items[0].nscredits_unit_price, 15000);
  assert.equal(out.items[0].nscredits_total, 30000);

  assert.equal(out.address.city, "Sao Paulo");

  assert.deepEqual(out.events.map((e) => e.to_status), ["processing", "credits_reserved", "compensated"]);
  assert.equal(out.events[2].meta.http_status, 400);
  assert.ok(!JSON.stringify(out.events).includes("segredo"));

  assert.equal(out.ledger.debit_cents, 3000000);
  assert.equal(out.ledger.compensation_cents, 3000000);
  assert.equal(out.ledger.matches_expectation, true);

  assert.equal(calls.length, 4, "cabecalho + itens + eventos + ledger");
});

test("detalhe de id inexistente responde 404 sem vazar existencia", async () => {
  const { query } = recordingQuery(() => ({ rows: [] }));
  await assert.rejects(
    () => getAdminRedemptionDetail(REDEMPTION_ID, { query }),
    (e) => e.code === "redemption_not_found" && e.status === 404
  );
});

test("detalhe com id que nao e uuid nao chega a consultar o banco", async () => {
  const { calls, query } = recordingQuery(() => ({ rows: [] }));
  await assert.rejects(() => getAdminRedemptionDetail("1 OR 1=1", { query }), (e) => e.status === 404);
  assert.equal(calls.length, 0);
});

test("detalhe nao consulta a Tray", async () => {
  let trayCalled = false;
  const { query } = recordingQuery(detailResponder());
  await getAdminRedemptionDetail(REDEMPTION_ID, { query, getTrayOrderFull: async () => { trayCalled = true; return {}; } });
  assert.equal(trayCalled, false);
});

/* ─────────────────────────── Tray read-only ─────────────────────────── */

test("status Tray e uma leitura sob demanda de GET /orders/:id/full", async () => {
  const { query } = recordingQuery(() => ({ rows: [{ tray_order_id: "25626" }] }));
  let asked = null;
  const out = await getAdminRedemptionTrayOrder(REDEMPTION_ID, {
    query,
    getTrayOrderFull: async (id) => {
      asked = id;
      return { raw: { id: 25626, status: "A enviar", total: "299.90" } };
    },
  });

  assert.equal(asked, "25626");
  assert.equal(out.tray_order_id, "25626");
  assert.equal(out.order.status, "A enviar");
  assert.ok(out.fetched_at);
});

test("resgate sem pedido Tray nao chama a Tray", async () => {
  let trayCalled = false;
  const { query } = recordingQuery(() => ({ rows: [{ tray_order_id: null }] }));
  await assert.rejects(
    () => getAdminRedemptionTrayOrder(REDEMPTION_ID, { query, getTrayOrderFull: async () => { trayCalled = true; return {}; } }),
    (e) => e.code === "tray_order_not_created" && e.status === 409
  );
  assert.equal(trayCalled, false);
});

/* ─────────────────────────── Relatorios ─────────────────────────── */

const REPORT_AGG = {
  total_attempts: 5,
  confirmed_redemptions: 2,
  unique_customers: 1,
  credits_redeemed: "60000",
  tray_orders_created: 2,
  in_progress: 1,
  reconciliation_required: 1,
  compensated: 1,
  failed: 0,
  blocked: 0,
  credits_compensated: "30000",
};

function reportResponder(rows = []) {
  return (sql) => {
    if (/total_attempts/.test(sql)) return { rows: [REPORT_AGG] };
    if (/group by r\.status/.test(sql)) {
      return { rows: [{ status: "confirmed", total: 2 }, { status: "compensated", total: 1 }] };
    }
    if (/count\(\*\)::int as total\b/.test(sql)) return { rows: [{ total: rows.length }] };
    if (/reward_redemption_items/.test(sql)) return { rows: [] };
    return { rows };
  };
}

test("relatorio devolve as metricas reais, sem placeholder", async () => {
  const { query } = recordingQuery(reportResponder());
  const out = await getRedemptionReportMetrics({}, { query });

  assert.equal(out.total_attempts, 5);
  assert.equal(out.confirmed_redemptions, 2);
  assert.equal(out.unique_customers, 1);
  assert.equal(out.credits_redeemed, 60000);
  assert.equal(out.tray_orders_created, 2);
  assert.equal(out.reconciliation_required, 1);
  assert.equal(out.compensated, 1);
  assert.equal(out.by_status.confirmed, 2);
  // Todo status factual aparece, mesmo zerado — nunca "—".
  assert.equal(out.by_status.blocked_tray_customer_ambiguous, 0);
  assert.deepEqual(Object.keys(out.by_status).sort(), [...REDEMPTION_STATUSES].sort());
});

test("NSCreditos resgatados soma SOMENTE confirmed — compensado nunca entra", async () => {
  const { calls, query } = recordingQuery(reportResponder());
  await getRedemptionReportMetrics({}, { query });

  const agg = calls.find((c) => /total_attempts/.test(c.sql)).sql;
  const creditsExpr = /coalesce\(sum\(r\.credits_amount\) filter \(where r\.status = 'confirmed'\), 0\)::bigint as credits_redeemed/;
  assert.match(agg, creditsExpr, "credits_redeemed precisa filtrar por confirmed");
  assert.match(agg, /count\(distinct r\.user_id\) filter \(where r\.status = 'confirmed'\)::int as unique_customers/);
  assert.match(agg, /count\(\*\) filter \(where r\.tray_order_id is not null\)::int as tray_orders_created/);
});

test("relatorio nunca faz uma consulta por resgate", async () => {
  const { calls, query } = recordingQuery(reportResponder([LIST_ROW]));
  await getRedemptionReport({}, { query });
  // agregacao + by_status + pagina dos ultimos + total + itens agregados
  assert.ok(calls.length <= 5, `esperado no maximo 5 consultas, veio ${calls.length}`);
});

test("ultimos resgates reusam a listagem, sem segunda implementacao", async () => {
  const { query } = recordingQuery(reportResponder([LIST_ROW]));
  const out = await getRedemptionReport({ recent_limit: 5 }, { query });
  assert.equal(out.recent.length, 1);
  assert.deepEqual(Object.keys(out.recent[0]).sort(), [
    "created_at",
    "credits_amount",
    "id",
    "item_count",
    "status",
    "tray_order_id",
    "updated_at",
    "user",
  ]);
});

test("banco vazio devolve zeros, nunca placeholder", async () => {
  const { query } = recordingQuery((sql) => {
    if (/total_attempts/.test(sql)) {
      return {
        rows: [{
          total_attempts: 0, confirmed_redemptions: 0, unique_customers: 0, credits_redeemed: "0",
          tray_orders_created: 0, in_progress: 0, reconciliation_required: 0, compensated: 0,
          failed: 0, blocked: 0, credits_compensated: "0",
        }],
      };
    }
    return { rows: [] };
  });

  const out = await getRedemptionReport({}, { query });
  assert.equal(out.redemptions.total_attempts, 0);
  assert.equal(out.redemptions.credits_redeemed, 0);
  assert.equal(out.recent.length, 0);
  for (const value of Object.values(out.redemptions.by_status)) assert.equal(value, 0);
});

/* ─────────────────── Webhook: rota pronta != entrega comprovada ─────────────────── */

test("sem evidencia persistida o webhook nunca aparece como comprovado", async () => {
  const { query } = recordingQuery(() => ({ rows: [{ total: 0, last_event_at: null }] }));
  const out = await getTrayOrderWebhookEvidence({ query });
  assert.deepEqual(out, { route: "ready", delivery: "unverified", events_total: 0, last_event_at: null });
});

test("com lancamento DIRECT_TRAY_SPEND a entrega passa a ser comprovada", async () => {
  const { calls, query } = recordingQuery(() => ({ rows: [{ total: 3, last_event_at: new Date("2026-08-21T10:00:00.000Z") }] }));
  const out = await getTrayOrderWebhookEvidence({ query });
  assert.equal(out.delivery, "verified");
  assert.equal(out.events_total, 3);
  assert.equal(out.last_event_at, "2026-08-21T10:00:00.000Z");
  assert.match(calls[0].sql, /event_type = 'DIRECT_TRAY_SPEND'/);
});
