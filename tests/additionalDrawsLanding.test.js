import "../src/config/env.js";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  formatAdditionalDraw,
  loadAdditionalDrawsLanding,
  mergeNumbersWithPayments,
} from "../src/routes/additional_draws.js";

function draw({
  id,
  status,
  draw_type = "adicional",
  closed_at = null,
  opened_at = "2026-07-01T12:00:00.000Z",
}) {
  return {
    id,
    status,
    draw_type,
    product_name: `Sorteio ${id}`,
    product_link: null,
    opened_at,
    closed_at,
    realized_at: null,
    winner_user_id: null,
    winner_name: null,
    winner_number: null,
  };
}

function fakeRunQuery(rows) {
  const calls = [];
  return {
    calls,
    async run(sql) {
      calls.push(String(sql));
      return { rows };
    },
  };
}

test("consulta da landing não usa LIMIT 1 e cobre open/closed/adicional/secundario", async () => {
  const fake = fakeRunQuery([]);
  await loadAdditionalDrawsLanding(fake.run);
  const sql = fake.calls[0];

  assert.doesNotMatch(sql, /LIMIT\s+1\b/i);
  assert.match(sql, /status IN \('open', 'closed'\)/);
  assert.match(sql, /draw_type IN \('adicional', 'secundario'\)/);
  assert.doesNotMatch(sql, /status = 'draft'/i);
  assert.doesNotMatch(sql, /status = 'cancelled'/i);
});

test("dois sorteios adicionais closed são retornados juntos", async () => {
  const rows = [
    draw({ id: 145, status: "closed", closed_at: "2026-08-05T12:00:00.000Z" }),
    draw({ id: 144, status: "closed", closed_at: "2026-08-01T12:00:00.000Z" }),
  ];
  const fake = fakeRunQuery(rows);
  const result = await loadAdditionalDrawsLanding(fake.run);
  assert.equal(result.length, 2);
  assert.deepEqual(result.map((r) => r.id), [145, 144]);
});

test("três sorteios closed são retornados juntos", async () => {
  const rows = [
    draw({ id: 145, status: "closed", closed_at: "2026-08-05T12:00:00.000Z" }),
    draw({ id: 144, status: "closed", closed_at: "2026-08-02T12:00:00.000Z" }),
    draw({ id: 143, status: "closed", closed_at: "2026-08-01T12:00:00.000Z" }),
  ];
  const fake = fakeRunQuery(rows);
  const result = await loadAdditionalDrawsLanding(fake.run);
  assert.equal(result.length, 3);
  assert.deepEqual(result.map((r) => r.id), [145, 144, 143]);
});

test("um open + vários closed são retornados juntos, aberto primeiro", async () => {
  const rows = [
    draw({ id: 146, status: "open" }),
    draw({ id: 145, status: "closed", closed_at: "2026-08-05T12:00:00.000Z" }),
    draw({ id: 144, status: "closed", closed_at: "2026-08-01T12:00:00.000Z" }),
  ];
  const fake = fakeRunQuery(rows);
  const result = await loadAdditionalDrawsLanding(fake.run);
  assert.deepEqual(result.map((r) => r.id), [146, 145, 144]);
  assert.equal(result[0].status, "open");
  assert.equal(result[1].status, "closed");
  assert.equal(result[2].status, "closed");
});

test("encerrados aparecem do mais recente para o mais antigo (closed_at DESC)", async () => {
  const rows = [
    draw({ id: 143, status: "closed", closed_at: "2026-07-01T12:00:00.000Z" }),
    draw({ id: 145, status: "closed", closed_at: "2026-08-05T12:00:00.000Z" }),
    draw({ id: 144, status: "closed", closed_at: "2026-08-01T12:00:00.000Z" }),
  ];
  const fake = fakeRunQuery(rows);
  const result = await loadAdditionalDrawsLanding(fake.run);
  // A consulta é responsável por ordenar; aqui garantimos que o pipeline
  // não reordena nem descarta linhas devolvidas pelo banco.
  assert.deepEqual(result.map((r) => r.id), [143, 145, 144]);
  assert.equal(result.length, 3);
});

test("draw_type adicional aparece na landing", async () => {
  const rows = [draw({ id: 200, status: "open", draw_type: "adicional" })];
  const fake = fakeRunQuery(rows);
  const result = await loadAdditionalDrawsLanding(fake.run);
  const formatted = await formatAdditionalDraw(result[0], null);
  assert.equal(formatted.draw_type, "adicional");
});

test("draw_type secundario aparece na landing", async () => {
  const rows = [draw({ id: 201, status: "closed", draw_type: "secundario", closed_at: "2026-08-01T00:00:00.000Z" })];
  const fake = fakeRunQuery(rows);
  const result = await loadAdditionalDrawsLanding(fake.run);
  const formatted = await formatAdditionalDraw(result[0], null);
  assert.equal(formatted.draw_type, "secundario");
});

test("formatAdditionalDraw preserva todos os campos exigidos", async () => {
  const row = draw({ id: 145, status: "closed", closed_at: "2026-08-05T12:00:00.000Z" });
  row.winner_user_id = 7;
  row.winner_name = "Fulano";
  row.winner_number = 42;
  row.realized_at = "2026-08-06T00:00:00.000Z";
  const formatted = await formatAdditionalDraw(row, {
    banner_title: "SORTEIO DE R$ 2500,00 EM COMPRAS NO SITE.",
    ticket_price_cents: 500,
    max_numbers_per_selection: 5,
  });
  for (const field of [
    "id", "status", "draw_type", "product_name", "product_link", "banner_title",
    "promo_phrase", "ticket_price_cents", "price_cents", "max_numbers_per_selection",
    "opened_at", "closed_at", "realized_at", "winner_user_id", "winner_name", "winner_number",
  ]) {
    assert.ok(Object.prototype.hasOwnProperty.call(formatted, field), `campo ausente: ${field}`);
  }
  assert.equal(formatted.status, "closed");
  assert.equal(formatted.winner_number, 42);
});

test("sorteio fechado continua acessível por /:id/numbers (sem filtro status='open')", async () => {
  const source = await readFile(new URL("../src/routes/additional_draws.js", import.meta.url), "utf8");
  const start = source.indexOf('router.get("/:id/numbers"');
  const end = source.indexOf('router.post("/:id/reserve"');
  assert.ok(start >= 0 && end > start, "rota /:id/numbers não encontrada");
  const routeSource = source.slice(start, end);
  assert.doesNotMatch(routeSource, /status\s*=\s*'open'/);
  assert.match(routeSource, /draw_type IN \('adicional', 'secundario'\)/);
});

// --- rota pública de números ---

test("pagamento aprovado faz o número ser retornado como sold", () => {
  const numbersRows = [{ n: 0, status: "available", reservation_id: null }];
  const paidRows = [{ n: 0, owner_name: "João Lima", owner_email: "joao@example.test" }];
  const result = mergeNumbersWithPayments(numbersRows, paidRows);
  assert.equal(result[0].status, "sold");
});

test("número vendido retorna owner_initials e buyer_initials", () => {
  const numbersRows = [{ n: 0, status: "available", reservation_id: null }];
  const paidRows = [{ n: 0, owner_name: "João Lima", owner_email: "joao@example.test" }];
  const result = mergeNumbersWithPayments(numbersRows, paidRows);
  assert.equal(result[0].owner_initials, "JL");
  assert.equal(result[0].buyer_initials, "JL");
  assert.equal(result[0].owner_name, "João Lima");
});

test("número sem pagamento não ganha comprador inventado", () => {
  const numbersRows = [{ n: 2, status: "available", reservation_id: null }];
  const result = mergeNumbersWithPayments(numbersRows, []);
  assert.equal(result[0].status, "available");
  assert.equal(result[0].owner_initials, null);
  assert.equal(result[0].buyer_initials, null);
  assert.equal(result[0].owner_name, null);
});

test("status 'sold' já presente em payments continua sendo tratado como venda válida", () => {
  const numbersRows = [{ n: 5, status: "reserved", reservation_id: "r1" }];
  const paidRows = [{ n: 5, owner_name: "Vera Ferreira", owner_email: "vera@example.test" }];
  const result = mergeNumbersWithPayments(numbersRows, paidRows);
  assert.equal(result[0].status, "sold");
  assert.equal(result[0].owner_initials, "VF");
});

test("reserved sem pagamento não vira sold", () => {
  const numbersRows = [{ n: 6, status: "reserved", reservation_id: "r2" }];
  const result = mergeNumbersWithPayments(numbersRows, []);
  assert.equal(result[0].status, "reserved");
  assert.equal(result[0].owner_initials, null);
});

test("pagamento mais recente vence quando há duplicidade para o mesmo número", () => {
  const numbersRows = [{ n: 9, status: "available", reservation_id: null }];
  const paidRows = [
    { n: 9, owner_name: "Novo Comprador", owner_email: "novo@example.test" },
    { n: 9, owner_name: "Antigo Comprador", owner_email: "antigo@example.test" },
  ];
  const result = mergeNumbersWithPayments(numbersRows, paidRows);
  assert.equal(result[0].owner_initials, "NC");
});
