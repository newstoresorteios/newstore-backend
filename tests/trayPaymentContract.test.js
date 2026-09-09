// tests/trayPaymentContract.test.js
//
// PROVA DE CONTRATO do pagamento do resgate — no mesmo espirito de
// trayNoMutation.test.js: falhar ruidosamente se alguem, algum dia, desfizer
// uma das garantias abaixo sem uma nova decisao de negocio explicita.
//
// A saga de resgate (rewardRedemption.js) roda contra Postgres real e tem
// cobertura em rewardRedemption.integration.test.js (exige TEST_DATABASE_URL).
// Este arquivo cobre, SEM banco, as invariantes estruturais que nao podem
// depender de um ambiente de integracao para serem defendidas.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { TRAY_REDEMPTION_PAYMENT_METHOD } from "../src/services/trayPaymentClient.js";
import { LOJA_NS_ORDER_DEFAULTS, trayOrderHasPayment, readTrayOrderTotal } from "../src/services/trayOrderClient.js";
import { TrayCatalogError } from "../src/services/trayCatalogClient.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const SRC = join(__dirname, "..", "src");
const read = (...parts) => readFileSync(join(SRC, ...parts), "utf8");

/* ─────────────────── a saga usa a liquidacao com Payment ─────────────────── */

test("a saga de resgate liquida via settleTrayRedemptionOrder", () => {
  const saga = read("services", "rewardRedemption.js");

  assert.match(saga, /import\s*\{[^}]*settleTrayRedemptionOrder[^}]*\}\s*from\s*"\.\/trayRedemptionOrder\.js"/s);
  assert.match(saga, /settleTrayRedemptionOrder:\s*deps\.settleTrayRedemptionOrder\s*\|\|\s*settleTrayRedemptionOrder/);
  assert.match(saga, /await\s+d\.settleTrayRedemptionOrder\(/);
});

test("o tray_order_id e persistido ANTES da liquidacao/Payment", () => {
  const saga = read("services", "rewardRedemption.js");

  const persist = saga.indexOf('tray_order_id: trayOrderId }');
  const settle = saga.indexOf("await d.settleTrayRedemptionOrder(");
  assert.ok(persist > 0, "a persistencia do tray_order_id existe");
  assert.ok(settle > 0, "a liquidacao existe");
  assert.ok(persist < settle, "gravar o vinculo com o pedido real vem ANTES de criar o Payment");
});

test("a liquidacao recebe o redemption_id — sem ele nao existe marker deterministico", () => {
  const saga = read("services", "rewardRedemption.js");
  const call = saga.slice(saga.indexOf("await d.settleTrayRedemptionOrder("), saga.indexOf("await d.settleTrayRedemptionOrder(") + 200);
  assert.match(call, /redemptionId/);
  assert.match(call, /orderId/);
});

/* ─────────────────── NSCreditos nunca viram dinheiro ─────────────────── */

test("nenhuma camada Tray converte NSCreditos/credits_amount em valor de Payment", () => {
  for (const file of [
    ["services", "trayPaymentClient.js"],
    ["services", "trayRedemptionOrder.js"],
    ["services", "trayOrderClient.js"],
  ]) {
    const source = read(...file);
    // Um `payment.value` so pode ser alimentado por Order.total. Procuramos
    // ATRIBUICAO de valor (`value:` / `value =`) a partir de credito — nao
    // basta a palavra "value" aparecer (ex.: `formatNsCredits(value)` formata
    // os NSCreditos da observacao do pedido, que nada tem a ver com dinheiro).
    const offenders = source
      .split("\n")
      .filter((line) => !line.trim().startsWith("//") && !line.trim().startsWith("*"))
      .filter((line) => /credits_amount|creditsAmount|nscredits/i.test(line) && /\bvalue\s*[:=]/.test(line));
    assert.deepEqual(offenders, [], `${file.join("/")} nao pode derivar valor monetario de NSCreditos`);
  }
});

test("o valor do Payment vem de Order.total e falha fechado sem ele", () => {
  assert.equal(readTrayOrderTotal({ id: "1", total: "489.99" }), "489.99");

  for (const total of [null, undefined, "", "gratis", "abc", "-5.00"]) {
    assert.throws(
      () => readTrayOrderTotal({ id: "1", total }),
      (e) => e instanceof TrayCatalogError && e.code === "tray_order_total_invalid",
      `total=${JSON.stringify(total)} deveria falhar fechado`
    );
  }
});

/* ─────────────────── has_payment e da Tray, nunca nosso ─────────────────── */

test("has_payment so conta quando a propria Tray devolve 1", () => {
  assert.equal(trayOrderHasPayment({ has_payment: "1" }), true);
  // A Tray devolve "1" como string; um 1 numerico e o MESMO fato e tambem
  // vale. Qualquer outra coisa (inclusive `true`, que nao e um fato da Tray)
  // nao libera a confirmacao.
  assert.equal(trayOrderHasPayment({ has_payment: 1 }), true);
  for (const value of ["0", 0, "", null, undefined, "sim", true]) {
    assert.equal(trayOrderHasPayment({ has_payment: value }), false, `has_payment=${JSON.stringify(value)} nao pode passar`);
  }
});

test("nenhuma camada define has_payment manualmente no payload do Order", () => {
  for (const file of [
    ["services", "trayOrderClient.js"],
    ["services", "trayPaymentClient.js"],
    ["services", "trayRedemptionOrder.js"],
    ["services", "rewardRedemption.js"],
  ]) {
    const source = read(...file);
    // Ler (`order?.has_payment`, comparacao) e legitimo; ATRIBUIR nao e.
    assert.doesNotMatch(
      source,
      /has_payment\s*[:=]\s*["'`]?1/,
      `${file.join("/")} nunca pode declarar has_payment = 1 por conta propria`
    );
  }
});

/* ─────────────────── o metodo representa NSCreditos ─────────────────── */

test("o method do Payment e o mesmo NSCreditos do Order.payment_form — sem gateway ficticio", () => {
  assert.equal(TRAY_REDEMPTION_PAYMENT_METHOD, "NSCréditos");
  assert.equal(TRAY_REDEMPTION_PAYMENT_METHOD, LOJA_NS_ORDER_DEFAULTS.payment_form);

  const source = read("services", "trayPaymentClient.js");
  const code = source
    .split("\n")
    .filter((line) => !line.trim().startsWith("//") && !line.trim().startsWith("*"))
    .join("\n");
  assert.doesNotMatch(code, /\b(pix|boleto|cartao|cart[aã]o|credit_card|gateway)\b/i);
});

/* ─────────────────── nenhuma mutacao alem da autorizada ─────────────────── */

test("a camada de pagamento so emite POST /payments — nunca PUT/PATCH/DELETE", () => {
  const source = read("services", "trayPaymentClient.js");

  const mutations = source.match(/trayMutationRequest\(\s*"[^"]+",\s*"[A-Z]+"/g) || [];
  assert.deepEqual(mutations, ['trayMutationRequest("TRAY_REDEMPTION_PAYMENT_CREATE", "POST"']);
  assert.doesNotMatch(source, /"(PUT|PATCH|DELETE)"/);
});

test("nenhum ID de status e hardcoded na liquidacao", () => {
  const settlement = read("services", "trayRedemptionOrder.js");
  const code = settlement
    .split("\n")
    .filter((line) => !line.trim().startsWith("//") && !line.trim().startsWith("*"))
    .join("\n");
  assert.doesNotMatch(code, /status_id\s*[:=]\s*["']?\d+/, "o ID de 'A ENVIAR' e sempre resolvido em runtime");
});

/* ─────────────────── pedidos legados intocados ─────────────────── */

test("nenhum pedido legado aparece no codigo — zero batch retroativo", () => {
  for (const file of [
    ["services", "trayPaymentClient.js"],
    ["services", "trayRedemptionOrder.js"],
    ["services", "trayOrderClient.js"],
    ["services", "rewardRedemption.js"],
  ]) {
    const source = read(...file);
    for (const legacyOrder of ["25666", "25668"]) {
      assert.equal(
        source.includes(legacyOrder),
        false,
        `${file.join("/")} nao pode referenciar o pedido legado ${legacyOrder}`
      );
    }
  }
});
