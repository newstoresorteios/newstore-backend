import assert from "node:assert/strict";
import test from "node:test";
import { readFile } from "node:fs/promises";

import {
  assertMultiDrawCheckout,
  CheckoutBatchError,
  checkoutBatchJson,
  normalizeCheckoutSelection,
  validateIdempotencyKey,
} from "../src/services/checkoutBatchService.js";
import {
  buildCheckoutBatchPixPayload,
  createCheckoutBatchPix,
  normalizeCheckoutProviderStatus,
  resolveBatchNotificationUrl,
} from "../src/services/checkoutBatchPaymentService.js";

const backendRoot = new URL("../", import.meta.url);
const read = (path) => readFile(new URL(path, backendRoot), "utf8");
const ids = {
  key: "11111111-1111-4111-8111-111111111111",
  batch: "22222222-2222-4222-8222-222222222222",
  reservation: "33333333-3333-4333-8333-333333333333",
};

function createPixServiceHarness({
  status = "reserved",
  providerPaymentId = null,
  paymentCreateStartedAt = null,
  databaseEmail = "cliente@newstore.test",
} = {}) {
  const now = new Date();
  const state = {
    batch: {
      id: ids.batch,
      user_id: 418,
      status,
      provider_payment_id: providerPaymentId,
      amount_cents: 84500,
      expires_at: new Date(now.getTime() + 5 * 60_000),
      payment_create_started_at: paymentCreateStartedAt,
      qr_code: providerPaymentId ? "existing-pix" : null,
      qr_code_base64: providerPaymentId ? "existing-base64" : null,
      error_code: null,
      settled_at: null,
      paid_at: null,
      created_at: now,
    },
    items: [
      {
        id: "44444444-4444-4444-8444-444444444444",
        batch_id: ids.batch,
        reservation_id: ids.reservation,
        draw_id: 140,
        draw_type: "principal",
        numbers: Array.from({ length: 15 }, (_, index) => index + 1),
        unit_price_cents: 5500,
        amount_cents: 82500,
        child_payment_id: null,
      },
      {
        id: "55555555-5555-4555-8555-555555555555",
        batch_id: ids.batch,
        reservation_id: "66666666-6666-4666-8666-666666666666",
        draw_id: 142,
        draw_type: "adicional",
        numbers: [18],
        unit_price_cents: 2000,
        amount_cents: 2000,
        child_payment_id: null,
      },
    ],
    reservations: [],
    queries: [],
    databaseEmail,
  };
  state.reservations = state.items.map((item) => ({
    id: item.reservation_id,
    user_id: 418,
    draw_id: item.draw_id,
    numbers: [...item.numbers],
    status: "active",
    expires_at: state.batch.expires_at,
    payment_id: null,
  }));

  const client = {
    async query(sql, params = []) {
      const normalizedSql = String(sql).replace(/\s+/g, " ").trim();
      state.queries.push({ sql: normalizedSql, params });
      if (["BEGIN", "COMMIT", "ROLLBACK"].includes(normalizedSql)) {
        return { rows: [], rowCount: 0 };
      }
      if (normalizedSql.includes("SELECT * FROM public.checkout_batches")) {
        return { rows: [{ ...state.batch }], rowCount: 1 };
      }
      if (normalizedSql.includes("SELECT * FROM public.checkout_batch_items")) {
        return { rows: state.items.map((item) => ({ ...item, numbers: [...item.numbers] })), rowCount: state.items.length };
      }
      if (normalizedSql.includes("SELECT * FROM public.reservations")) {
        return { rows: state.reservations.map((row) => ({ ...row, numbers: [...row.numbers] })), rowCount: state.reservations.length };
      }
      if (normalizedSql.startsWith("WITH requested AS") && normalizedSql.includes("JOIN public.numbers")) {
        const rows = state.items.flatMap((item) => item.numbers.map((number) => ({
          draw_id: item.draw_id,
          n: number,
          status: "reserved",
          reservation_id: item.reservation_id,
        })));
        return { rows, rowCount: rows.length };
      }
      if (normalizedSql.includes("SELECT email FROM public.users")) {
        return { rows: state.databaseEmail ? [{ email: state.databaseEmail }] : [], rowCount: state.databaseEmail ? 1 : 0 };
      }
      if (normalizedSql.includes("SET status = 'creating_payment'")) {
        state.batch.status = "creating_payment";
        state.batch.payment_create_started_at = now;
        state.batch.error_code = null;
        return { rows: [], rowCount: 1 };
      }
      if (normalizedSql.includes("SET status = 'reserved'")) {
        state.batch.status = "reserved";
        state.batch.payment_create_started_at = null;
        state.batch.error_code = params[1] || "payment_creation_recovery";
        return { rows: [], rowCount: 1 };
      }
      if (normalizedSql.includes("SET provider_payment_id = $2")) {
        state.batch.provider_payment_id = String(params[1]);
        state.batch.status = params[2];
        state.batch.qr_code = params[3];
        state.batch.qr_code_base64 = params[4];
        state.batch.payment_create_started_at = null;
        state.batch.error_code = null;
        return { rows: [], rowCount: 1 };
      }
      throw new Error(`unexpected_test_query: ${normalizedSql}`);
    },
    release() {},
  };
  return {
    state,
    getPool: async () => ({ connect: async () => client }),
    now: () => now,
  };
}

function normalized(items) {
  return normalizeCheckoutSelection(items).items;
}

test("checkout agrupado rejeita um unico sorteio com status 422", () => {
  assert.throws(
    () => assertMultiDrawCheckout(normalized([{ draw_id: 140, numbers: [3] }])),
    (error) => error instanceof CheckoutBatchError &&
      error.status === 422 &&
      error.code === "multi_draw_checkout_requires_multiple_draws" &&
      error.payload.message === "O pagamento agrupado exige seleções em pelo menos dois sorteios diferentes."
  );
});

test("checkout agrupado aceita dois sorteios distintos", () => {
  const items = normalized([{ draw_id: 140, numbers: [3] }, { draw_id: 142, numbers: [18] }]);
  assert.equal(assertMultiDrawCheckout(items), items);
});

test("varios numeros no mesmo sorteio continuam rejeitados no agrupado", () => {
  assert.throws(
    () => assertMultiDrawCheckout(normalized([{ draw_id: 140, numbers: [1, 2, 3, 4, 5] }])),
    (error) => error.code === "multi_draw_checkout_requires_multiple_draws"
  );
});

test("grupos vazios sao removidos antes da validacao multi-sorteio", () => {
  const items = normalized([
    { draw_id: 140, numbers: [3] },
    { draw_id: 142, numbers: [] },
  ]);
  assert.deepEqual(items, [{ draw_id: 140, numbers: [3] }]);
  assert.throws(() => assertMultiDrawCheckout(items), /multi_draw_checkout_requires_multiple_draws/);
});

test("1. principal sozinho", () => {
  assert.deepEqual(normalized([{ draw_id: 140, numbers: [44, 3, 12] }]), [
    { draw_id: 140, numbers: [3, 12, 44] },
  ]);
});

test("2. adicional sozinho", () => {
  assert.deepEqual(normalized([{ draw_id: 142, numbers: [73, 8] }])[0].numbers, [8, 73]);
});

test("3. principal mais dois adicionais ordena sorteios", () => {
  assert.deepEqual(
    normalized([
      { draw_id: 145, numbers: [91] },
      { draw_id: 140, numbers: [3] },
      { draw_id: 142, numbers: [8] },
    ]).map((item) => item.draw_id),
    [140, 142, 145]
  );
});

test("4. dois adicionais sem principal", () => {
  assert.equal(normalized([{ draw_id: 142, numbers: [8] }, { draw_id: 145, numbers: [9] }]).length, 2);
});

test("5. mesmo numero em sorteios diferentes e permitido", () => {
  assert.deepEqual(normalized([{ draw_id: 142, numbers: [8] }, { draw_id: 145, numbers: [8] }]), [
    { draw_id: 142, numbers: [8] }, { draw_id: 145, numbers: [8] },
  ]);
});

test("6. numeros duplicados no mesmo sorteio sao removidos", () => {
  assert.deepEqual(normalized([{ draw_id: 1, numbers: [8, 8, 9] }])[0].numbers, [8, 9]);
});

test("7. nao limita numeros a 0-99", () => {
  assert.deepEqual(normalized([{ draw_id: 1, numbers: [999, 100, 0] }])[0].numbers, [0, 100, 999]);
});

test("normalizacao exige inteiros JSON, sem converter strings", () => {
  assert.throws(
    () => normalized([{ draw_id: "140", numbers: [3] }]),
    (error) => error instanceof CheckoutBatchError && error.code === "invalid_draw_id"
  );
  assert.throws(
    () => normalized([{ draw_id: 140, numbers: ["3"] }]),
    (error) => error instanceof CheckoutBatchError && error.code === "invalid_numbers"
  );
});

test("8. numero inexistente e validado no banco", async () => {
  const source = await read("src/services/checkoutBatchService.js");
  assert.match(source, /numbers_not_found/);
  assert.match(source, /JOIN public\.numbers/);
});

test("9. numero reservado gera conflito agrupado", async () => {
  const source = await read("src/services/checkoutBatchService.js");
  assert.match(source, /batch_numbers_unavailable/);
  assert.match(source, /String\(row\.status/);
});

test("10. numero vendido tambem nao e tratado como disponivel", async () => {
  const source = await read("src/services/checkoutBatchService.js");
  assert.match(source, /!== "available"/);
});

test("11. sorteio fechado e rejeitado", async () => {
  assert.match(await read("src/services/checkoutBatchService.js"), /draw_not_open/);
});

test("12. rollback completo usa uma transacao real", async () => {
  const source = await read("src/services/checkoutBatchService.js");
  assert.match(source, /await client\.query\("BEGIN"\)/);
  assert.match(source, /await client\.query\("ROLLBACK"\)/);
});

test("13. todas as queries de reserva usam um unico client PostgreSQL", async () => {
  const source = await read("src/services/checkoutBatchService.js");
  assert.match(source, /const client = await pool\.connect\(\)/);
  assert.doesNotMatch(source, /query\('BEGIN'\)|query\("BEGIN"\)(?!;)/);
});

test("14. chave idempotente repetida compara selection_hash", async () => {
  const source = await read("src/services/checkoutBatchService.js");
  assert.match(source, /selection_hash !== normalized\.selectionHash/);
  assert.equal(validateIdempotencyKey(ids.key), ids.key);
});

test("15. chave repetida com payload diferente retorna conflito", () => {
  assert.notEqual(
    normalizeCheckoutSelection([{ draw_id: 1, numbers: [1] }]).selectionHash,
    normalizeCheckoutSelection([{ draw_id: 1, numbers: [2] }]).selectionHash
  );
  assert.throws(
    () => normalized([{ draw_id: 1, numbers: [1] }, { draw_id: 1, numbers: [2] }]),
    (error) => error instanceof CheckoutBatchError && error.code === "duplicate_draw_id"
  );
});

test("16. concorrencia do PIX possui gate creating_payment antes da chamada externa", async () => {
  const source = await read("src/services/checkoutBatchPaymentService.js");
  assert.ok(source.indexOf("status === \"creating_payment\"") < source.indexOf("providerPayment = await createPix"));
  assert.match(source, /payment_create_started_at = now\(\)/);
});

test("17. PIX pai nao e inserido em payments", async () => {
  const source = await read("src/services/checkoutBatchPaymentService.js");
  const beforeSettlement = source.slice(0, source.indexOf("export async function settleApprovedCheckoutBatch"));
  assert.doesNotMatch(beforeSettlement, /INSERT INTO public\.payments/);
});

test("18. cria pagamento filho estavel por reserva", async () => {
  assert.match(await read("src/services/checkoutBatchPaymentService.js"), /batch:\$\{batch\.provider_payment_id\}:\$\{item\.reservation_id\}/);
});

test("19. liga reservations.payment_id ao filho", async () => {
  assert.match(await read("src/services/checkoutBatchPaymentService.js"), /SET payment_id = \$2, status = 'paid'/);
});

test("20. soma dos filhos deriva dos snapshots dos itens", async () => {
  const source = await read("src/services/checkoutBatchPaymentService.js");
  assert.match(source, /sum \+= Number\(item\.amount_cents\)/);
  assert.match(source, /sum !== Number\(batch\.amount_cents\)/);
});

test("21. venda remove reservation_id somente da reserva correta", async () => {
  const source = await read("src/services/checkoutBatchPaymentService.js");
  assert.match(source, /status = 'sold', reservation_id = NULL/);
  assert.match(source, /reservation_id = \$3 AND status = 'reserved'/);
});

test("22. fechamento ocorre independentemente por draw", async () => {
  assert.match(await read("src/services/checkoutBatchPaymentService.js"), /closeDrawIfSoldOut\(item\.draw_id, client\)/);
});

test("23. credito ocorre por pagamento filho com metadata do batch", async () => {
  const source = await read("src/services/checkoutBatchPaymentService.js");
  assert.match(source, /creditCoupon\(childId/);
  assert.match(source, /pgClient: client/);
  assert.match(source, /pricing_source: "payments\.amount_cents"/);
});

test("24. webhook duplicado reutiliza liquidacao idempotente", async () => {
  const paymentRoute = await read("src/routes/payments.js");
  assert.match(paymentRoute, /syncCheckoutBatchFromProviderPayment\(body/);
  assert.match(await read("src/services/checkoutBatchPaymentService.js"), /batch\.settled_at \|\| batch\.status === "settled"/);
});

test("25. webhook e polling usam o mesmo sincronizador", async () => {
  const source = await read("src/services/checkoutBatchPaymentService.js");
  assert.match(source, /getCheckoutBatchStatus[\s\S]*syncCheckoutBatchFromProviderPayment/);
});

test("sincronizacao nao regride estados finais ou aprovados", async () => {
  const source = await read("src/services/checkoutBatchPaymentService.js");
  assert.match(source, /status IN \('settled', 'manual_review'\)/);
  assert.match(source, /status IN \('approved', 'expired'\) AND \$3 <> 'approved'/);
});

test("26. reconciliacao duplicada ignora settled_at", async () => {
  const source = await read("src/services/checkoutBatchPaymentService.js");
  assert.match(source, /settled_at IS NULL/);
  assert.match(source, /status IN \('creating_payment', 'pending', 'approved'\)/);
});

test("27. expiracao libera somente reservas pertencentes ao batch", async () => {
  const source = await read("src/services/checkoutBatchPaymentService.js");
  assert.match(source, /reservation\.id = ANY\(\$1::uuid\[\]\)/);
  assert.match(source, /number\.reservation_id = ANY\(\$1::uuid\[\]\)/);
});

test("28. perda da reserva leva a manual_review", async () => {
  assert.match(await read("src/services/checkoutBatchPaymentService.js"), /settlement_reservation_lost/);
});

test("29. protecao de cativo pendente esta nas expiracoes", async () => {
  const reserve = await read("src/services/checkoutBatchService.js");
  const payment = await read("src/services/checkoutBatchPaymentService.js");
  assert.match(reserve, /pendingCaptivePreauthReservationGuardSql/);
  assert.match(payment, /pendingCaptivePreauthReservationGuardSql/);
});

test("30. endpoints antigos permanecem montados e webhook individual permanece como fallback", async () => {
  const index = await read("src/index.js");
  for (const route of [
    "/api/reservations", "/api/payments", "/api/additional-draws",
    "/api/additional-payments", "/api/secondary-draws", "/api/secondary-payments",
  ]) assert.ok(index.includes(route), route);
  const payments = await read("src/routes/payments.js");
  assert.ok(payments.indexOf("if (checkoutBatch.handled)") < payments.indexOf("UPDATE public.payments", payments.indexOf("router.post('/webhook'")));
});

test("status do provider e normalizado sem confundir manual review", () => {
  assert.equal(normalizeCheckoutProviderStatus("approved"), "approved");
  assert.equal(normalizeCheckoutProviderStatus("rejected"), "failed");
  assert.equal(normalizeCheckoutProviderStatus("cancelled"), "expired");
  assert.equal(normalizeCheckoutProviderStatus("in_process"), "pending");
});

test("resposta usa total definitivo e filhos separados", () => {
  const response = checkoutBatchJson(
    { id: ids.batch, status: "reserved", amount_cents: 7000, expires_at: new Date("2026-08-05T12:00:00Z") },
    [{ draw_id: 1, draw_type: "principal", reservation_id: ids.reservation, numbers: [1, 2], unit_price_cents: 3500, amount_cents: 7000 }]
  );
  assert.equal(response.amount_cents, 7000);
  assert.equal(response.total_numbers, 2);
  assert.equal(response.payment_type, "checkout_batch");
  assert.equal(response.copy_paste_code, null);
  assert.equal(response.paid, false);
  assert.equal(response.settled, false);
  assert.equal(response.items[0].reservation_id, ids.reservation);
});

test("resposta PIX usa QR como copia e cola", () => {
  const response = checkoutBatchJson(
    { id: ids.batch, status: "pending", provider_payment_id: "123", amount_cents: 7500,
      qr_code: "000201", qr_code_base64: "base64", expires_at: new Date("2026-08-05T12:00:00Z") },
    []
  );
  assert.equal(response.payment_id, "123");
  assert.equal(response.qr_code, "000201");
  assert.equal(response.copy_paste_code, "000201");
  assert.equal(response.payment_type, "checkout_batch");
});

test("approved ainda nao e paid sem liquidacao", () => {
  const response = checkoutBatchJson(
    { id: ids.batch, status: "approved", paid_at: new Date(), amount_cents: 7500,
      expires_at: new Date("2026-08-05T12:00:00Z") },
    []
  );
  assert.equal(response.paid, false);
  assert.equal(response.settled, false);
});

test("settled e o unico sucesso completo do batch", () => {
  const response = checkoutBatchJson(
    { id: ids.batch, status: "settled", paid_at: new Date(), settled_at: new Date(),
      amount_cents: 7500, expires_at: new Date("2026-08-05T12:00:00Z") },
    []
  );
  assert.equal(response.paid, true);
  assert.equal(response.settled, true);
});

test("webhook reconhece metadata multi_draw_checkout", async () => {
  const source = await read("src/services/checkoutBatchPaymentService.js");
  assert.match(source, /metadataSource === "multi_draw_checkout"/);
  assert.match(source, /metadata\?\.checkout_batch_id/);
});

test("PIX agrupado monta transaction_amount numerico e metadados estaveis", () => {
  const harness = createPixServiceHarness();
  const payload = buildCheckoutBatchPixPayload({
    batch: harness.state.batch,
    items: harness.state.items,
    payerEmail: "cliente@newstore.test",
    notificationUrl: "https://api.newstore.test/api/payments/webhook",
  });
  assert.equal(payload.transaction_amount, 845);
  assert.equal(typeof payload.transaction_amount, "number");
  assert.equal(payload.idempotencyKey, ids.batch);
  assert.equal(payload.external_reference, ids.batch);
  assert.equal(payload.metadata.source, "multi_draw_checkout");
  assert.equal(payload.metadata.checkout_batch_id, ids.batch);
  assert.equal(payload.metadata.total_draws, 2);
});

test("PIX agrupado reutiliza mpCreatePixPayment do servico comum", async () => {
  const source = await read("src/services/checkoutBatchPaymentService.js");
  assert.match(source, /mpCreatePixPayment as defaultCreatePix/);
  assert.match(source, /providerPayment = await createPix\(pixPayload\)/);
  assert.doesNotMatch(source.slice(0, source.indexOf("export async function settleApprovedCheckoutBatch")), /new MercadoPagoConfig|new Payment\(/);
});

test("notification URL aceita somente PUBLIC_URL publica HTTPS", () => {
  assert.equal(
    resolveBatchNotificationUrl("https://api.newstore.test/"),
    "https://api.newstore.test/api/payments/webhook"
  );
  assert.equal(resolveBatchNotificationUrl("http://localhost:5000"), undefined);
  assert.equal(resolveBatchNotificationUrl("http://127.0.0.1:5000"), undefined);
  assert.equal(resolveBatchNotificationUrl("http://api.newstore.test"), undefined);
  assert.equal(resolveBatchNotificationUrl(""), undefined);
});

test("notification_url ausente e omitida pelo JSON sem bloquear o PIX", async () => {
  const harness = createPixServiceHarness();
  let sentPayload;
  const result = await createCheckoutBatchPix(
    { batchId: ids.batch, userId: 418, payerEmail: "request@newstore.test" },
    {
      getPool: harness.getPool,
      now: harness.now,
      createPix: async (payload) => {
        sentPayload = payload;
        return {
          id: 123456,
          status: "pending",
          point_of_interaction: { transaction_data: { qr_code: "000201", qr_code_base64: " base64\n" } },
        };
      },
    }
  );
  assert.equal(sentPayload.notification_url, undefined);
  assert.equal(JSON.stringify(sentPayload).includes("notification_url"), false);
  assert.equal(sentPayload.payerEmail, "cliente@newstore.test");
  assert.equal(result.response.payment_id, "123456");
  assert.equal(result.response.paymentId, "123456");
  assert.equal(result.response.copy_paste_code, "000201");
  assert.equal(result.response.qr_code_base64, "base64");
  assert.equal(harness.state.batch.status, "pending");
  assert.equal(harness.state.batch.qr_code, "000201");
});

test("erro do Mercado Pago retorna 502 retryable e restaura o mesmo batch", async () => {
  const harness = createPixServiceHarness();
  const reservationSnapshot = structuredClone(harness.state.reservations);
  const providerError = Object.assign(new Error("invalid notification_url"), {
    status: 400,
    response: { error: "bad_request", message: "notification_url must be a valid URL" },
  });
  await assert.rejects(
    createCheckoutBatchPix(
      { batchId: ids.batch, userId: 418, payerEmail: "request@newstore.test" },
      { getPool: harness.getPool, now: harness.now, createPix: async () => { throw providerError; } }
    ),
    (error) => error instanceof CheckoutBatchError &&
      error.status === 502 &&
      error.code === "mp_pix_create_failed" &&
      error.payload.retryable === true &&
      error.payload.batch_id === ids.batch &&
      error.payload.details === "notification_url must be a valid URL"
  );
  assert.equal(harness.state.batch.id, ids.batch);
  assert.equal(harness.state.batch.status, "reserved");
  assert.equal(harness.state.batch.payment_create_started_at, null);
  assert.equal(harness.state.batch.provider_payment_id, null);
  assert.deepEqual(harness.state.reservations, reservationSnapshot);
  assert.equal(harness.state.queries.some(({ sql }) => /DELETE FROM/i.test(sql)), false);
});

test("retry reutiliza batch e chave de idempotencia apos falha", async () => {
  const harness = createPixServiceHarness();
  const keys = [];
  let attempts = 0;
  const createPix = async (payload) => {
    keys.push(payload.idempotencyKey);
    attempts += 1;
    if (attempts === 1) throw new Error("temporary network failure");
    return {
      id: "mp-retry-1",
      status: "pending",
      point_of_interaction: { transaction_data: { qr_code: "retry-pix", qr_code_base64: "retry-base64" } },
    };
  };
  await assert.rejects(
    createCheckoutBatchPix(
      { batchId: ids.batch, userId: 418, payerEmail: "request@newstore.test" },
      { getPool: harness.getPool, now: harness.now, createPix }
    ),
    /mp_pix_create_failed/
  );
  const retry = await createCheckoutBatchPix(
    { batchId: ids.batch, userId: 418, payerEmail: "request@newstore.test" },
    { getPool: harness.getPool, now: harness.now, createPix }
  );
  assert.deepEqual(keys, [ids.batch, ids.batch]);
  assert.equal(retry.response.batch_id, ids.batch);
  assert.equal(retry.response.payment_id, "mp-retry-1");
});

test("pagamento ja persistido e devolvido sem criar segundo PIX", async () => {
  const harness = createPixServiceHarness({ providerPaymentId: "mp-existing", status: "pending" });
  let calls = 0;
  const result = await createCheckoutBatchPix(
    { batchId: ids.batch, userId: 418, payerEmail: "request@newstore.test" },
    { getPool: harness.getPool, now: harness.now, createPix: async () => { calls += 1; } }
  );
  assert.equal(result.action, "existing");
  assert.equal(result.response.payment_id, "mp-existing");
  assert.equal(calls, 0);
});

test("batch creating_payment recente continua protegido contra concorrencia", async () => {
  const harness = createPixServiceHarness({
    status: "creating_payment",
    paymentCreateStartedAt: new Date(Date.now() - 5_000),
  });
  let calls = 0;
  const result = await createCheckoutBatchPix(
    { batchId: ids.batch, userId: 418, payerEmail: "request@newstore.test" },
    { getPool: harness.getPool, now: harness.now, createPix: async () => { calls += 1; } }
  );
  assert.equal(result.action, "creating");
  assert.equal(calls, 0);
});

test("batch travado em creating_payment recupera com a mesma chave", async () => {
  const harness = createPixServiceHarness({
    status: "creating_payment",
    paymentCreateStartedAt: new Date(Date.now() - 31_000),
  });
  let sentPayload;
  const result = await createCheckoutBatchPix(
    { batchId: ids.batch, userId: 418, payerEmail: "request@newstore.test" },
    {
      getPool: harness.getPool,
      now: harness.now,
      createPix: async (payload) => {
        sentPayload = payload;
        return {
          id: "mp-recovered",
          status: "pending",
          point_of_interaction: { transaction_data: { qr_code: "recovered-pix" } },
        };
      },
    }
  );
  assert.equal(sentPayload.idempotencyKey, ids.batch);
  assert.equal(result.response.payment_id, "mp-recovered");
  assert.equal(harness.state.batch.status, "pending");
});

test("resposta do provedor sem QR restaura reserved e nao cria filhos", async () => {
  const harness = createPixServiceHarness();
  await assert.rejects(
    createCheckoutBatchPix(
      { batchId: ids.batch, userId: 418, payerEmail: "request@newstore.test" },
      {
        getPool: harness.getPool,
        now: harness.now,
        createPix: async () => ({ id: "mp-without-qr", status: "pending" }),
      }
    ),
    (error) => error.status === 502 && error.payload.retryable === true
  );
  assert.equal(harness.state.batch.status, "reserved");
  assert.equal(harness.state.batch.payment_create_started_at, null);
  assert.equal(
    harness.state.queries.some(({ sql }) => sql.includes("INSERT INTO public.payments")),
    false
  );
});

test("PIX individual e adicional permanecem nos fluxos existentes", async () => {
  const individual = await read("src/routes/payments.js");
  const additional = await read("src/routes/additional_payments.js");
  assert.match(individual, /router\.post\('\/pix'/);
  assert.match(additional, /mpCreatePixPayment\(/);
  assert.match(additional, /source: "additional_draw"/);
});
