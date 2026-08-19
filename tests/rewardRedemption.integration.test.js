// tests/rewardRedemption.integration.test.js
//
// Saga do resgate real contra Postgres REAL — unico jeito honesto de provar
// atomicidade, compensacao e idempotencia ponta a ponta.
//
//   TEST_DATABASE_URL=postgres://... npm test
//
// Sem TEST_DATABASE_URL os testes sao pulados (nunca usam banco de producao).
//
// A Tray (catalogo) e sempre um mock controlado. O passo de pedido Tray real
// (createTrayRedemptionOrder, em trayRedemptionOrder.js) tem cliente/cobertura
// contratual PROPRIA em trayCustomerClient.test.js / trayOrderClient.test.js
// / trayMutationClient.test.js — aqui a saga usa um MOCK dessa funcao
// (nunca a rede/DB reais) para poder controlar deterministicamente cada
// desfecho: sucesso, cliente Tray nao mapeado, timeout ambiguo.
import test, { before, after, beforeEach } from "node:test";
import assert from "node:assert/strict";

import { prepareRedemption, confirmRedemption, getRedemption, RedemptionError } from "../src/services/rewardRedemption.js";
import { addItem } from "../src/services/rewardCart.js";
import { createUserAddress } from "../src/services/userAddress.js";
import { applyCouponLedgerEntry, getCouponBalance } from "../src/services/couponLedger.js";
import { TrayOrderAmbiguousError, TrayCustomerNotFoundError } from "../src/services/trayRedemptionOrder.js";

const TEST_DB = process.env.TEST_DATABASE_URL || "";
const SKIP = !TEST_DB;
const skipOpts = { skip: SKIP ? "defina TEST_DATABASE_URL para rodar os testes de integracao" : false };

let pool;
let deps;
let userId;
let productId;
let addressId;

function sslFor(url) {
  try {
    const host = new URL(url).hostname;
    return { rejectUnauthorized: false, servername: /^\d+\.\d+\.\d+\.\d+$/.test(host) ? undefined : host };
  } catch {
    return { rejectUnauthorized: false };
  }
}

const TRAY_PRODUCT = {
  tray_product_id: "900010",
  name: "Kit Relogio (teste)",
  stock: 5,
  tray_available: 1,
  tray_available_in_store: 1,
  availability_text: "Disponivel",
  has_variation: false,
  variants: [],
  presentation: { is_available: true, reason: "available" },
};

before(async () => {
  if (SKIP) return;
  const pg = (await import("pg")).default;
  pool = new pg.Pool({ connectionString: TEST_DB, ssl: sslFor(TEST_DB), max: 10 });

  const base = {
    query: (sql, params) => pool.query(sql, params),
    withTransaction: async (fn) => {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        const out = await fn(client);
        await client.query("COMMIT");
        return out;
      } catch (e) {
        await client.query("ROLLBACK").catch(() => {});
        throw e;
      } finally {
        client.release();
      }
    },
    getCatalogProduct: async () => JSON.parse(JSON.stringify(TRAY_PRODUCT)),
    // Mock de sucesso — representa o cliente Tray ja mapeado e o pedido
    // criado. Testes especificos (cliente nao encontrado, timeout ambiguo)
    // substituem este mock explicitamente.
    createTrayRedemptionOrder: async () => ({ orderId: "TEST-TRAY-ORDER-1" }),
    // Fase F: sincronizacao do cupom Tray e SEMPRE mockada aqui — nunca
    // toca rede/DB real de producao a partir de um teste de saga.
    ensureTrayCouponForUser: async () => ({ ok: true, status: "SYNCED" }),
    // Kill-switch (item 24/25): estes testes cobrem o comportamento da saga
    // com o resgate LIGADO. O comportamento DESLIGADO (default de produção)
    // tem testes dedicados abaixo, sem essa flag.
    env: { REWARD_REDEMPTION_ENABLED: "true" },
  };
  deps = base;

  const stamp = Date.now();
  userId = (await pool.query(
    `insert into public.users (name, email, pass_hash, is_admin) values ($1,$2,'x',false) returning id`,
    ["Redemption Test", `redemption-test-${stamp}@exemplo.local`]
  )).rows[0].id;

  productId = (await pool.query(
    `insert into public.reward_products (tray_product_id, nscredits_price, is_published, name, image_url, has_variation, variants_snapshot, images_snapshot)
     values ($1,1500,true,'Kit Relogio','https://cdn/x.jpg',false,'[]'::jsonb,'[]'::jsonb) returning id`,
    ["900010"]
  )).rows[0].id;

  const addr = await createUserAddress(userId, {
    recipient_name: "Joao Pedro", zipcode: "01304001", street: "Rua Augusta", number: "123",
    neighborhood: "Consolacao", city: "Sao Paulo", state: "SP",
  }, deps);
  addressId = addr.id;
});

after(async () => {
  if (SKIP || !pool) return;
  await pool.query("delete from public.reward_redemption_events where redemption_id in (select id from public.reward_redemptions where user_id=$1)", [userId]).catch(() => {});
  await pool.query("delete from public.reward_redemption_items where redemption_id in (select id from public.reward_redemptions where user_id=$1)", [userId]).catch(() => {});
  await pool.query("delete from public.reward_redemptions where user_id=$1", [userId]).catch(() => {});
  await pool.query("delete from public.reward_carts where user_id=$1", [userId]).catch(() => {});
  await pool.query("delete from public.user_addresses where user_id=$1", [userId]).catch(() => {});
  await pool.query("delete from public.coupon_balance_history where user_id=$1", [userId]).catch(() => {});
  await pool.query("delete from public.reward_products where id=$1", [productId]).catch(() => {});
  await pool.query("delete from public.users where id=$1", [userId]).catch(() => {});
  await pool.end().catch(() => {});
});

beforeEach(async () => {
  if (SKIP) return;
  await pool.query("delete from public.reward_redemption_events where redemption_id in (select id from public.reward_redemptions where user_id=$1)", [userId]);
  await pool.query("delete from public.reward_redemption_items where redemption_id in (select id from public.reward_redemptions where user_id=$1)", [userId]);
  await pool.query("delete from public.reward_redemptions where user_id=$1", [userId]);
  await pool.query("delete from public.reward_carts where user_id=$1", [userId]);
  await pool.query("delete from public.coupon_balance_history where user_id=$1", [userId]);
  await pool.query("update public.users set coupon_value_cents=0, coupon_expires_at=null, coupon_code='NSU-TEST', tray_coupon_id='777' where id=$1", [userId]);
});

async function creditUser(cents) {
  await applyCouponLedgerEntry({ userId, operation: "credit", amountCents: cents, eventType: "ADMIN_BALANCE_ADJUSTMENT", channel: "ADMIN" }, deps);
}

test("prepare NAO debita e devolve o resumo de confirmacao", skipOpts, async () => {
  await creditUser(200000); // 2000 NSCreditos
  await addItem({ userId, rewardProductId: productId, quantity: 1 }, deps); // 1500

  const out = await prepareRedemption(userId, { addressId }, deps);

  assert.equal(out.credits_amount, 1500);
  assert.equal(out.coupon_balance_before, 2000);
  assert.equal(out.coupon_balance_after_preview, 500);
  assert.equal((await getCouponBalance(userId, deps)).balance_cents, 200000, "prepare nao pode debitar");
});

test("confirm debita uma vez e, com pedido Tray criado, fica confirmed", skipOpts, async () => {
  await creditUser(200000);
  await addItem({ userId, rewardProductId: productId, quantity: 1 }, deps); // 1500

  const key = `redeem-${Date.now()}`;
  const out = await confirmRedemption(userId, { addressId, idempotencyKey: key }, deps);

  assert.equal(out.replayed, false);
  assert.equal(out.redemption.status, "confirmed");
  assert.equal(out.redemption.tray_order_id, "TEST-TRAY-ORDER-1");
  assert.equal(out.redemption.coupon_value_after_cents, 50000, "credito debitado permanece debitado quando o pedido e criado");

  const finalBalance = await getCouponBalance(userId, deps);
  assert.equal(finalBalance.balance_cents, 50000);

  // creditUser() ja grava 1 linha (ADMIN_BALANCE_ADJUSTMENT) no MESMO ledger
  // unificado — filtramos pelos eventos do resgate, nao pelo total do usuario.
  const hist = await pool.query(
    // id e uuid (nao sequencial) — ordenar por created_at, nunca por id.
    "select event_type, delta_cents from public.coupon_balance_history where user_id=$1 and event_type like 'REDEMPTION_%' order by created_at",
    [userId]
  );
  assert.equal(hist.rows.length, 1);
  assert.equal(hist.rows[0].event_type, "REDEMPTION_DEBIT");
  assert.equal(hist.rows[0].delta_cents, -150000);
});

test("cliente Tray nao mapeado por e-mail: compensa e fica blocked_tray_customer_unmapped", skipOpts, async () => {
  await creditUser(200000);
  await addItem({ userId, rewardProductId: productId, quantity: 1 }, deps);

  const unmappedDeps = { ...deps, createTrayRedemptionOrder: async () => { throw new TrayCustomerNotFoundError("tray_customer_not_found"); } };
  const key = `redeem-unmapped-${Date.now()}`;
  const out = await confirmRedemption(userId, { addressId, idempotencyKey: key }, unmappedDeps);

  assert.equal(out.replayed, false);
  assert.equal(out.redemption.status, "blocked_tray_customer_unmapped");
  assert.equal(out.redemption.failure_reason, "tray_customer_not_found");
  assert.equal(out.redemption.coupon_value_after_cents, 200000, "credito tem que voltar integralmente");

  const finalBalance = await getCouponBalance(userId, deps);
  assert.equal(finalBalance.balance_cents, 200000, "saldo final tem que ser identico ao inicial (debito + compensacao)");

  const hist = await pool.query(
    "select event_type, delta_cents from public.coupon_balance_history where user_id=$1 and event_type like 'REDEMPTION_%' order by created_at",
    [userId]
  );
  assert.equal(hist.rows.length, 2);
  assert.equal(hist.rows[0].event_type, "REDEMPTION_DEBIT");
  assert.equal(hist.rows[0].delta_cents, -150000);
  assert.equal(hist.rows[1].event_type, "REDEMPTION_COMPENSATION");
  assert.equal(hist.rows[1].delta_cents, 150000);
});

test("Fase F: cupom Tray e sincronizado logo apos o debito, no pedido confirmado", skipOpts, async () => {
  await creditUser(200000);
  await addItem({ userId, rewardProductId: productId, quantity: 1 }, deps);

  const syncCalls = [];
  const trackedDeps = { ...deps, ensureTrayCouponForUser: async (uid) => { syncCalls.push(uid); return { ok: true, status: "SYNCED" }; } };

  await confirmRedemption(userId, { addressId, idempotencyKey: `redeem-sync-${Date.now()}` }, trackedDeps);

  assert.equal(syncCalls.length, 1, "sincroniza exatamente uma vez no caminho feliz (apos o debito)");
  assert.equal(syncCalls[0], userId);
});

test("Fase F: cupom Tray e sincronizado duas vezes quando compensa (debito + devolucao)", skipOpts, async () => {
  await creditUser(200000);
  await addItem({ userId, rewardProductId: productId, quantity: 1 }, deps);

  const syncCalls = [];
  const trackedDeps = {
    ...deps,
    createTrayRedemptionOrder: async () => { throw new TrayCustomerNotFoundError("tray_customer_not_found"); },
    ensureTrayCouponForUser: async (uid) => { syncCalls.push(uid); return { ok: true, status: "SYNCED" }; },
  };

  await confirmRedemption(userId, { addressId, idempotencyKey: `redeem-sync-comp-${Date.now()}` }, trackedDeps);

  assert.equal(syncCalls.length, 2, "sincroniza apos o debito E apos a compensacao");
});

test("Fase F: falha na sincronizacao do cupom Tray NUNCA bloqueia o resgate", skipOpts, async () => {
  await creditUser(200000);
  await addItem({ userId, rewardProductId: productId, quantity: 1 }, deps);

  const flakyDeps = { ...deps, ensureTrayCouponForUser: async () => { throw new Error("tray unreachable"); } };
  const out = await confirmRedemption(userId, { addressId, idempotencyKey: `redeem-sync-fail-${Date.now()}` }, flakyDeps);

  assert.equal(out.redemption.status, "confirmed", "resgate segue confirmado mesmo com a sincronizacao do cupom falhando");
});

test("idempotency_key repetida nao debita nem compensa duas vezes", skipOpts, async () => {
  await creditUser(200000);
  await addItem({ userId, rewardProductId: productId, quantity: 1 }, deps);

  const key = `redeem-idem-${Date.now()}`;
  const a = await confirmRedemption(userId, { addressId, idempotencyKey: key }, deps);
  const b = await confirmRedemption(userId, { addressId, idempotencyKey: key }, deps);

  assert.equal(a.replayed, false);
  assert.equal(b.replayed, true);
  assert.equal(a.redemption.id, b.redemption.id);

  const hist = await pool.query("select count(*)::int as n from public.coupon_balance_history where user_id=$1 and event_type like 'REDEMPTION_%'", [userId]);
  assert.equal(hist.rows[0].n, 1, "so o debito, nunca reaplicado mesmo com retry");

  assert.equal((await getCouponBalance(userId, deps)).balance_cents, 50000);
});

test("saldo insuficiente falha ANTES de qualquer tentativa de pedido Tray", skipOpts, async () => {
  await creditUser(1000); // so 10 NSCreditos
  await addItem({ userId, rewardProductId: productId, quantity: 1 }, deps); // precisa de 1500

  // confirmRedemption encapsula TODOS os problemas de validateCart (saldo
  // insuficiente, cupom vencido, produto indisponivel etc.) num unico codigo
  // "cart_invalid", com o codigo especifico em details.issues — nao existe
  // um "insufficient_nscredits" solto no topo.
  await assert.rejects(
    () => confirmRedemption(userId, { addressId, idempotencyKey: `redeem-fail-${Date.now()}` }, deps),
    (e) => e instanceof RedemptionError && e.code === "cart_invalid" && e.details.issues.includes("insufficient_nscredits")
  );

  const hist = await pool.query("select count(*)::int as n from public.coupon_balance_history where user_id=$1 and event_type like 'REDEMPTION_%'", [userId]);
  assert.equal(hist.rows[0].n, 0, "nenhum lancamento de resgate se o carrinho ja acusa saldo insuficiente");
});

test("timeout/resultado ambiguo NAO compensa automaticamente (reconciliation_required)", skipOpts, async () => {
  await creditUser(200000);
  await addItem({ userId, rewardProductId: productId, quantity: 1 }, deps);

  const ambiguousDeps = { ...deps, createTrayRedemptionOrder: async () => { throw new TrayOrderAmbiguousError("tray_order_timeout"); } };
  const out = await confirmRedemption(userId, { addressId, idempotencyKey: `redeem-ambig-${Date.now()}` }, ambiguousDeps);

  assert.equal(out.redemption.status, "reconciliation_required");
  assert.equal(out.redemption.coupon_value_after_cents, 50000, "credito FICA debitado ate reconciliar — nao inventa compensacao as cegas");

  const hist = await pool.query("select event_type from public.coupon_balance_history where user_id=$1 and event_type like 'REDEMPTION_%' order by created_at", [userId]);
  assert.equal(hist.rows.length, 1, "so o debito, nenhuma compensacao automatica");
  assert.equal(hist.rows[0].event_type, "REDEMPTION_DEBIT");
});

test("endereco de outro usuario nao e aceito no prepare nem no confirm", skipOpts, async () => {
  await creditUser(200000);
  await addItem({ userId, rewardProductId: productId, quantity: 1 }, deps);

  const otherAddr = "00000000-0000-0000-0000-000000000000";
  await assert.rejects(() => prepareRedemption(userId, { addressId: otherAddr }, deps), (e) => e.code === "address_not_found");
  await assert.rejects(() => confirmRedemption(userId, { addressId: otherAddr, idempotencyKey: `redeem-addr-${Date.now()}` }, deps), (e) => e.code === "address_not_found");

  assert.equal((await getCouponBalance(userId, deps)).balance_cents, 200000, "nada debitado quando o endereco falha");
});

test("carrinho vazio nunca chega a debitar", skipOpts, async () => {
  await creditUser(200000);
  await assert.rejects(
    () => confirmRedemption(userId, { addressId, idempotencyKey: `redeem-empty-${Date.now()}` }, deps),
    (e) => e.code === "cart_empty"
  );
  assert.equal((await getCouponBalance(userId, deps)).balance_cents, 200000);
});

test("historico de eventos registra cada transicao de estado", skipOpts, async () => {
  await creditUser(200000);
  await addItem({ userId, rewardProductId: productId, quantity: 1 }, deps);

  const out = await confirmRedemption(userId, { addressId, idempotencyKey: `redeem-events-${Date.now()}` }, deps);
  const events = await pool.query(
    "select from_status, to_status from public.reward_redemption_events where redemption_id=$1 order by id",
    [out.redemption.id]
  );
  assert.deepEqual(
    events.rows.map((r) => r.to_status),
    ["processing", "credits_reserved", "tray_order_pending", "confirmed"]
  );
});

test("concorrencia (item 11): dois confirms simultaneos, saldo 200000, cada um pedindo 150000 — so um vence, saldo nunca fica negativo", skipOpts, async () => {
  await creditUser(200000);
  await addItem({ userId, rewardProductId: productId, quantity: 1 }, deps); // 1500 creditos = 150000 cents

  // O stub de producao (TrayOrderNotImplementedError) falha e compensa de
  // forma SINCRONA e rapida — a janela "debitado mas ainda nao compensado"
  // fecha rapido demais para uma corrida de verdade acontecer, e os dois
  // confirms acabam vencendo em sequencia (a segunda tentativa so debita
  // DEPOIS que a primeira ja devolveu o credito). Isso nao prova nada sobre
  // o lock. Para testar a janela de risco REAL — a que vai existir quando a
  // Fase E criar um pedido Tray de verdade, com latencia de rede real — o
  // mock aqui atrasa a resposta da Tray, mantendo o debito "vivo" tempo
  // suficiente para as duas tentativas colidirem de fato no FOR UPDATE.
  const slowFailingTray = { ...deps, createTrayRedemptionOrder: async () => {
    await new Promise((resolve) => setTimeout(resolve, 250));
    throw new (await import("../src/services/trayRedemptionOrder.js")).TrayOrderNotImplementedError();
  } };

  const results = await Promise.allSettled([
    confirmRedemption(userId, { addressId, idempotencyKey: `redeem-race-a-${Date.now()}` }, slowFailingTray),
    confirmRedemption(userId, { addressId, idempotencyKey: `redeem-race-b-${Date.now()}` }, slowFailingTray),
  ]);

  // Um dos dois passa pela saga inteira (debita e, sem contrato Tray, compensa);
  // o outro tem que ser barrado pelo lock FOR UPDATE no momento do debito —
  // nunca os dois debitando ao mesmo tempo, nunca saldo negativo no meio.
  const fulfilled = results.filter((r) => r.status === "fulfilled");
  const rejected = results.filter((r) => r.status === "rejected");
  assert.equal(fulfilled.length, 1, `esperado exatamente 1 sucesso, veio ${fulfilled.length}`);
  assert.equal(rejected.length, 1, `esperado exatamente 1 falha controlada, veio ${rejected.length}`);

  // Duas camadas legitimas podem barrar a segunda tentativa, dependendo do
  // timing exato: o pre-check otimista de validateCart (sem lock, mais
  // rapido — cart_invalid/insufficient_nscredits) OU o FOR UPDATE do ledger
  // (garantia final — insufficient_balance). As duas sao corretas: o que
  // importa e que NUNCA as duas debitam ao mesmo tempo.
  const reason = rejected[0].reason;
  const isExpectedRejection =
    reason?.code === "insufficient_balance" ||
    (reason?.code === "cart_invalid" && reason?.details?.issues?.includes("insufficient_nscredits"));
  assert.ok(isExpectedRejection, `rejeicao inesperada: ${reason?.code} ${JSON.stringify(reason?.details)}`);

  const finalBalance = await getCouponBalance(userId, deps);
  assert.equal(finalBalance.balance_cents, 200000, "saldo final identico ao inicial — nunca negativo, nunca duplicado");

  const hist = await pool.query(
    "select event_type, delta_cents from public.coupon_balance_history where user_id=$1 and event_type like 'REDEMPTION_%' order by created_at",
    [userId]
  );
  assert.equal(hist.rows.length, 2, "so o vencedor grava debito+compensacao — o perdedor nao grava nada");
  assert.equal(hist.rows[0].event_type, "REDEMPTION_DEBIT");
  assert.equal(hist.rows[1].event_type, "REDEMPTION_COMPENSATION");
});

test("getRedemption nunca devolve resgate de outro usuario", skipOpts, async () => {
  await creditUser(200000);
  await addItem({ userId, rewardProductId: productId, quantity: 1 }, deps);
  const out = await confirmRedemption(userId, { addressId, idempotencyKey: `redeem-get-${Date.now()}` }, deps);

  assert.ok(await getRedemption(userId, out.redemption.id, deps));
  assert.equal(await getRedemption(userId + 999999, out.redemption.id, deps), null);
});

/* ─────────────────────────── Kill-switch (item 24/25) ─────────────────────────── */
// Default de producao: REWARD_REDEMPTION_ENABLED ausente/false. O confirm
// tem que parar ANTES de qualquer leitura/escrita — nem o carrinho, nem o
// saldo, nem o endereco podem ser tocados.

test("kill-switch desligado (default): confirm recusa ANTES de qualquer query, zero writes", skipOpts, async () => {
  await creditUser(200000);
  await addItem({ userId, rewardProductId: productId, quantity: 1 }, deps);
  const balanceBefore = (await getCouponBalance(userId, deps)).balance_cents;

  const disabledDeps = { ...deps, env: {} }; // ausente = desligado
  await assert.rejects(
    () => confirmRedemption(userId, { addressId, idempotencyKey: `redeem-killswitch-${Date.now()}` }, disabledDeps),
    (e) => e instanceof RedemptionError && e.code === "reward_redemption_disabled" && e.status === 503
  );

  assert.equal((await getCouponBalance(userId, deps)).balance_cents, balanceBefore, "saldo intacto");
  const hist = await pool.query("select count(*)::int as n from public.coupon_balance_history where user_id=$1 and event_type like 'REDEMPTION_%'", [userId]);
  assert.equal(hist.rows[0].n, 0, "nenhum lancamento de resgate no ledger");
  const red = await pool.query("select count(*)::int as n from public.reward_redemptions where user_id=$1", [userId]);
  assert.equal(red.rows[0].n, 0, "nenhum registro de resgate criado");
});

test("kill-switch com valor explicito 'false' tambem bloqueia", skipOpts, async () => {
  await creditUser(200000);
  await addItem({ userId, rewardProductId: productId, quantity: 1 }, deps);

  const disabledDeps = { ...deps, env: { REWARD_REDEMPTION_ENABLED: "false" } };
  await assert.rejects(
    () => confirmRedemption(userId, { addressId, idempotencyKey: `redeem-killswitch-false-${Date.now()}` }, disabledDeps),
    (e) => e.code === "reward_redemption_disabled"
  );
});

test("kill-switch nao afeta prepare (somente leitura continua disponivel)", skipOpts, async () => {
  await creditUser(200000);
  await addItem({ userId, rewardProductId: productId, quantity: 1 }, deps);

  const disabledDeps = { ...deps, env: {} };
  const out = await prepareRedemption(userId, { addressId }, disabledDeps);
  assert.equal(out.credits_amount, 1500);
});
