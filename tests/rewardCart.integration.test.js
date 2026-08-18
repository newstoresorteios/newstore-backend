// tests/rewardCart.integration.test.js
//
// Carrinho contra PostgreSQL REAL: persistencia, indices unicos parciais,
// isolamento entre usuarios e prova de que a carteira nao e tocada.
//
//   TEST_DATABASE_URL=postgres://... npm test
//
// Sem TEST_DATABASE_URL os testes sao pulados (nunca usam banco de producao).
// A Tray e sempre um mock controlado — nenhum produto real e alterado.
import test, { before, after } from "node:test";
import assert from "node:assert/strict";

import { getCart, addItem, updateItem, removeItem, clearCart, CART_ISSUES } from "../src/services/rewardCart.js";
import { validateCart } from "../src/services/rewardCartValidator.js";
import { applyCouponLedgerEntry, getCouponBalance } from "../src/services/couponLedger.js";

const TEST_DB = process.env.TEST_DATABASE_URL || "";
const SKIP = !TEST_DB;
const skipOpts = { skip: SKIP ? "defina TEST_DATABASE_URL para rodar os testes de integracao" : false };

let pool;
let deps;
let walletDeps;
let userId;
let otherUserId;
let adminId;
let simpleProductId;
let variantProductId;
let trayCalls;

function sslFor(url) {
  try {
    const host = new URL(url).hostname;
    return { rejectUnauthorized: false, servername: /^\d+\.\d+\.\d+\.\d+$/.test(host) ? undefined : host };
  } catch {
    return { rejectUnauthorized: false };
  }
}

/** Catalogo Tray simulado. Registra toda chamada para provar que so ha GET. */
const trayCatalog = {
  "900001": {
    tray_product_id: "900001",
    name: "Citizen Promaster (teste)",
    stock: 3,
    tray_available: 1,
    tray_available_in_store: 1,
    availability_text: "Disponivel",
    has_variation: false,
    variants: [],
    presentation: { is_available: true, reason: "available" },
  },
  "900002": {
    tray_product_id: "900002",
    name: "Tenis XPTO (teste)",
    stock: 0,
    tray_available: 1,
    tray_available_in_store: 1,
    availability_text: "Disponivel",
    has_variation: true,
    presentation: { is_available: true, reason: "available_in_variant" },
    variants: [
      { variant_id: "2003", tray_product_id: "900002", reference: "T-41", stock: 5, tray_available: 1, values: [{ type: "Tamanho", value: "41" }] },
      { variant_id: "2004", tray_product_id: "900002", reference: "T-40", stock: 0, tray_available: 0, values: [{ type: "Tamanho", value: "40" }] },
    ],
  },
};

before(async () => {
  if (SKIP) return;

  const pg = (await import("pg")).default;
  pool = new pg.Pool({ connectionString: TEST_DB, ssl: sslFor(TEST_DB), max: 10 });
  trayCalls = [];

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
  };

  walletDeps = base;
  deps = {
    ...base,
    getCatalogProduct: async (trayProductId, options) => {
      trayCalls.push({ method: "GET", trayProductId, options });
      const p = trayCatalog[String(trayProductId)];
      if (!p) throw Object.assign(new Error("tray_product_not_found"), { code: "tray_product_not_found", status: 404 });
      return JSON.parse(JSON.stringify(p));
    },
    // Sem override: exercita o caminho default real do validador, que le
    // users.coupon_value_cents (Fase 5) via couponLedger.js.
  };

  const stamp = Date.now();
  const mk = async (name, email, admin = false) =>
    (await pool.query(
      `insert into public.users (name, email, pass_hash, is_admin) values ($1,$2,'x',$3) returning id`,
      [name, email, admin]
    )).rows[0].id;

  userId = await mk("Cart Test", `cart-test-${stamp}@exemplo.local`);
  otherUserId = await mk("Cart Other", `cart-other-${stamp}@exemplo.local`);
  adminId = await mk("Cart Admin", `cart-admin-${stamp}@exemplo.local`, true);

  const mkProduct = async (trayId, price, hasVariation) =>
    (await pool.query(
      `insert into public.reward_products
         (tray_product_id, nscredits_price, is_published, name, image_url, has_variation, variants_snapshot, images_snapshot)
       values ($1,$2,true,$3,'https://cdn/x.jpg',$4,'[]'::jsonb,'[]'::jsonb)
       returning id`,
      [trayId, price, `Produto ${trayId}`, hasVariation]
    )).rows[0].id;

  simpleProductId = await mkProduct("900001", 5000, false);
  variantProductId = await mkProduct("900002", 3000, true);
});

after(async () => {
  if (SKIP || !pool) return;
  await pool.query("delete from public.users where id = any($1::int[])", [[userId, otherUserId, adminId]]).catch(() => {});
  await pool.query("delete from public.reward_products where tray_product_id = any($1::text[])", [["900001", "900002"]]).catch(() => {});
  await pool.end().catch(() => {});
});

async function resetCart() {
  await pool.query("delete from public.reward_carts where user_id = any($1::int[])", [[userId, otherUserId]]);
  trayCalls.length = 0;
}

/* ─────────────────────────── Persistencia ─────────────────────────── */

test("carrinho vazio nao cria linha no banco", skipOpts, async () => {
  await resetCart();
  const cart = await getCart(userId, deps);
  assert.deepEqual(cart.items, []);

  const r = await pool.query("select count(*)::int n from public.reward_carts where user_id=$1", [userId]);
  assert.equal(r.rows[0].n, 0, "ler carrinho nao pode criar carrinho");
});

test("fluxo completo: add -> persiste -> reload -> update -> remove", skipOpts, async () => {
  await resetCart();

  await addItem({ userId, rewardProductId: simpleProductId, quantity: 1 }, deps);

  // Reload independente: prova que o backend e a autoridade, nao o estado do front.
  const recarregado = await getCart(userId, deps);
  assert.equal(recarregado.items.length, 1);
  assert.equal(recarregado.items[0].quantity, 1);
  assert.equal(recarregado.items[0].nscredits_unit_price, 5000);
  assert.equal(recarregado.totals.nscredits, 5000);

  const itemId = recarregado.items[0].id;
  await updateItem({ userId, itemId, quantity: 3 }, deps);
  assert.equal((await getCart(userId, deps)).totals.nscredits, 15000);

  await removeItem({ userId, itemId }, deps);
  assert.deepEqual((await getCart(userId, deps)).items, []);

  const linhas = await pool.query(
    "select count(*)::int n from public.reward_cart_items i join public.reward_carts c on c.id=i.cart_id where c.user_id=$1",
    [userId]
  );
  assert.equal(linhas.rows[0].n, 0);
});

test("no maximo um carrinho ativo por usuario", skipOpts, async () => {
  await resetCart();
  await addItem({ userId, rewardProductId: simpleProductId, quantity: 1 }, deps);

  await assert.rejects(
    () => pool.query("insert into public.reward_carts (user_id) values ($1)", [userId]),
    (e) => {
      assert.equal(e.code, "23505");
      return true;
    }
  );
});

test("o indice unico impede item duplicado (produto simples)", skipOpts, async () => {
  await resetCart();
  await addItem({ userId, rewardProductId: simpleProductId, quantity: 1 }, deps);
  const cart = await getCart(userId, deps);

  await assert.rejects(
    () =>
      pool.query(
        `insert into public.reward_cart_items
           (cart_id, reward_product_id, tray_product_id, tray_variant_id, quantity, nscredits_unit_price_snapshot)
         values ($1,$2,'900001',null,1,5000)`,
        [cart.id, simpleProductId]
      ),
    (e) => {
      assert.equal(e.code, "23505");
      return true;
    }
  );
});

// Regressao: os dois indices unicos sao PARCIAIS e complementares. O
// ON CONFLICT precisa apontar para o indice do CAMINHO (simples ou variacao);
// apontar sempre para o de variacao faz a segunda adicao de um produto simples
// violar o indice simples e estourar 23505 em vez de somar a quantidade.
test("adicionar o mesmo produto SIMPLES duas vezes soma a quantidade", skipOpts, async () => {
  await resetCart();

  await addItem({ userId, rewardProductId: simpleProductId, quantity: 1 }, deps);
  await addItem({ userId, rewardProductId: simpleProductId, quantity: 2 }, deps);

  const cart = await getCart(userId, deps);
  assert.equal(cart.items.length, 1, "produto simples nao pode virar duas linhas");
  assert.equal(cart.items[0].quantity, 3, "a segunda adicao tem que somar, nao falhar");
});

test("adicionar a mesma VARIACAO duas vezes soma a quantidade", skipOpts, async () => {
  await resetCart();

  await addItem({ userId, rewardProductId: variantProductId, trayVariantId: "2003", quantity: 1 }, deps);
  await addItem({ userId, rewardProductId: variantProductId, trayVariantId: "2003", quantity: 2 }, deps);

  const cart = await getCart(userId, deps);
  assert.equal(cart.items.length, 1);
  assert.equal(cart.items[0].quantity, 3);
});

test("adicoes concorrentes do mesmo item nao criam linha duplicada", skipOpts, async () => {
  await resetCart();

  const results = await Promise.allSettled([
    addItem({ userId, rewardProductId: simpleProductId, quantity: 1 }, deps),
    addItem({ userId, rewardProductId: simpleProductId, quantity: 1 }, deps),
    addItem({ userId, rewardProductId: simpleProductId, quantity: 1 }, deps),
  ]);

  const linhas = await pool.query(
    "select count(*)::int n, coalesce(sum(quantity),0)::int q from public.reward_cart_items i join public.reward_carts c on c.id=i.cart_id where c.user_id=$1",
    [userId]
  );
  assert.equal(linhas.rows[0].n, 1, "so pode existir UMA linha para o mesmo produto simples");

  const ok = results.filter((r) => r.status === "fulfilled").length;
  assert.ok(ok >= 1, "ao menos uma adicao deve ter passado");
  assert.equal(linhas.rows[0].q, ok, "a quantidade tem que refletir exatamente as adicoes bem-sucedidas");
});

test("variacoes diferentes do mesmo produto convivem", skipOpts, async () => {
  await resetCart();
  await addItem({ userId, rewardProductId: variantProductId, trayVariantId: "2003", quantity: 1 }, deps);

  const cart = await getCart(userId, deps);
  assert.equal(cart.items.length, 1);
  assert.equal(cart.items[0].tray_variant_id, "2003");
  assert.equal(cart.items[0].variant_name, "Tamanho: 41");
});

/* ─────────────────────────── Isolamento ─────────────────────────── */

test("um usuario nao ve nem altera o carrinho do outro", skipOpts, async () => {
  await resetCart();
  await addItem({ userId, rewardProductId: simpleProductId, quantity: 1 }, deps);
  const itemId = (await getCart(userId, deps)).items[0].id;

  assert.deepEqual((await getCart(otherUserId, deps)).items, []);
  await assert.rejects(() => updateItem({ userId: otherUserId, itemId, quantity: 2 }, deps), (e) => e.status === 404);
  await assert.rejects(() => removeItem({ userId: otherUserId, itemId }, deps), (e) => e.status === 404);

  assert.equal((await getCart(userId, deps)).items[0].quantity, 1);
});

/* ─────────────────────────── Carteira intacta ─────────────────────────── */

test("validar o carrinho NAO altera saldo nem ledger", skipOpts, async () => {
  await resetCart();
  await pool.query("delete from public.coupon_balance_history where user_id=$1", [userId]);
  await pool.query("update public.users set coupon_value_cents=0, coupon_expires_at=null where id=$1", [userId]);

  await applyCouponLedgerEntry(
    { userId, operation: "credit", amountCents: 845000, eventType: "ADMIN_BALANCE_ADJUSTMENT", channel: "ADMIN" },
    walletDeps
  );

  const saldoAntes = (await getCouponBalance(userId, walletDeps)).balance_cents;
  const ledgerAntes = await pool.query("select id from public.coupon_balance_history where user_id=$1 order by created_at", [userId]);
  assert.equal(saldoAntes, 845000);

  await addItem({ userId, rewardProductId: simpleProductId, quantity: 1 }, deps); // 5000
  const out = await validateCart(userId, deps);

  assert.equal(out.valid, true);
  assert.equal(out.cart.total_nscredits, 5000);
  assert.equal(out.wallet.balance, 8450);
  assert.equal(out.wallet.sufficient, true);

  const saldoDepois = (await getCouponBalance(userId, walletDeps)).balance_cents;
  const ledgerDepois = await pool.query("select id from public.coupon_balance_history where user_id=$1 order by created_at", [userId]);

  assert.equal(saldoDepois, 845000, "o saldo NAO pode mudar");
  assert.equal(ledgerDepois.rows.length, ledgerAntes.rows.length, "o ledger NAO pode ganhar linhas");
  assert.deepEqual(
    ledgerDepois.rows.map((r) => r.id),
    ledgerAntes.rows.map((r) => r.id),
    "o ledger tem que ser exatamente o mesmo"
  );
});

test("saldo insuficiente mantem o carrinho e nao debita", skipOpts, async () => {
  await resetCart();
  const saldoCents = (await getCouponBalance(userId, walletDeps)).balance_cents;
  const saldo = saldoCents / 100;

  await addItem({ userId, rewardProductId: simpleProductId, quantity: 2 }, deps); // 10000
  const out = await validateCart(userId, deps);

  assert.equal(out.valid, false);
  assert.equal(out.cart.total_nscredits, 10000);
  assert.equal(out.wallet.sufficient, false);
  assert.equal(out.wallet.missing, 10000 - saldo);
  assert.ok(out.issues.includes(CART_ISSUES.INSUFFICIENT_NSCREDITS));

  assert.equal((await getCouponBalance(userId, walletDeps)).balance_cents, saldoCents, "saldo intacto");
  assert.equal((await getCart(userId, deps)).items.length, 1, "o carrinho continua salvo");
});

test("cupom expirado bloqueia o carrinho mesmo com saldo suficiente", skipOpts, async () => {
  await resetCart();
  await pool.query("delete from public.coupon_balance_history where user_id=$1", [userId]);
  // A view canonica de expiracao (migration 027) resolve por DATA de
  // calendario em America/Sao_Paulo, nao por timestamp exato: "1h atras" no
  // mesmo dia ainda conta como valido. Precisa ser um dia anterior de verdade.
  await pool.query(
    "update public.users set coupon_value_cents=845000, coupon_expires_at=$2 where id=$1",
    [userId, new Date(Date.now() - 2 * 24 * 3600_000).toISOString()]
  );

  await addItem({ userId, rewardProductId: simpleProductId, quantity: 1 }, deps); // 5000
  const out = await validateCart(userId, deps);

  assert.equal(out.valid, false);
  assert.ok(out.issues.includes(CART_ISSUES.COUPON_EXPIRED));
  assert.equal(out.wallet.sufficient, false);

  await pool.query("update public.users set coupon_expires_at=null where id=$1", [userId]);
});

/* ─────────────────────────── Nao-mutacao da Tray ─────────────────────────── */

test("nenhuma operacao do carrinho emite algo diferente de GET na Tray", skipOpts, async () => {
  await resetCart();

  await addItem({ userId, rewardProductId: simpleProductId, quantity: 1 }, deps);
  const itemId = (await getCart(userId, deps)).items[0].id;
  await updateItem({ userId, itemId, quantity: 2 }, deps);
  await validateCart(userId, deps);
  await removeItem({ userId, itemId }, deps);
  await clearCart({ userId }, deps);

  assert.ok(trayCalls.length > 0);
  for (const c of trayCalls) assert.equal(c.method, "GET");
});

test("nao existem tabelas de pedido nem de reserva nesta fase", skipOpts, async () => {
  const r = await pool.query(
    `select tablename from pg_tables
      where schemaname='public' and tablename in ('reward_orders','reward_order_items','reward_stock_reservations')`
  );
  assert.equal(r.rowCount, 0, `tabelas fora de escopo encontradas: ${r.rows.map((x) => x.tablename).join(", ")}`);
});

test("o carrinho nao cria carrinho na Tray", skipOpts, async () => {
  await resetCart();
  await addItem({ userId, rewardProductId: simpleProductId, quantity: 1 }, deps);
  // O mock so responde catalogo; qualquer tentativa de carrinho/pedido Tray
  // apareceria aqui como chamada com method != GET ou path desconhecido.
  assert.ok(trayCalls.every((c) => c.method === "GET" && c.trayProductId));
});
