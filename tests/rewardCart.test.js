// tests/rewardCart.test.js
//
// Carrinho da Loja de Premios + pre-validacao factual na Tray.
// O carrinho e da NEWSTORE: nenhuma operacao toca carrinho ou pedido da Tray,
// nao reserva estoque e nao debita NSCreditos.
import test from "node:test";
import assert from "node:assert/strict";

import {
  CART_ISSUES,
  RewardCartError,
  parseQuantity,
  getCart,
  addItem,
  updateItem,
  removeItem,
  clearCart,
} from "../src/services/rewardCart.js";
import { validateCart } from "../src/services/rewardCartValidator.js";

const USER = 6;
const OTHER_USER = 99;
const PROD_SIMPLE = "11111111-1111-1111-1111-111111111111";
const PROD_VARIANT = "22222222-2222-2222-2222-222222222222";
const PROD_UNPUBLISHED = "33333333-3333-3333-3333-333333333333";

function rewardRow(overrides = {}) {
  return {
    id: PROD_SIMPLE,
    tray_product_id: "123",
    nscredits_price: "5000",
    is_published: true,
    name: "Citizen Promaster",
    image_url: "https://cdn/1.jpg",
    has_variation: false,
    ...overrides,
  };
}

function trayProduct(overrides = {}) {
  return {
    tray_product_id: "123",
    name: "Citizen Promaster",
    stock: 3,
    tray_available: 1,
    tray_available_in_store: 1,
    availability_text: "Disponivel",
    has_variation: false,
    variants: [],
    presentation: { is_available: true, reason: "available" },
    ...overrides,
  };
}

function trayVariant(id, overrides = {}) {
  return {
    variant_id: String(id),
    tray_product_id: "456",
    reference: `REF-${id}`,
    stock: 5,
    tray_available: 1,
    values: [{ type: "Cor", value: "Azul" }, { type: "Tamanho", value: "41" }],
    ...overrides,
  };
}

/** Banco falso: carrinhos, itens, produtos locais + spy de chamadas Tray. */
function makeDb({ rewards = {}, tray = {}, trayFails = {}, balance = 8450 } = {}) {
  const state = {
    carts: [],
    items: [],
    rewards: {
      [PROD_SIMPLE]: rewardRow(),
      [PROD_VARIANT]: rewardRow({
        id: PROD_VARIANT,
        tray_product_id: "456",
        nscredits_price: "3000",
        name: "Tenis XPTO",
        has_variation: true,
      }),
      [PROD_UNPUBLISHED]: rewardRow({ id: PROD_UNPUBLISHED, tray_product_id: "789", is_published: false }),
      ...rewards,
    },
    tray: {
      123: trayProduct(),
      456: trayProduct({
        tray_product_id: "456",
        name: "Tenis XPTO",
        has_variation: true,
        stock: 0,
        variants: [trayVariant(2003), trayVariant(2004, { stock: 0, tray_available: 0 })],
      }),
      789: trayProduct({ tray_product_id: "789" }),
      ...tray,
    },
    trayFails,
    balance,
    trayCalls: [],
    sql: [],
    nextId: 1,
    walletReads: 0,
  };

  function run(sql, params = []) {
    state.sql.push({ sql, params });
    const s = String(sql).toLowerCase();

    if (/from public\.reward_carts/.test(s) && /select/.test(s)) {
      const c = state.carts.find((x) => x.user_id === Number(params[0]) && x.status === "active");
      return { rows: c ? [c] : [], rowCount: c ? 1 : 0 };
    }
    if (/insert into public\.reward_carts/.test(s)) {
      let c = state.carts.find((x) => x.user_id === Number(params[0]) && x.status === "active");
      if (!c) {
        c = { id: `cart-${state.nextId++}`, user_id: Number(params[0]), status: "active" };
        state.carts.push(c);
      }
      return { rows: [c], rowCount: 1 };
    }
    if (/from public\.reward_products/.test(s)) {
      const r = state.rewards[String(params[0])];
      return { rows: r ? [r] : [], rowCount: r ? 1 : 0 };
    }
    if (/insert into public\.reward_cart_items/.test(s)) {
      const [cart_id, reward_product_id, tray_product_id, tray_variant_id, quantity, price, name, variantName, image] = params;
      const existing = state.items.find(
        (i) =>
          i.cart_id === cart_id &&
          i.reward_product_id === reward_product_id &&
          String(i.tray_variant_id ?? "") === String(tray_variant_id ?? "")
      );
      if (existing) {
        existing.quantity += Number(quantity);
        existing.nscredits_unit_price_snapshot = String(price);
        return { rows: [existing], rowCount: 1 };
      }
      const row = {
        id: `item-${state.nextId++}`,
        cart_id,
        reward_product_id,
        tray_product_id,
        tray_variant_id: tray_variant_id ?? null,
        quantity: Number(quantity),
        nscredits_unit_price_snapshot: String(price),
        product_name_snapshot: name ?? null,
        variant_name_snapshot: variantName ?? null,
        image_url_snapshot: image ?? null,
        created_at: new Date("2026-08-16T12:00:00.000Z"),
      };
      state.items.push(row);
      return { rows: [row], rowCount: 1 };
    }
    // Quantidade ja existente do mesmo produto/variacao (join com reward_carts).
    if (/from public\.reward_cart_items/.test(s) && /c\.user_id\s*=\s*\$1/.test(s) && /reward_product_id\s*=\s*\$2/.test(s)) {
      const [uid, productId, variantId] = params;
      const cart = state.carts.find((c) => c.user_id === Number(uid) && c.status === "active");
      if (!cart) return { rows: [], rowCount: 0 };
      const it = state.items.find(
        (i) =>
          i.cart_id === cart.id &&
          i.reward_product_id === productId &&
          (variantId === undefined ? i.tray_variant_id === null : String(i.tray_variant_id) === String(variantId))
      );
      return it ? { rows: [{ quantity: it.quantity }], rowCount: 1 } : { rows: [], rowCount: 0 };
    }

    // Item pertencente AO usuario (join com reward_carts).
    if (/from public\.reward_cart_items/.test(s) && /i\.id\s*=\s*\$1/.test(s) && /c\.user_id\s*=\s*\$2/.test(s)) {
      const [itemId, uid] = params;
      const it = state.items.find((i) => i.id === itemId);
      if (!it) return { rows: [], rowCount: 0 };
      const cart = state.carts.find((c) => c.id === it.cart_id);
      if (!cart || cart.user_id !== Number(uid) || cart.status !== "active") return { rows: [], rowCount: 0 };
      return { rows: [{ ...it, cart_user_id: cart.user_id }], rowCount: 1 };
    }

    // `delete from ...` tambem casa com /from .../, entao a leitura generica
    // so vale para SELECT.
    if (/from public\.reward_cart_items/.test(s) && s.trimStart().startsWith("select")) {
      const rows = state.items
        .filter((i) => i.cart_id === params[0])
        .map((i) => {
          const r = Object.values(state.rewards).find((x) => x.id === i.reward_product_id) || {};
          return { ...i, current_nscredits_price: r.nscredits_price, is_published: r.is_published, product_has_variation: r.has_variation };
        });
      return { rows, rowCount: rows.length };
    }
    if (/update public\.reward_cart_items/.test(s)) {
      const it = state.items.find((i) => i.id === params[1]);
      if (!it) return { rows: [], rowCount: 0 };
      it.quantity = Number(params[0]);
      return { rows: [it], rowCount: 1 };
    }
    if (/delete from public\.reward_cart_items/.test(s)) {
      const before = state.items.length;
      if (/where id/.test(s)) state.items = state.items.filter((i) => i.id !== params[0]);
      else state.items = state.items.filter((i) => i.cart_id !== params[0]);
      return { rows: [], rowCount: before - state.items.length };
    }
    return { rows: [], rowCount: 0 };
  }

  const deps = {
    query: async (sql, params) => run(sql, params),
    withTransaction: async (fn) => {
      const snapshot = { carts: [...state.carts], items: state.items.map((i) => ({ ...i })) };
      try {
        return await fn({ query: async (sql, params) => run(sql, params) });
      } catch (e) {
        state.carts = snapshot.carts;
        state.items = snapshot.items;
        throw e;
      }
    },
    getCatalogProduct: async (trayProductId, options) => {
      state.trayCalls.push({ method: "GET", trayProductId, options });
      const fail = state.trayFails[String(trayProductId)];
      if (fail) throw Object.assign(new Error(fail.code), { code: fail.code, status: fail.status });
      const p = state.tray[String(trayProductId)];
      if (!p) throw Object.assign(new Error("tray_product_not_found"), { code: "tray_product_not_found", status: 404 });
      return p;
    },
    getWalletBalance: async () => {
      state.walletReads += 1;
      return { balance: state.balance };
    },
  };

  return { state, deps };
}

/* ─────────────────────────── Quantidade ─────────────────────────── */

test("quantidade aceita inteiros >= 1", () => {
  for (const v of [1, 2, 10, "3"]) assert.equal(parseQuantity(v), Number(v));
});

test("quantidade rejeita zero, negativo, decimal e nao-numerico", () => {
  for (const bad of [0, -1, 1.5, "abc", "", null, undefined, true, {}, [], NaN, Infinity, "1.5"]) {
    assert.throws(() => parseQuantity(bad), (e) => {
      assert.ok(e instanceof RewardCartError);
      assert.equal(e.status, 400);
      assert.equal(e.code, "invalid_quantity");
      return true;
    }, `deveria rejeitar ${JSON.stringify(bad)}`);
  }
});

/* ─────────────────────────── Leitura ─────────────────────────── */

test("GET carrinho vazio para usuario sem carrinho", async () => {
  const { deps } = makeDb();
  const cart = await getCart(USER, deps);
  assert.deepEqual(cart.items, []);
  assert.deepEqual(cart.totals, { items: 0, units: 0, nscredits: 0 });
  assert.equal(cart.id, null);
});

test("leitura NAO cria carrinho no banco", async () => {
  const { state, deps } = makeDb();
  await getCart(USER, deps);
  assert.equal(state.carts.length, 0);
  assert.ok(!state.sql.some((q) => /insert into public\.reward_carts/i.test(q.sql)));
});

test("leitura do carrinho NAO chama a Tray", async () => {
  const { state, deps } = makeDb();
  await getCart(USER, deps);
  assert.equal(state.trayCalls.length, 0);
});

/* ─────────────────────────── Adicionar ─────────────────────────── */

test("primeiro item cria o carrinho", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);
  assert.equal(state.carts.length, 1);
  assert.equal(state.carts[0].user_id, USER);
  assert.equal(state.items.length, 1);
});

test("o backend usa o preco de reward_products e IGNORA o do frontend", async () => {
  const { state, deps } = makeDb();

  await addItem(
    {
      userId: USER,
      rewardProductId: PROD_SIMPLE,
      quantity: 1,
      // Injecao do navegador — tudo abaixo tem que ser descartado:
      nscredits_unit_price_snapshot: 1,
      nscredits_price: 1,
      product_name_snapshot: "NOME FALSO",
      image_url_snapshot: "https://malicioso/x.jpg",
      tray_product_id: "999",
      price: 1,
    },
    deps
  );

  const item = state.items[0];
  assert.equal(item.nscredits_unit_price_snapshot, "5000", "preco tem que vir do reward_product");
  assert.equal(item.product_name_snapshot, "Citizen Promaster");
  assert.equal(item.image_url_snapshot, "https://cdn/1.jpg");
  assert.equal(item.tray_product_id, "123", "tray_product_id vem do produto local");
});

test("produto inexistente e rejeitado", async () => {
  const { state, deps } = makeDb();
  await assert.rejects(
    () => addItem({ userId: USER, rewardProductId: "44444444-4444-4444-4444-444444444444", quantity: 1 }, deps),
    (e) => {
      assert.equal(e.code, CART_ISSUES.PRODUCT_NOT_FOUND);
      assert.equal(e.status, 404);
      return true;
    }
  );
  assert.equal(state.items.length, 0);
});

test("produto despublicado nao pode ser adicionado", async () => {
  const { state, deps } = makeDb();
  await assert.rejects(
    () => addItem({ userId: USER, rewardProductId: PROD_UNPUBLISHED, quantity: 1 }, deps),
    (e) => {
      assert.equal(e.code, CART_ISSUES.PRODUCT_NOT_PUBLISHED);
      return true;
    }
  );
  assert.equal(state.items.length, 0);
  assert.equal(state.trayCalls.length, 0, "nem precisa consultar a Tray");
});

test("adicionar consulta a Tray novamente (snapshot local nao basta)", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);

  assert.equal(state.trayCalls.length, 1);
  assert.equal(state.trayCalls[0].trayProductId, "123");
  assert.equal(state.trayCalls[0].options.withVariants, true);
});

test("produto indisponivel na Tray e rejeitado", async () => {
  const { deps } = makeDb({
    tray: { 123: trayProduct({ presentation: { is_available: false, reason: "unavailable_in_tray" } }) },
  });
  await assert.rejects(
    () => addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps),
    (e) => {
      assert.equal(e.code, CART_ISSUES.PRODUCT_UNAVAILABLE);
      return true;
    }
  );
});

test("estoque insuficiente e rejeitado", async () => {
  const { deps } = makeDb();
  await assert.rejects(
    () => addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 4 }, deps),
    (e) => {
      assert.equal(e.code, CART_ISSUES.INSUFFICIENT_STOCK);
      assert.deepEqual(e.details, { stock: 3, requested: 4 });
      return true;
    }
  );
});

test("quantidade igual ao estoque e aceita", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 3 }, deps);
  assert.equal(state.items[0].quantity, 3);
});

test("produto vendido SOB ENCOMENDA nao e travado pelo estoque zerado", async () => {
  // Caso real: estoque 0, available 1, "Disponivel em 45 dias uteis".
  // A Tray afirma que vende; travar pelo estoque contradiria a Tray.
  const { state, deps } = makeDb({
    tray: {
      123: trayProduct({
        stock: 0,
        availability_text: "Disponivel em 45 dias uteis",
        presentation: { is_available: true, reason: "available_extended_lead_time" },
      }),
    },
  });

  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 2 }, deps);
  assert.equal(state.items[0].quantity, 2);
});

test("produto com estoque continua limitado pelo estoque", async () => {
  const { deps } = makeDb({
    tray: { 123: trayProduct({ stock: 2, presentation: { is_available: true, reason: "available" } }) },
  });
  await assert.rejects(
    () => addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 3 }, deps),
    (e) => e.code === CART_ISSUES.INSUFFICIENT_STOCK
  );
});

test("o estoque da variacao sempre limita, mesmo em produto sob encomenda", async () => {
  const { deps } = makeDb({
    tray: {
      456: trayProduct({
        tray_product_id: "456",
        has_variation: true,
        stock: 0,
        presentation: { is_available: true, reason: "available_extended_lead_time" },
        variants: [trayVariant(2003, { stock: 2 })],
      }),
    },
  });
  await assert.rejects(
    () => addItem({ userId: USER, rewardProductId: PROD_VARIANT, trayVariantId: "2003", quantity: 3 }, deps),
    (e) => e.code === CART_ISSUES.INSUFFICIENT_STOCK
  );
});

/* ─────────────────────────── Variacoes ─────────────────────────── */

test("produto simples aceita variant null", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);
  assert.equal(state.items[0].tray_variant_id, null);
});

test("produto com variacao EXIGE variacao", async () => {
  const { deps } = makeDb();
  await assert.rejects(
    () => addItem({ userId: USER, rewardProductId: PROD_VARIANT, quantity: 1 }, deps),
    (e) => {
      assert.equal(e.code, CART_ISSUES.VARIANT_REQUIRED);
      return true;
    }
  );
});

test("variacao inexistente e rejeitada", async () => {
  const { deps } = makeDb();
  await assert.rejects(
    () => addItem({ userId: USER, rewardProductId: PROD_VARIANT, trayVariantId: "9999", quantity: 1 }, deps),
    (e) => {
      assert.equal(e.code, CART_ISSUES.VARIANT_NOT_FOUND);
      return true;
    }
  );
});

test("variacao de outro produto e rejeitada", async () => {
  const { deps } = makeDb({
    tray: {
      456: trayProduct({
        tray_product_id: "456",
        has_variation: true,
        variants: [trayVariant(2003, { tray_product_id: "999" })],
      }),
    },
  });
  await assert.rejects(
    () => addItem({ userId: USER, rewardProductId: PROD_VARIANT, trayVariantId: "2003", quantity: 1 }, deps),
    (e) => {
      assert.equal(e.code, CART_ISSUES.VARIANT_NOT_BELONGS_TO_PRODUCT);
      return true;
    }
  );
});

test("variacao indisponivel e rejeitada", async () => {
  const { deps } = makeDb();
  await assert.rejects(
    () => addItem({ userId: USER, rewardProductId: PROD_VARIANT, trayVariantId: "2004", quantity: 1 }, deps),
    (e) => {
      assert.equal(e.code, CART_ISSUES.VARIANT_UNAVAILABLE);
      return true;
    }
  );
});

test("variacao valida e aceita e guarda o nome legivel", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_VARIANT, trayVariantId: "2003", quantity: 1 }, deps);

  const item = state.items[0];
  assert.equal(item.tray_variant_id, "2003");
  assert.equal(item.variant_name_snapshot, "Cor: Azul · Tamanho: 41");
});

test("estoque da VARIACAO e o que limita, nao o do produto", async () => {
  // O produto 456 tem stock 0, mas a variacao 2003 tem 5.
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_VARIANT, trayVariantId: "2003", quantity: 5 }, deps);
  assert.equal(state.items[0].quantity, 5);

  await assert.rejects(
    () => addItem({ userId: USER, rewardProductId: PROD_VARIANT, trayVariantId: "2003", quantity: 1 }, deps),
    (e) => {
      assert.equal(e.code, CART_ISSUES.INSUFFICIENT_STOCK);
      return true;
    }
  );
});

/* ─────────────────────────── Item duplicado ─────────────────────────── */

test("mesmo produto e mesma variacao incrementam o item existente", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_VARIANT, trayVariantId: "2003", quantity: 1 }, deps);
  await addItem({ userId: USER, rewardProductId: PROD_VARIANT, trayVariantId: "2003", quantity: 2 }, deps);

  assert.equal(state.items.length, 1, "nao pode duplicar linha");
  assert.equal(state.items[0].quantity, 3);
});

test("mesmo produto com variacoes diferentes gera itens distintos", async () => {
  const { state, deps } = makeDb({
    tray: {
      456: trayProduct({
        tray_product_id: "456",
        has_variation: true,
        variants: [trayVariant(2003), trayVariant(2005)],
      }),
    },
  });
  await addItem({ userId: USER, rewardProductId: PROD_VARIANT, trayVariantId: "2003", quantity: 1 }, deps);
  await addItem({ userId: USER, rewardProductId: PROD_VARIANT, trayVariantId: "2005", quantity: 1 }, deps);
  assert.equal(state.items.length, 2);
});

test("produto simples adicionado duas vezes nao duplica (variant NULL)", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);
  assert.equal(state.items.length, 1);
  assert.equal(state.items[0].quantity, 2);
});

test("incremento respeita o estoque total (quantidade acumulada)", async () => {
  const { deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 2 }, deps);
  await assert.rejects(
    () => addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 2 }, deps),
    (e) => {
      assert.equal(e.code, CART_ISSUES.INSUFFICIENT_STOCK);
      assert.deepEqual(e.details, { stock: 3, requested: 4 });
      return true;
    }
  );
});

/* ─────────────────────────── Alterar / remover ─────────────────────────── */

test("alterar quantidade revalida na Tray", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);
  const itemId = state.items[0].id;
  const antes = state.trayCalls.length;

  await updateItem({ userId: USER, itemId, quantity: 2 }, deps);

  assert.equal(state.items[0].quantity, 2);
  assert.ok(state.trayCalls.length > antes, "update tem que consultar a Tray de novo");
});

test("alterar quantidade acima do estoque e rejeitado", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);
  await assert.rejects(
    () => updateItem({ userId: USER, itemId: state.items[0].id, quantity: 99 }, deps),
    (e) => {
      assert.equal(e.code, CART_ISSUES.INSUFFICIENT_STOCK);
      return true;
    }
  );
  assert.equal(state.items[0].quantity, 1, "quantidade nao pode ter mudado");
});

test("remover item NAO chama a Tray", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);
  const antes = state.trayCalls.length;

  await removeItem({ userId: USER, itemId: state.items[0].id }, deps);

  assert.equal(state.items.length, 0);
  assert.equal(state.trayCalls.length, antes, "remover e operacao 100% local");
});

test("limpar carrinho NAO chama a Tray", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);
  const antes = state.trayCalls.length;

  await clearCart({ userId: USER }, deps);

  assert.equal(state.items.length, 0);
  assert.equal(state.trayCalls.length, antes);
});

/* ─────────────────────────── Isolamento entre usuarios ─────────────────────────── */

test("usuario nao altera item de outro usuario", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);
  const itemId = state.items[0].id;

  await assert.rejects(() => updateItem({ userId: OTHER_USER, itemId, quantity: 2 }, deps), (e) => {
    assert.equal(e.status, 404);
    assert.equal(e.code, "cart_item_not_found");
    return true;
  });
  assert.equal(state.items[0].quantity, 1);
});

test("usuario nao remove item de outro usuario", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);

  await assert.rejects(() => removeItem({ userId: OTHER_USER, itemId: state.items[0].id }, deps), (e) => {
    assert.equal(e.status, 404);
    return true;
  });
  assert.equal(state.items.length, 1);
});

test("carrinho de um usuario nao vaza para outro", async () => {
  const { deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);

  const outro = await getCart(OTHER_USER, deps);
  assert.deepEqual(outro.items, []);
});

/* ─────────────────────────── Totais ─────────────────────────── */

test("totais do carrinho sao calculados a partir do preco local", async () => {
  const { deps } = makeDb({
    tray: {
      456: trayProduct({ tray_product_id: "456", has_variation: true, variants: [trayVariant(2003)] }),
    },
  });
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);       // 5000
  await addItem({ userId: USER, rewardProductId: PROD_VARIANT, trayVariantId: "2003", quantity: 1 }, deps); // 3000

  const cart = await getCart(USER, deps);
  assert.equal(cart.totals.items, 2);
  assert.equal(cart.totals.units, 2);
  assert.equal(cart.totals.nscredits, 8000);
});

test("total considera a quantidade", async () => {
  const { deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 3 }, deps);
  const cart = await getCart(USER, deps);
  assert.equal(cart.totals.units, 3);
  assert.equal(cart.totals.nscredits, 15000);
});

/* ─────────────────────────── Validacao do carrinho ─────────────────────────── */

test("carrinho valido com saldo suficiente", async () => {
  const { state, deps } = makeDb({ balance: 8450 });
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);

  const out = await validateCart(USER, deps);

  assert.equal(out.valid, true);
  assert.equal(out.cart.total_nscredits, 5000);
  assert.equal(out.wallet.balance, 8450);
  assert.equal(out.wallet.sufficient, true);
  assert.equal(out.wallet.missing, 0);
  assert.equal(out.items[0].valid, true);
  assert.deepEqual(out.items[0].issues, []);
  assert.equal(state.balance, 8450, "validar NAO altera o saldo");
});

test("saldo insuficiente invalida o carrinho mas nao debita nada", async () => {
  const { state, deps } = makeDb({ balance: 8450 });
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 2 }, deps); // 10000

  const out = await validateCart(USER, deps);

  assert.equal(out.valid, false);
  assert.equal(out.cart.total_nscredits, 10000);
  assert.equal(out.wallet.sufficient, false);
  assert.equal(out.wallet.missing, 1550);
  assert.ok(out.issues.includes(CART_ISSUES.INSUFFICIENT_NSCREDITS));
  assert.equal(state.balance, 8450, "saldo intacto");
});

test("validacao NAO grava ledger nem cria pedido", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);
  await validateCart(USER, deps);

  const escritas = state.sql.filter((q) => /insert into|update |delete from/i.test(q.sql));
  assert.ok(
    !escritas.some((q) => /nscredit_transactions|nscredit_wallets|reward_orders/i.test(q.sql)),
    "validar nao pode escrever em carteira, ledger ou pedido"
  );
});

test("preco alterado pelo admin e detectado", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);

  state.rewards[PROD_SIMPLE].nscredits_price = "6000";

  const out = await validateCart(USER, deps);
  const item = out.items[0];

  assert.equal(item.valid, false);
  assert.ok(item.issues.includes(CART_ISSUES.PRICE_CHANGED));
  assert.equal(item.snapshot_nscredits_price, 5000);
  assert.equal(item.current_nscredits_price, 6000);
  assert.equal(out.cart.total_nscredits, 6000, "o total usa o preco VIGENTE");
});

test("produto despublicado depois de adicionado nao some do carrinho", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);

  state.rewards[PROD_SIMPLE].is_published = false;

  const out = await validateCart(USER, deps);
  assert.equal(out.valid, false);
  assert.equal(out.items.length, 1, "o item continua no carrinho");
  assert.ok(out.items[0].issues.includes(CART_ISSUES.PRODUCT_NOT_PUBLISHED));
  assert.equal(state.items.length, 1, "nada apagado silenciosamente");
});

test("estoque que caiu depois de adicionado e detectado", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 3 }, deps);

  state.tray["123"] = trayProduct({ stock: 1 });

  const out = await validateCart(USER, deps);
  assert.equal(out.valid, false);
  assert.ok(out.items[0].issues.includes(CART_ISSUES.INSUFFICIENT_STOCK));
  assert.equal(state.items.length, 1);
});

test("variacao que ficou indisponivel e detectada", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_VARIANT, trayVariantId: "2003", quantity: 1 }, deps);

  state.tray["456"] = trayProduct({
    tray_product_id: "456",
    has_variation: true,
    variants: [trayVariant(2003, { tray_available: 0, stock: 0 })],
  });

  const out = await validateCart(USER, deps);
  assert.ok(out.items[0].issues.includes(CART_ISSUES.VARIANT_UNAVAILABLE));
});

test("Tray fora do ar torna a validacao invalida — nao usa snapshot antigo", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);

  state.trayFails["123"] = { code: "tray_unavailable", status: 503 };

  const out = await validateCart(USER, deps);

  assert.equal(out.valid, false);
  assert.ok(out.items[0].issues.includes(CART_ISSUES.TRAY_UNAVAILABLE));
  assert.equal(state.items.length, 1, "o carrinho local continua existindo");
});

test("carrinho vazio nao e declarado pronto", async () => {
  const { deps } = makeDb();
  const out = await validateCart(USER, deps);
  assert.equal(out.valid, false);
  assert.ok(out.issues.includes(CART_ISSUES.CART_EMPTY));
  assert.equal(out.cart.total_nscredits, 0);
});

test("varios problemas sao reportados separadamente, nao como erro generico", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 3 }, deps);
  await addItem({ userId: USER, rewardProductId: PROD_VARIANT, trayVariantId: "2003", quantity: 1 }, deps);

  state.rewards[PROD_SIMPLE].nscredits_price = "6000";
  state.tray["123"] = trayProduct({ stock: 1 });

  const out = await validateCart(USER, deps);
  const simples = out.items.find((i) => i.tray_product_id === "123");
  const comVariacao = out.items.find((i) => i.tray_product_id === "456");

  assert.ok(simples.issues.includes(CART_ISSUES.PRICE_CHANGED));
  assert.ok(simples.issues.includes(CART_ISSUES.INSUFFICIENT_STOCK));
  assert.equal(comVariacao.valid, true, "o outro item continua valido");
});

/* ─────────────────────────── Nao-mutacao da Tray ─────────────────────────── */

test("NENHUMA operacao do carrinho escreve na Tray", async () => {
  const { state, deps } = makeDb();

  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);
  const itemId = state.items[0].id;
  await updateItem({ userId: USER, itemId, quantity: 2 }, deps);
  await validateCart(USER, deps);
  await removeItem({ userId: USER, itemId }, deps);
  await clearCart({ userId: USER }, deps);

  assert.ok(state.trayCalls.length > 0, "leitura de catalogo tem que acontecer");
  for (const c of state.trayCalls) {
    assert.equal(c.method, "GET", `metodo proibido na Tray: ${c.method}`);
  }
});

test("o carrinho nao cria reserva de estoque nem pedido", async () => {
  const { state, deps } = makeDb();
  await addItem({ userId: USER, rewardProductId: PROD_SIMPLE, quantity: 1 }, deps);
  await validateCart(USER, deps);

  const sql = state.sql.map((q) => String(q.sql).toLowerCase()).join(" ");
  assert.ok(!/reward_stock_reservations/.test(sql), "nao pode existir reserva de estoque");
  assert.ok(!/reward_orders/.test(sql), "nao pode existir pedido");
});
