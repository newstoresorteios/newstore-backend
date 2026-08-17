// tests/rewardStore.test.js
// Regras da Loja de Premios: NSCreditos, publicacao em lote atomica, sync e listagens.
import test from "node:test";
import assert from "node:assert/strict";

import {
  MAX_PUBLISH_BATCH,
  RewardStoreError,
  parseNsCreditsPrice,
  validatePublishBatch,
  publishRewardProducts,
  patchRewardProduct,
  syncRewardProducts,
  listPublishedRewardProducts,
  listAdminRewardProducts,
  getRewardsByTrayIds,
  getRewardCatalogStats,
} from "../src/services/rewardStore.js";

function trayProduct(id, overrides = {}) {
  return {
    tray_product_id: String(id),
    name: `Produto ${id}`,
    description_small: "desc",
    reference: `REF-${id}`,
    brand: "Citizen",
    image_url: `https://cdn/${id}.jpg`,
    images: [`https://cdn/${id}.jpg`],
    tray_product_url: `https://loja/p/${id}`,
    stock: 3,
    tray_available: 1,
    tray_available_in_store: 1,
    availability_text: "Disponivel",
    has_variation: false,
    when_stock_runs_out: "deactivate_product",
    order_days_availability: 0,
    tray_price: 4599,
    tray_modified_at: "2026-08-10T14:32:11.000Z",
    variants: [],
    presentation: { is_available: true, reason: "available" },
    ...overrides,
  };
}

function dbRow(id, overrides = {}) {
  return {
    id: `uuid-${id}`,
    tray_product_id: String(id),
    nscredits_price: 5000,
    is_published: true,
    display_order: 0,
    name: `Produto ${id}`,
    description_small: "desc",
    reference: `REF-${id}`,
    brand: "Citizen",
    image_url: `https://cdn/${id}.jpg`,
    images_snapshot: [`https://cdn/${id}.jpg`],
    tray_product_url: `https://loja/p/${id}`,
    stock: 3,
    tray_available: 1,
    tray_available_in_store: 1,
    availability_text: "Disponivel",
    has_variation: false,
    when_stock_runs_out: "deactivate_product",
    order_days_availability: 0,
    variants_snapshot: [],
    tray_price_snapshot: "4599.00",
    tray_modified_at: new Date("2026-08-10T14:32:11.000Z"),
    last_synced_at: new Date("2026-08-12T00:00:00.000Z"),
    published_at: new Date("2026-08-11T00:00:00.000Z"),
    published_by: 7,
    ...overrides,
  };
}

function makeDeps({ catalog = {}, rows = [], failCatalogFor = [] } = {}) {
  const state = {
    catalogCalls: [],
    queries: [],
    txQueries: [],
    transactions: 0,
    commits: 0,
    rollbacks: 0,
  };

  const deps = {
    query: async (sql, params) => {
      state.queries.push({ sql, params });
      return { rows, rowCount: rows.length };
    },
    withTransaction: async (fn) => {
      state.transactions += 1;
      const client = {
        query: async (sql, params) => {
          state.txQueries.push({ sql, params });
          return { rows: rows.length ? rows : [dbRow(String(params?.[0] ?? "0"))], rowCount: 1 };
        },
      };
      try {
        const out = await fn(client);
        state.commits += 1;
        return out;
      } catch (e) {
        state.rollbacks += 1;
        throw e;
      }
    },
    getCatalogProduct: async (id, options) => {
      state.catalogCalls.push({ id: String(id), options });
      if (failCatalogFor.includes(String(id))) {
        const e = new Error("tray_product_not_found");
        e.code = "tray_product_not_found";
        e.status = 404;
        throw e;
      }
      return catalog[String(id)] || trayProduct(id);
    },
  };

  return { deps, state };
}

/* ───────────────────────── NSCreditos ───────────────────────── */

test("NSCreditos aceita inteiros positivos", () => {
  assert.equal(parseNsCreditsPrice(1), 1);
  assert.equal(parseNsCreditsPrice(500), 500);
  assert.equal(parseNsCreditsPrice(5000), 5000);
  assert.equal(parseNsCreditsPrice(15000), 15000);
  assert.equal(parseNsCreditsPrice("5000"), 5000);
  assert.equal(parseNsCreditsPrice(" 5000 "), 5000);
});

test("NSCreditos rejeita zero, negativo, fracionario e nao-numerico", () => {
  for (const bad of [0, -1, 1.5, "abc", null, undefined, "", true, false, NaN, Infinity, [], {}, "1.5", "5e3", "-3"]) {
    assert.throws(
      () => parseNsCreditsPrice(bad),
      (e) => {
        assert.ok(e instanceof RewardStoreError, `esperado RewardStoreError para ${JSON.stringify(bad)}`);
        assert.equal(e.status, 400);
        assert.equal(e.code, "invalid_nscredits_price");
        return true;
      },
      `deveria rejeitar ${JSON.stringify(bad)}`
    );
  }
});

/* ───────────────────────── Lote ───────────────────────── */

test("lote vazio e rejeitado", () => {
  for (const bad of [[], null, undefined, "x", {}]) {
    assert.throws(() => validatePublishBatch(bad), (e) => {
      assert.equal(e.status, 400);
      assert.equal(e.code, "empty_batch");
      return true;
    });
  }
});

test("lote com tray_product_id duplicado e rejeitado", () => {
  assert.throws(
    () =>
      validatePublishBatch([
        { tray_product_id: "123", nscredits_price: 5000 },
        { tray_product_id: "123", nscredits_price: 3000 },
      ]),
    (e) => {
      assert.equal(e.status, 400);
      assert.equal(e.code, "duplicate_tray_product_id");
      return true;
    }
  );
});

test("lote acima do limite e rejeitado", () => {
  const big = Array.from({ length: MAX_PUBLISH_BATCH + 1 }, (_, i) => ({
    tray_product_id: String(i + 1),
    nscredits_price: 100,
  }));
  assert.throws(() => validatePublishBatch(big), (e) => {
    assert.equal(e.status, 400);
    assert.equal(e.code, "batch_too_large");
    return true;
  });
});

test("o lote so carrega a INTENCAO administrativa — snapshot do frontend e descartado", () => {
  const out = validatePublishBatch([
    {
      tray_product_id: "123",
      nscredits_price: 5000,
      // Tudo abaixo e injecao do frontend e precisa sumir:
      name: "NOME FALSO",
      stock: 999999,
      available: 1,
      price: 1,
      image_url: "https://malicioso/x.jpg",
      variants: [{ id: 1 }],
      is_published: true,
      published_by: 1,
    },
  ]);

  assert.deepEqual(out, [{ tray_product_id: "123", nscredits_price: 5000 }]);
});

/* ───────────────────────── Publicacao ───────────────────────── */

test("publicacao consulta a Tray novamente para cada produto do lote", async () => {
  const { deps, state } = makeDeps();

  await publishRewardProducts(
    [
      { tray_product_id: "123", nscredits_price: 5000 },
      { tray_product_id: "456", nscredits_price: 3000 },
    ],
    { adminUserId: 7 },
    deps
  );

  assert.deepEqual(state.catalogCalls.map((c) => c.id), ["123", "456"]);
  for (const c of state.catalogCalls) {
    assert.equal(c.options.withVariants, true);
  }
});

test("publicacao usa o snapshot da Tray, nunca o payload do frontend", async () => {
  const { deps, state } = makeDeps({
    catalog: { 123: trayProduct("123", { name: "Citizen Promaster", stock: 3 }) },
  });

  await publishRewardProducts(
    [{ tray_product_id: "123", nscredits_price: 5000, name: "NOME FALSO", stock: 999999 }],
    { adminUserId: 7 },
    deps
  );

  const insert = state.txQueries.find((q) => /insert into public\.reward_products/i.test(q.sql));
  assert.ok(insert, "esperado INSERT em reward_products");
  assert.ok(insert.params.includes("Citizen Promaster"), "nome deveria vir da Tray");
  assert.ok(!insert.params.includes("NOME FALSO"), "nome do frontend nao pode ser persistido");
  assert.ok(!insert.params.includes(999999), "estoque do frontend nao pode ser persistido");
  assert.ok(insert.params.includes(3), "estoque deveria vir da Tray");
});

test("se um item do lote nao valida, NENHUM produto e publicado e a transacao nem abre", async () => {
  const { deps, state } = makeDeps({ failCatalogFor: ["456"] });

  await assert.rejects(
    () =>
      publishRewardProducts(
        [
          { tray_product_id: "123", nscredits_price: 5000 },
          { tray_product_id: "456", nscredits_price: 3000 },
        ],
        { adminUserId: 7 },
        deps
      ),
    (e) => {
      assert.equal(e.status, 404);
      return true;
    }
  );

  assert.equal(state.transactions, 0, "a transacao nao pode abrir se a validacao previa falhou");
  assert.equal(state.txQueries.length, 0);
});

test("publicacao em lote e atomica: uma unica transacao para todos os produtos", async () => {
  const { deps, state } = makeDeps();

  await publishRewardProducts(
    [
      { tray_product_id: "123", nscredits_price: 5000 },
      { tray_product_id: "456", nscredits_price: 3000 },
      { tray_product_id: "789", nscredits_price: 1000 },
    ],
    { adminUserId: 7 },
    deps
  );

  assert.equal(state.transactions, 1);
  assert.equal(state.commits, 1);
  assert.equal(state.rollbacks, 0);
  const inserts = state.txQueries.filter((q) => /insert into public\.reward_products/i.test(q.sql));
  assert.equal(inserts.length, 3);
});

test("falha no meio da persistencia derruba o lote inteiro (rollback)", async () => {
  const { deps, state } = makeDeps();
  let n = 0;
  deps.withTransaction = async (fn) => {
    state.transactions += 1;
    const client = {
      query: async (sql, params) => {
        n += 1;
        if (n === 2) throw Object.assign(new Error("boom"), { code: "23514" });
        state.txQueries.push({ sql, params });
        return { rows: [dbRow("123")], rowCount: 1 };
      },
    };
    try {
      const out = await fn(client);
      state.commits += 1;
      return out;
    } catch (e) {
      state.rollbacks += 1;
      throw e;
    }
  };

  await assert.rejects(() =>
    publishRewardProducts(
      [
        { tray_product_id: "123", nscredits_price: 5000 },
        { tray_product_id: "456", nscredits_price: 3000 },
      ],
      { adminUserId: 7 },
      deps
    )
  );

  assert.equal(state.commits, 0);
  assert.equal(state.rollbacks, 1);
});

test("publicar NAO dispara nenhuma escrita na Tray", async () => {
  const { deps, state } = makeDeps();

  await publishRewardProducts([{ tray_product_id: "123", nscredits_price: 5000 }], { adminUserId: 7 }, deps);

  // A unica interacao possivel com a Tray e a leitura de catalogo.
  assert.equal(state.catalogCalls.length, 1);
  assert.deepEqual(Object.keys(deps).sort(), ["getCatalogProduct", "query", "withTransaction"]);
});

/* ───────────────────────── PATCH ───────────────────────── */

test("PATCH aceita apenas propriedades da NewStore", async () => {
  const { deps, state } = makeDeps({ rows: [dbRow("123", { nscredits_price: 6000 })] });

  await patchRewardProduct("123", { nscredits_price: 6000, is_published: false, display_order: 5 }, deps);

  const sql = state.queries[0].sql;
  assert.match(sql, /update public\.reward_products/i);
  assert.match(sql, /nscredits_price/);
  assert.match(sql, /is_published/);
  assert.match(sql, /display_order/);
});

test("PATCH rejeita qualquer atributo da Tray", async () => {
  const forbidden = [
    { stock: 1 },
    { available: 1 },
    { name: "x" },
    { reference: "y" },
    { brand: "z" },
    { price: 10 },
    { image_url: "u" },
    { variants_snapshot: [] },
    { tray_product_id: "999" },
    { published_by: 1 },
  ];

  for (const patch of forbidden) {
    const { deps, state } = makeDeps();
    await assert.rejects(
      () => patchRewardProduct("123", patch, deps),
      (e) => {
        assert.equal(e.status, 400);
        assert.equal(e.code, "invalid_patch_field");
        return true;
      },
      `deveria rejeitar ${JSON.stringify(patch)}`
    );
    assert.equal(state.queries.length, 0, "nao pode nem chegar ao banco");
  }
});

test("PATCH vazio e rejeitado", async () => {
  const { deps } = makeDeps();
  await assert.rejects(() => patchRewardProduct("123", {}, deps), (e) => {
    assert.equal(e.code, "empty_patch");
    return true;
  });
});

test("PATCH em produto inexistente devolve 404", async () => {
  const { deps } = makeDeps({ rows: [] });
  await assert.rejects(() => patchRewardProduct("999", { is_published: false }, deps), (e) => {
    assert.equal(e.status, 404);
    assert.equal(e.code, "reward_product_not_found");
    return true;
  });
});

/* ───────────────────────── Sync ───────────────────────── */

test("sync NUNCA sobrescreve nscredits_price, is_published, display_order nem published_by", async () => {
  const { deps, state } = makeDeps({ rows: [dbRow("123")] });

  await syncRewardProducts(["123"], deps);

  const update = state.queries.find((q) => /update public\.reward_products/i.test(q.sql));
  assert.ok(update, "esperado UPDATE de snapshot");
  const setClause = update.sql.split(/\bwhere\b/i)[0];
  for (const protectedCol of ["nscredits_price", "is_published", "display_order", "published_by", "published_at"]) {
    assert.ok(
      !new RegExp(`\\b${protectedCol}\\s*=`).test(setClause),
      `sync nao pode escrever em ${protectedCol}`
    );
  }
});

test("sync consulta a Tray e atualiza o snapshot factual", async () => {
  const { deps, state } = makeDeps({
    rows: [dbRow("123")],
    catalog: { 123: trayProduct("123", { stock: 0, tray_available: 0, availability_text: "Esgotado" }) },
  });

  const out = await syncRewardProducts(["123"], deps);

  assert.equal(state.catalogCalls.length, 1);
  assert.equal(out.results.length, 1);
  assert.equal(out.results[0].tray_product_id, "123");
  assert.equal(out.results[0].ok, true);
});

test("sync devolve resultado individual por produto e nao derruba o lote inteiro", async () => {
  const { deps } = makeDeps({ rows: [dbRow("123")], failCatalogFor: ["456"] });

  const out = await syncRewardProducts(["123", "456"], deps);

  assert.equal(out.results.length, 2);
  assert.equal(out.results[0].ok, true);
  assert.equal(out.results[1].ok, false);
  assert.equal(out.results[1].error, "tray_product_not_found");
});

test("sync sem ids usa uma estrategia limitada sobre os publicados", async () => {
  const { deps, state } = makeDeps({ rows: [dbRow("123"), dbRow("456")] });

  await syncRewardProducts(null, deps);

  const select = state.queries[0];
  assert.match(select.sql, /select .*tray_product_id.* from public\.reward_products/is);
  assert.match(select.sql, /is_published\s*=\s*true/i);
  assert.match(select.sql, /limit/i);
});

/* ───────────────────────── Listagens ───────────────────────── */

test("listagem publica retorna somente publicados e NAO chama a Tray", async () => {
  const { deps, state } = makeDeps({ rows: [dbRow("123")] });

  const out = await listPublishedRewardProducts({}, deps);

  assert.equal(state.catalogCalls.length, 0, "endpoint publico nunca pode chamar a Tray");
  assert.match(state.queries[0].sql, /is_published\s*=\s*true/i);
  assert.equal(out.items.length, 1);
});

test("listagem publica nao expoe preco em reais nem estoque bruto", async () => {
  const { deps } = makeDeps({ rows: [dbRow("123")] });

  const out = await listPublishedRewardProducts({}, deps);
  const item = out.items[0];

  assert.equal(item.nscredits_price, 5000);
  assert.equal(item.tray_price_snapshot, undefined);
  assert.equal(item.tray_price, undefined);
  assert.equal(item.stock, undefined);
  assert.equal(item.published_by, undefined);
  assert.equal(item.is_available, true);
});

test("produto publicado que ficou indisponivel continua no catalogo local, marcado", async () => {
  const { deps } = makeDeps({
    rows: [dbRow("123", { tray_available: 0, stock: 0, availability_text: "Produto esgotado" })],
  });

  const out = await listPublishedRewardProducts({}, deps);

  assert.equal(out.items.length, 1, "nao pode sumir do catalogo");
  assert.equal(out.items[0].is_available, false);
  assert.equal(out.items[0].availability_reason, "unavailable_in_tray");
});

test("listagem publica nao filtra por disponibilidade no SQL", async () => {
  const { deps, state } = makeDeps({ rows: [] });
  await listPublishedRewardProducts({}, deps);

  const where = state.queries[0].sql.split(/\bwhere\b/i)[1] || "";
  assert.ok(!/tray_available/i.test(where), "disponibilidade nao pode virar filtro do catalogo publico");
  assert.ok(!/\bstock\b/i.test(where), "estoque nao pode virar filtro do catalogo publico");
});

/* ───────────────────────── A5: paginacao publica ───────────────────────── */

test("listagem publica pagina com limit/offset e devolve paging.total/pages (A5)", async () => {
  const { deps, state } = makeDeps({
    rows: [dbRow("1", { total_count: 50 }), dbRow("2", { total_count: 50 })],
  });

  const out = await listPublishedRewardProducts({ page: 2, limit: 2 }, deps);

  assert.match(state.queries[0].sql, /limit\s+\$\d/i);
  assert.match(state.queries[0].sql, /offset\s+\$\d/i);
  assert.deepEqual(state.queries[0].params, [2, 2]);
  assert.deepEqual(out.paging, { page: 2, limit: 2, total: 50, pages: 25 });
  assert.equal(out.items.length, 2);
});

test("listagem publica sem parametros usa o default de 24 por pagina", async () => {
  const { deps, state } = makeDeps({ rows: [] });
  const out = await listPublishedRewardProducts({}, deps);

  assert.deepEqual(state.queries[0].params, [24, 0]);
  assert.equal(out.paging.page, 1);
  assert.equal(out.paging.limit, 24);
});

test("listagem publica: limit nunca ultrapassa 100 nem fica <= 0", async () => {
  const { deps, state: overLimit } = makeDeps({ rows: [] });
  await listPublishedRewardProducts({ limit: 500 }, deps);
  assert.equal(overLimit.queries[0].params[0], 100);

  const { deps: deps2, state: zeroLimit } = makeDeps({ rows: [] });
  await listPublishedRewardProducts({ limit: 0, page: -3 }, deps2);
  assert.equal(zeroLimit.queries[0].params[0], 24, "limit invalido cai no default, nunca 0");
  assert.equal(zeroLimit.queries[0].params[1], 0, "page invalido cai na pagina 1 (offset 0)");
});

test("listagem publica: catalogo vazio devolve paging.total=0 e pages=0, sem erro", async () => {
  const { deps } = makeDeps({ rows: [] });
  const out = await listPublishedRewardProducts({}, deps);

  assert.deepEqual(out.items, []);
  assert.deepEqual(out.paging, { page: 1, limit: 24, total: 0, pages: 0 });
});

test("listagem admin de publicados consulta apenas o PostgreSQL", async () => {
  const { deps, state } = makeDeps({ rows: [dbRow("123")] });

  const out = await listAdminRewardProducts({ page: 1, limit: 20 }, deps);

  assert.equal(state.catalogCalls.length, 0, "deve funcionar mesmo com a Tray fora do ar");
  assert.equal(out.items[0].tray_product_id, "123");
  assert.equal(out.items[0].nscredits_price, 5000);
  assert.equal(out.items[0].tray_price_snapshot, 4599);
});

test("getRewardsByTrayIds devolve um mapa por tray_product_id", async () => {
  const { deps, state } = makeDeps({ rows: [dbRow("123")] });

  const map = await getRewardsByTrayIds(["123", "456"], deps);

  assert.equal(state.catalogCalls.length, 0);
  assert.equal(map["123"].is_published, true);
  assert.equal(map["123"].nscredits_price, 5000);
  assert.equal(map["456"], undefined);
});

test("estatisticas do catalogo saem so do PostgreSQL", async () => {
  const { deps, state } = makeDeps({
    rows: [{ total: "3", published: "2", last_synced_at: new Date("2026-08-14T06:53:32.816Z") }],
  });

  const stats = await getRewardCatalogStats(deps);

  assert.equal(state.catalogCalls.length, 0, "status nao pode depender da Tray");
  assert.deepEqual(stats, {
    total: 3,
    published: 2,
    unpublished: 1,
    last_synced_at: "2026-08-14T06:53:32.816Z",
  });
});

test("catalogo vazio devolve zeros, nao null", async () => {
  const { deps } = makeDeps({ rows: [{ total: "0", published: "0", last_synced_at: null }] });
  assert.deepEqual(await getRewardCatalogStats(deps), {
    total: 0,
    published: 0,
    unpublished: 0,
    last_synced_at: null,
  });
});

test("getRewardsByTrayIds com lista vazia nao consulta o banco", async () => {
  const { deps, state } = makeDeps();
  const map = await getRewardsByTrayIds([], deps);
  assert.deepEqual(map, {});
  assert.equal(state.queries.length, 0);
});
