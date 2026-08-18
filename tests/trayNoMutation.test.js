// tests/trayNoMutation.test.js
//
// PROVA DE NAO-MUTACAO.
//
// A Loja de Premios e OBSERVADORA do catalogo Tray. Publicar, despublicar e
// sincronizar sao operacoes locais da NewStore. Este arquivo existe para
// falhar ruidosamente se alguem, algum dia, introduzir escrita na Tray.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { assertReadOnlyMethod, assertAllowedTrayMutation, TrayCatalogError } from "../src/services/trayCatalogClient.js";
import { getTrayCatalogProduct, listTrayCatalog } from "../src/services/trayCatalog.js";
import { publishRewardProducts, syncRewardProducts, patchRewardProduct } from "../src/services/rewardStore.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const SRC = join(__dirname, "..", "src");

/** Arquivos do modulo Loja de Premios criados/alterados por este escopo. */
const LOJA_MODULE_FILES = [
  join(SRC, "services", "trayCatalogClient.js"),
  join(SRC, "services", "trayCatalog.js"),
  join(SRC, "services", "rewardStore.js"),
  join(SRC, "routes", "admin_store.js"),
  join(SRC, "routes", "store.js"),
];

const RAW_PRODUCT = {
  id: 123,
  name: "Citizen Promaster",
  reference: "NY0129",
  brand: "Citizen",
  available: 1,
  available_in_store: 1,
  stock: 3,
  availability: "Disponivel",
  has_variation: 1,
  price: "4599.00",
  modified: "2026-08-10 14:32:11",
  ProductImage: [{ https: "https://cdn/1.jpg" }],
  url: { https: "https://loja/p/123" },
};

/** fetch espiao: registra tudo e recusa qualquer metodo diferente de GET. */
function makeSpy() {
  const requests = [];
  const deps = {
    getToken: async () => "token-de-teste",
    getApiBase: async () => "https://www.exemplo-loja.com.br/web_api",
    fetchImpl: async (url, options = {}) => {
      const method = String(options.method || "GET").toUpperCase();
      requests.push({ url: String(url), method });

      if (method !== "GET") {
        throw new Error(`MUTACAO PROIBIDA NA TRAY: ${method} ${url}`);
      }

      const u = String(url);
      const singleId = u.match(/\/products\/(\d+)/)?.[1] || null;
      const body = u.includes("/variants")
        ? { Variants: [{ Variant: { id: 1, product_id: 123, stock: 2, available: 1, reference: "TAM-40" } }] }
        : singleId
        ? { Product: { ...RAW_PRODUCT, id: Number(singleId) } }
        : { Products: [{ Product: RAW_PRODUCT }], paging: { total: 1, page: 1, limit: 20, maxLimit: 50 } };

      return {
        ok: true,
        status: 200,
        headers: { get: (n) => (String(n).toLowerCase() === "content-type" ? "application/json" : null) },
        json: async () => body,
        text: async () => JSON.stringify(body),
      };
    },
  };
  return { requests, deps };
}

function makeDbStub() {
  const queries = [];
  const row = {
    tray_product_id: "123",
    nscredits_price: 5000,
    is_published: true,
    display_order: 0,
    images_snapshot: [],
    variants_snapshot: [],
    has_variation: true,
    tray_available: 1,
    tray_available_in_store: 1,
    stock: 3,
  };
  return {
    queries,
    query: async (sql, params) => {
      queries.push({ sql, params });
      return { rows: [row], rowCount: 1 };
    },
    withTransaction: async (fn) => fn({
      query: async (sql, params) => {
        queries.push({ sql, params });
        return { rows: [row], rowCount: 1 };
      },
    }),
  };
}

test("a camada de catalogo recusa qualquer metodo que nao seja GET", () => {
  for (const method of ["POST", "PUT", "DELETE", "PATCH", "post", "put", "delete"]) {
    assert.throws(
      () => assertReadOnlyMethod(method),
      (e) => {
        assert.ok(e instanceof TrayCatalogError);
        assert.equal(e.code, "tray_catalog_is_read_only");
        return true;
      },
      `${method} deveria ser recusado`
    );
  }
  assert.equal(assertReadOnlyMethod("GET"), "GET");
});

test("allow-list de mutacoes Tray (item 16, Fase 5): NADA esta autorizado hoje", () => {
  // A guarda existe para o dia em que a Fase E for desbloqueada, mas ate la
  // qualquer operacao — inclusive nomes plausiveis — tem que ser recusada.
  for (const operation of ["tray_order_create", "tray_cart_create", "tray_shipping_create", "unknown_operation", ""]) {
    for (const method of ["POST", "PUT", "PATCH", "DELETE", "GET"]) {
      assert.throws(
        () => assertAllowedTrayMutation(operation, method),
        (e) => e instanceof TrayCatalogError && e.code === "tray_mutation_not_allowed",
        `${operation || "(vazio)"} ${method} deveria ser recusado`
      );
    }
  }
});

test("listar o catalogo Tray so emite GET", async () => {
  const { requests, deps } = makeSpy();
  await listTrayCatalog({ page: 1, limit: 20 }, { deps });

  assert.ok(requests.length > 0);
  for (const r of requests) assert.equal(r.method, "GET");
});

test("PUBLICAR nao emite nenhuma escrita na Tray", async () => {
  const { requests, deps } = makeSpy();
  const db = makeDbStub();

  await publishRewardProducts(
    [
      { tray_product_id: "123", nscredits_price: 5000 },
      { tray_product_id: "456", nscredits_price: 3000 },
    ],
    { adminUserId: 7 },
    {
      query: db.query,
      withTransaction: db.withTransaction,
      getCatalogProduct: (id, options) => getTrayCatalogProduct(id, { ...options, deps }),
    }
  );

  assert.ok(requests.length > 0, "a publicacao precisa reconsultar a Tray");
  for (const r of requests) {
    assert.equal(r.method, "GET", `metodo proibido detectado: ${r.method} ${r.url}`);
  }
});

test("SINCRONIZAR nao emite nenhuma escrita na Tray", async () => {
  const { requests, deps } = makeSpy();
  const db = makeDbStub();

  await syncRewardProducts(["123"], {
    query: db.query,
    withTransaction: db.withTransaction,
    getCatalogProduct: (id, options) => getTrayCatalogProduct(id, { ...options, deps }),
  });

  assert.ok(requests.length > 0);
  for (const r of requests) assert.equal(r.method, "GET");
});

test("DESPUBLICAR nao toca na Tray de forma alguma", async () => {
  const { requests, deps } = makeSpy();
  const db = makeDbStub();

  await patchRewardProduct(
    "123",
    { is_published: false },
    {
      query: db.query,
      withTransaction: db.withTransaction,
      getCatalogProduct: (id, options) => getTrayCatalogProduct(id, { ...options, deps }),
    }
  );

  assert.equal(requests.length, 0, "despublicar e operacao 100% local");
});

test("nenhuma URL de mutacao de produto/variacao/estoque e alcancada em nenhum fluxo", async () => {
  const { requests, deps } = makeSpy();
  const db = makeDbStub();
  const storeDeps = {
    query: db.query,
    withTransaction: db.withTransaction,
    getCatalogProduct: (id, options) => getTrayCatalogProduct(id, { ...options, deps }),
  };

  await listTrayCatalog({ page: 1, limit: 20 }, { deps });
  await publishRewardProducts([{ tray_product_id: "123", nscredits_price: 5000 }], { adminUserId: 1 }, storeDeps);
  await syncRewardProducts(["123"], storeDeps);
  await patchRewardProduct("123", { nscredits_price: 6000 }, storeDeps);

  const mutations = requests.filter((r) => r.method !== "GET");
  assert.deepEqual(mutations, [], `mutacoes detectadas: ${JSON.stringify(mutations)}`);

  // E nenhum endpoint de escrita conhecido da Tray foi tocado.
  for (const r of requests) {
    assert.ok(!/\/stocks?\b/i.test(r.url), `endpoint de estoque alcancado: ${r.url}`);
    assert.ok(!/\/products\/\d+\/(stock|price)/i.test(r.url), `endpoint de mutacao alcancado: ${r.url}`);
  }
});

test("o codigo-fonte do modulo Loja de Premios nao contem escrita na Tray", () => {
  const forbidden = [
    /method\s*:\s*["'`]\s*POST/i,
    /method\s*:\s*["'`]\s*PUT/i,
    /method\s*:\s*["'`]\s*DELETE/i,
    /method\s*:\s*["'`]\s*PATCH/i,
    /trayCreateCoupon|trayUpdateCouponById|trayDeleteCoupon/,
  ];

  for (const file of LOJA_MODULE_FILES) {
    const source = readFileSync(file, "utf8");
    for (const pattern of forbidden) {
      assert.ok(
        !pattern.test(source),
        `${file} contem padrao de mutacao proibido: ${pattern}`
      );
    }
  }
});

test("o modulo Loja de Premios nao importa o servico de escrita de cupons da Tray", () => {
  for (const file of LOJA_MODULE_FILES) {
    const source = readFileSync(file, "utf8");
    assert.ok(
      !/from\s+["'].*services\/trayCoupon/i.test(source),
      `${file} nao deve importar servicos de cupom (escrita na Tray)`
    );
  }
});
