// tests/trayCatalogClient.test.js
// Cliente Tray de catalogo: SOMENTE LEITURA.
import test from "node:test";
import assert from "node:assert/strict";

import {
  TRAY_MAX_LIMIT,
  TRAY_DEFAULT_SORT,
  TrayCatalogError,
  sanitizeTrayUrl,
  fetchTrayProducts,
  fetchTrayProduct,
  fetchTrayVariants,
  fetchTrayBrands,
} from "../src/services/trayCatalogClient.js";

const API_BASE = "https://www.exemplo-loja.com.br/web_api";
const TOKEN = "super-secret-access-token-abcdef";

function makeResponse({ status = 200, body = {}, headers = {} } = {}) {
  const lower = {};
  for (const [k, v] of Object.entries(headers)) lower[k.toLowerCase()] = String(v);
  if (!lower["content-type"]) lower["content-type"] = "application/json";
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: (name) => lower[String(name).toLowerCase()] ?? null },
    json: async () => body,
    text: async () => (typeof body === "string" ? body : JSON.stringify(body)),
  };
}

function makeDeps(handler) {
  const calls = [];
  return {
    calls,
    deps: {
      getToken: async () => TOKEN,
      getApiBase: async () => API_BASE,
      fetchImpl: async (url, options) => {
        calls.push({ url: String(url), options: options || {} });
        return handler(String(url), options || {}, calls.length);
      },
    },
  };
}

const PRODUCT_LIST_BODY = {
  Products: [
    {
      Product: {
        id: 123,
        name: "Citizen Promaster",
        reference: "NY0129",
        brand: "Citizen",
        available: 1,
        available_in_store: 1,
        stock: 3,
        price: "4599.00",
        has_variation: 0,
      },
    },
  ],
  paging: { total: 100, page: 1, offset: 0, limit: 20, maxLimit: 50 },
};

test("usa o api_address autorizado como base da URL", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ body: PRODUCT_LIST_BODY }));
  await fetchTrayProducts({ page: 1, limit: 20 }, { deps });

  assert.equal(calls.length, 1);
  assert.ok(
    calls[0].url.startsWith(`${API_BASE}/products`),
    `URL deveria comecar com o api_address autorizado, veio: ${calls[0].url}`
  );
});

test("todo request de catalogo usa method GET", async () => {
  const { calls, deps } = makeDeps((url) => {
    if (url.includes("/variants")) return makeResponse({ body: { Variants: [] } });
    if (/\/products\/\d+/.test(url)) return makeResponse({ body: { Product: { id: 123 } } });
    if (url.includes("/brands")) return makeResponse({ body: { Brands: [] } });
    return makeResponse({ body: PRODUCT_LIST_BODY });
  });

  await fetchTrayProducts({}, { deps });
  await fetchTrayProduct("123", { deps });
  await fetchTrayVariants("123", { deps });
  await fetchTrayBrands({ deps });

  assert.equal(calls.length, 4);
  for (const c of calls) {
    assert.equal(c.options.method, "GET", `metodo diferente de GET em ${sanitizeTrayUrl(c.url)}`);
  }
});

test("a ordenacao enviada carrega direcao (a Tray recusa sort sem direcao)", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ body: PRODUCT_LIST_BODY }));
  await fetchTrayProducts({}, { deps });

  const sort = new URL(calls[0].url).searchParams.get("sort");
  assert.equal(sort, TRAY_DEFAULT_SORT);
  assert.match(sort, /_(asc|desc)$/, "sort sem direcao devolve 400 na Tray");
});

test("limit nunca ultrapassa o maxLimit da Tray", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ body: PRODUCT_LIST_BODY }));
  await fetchTrayProducts({ page: 2, limit: 500 }, { deps });

  const u = new URL(calls[0].url);
  assert.equal(u.searchParams.get("limit"), String(TRAY_MAX_LIMIT));
  assert.equal(u.searchParams.get("page"), "2");
});

test("busca numerica vira filtro id e busca textual vira filtro name parcial", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ body: PRODUCT_LIST_BODY }));

  await fetchTrayProducts({ q: "123" }, { deps });
  await fetchTrayProducts({ q: "Citizen Promaster" }, { deps });

  const first = new URL(calls[0].url).searchParams;
  assert.equal(first.get("id"), "123");
  assert.equal(first.get("name"), null);

  const second = new URL(calls[1].url).searchParams;
  // A Tray so faz correspondencia parcial com curinga: `name=Bone` devolve 0.
  assert.equal(second.get("name"), "%Citizen Promaster%");
  assert.equal(second.get("id"), null);
});

test("nome e referencia usam curinga; id e marca sao exatos", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ body: PRODUCT_LIST_BODY }));

  await fetchTrayProducts({ q: "NY0129", qField: "reference" }, { deps });
  await fetchTrayProducts({ q: "Citizen", qField: "brand" }, { deps });
  await fetchTrayProducts({ q: "14570", qField: "id" }, { deps });
  await fetchTrayProducts({ q: "Promaster", qField: "name" }, { deps });

  assert.equal(new URL(calls[0].url).searchParams.get("reference"), "%NY0129%");
  assert.equal(new URL(calls[1].url).searchParams.get("brand"), "Citizen");
  assert.equal(new URL(calls[2].url).searchParams.get("id"), "14570");
  assert.equal(new URL(calls[3].url).searchParams.get("name"), "%Promaster%");
});

test("curinga informado pelo admin e respeitado sem duplicar", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ body: PRODUCT_LIST_BODY }));
  await fetchTrayProducts({ q: "Bone%", qField: "name" }, { deps });
  assert.equal(new URL(calls[0].url).searchParams.get("name"), "Bone%");
});

test("filtros de disponibilidade usam o contrato da Tray (0/1)", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ body: PRODUCT_LIST_BODY }));

  await fetchTrayProducts({ available: true }, { deps });
  await fetchTrayProducts({ available: false }, { deps });
  await fetchTrayProducts({}, { deps });

  assert.equal(new URL(calls[0].url).searchParams.get("available"), "1");
  assert.equal(new URL(calls[1].url).searchParams.get("available"), "0");
  assert.equal(new URL(calls[2].url).searchParams.get("available"), null);
});

test("paginacao da Tray e devolvida normalizada", async () => {
  const { deps } = makeDeps(() => makeResponse({ body: PRODUCT_LIST_BODY }));
  const out = await fetchTrayProducts({ page: 1, limit: 20 }, { deps });

  assert.deepEqual(out.paging, { page: 1, limit: 20, total: 100, maxLimit: 50 });
  assert.equal(out.rawProducts.length, 1);
  assert.equal(out.rawProducts[0].id, 123);
});

test("429 vira erro 429 com retryAfterSeconds", async () => {
  const { deps } = makeDeps(() =>
    makeResponse({ status: 429, body: { message: "rate limit" }, headers: { "Retry-After": "7" } })
  );

  await assert.rejects(
    () => fetchTrayProducts({}, { deps }),
    (e) => {
      assert.ok(e instanceof TrayCatalogError);
      assert.equal(e.status, 429);
      assert.equal(e.code, "tray_rate_limited");
      assert.equal(e.retryAfterSeconds, 7);
      return true;
    }
  );
});

test("nao faz retry agressivo em 429", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 429, body: {} }));
  await assert.rejects(() => fetchTrayProducts({}, { deps }));
  assert.equal(calls.length, 1);
});

test("timeout vira 503 tray_timeout", async () => {
  const deps = {
    getToken: async () => TOKEN,
    getApiBase: async () => API_BASE,
    fetchImpl: (_url, options) =>
      new Promise((_resolve, reject) => {
        options.signal.addEventListener("abort", () => {
          const e = new Error("aborted");
          e.name = "AbortError";
          reject(e);
        });
      }),
  };

  await assert.rejects(
    () => fetchTrayProducts({}, { deps, timeoutMs: 20 }),
    (e) => {
      assert.equal(e.status, 503);
      assert.equal(e.code, "tray_timeout");
      return true;
    }
  );
});

test("erro de rede vira 503 tray_unreachable", async () => {
  const { deps } = makeDeps(() => {
    throw new Error("ECONNRESET");
  });

  await assert.rejects(
    () => fetchTrayProducts({}, { deps }),
    (e) => {
      assert.equal(e.status, 503);
      assert.equal(e.code, "tray_unreachable");
      return true;
    }
  );
});

test("produto inexistente vira 404", async () => {
  const { deps } = makeDeps(() => makeResponse({ status: 404, body: { message: "not found" } }));

  await assert.rejects(
    () => fetchTrayProduct("999999", { deps }),
    (e) => {
      assert.equal(e.status, 404);
      assert.equal(e.code, "tray_product_not_found");
      return true;
    }
  );
});

test("5xx da Tray vira 503", async () => {
  const { deps } = makeDeps(() => makeResponse({ status: 502, body: "<html>bad gateway</html>", headers: { "content-type": "text/html" } }));

  await assert.rejects(
    () => fetchTrayProducts({}, { deps }),
    (e) => {
      assert.equal(e.status, 503);
      assert.equal(e.code, "tray_unavailable");
      return true;
    }
  );
});

test("401 da Tray vira 502 (problema de integracao, nao do usuario NewStore)", async () => {
  const { deps } = makeDeps(() => makeResponse({ status: 401, body: { error_code: 1099 } }));

  await assert.rejects(
    () => fetchTrayProducts({}, { deps }),
    (e) => {
      assert.equal(e.status, 502);
      assert.equal(e.code, "tray_auth_failed");
      return true;
    }
  );
});

test("payload inesperado da Tray vira 502 tray_invalid_response", async () => {
  const { deps } = makeDeps(() => makeResponse({ body: { unexpected: true } }));

  await assert.rejects(
    () => fetchTrayProducts({}, { deps }),
    (e) => {
      assert.equal(e.status, 502);
      assert.equal(e.code, "tray_invalid_response");
      return true;
    }
  );
});

test("o access_token nunca aparece no erro nem na URL sanitizada", async () => {
  const { calls, deps } = makeDeps(() => makeResponse({ status: 500, body: { message: "boom" } }));

  await assert.rejects(
    () => fetchTrayProducts({}, { deps }),
    (e) => {
      const dump = `${e.message} ${e.code} ${JSON.stringify(e.publicDetails || {})} ${e.stack || ""}`;
      assert.ok(!dump.includes(TOKEN), "erro nao pode conter o access_token");
      assert.ok(!dump.includes(API_BASE), "erro nao precisa expor o host da loja");
      return true;
    }
  );

  const sanitized = sanitizeTrayUrl(calls[0].url);
  assert.ok(!sanitized.includes(TOKEN), "URL sanitizada nao pode conter o token");
  assert.ok(sanitized.startsWith("/products"), `esperado apenas o path, veio: ${sanitized}`);
});

test("variantes sao consultadas por product_id", async () => {
  const { calls, deps } = makeDeps(() =>
    makeResponse({
      body: {
        Variants: [{ Variant: { id: 10, product_id: 123, stock: 2, reference: "V-40", price: "199.00", available: 1 } }],
        paging: { total: 1, page: 1, limit: 50, maxLimit: 50 },
      },
    })
  );

  const variants = await fetchTrayVariants("123", { deps });

  assert.equal(new URL(calls[0].url).searchParams.get("product_id"), "123");
  assert.equal(variants.length, 1);
  assert.equal(variants[0].id, 10);
});

test("marcas sao lidas do endpoint de brands (o nome vem no campo `brand`)", async () => {
  // Payload factual da loja: { Brand: { id, slug, brand } } — nao existe `name`.
  const { calls, deps } = makeDeps(() =>
    makeResponse({
      body: {
        Brands: [
          { Brand: { id: "1", slug: "citizen", brand: "Citizen" } },
          { Brand: { id: "2", slug: "orient", brand: "Orient" } },
        ],
      },
    })
  );

  const brands = await fetchTrayBrands({ deps });

  assert.ok(calls[0].url.startsWith(`${API_BASE}/brands`));
  assert.deepEqual(brands, ["Citizen", "Orient"]);
});
