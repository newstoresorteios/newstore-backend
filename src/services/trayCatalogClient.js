// src/services/trayCatalogClient.js
//
// Cliente HTTP da Tray para CATALOGO — SOMENTE LEITURA (GET).
//
// Esta camada NAO conhece SQL e NAO conhece reward_products.
// Ela so sabe: pegar token (via OAuth ja existente em services/tray.js),
// resolver o api_address autorizado, montar querystring, aplicar timeout,
// e normalizar erro HTTP.
//
// Nenhuma funcao deste arquivo escreve na Tray. O unico metodo HTTP
// permitido e GET — ver assertReadOnlyMethod().

export const TRAY_MAX_LIMIT = 50;
export const TRAY_DEFAULT_LIMIT = 20;
/** A Tray recusa `sort` sem direcao (400). Ordenacao padrao do catalogo admin. */
export const TRAY_DEFAULT_SORT = "name_asc";
const DEFAULT_TIMEOUT_MS = Number(process.env.TRAY_CATALOG_TIMEOUT_MS || 12000);

/** Metodos HTTP permitidos nesta camada. Catalogo Tray e read-only. */
const READ_ONLY_METHODS = new Set(["GET"]);

/**
 * Allow-list explicita de mutacoes Tray autorizadas, por operacao de
 * dominio. NENHUMA operacao esta autorizada hoje — ver relatorio da Fase E
 * (resgate real): a criacao de pedido depende de uma decisao de produto
 * sobre payment_type/payment_method que ainda nao existe, e nao adivinhamos
 * aqui. Quando uma mutacao for autorizada, ela entra nomeada nesta lista
 * (ex.: { operation: "tray_order_create", methods: ["POST"] }) — nunca como
 * uma liberacao geral de metodo.
 */
const ALLOWED_MUTATIONS = new Map([
  // "tray_order_create" -> new Set(["POST"])  // Fase E, quando desbloqueada.
]);

export class TrayCatalogError extends Error {
  constructor(code, { status = 502, retryAfterSeconds = null, publicDetails = null } = {}) {
    super(code);
    this.name = "TrayCatalogError";
    this.code = code;
    this.status = status;
    this.retryAfterSeconds = retryAfterSeconds;
    this.publicDetails = publicDetails;
  }
}

/**
 * Guarda dura: o catalogo (produtos/variantes/marcas/health) so pode
 * emitir GET. Qualquer tentativa de mutacao aqui falha antes da rede.
 */
export function assertReadOnlyMethod(method) {
  const m = String(method || "").toUpperCase();
  if (!READ_ONLY_METHODS.has(m)) {
    throw new TrayCatalogError("tray_catalog_is_read_only", { status: 500 });
  }
  return m;
}

/**
 * Guarda para qualquer mutacao FORA do catalogo (carrinho, frete, pedido,
 * sincronizacao de cupom). So passa se `operation` estiver explicitamente
 * na allow-list PARA aquele metodo. Hoje a lista esta vazia: toda chamada
 * aqui falha, de proposito, ate a Fase E decidir o contrato de pagamento.
 */
export function assertAllowedTrayMutation(operation, method) {
  const m = String(method || "").toUpperCase();
  const allowedMethods = ALLOWED_MUTATIONS.get(String(operation || ""));
  if (!allowedMethods || !allowedMethods.has(m)) {
    throw new TrayCatalogError("tray_mutation_not_allowed", {
      status: 500,
      publicDetails: { operation: String(operation || ""), method: m },
    });
  }
  return m;
}

/** Remove host e access_token — o que sobra pode ir para log com seguranca. */
export function sanitizeTrayUrl(url) {
  const raw = String(url || "");
  const withoutHost = raw.replace(/^https?:\/\/[^/]+/i, "");
  const withoutBase = withoutHost.replace(/^\/web_api/i, "");
  return withoutBase.replace(/(access_token=)[^&]*/gi, "$1***");
}

async function defaultDeps() {
  // Import dinamico: mantem o grafo de modulos (pg, dotenv) fora dos testes unitarios.
  const [{ trayToken }, { getTrayApiBase }] = await Promise.all([
    import("./tray.js"),
    import("./trayConfig.js"),
  ]);
  return {
    fetchImpl: (url, options) => fetch(url, options),
    getToken: (opts) => trayToken(opts),
    getApiBase: () => getTrayApiBase(),
  };
}

async function readBody(response) {
  const contentType = String(response.headers?.get?.("content-type") || "").toLowerCase();
  if (contentType.includes("application/json")) {
    return await response.json().catch(() => null);
  }
  const text = await response.text().catch(() => "");
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function parseRetryAfter(response) {
  const raw = response.headers?.get?.("retry-after");
  if (!raw) return null;
  const seconds = Number(String(raw).trim());
  return Number.isFinite(seconds) && seconds >= 0 ? seconds : null;
}

function errorForStatus(response) {
  const status = Number(response.status);
  if (status === 429) {
    return new TrayCatalogError("tray_rate_limited", {
      status: 429,
      retryAfterSeconds: parseRetryAfter(response),
    });
  }
  if (status === 401 || status === 403) {
    return new TrayCatalogError("tray_auth_failed", { status: 502 });
  }
  if (status === 404) {
    return new TrayCatalogError("tray_product_not_found", { status: 404 });
  }
  if (status >= 500) {
    return new TrayCatalogError("tray_unavailable", { status: 503 });
  }
  return new TrayCatalogError("tray_request_failed", {
    status: 502,
    publicDetails: { tray_status: status },
  });
}

/**
 * GET cru no catalogo da Tray.
 * @returns {Promise<any>} corpo JSON da resposta
 */
export async function trayCatalogGet(path, params = {}, options = {}) {
  const method = assertReadOnlyMethod("GET");
  const deps = options.deps || (await defaultDeps());
  const timeoutMs = Number(options.timeoutMs) > 0 ? Number(options.timeoutMs) : DEFAULT_TIMEOUT_MS;

  const apiBase = String(await deps.getApiBase()).replace(/\/+$/, "");
  const token = await deps.getToken({ signal: options.signal });

  const search = new URLSearchParams();
  search.set("access_token", String(token));
  for (const [key, value] of Object.entries(params)) {
    if (value === undefined || value === null || String(value).trim() === "") continue;
    search.set(key, String(value));
  }

  const url = `${apiBase}${path}?${search.toString()}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  if (options.signal) {
    if (options.signal.aborted) controller.abort();
    else options.signal.addEventListener("abort", () => controller.abort(), { once: true });
  }

  let response;
  try {
    response = await deps.fetchImpl(url, { method, signal: controller.signal });
  } catch (e) {
    if (e?.name === "AbortError") {
      throw new TrayCatalogError("tray_timeout", { status: 503 });
    }
    throw new TrayCatalogError("tray_unreachable", { status: 503 });
  } finally {
    clearTimeout(timer);
  }

  if (!response?.ok) {
    const error = errorForStatus(response || { status: 0 });
    console.warn("[tray.catalog] request failed", {
      path: sanitizeTrayUrl(url),
      tray_status: response?.status ?? null,
      code: error.code,
    });
    throw error;
  }

  return await readBody(response);
}

/** Desembrulha `{ Products: [{ Product: {...} }] }` e formatos equivalentes. */
function unwrapCollection(body, pluralKey, singularKey) {
  if (!body || typeof body !== "object") return null;
  const list = body[pluralKey];
  if (!Array.isArray(list)) return null;
  return list.map((entry) => {
    if (entry && typeof entry === "object" && entry[singularKey] && typeof entry[singularKey] === "object") {
      return entry[singularKey];
    }
    return entry;
  });
}

function normalizePaging(body, requestedPage, requestedLimit) {
  const paging = body?.paging || {};
  const total = Number(paging.total);
  const limit = Number(paging.limit);
  const page = Number(paging.page);
  const maxLimit = Number(paging.maxLimit);
  return {
    page: Number.isFinite(page) && page > 0 ? page : requestedPage,
    limit: Number.isFinite(limit) && limit > 0 ? limit : requestedLimit,
    total: Number.isFinite(total) && total >= 0 ? total : null,
    maxLimit: Number.isFinite(maxLimit) && maxLimit > 0 ? maxLimit : TRAY_MAX_LIMIT,
  };
}

export function clampTrayLimit(limit) {
  const n = Number(limit);
  if (!Number.isFinite(n) || n <= 0) return TRAY_DEFAULT_LIMIT;
  return Math.min(Math.trunc(n), TRAY_MAX_LIMIT);
}

/** Campos de busca suportados pelo contrato de filtro da Tray. */
export const TRAY_SEARCH_FIELDS = new Set(["name", "reference", "id", "brand"]);

/**
 * Campos em que a Tray faz correspondencia PARCIAL quando o termo vem com `%`.
 * Validado na loja real: `name=Bone` -> 0 resultados; `name=%Bone%` -> encontra.
 * `id` e `brand` sao correspondencia exata e nao aceitam curinga.
 */
const TRAY_WILDCARD_FIELDS = new Set(["name", "reference"]);

function withWildcard(term) {
  return term.includes("%") ? term : `%${term}%`;
}

/**
 * Mapeia a busca livre do admin para UM filtro da Tray.
 * Uma unica chamada por busca — nao dispara N requests por campo.
 */
export function resolveSearchFilter(q, qField) {
  const term = String(q ?? "").trim();
  if (!term) return null;

  const field = String(qField || "auto").trim().toLowerCase();
  if (TRAY_SEARCH_FIELDS.has(field)) {
    return { [field]: TRAY_WILDCARD_FIELDS.has(field) ? withWildcard(term) : term };
  }

  // auto: so digitos parece codigo de produto Tray; caso contrario, nome parcial.
  if (/^\d+$/.test(term)) return { id: term };
  return { name: withWildcard(term) };
}

/** Traduz o filtro de disponibilidade para o contrato 0/1 da Tray. */
export function resolveAvailableFilter(available) {
  if (available === undefined || available === null || available === "") return null;
  if (available === true || available === 1 || available === "1" || available === "true") return "1";
  if (available === false || available === 0 || available === "0" || available === "false") return "0";
  return null;
}

export async function fetchTrayProducts(
  { page = 1, limit = TRAY_DEFAULT_LIMIT, q = "", qField = "auto", available = null, brand = "" } = {},
  options = {}
) {
  const safePage = Number.isFinite(Number(page)) && Number(page) > 0 ? Math.trunc(Number(page)) : 1;
  const safeLimit = clampTrayLimit(limit);

  // A Tray exige a DIRECAO na ordenacao: `sort=name` responde 400
  // ("Parametro direção da ordenação invalido."). Validado na loja real.
  const params = { page: safePage, limit: safeLimit, sort: TRAY_DEFAULT_SORT };

  const searchFilter = resolveSearchFilter(q, qField);
  if (searchFilter) Object.assign(params, searchFilter);

  const availableFilter = resolveAvailableFilter(available);
  if (availableFilter !== null) params.available = availableFilter;

  const brandTerm = String(brand || "").trim();
  if (brandTerm && !params.brand) params.brand = brandTerm;

  const body = await trayCatalogGet("/products", params, options);
  const rawProducts = unwrapCollection(body, "Products", "Product");
  if (!rawProducts) {
    throw new TrayCatalogError("tray_invalid_response", { status: 502 });
  }

  return { rawProducts, paging: normalizePaging(body, safePage, safeLimit) };
}

export async function fetchTrayProduct(trayProductId, options = {}) {
  const id = String(trayProductId ?? "").trim();
  if (!id) throw new TrayCatalogError("tray_product_id_missing", { status: 400 });

  const body = await trayCatalogGet(`/products/${encodeURIComponent(id)}`, {}, options);
  const raw = body?.Product && typeof body.Product === "object" ? body.Product : null;
  if (!raw) {
    throw new TrayCatalogError("tray_invalid_response", { status: 502 });
  }
  return raw;
}

/**
 * Teto de seguranca contra loop infinito caso a Tray nunca sinalize o fim
 * (nem `paging.total` nem uma pagina curta). Nao e um limite de produto: um
 * produto real jamais precisa de milhares de variantes.
 */
const TRAY_VARIANTS_MAX_PAGES = 200;

export async function fetchTrayVariants(trayProductId, options = {}) {
  const id = String(trayProductId ?? "").trim();
  if (!id) throw new TrayCatalogError("tray_product_id_missing", { status: 400 });

  const limit = TRAY_MAX_LIMIT;
  const variants = [];

  for (let page = 1; page <= TRAY_VARIANTS_MAX_PAGES; page += 1) {
    const body = await trayCatalogGet(
      "/variants",
      { product_id: id, limit, page },
      options
    );
    const raw = unwrapCollection(body, "Variants", "Variant");
    if (!raw) {
      throw new TrayCatalogError("tray_invalid_response", { status: 502 });
    }
    variants.push(...raw);

    // Fim factual: a Tray so devolve menos que `limit` itens na ultima pagina
    // (ou zero, se a paginacao pedida ja passou do fim). Isso vale mesmo sem
    // `paging.total` no corpo.
    if (raw.length < limit) return variants;

    // Quando `paging.total` esta presente e ja foi atingido, nao vale a pena
    // pedir mais uma pagina so para receber uma lista vazia de confirmacao.
    const { total } = normalizePaging(body, page, limit);
    if (total !== null && variants.length >= total) return variants;
  }

  throw new TrayCatalogError("tray_variants_pagination_exceeded", {
    status: 502,
    publicDetails: { pages: TRAY_VARIANTS_MAX_PAGES },
  });
}

export async function fetchTrayBrands(options = {}) {
  const body = await trayCatalogGet("/brands", { limit: TRAY_MAX_LIMIT, page: 1 }, options);
  const raw = unwrapCollection(body, "Brands", "Brand");
  if (!raw) {
    throw new TrayCatalogError("tray_invalid_response", { status: 502 });
  }
  // A Tray devolve `{ Brand: { id, slug, brand } }` — o nome esta em `brand`.
  return raw
    .map((b) => String(b?.brand ?? b?.name ?? "").trim())
    .filter(Boolean);
}
