// src/services/rewardStore.js
//
// Regras da Loja de Premios (reward_products).
//
// O que e propriedade EXCLUSIVA da NewStore:
//   nscredits_price, is_published, display_order, published_at, published_by
// O que e SNAPSHOT factual da Tray (somente leitura):
//   name, reference, brand, imagens, stock, available, available_in_store,
//   availability, has_variation, variacoes, preco Tray, modified
//
// Publicar/despublicar/sincronizar aqui NAO escreve nada na Tray.
// A unica interacao com a Tray e a leitura de catalogo (deps.getCatalogProduct).

import { getPool, query as dbQuery } from "../db.js";
import { getTrayCatalogProduct } from "./trayCatalog.js";
import { derivePresentation } from "./trayCatalog.js";

/** Teto do lote de publicacao — alinhado ao maxLimit de paginacao da Tray. */
export const MAX_PUBLISH_BATCH = 50;

/** Teto de produtos sincronizados quando o admin nao informa ids. */
export const MAX_SYNC_WITHOUT_IDS = 50;

/** Unicas colunas que o PATCH administrativo pode tocar. */
export const PATCHABLE_FIELDS = new Set(["nscredits_price", "is_published", "display_order"]);

export class RewardStoreError extends Error {
  constructor(code, { status = 400, details = null } = {}) {
    super(code);
    this.name = "RewardStoreError";
    this.code = code;
    this.status = status;
    this.details = details;
  }
}

/* ─────────────────────────── Validacao ─────────────────────────── */

/**
 * NSCreditos: inteiro positivo definido MANUALMENTE pelo admin.
 * Nao tem conversao com preco Tray, coupon_value_cents, reais ou qualquer gateway.
 */
export function parseNsCreditsPrice(value) {
  const invalid = () => new RewardStoreError("invalid_nscredits_price", { status: 400 });

  if (typeof value === "boolean") throw invalid();

  if (typeof value === "number") {
    if (!Number.isInteger(value) || value <= 0) throw invalid();
    return value;
  }

  if (typeof value === "string") {
    const s = value.trim();
    if (!/^\d+$/.test(s)) throw invalid();
    const n = Number(s);
    if (!Number.isInteger(n) || n <= 0) throw invalid();
    return n;
  }

  throw invalid();
}

function parseTrayProductId(value) {
  if (typeof value === "boolean" || value === null || value === undefined) {
    throw new RewardStoreError("invalid_tray_product_id", { status: 400 });
  }
  const s = String(value).trim();
  if (!s || !/^\d+$/.test(s)) {
    throw new RewardStoreError("invalid_tray_product_id", { status: 400 });
  }
  return s;
}

/**
 * Reduz o payload do frontend a INTENCAO administrativa pura.
 * Qualquer campo de catalogo enviado pelo navegador e descartado aqui —
 * o snapshot factual vem exclusivamente da consulta a Tray feita no backend.
 */
export function validatePublishBatch(products) {
  if (!Array.isArray(products) || products.length === 0) {
    throw new RewardStoreError("empty_batch", { status: 400 });
  }
  if (products.length > MAX_PUBLISH_BATCH) {
    throw new RewardStoreError("batch_too_large", {
      status: 400,
      details: { max: MAX_PUBLISH_BATCH, received: products.length },
    });
  }

  const seen = new Set();
  return products.map((item) => {
    const tray_product_id = parseTrayProductId(item?.tray_product_id);
    if (seen.has(tray_product_id)) {
      throw new RewardStoreError("duplicate_tray_product_id", {
        status: 400,
        details: { tray_product_id },
      });
    }
    seen.add(tray_product_id);

    return { tray_product_id, nscredits_price: parseNsCreditsPrice(item?.nscredits_price) };
  });
}

/* ─────────────────────────── Deps ─────────────────────────── */

async function defaultWithTransaction(fn) {
  const pool = await getPool();
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
}

function resolveDeps(deps = {}) {
  return {
    query: deps.query || dbQuery,
    withTransaction: deps.withTransaction || defaultWithTransaction,
    getCatalogProduct: deps.getCatalogProduct || ((id, options) => getTrayCatalogProduct(id, options)),
  };
}

/* ─────────────────────────── Mapeamento de linha ─────────────────────────── */

function toJsonArray(value) {
  if (Array.isArray(value)) return value;
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
}

function toNumberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function toIsoOrNull(value) {
  if (!value) return null;
  const d = value instanceof Date ? value : new Date(value);
  const ms = d.getTime();
  return Number.isFinite(ms) ? d.toISOString() : null;
}

function presentationOfRow(row) {
  const variants = toJsonArray(row.variants_snapshot);
  return derivePresentation({
    tray_available: row.tray_available,
    tray_available_in_store: row.tray_available_in_store,
    stock: row.stock,
    when_stock_runs_out: row.when_stock_runs_out,
    availability_days: row.availability_days,
    availability_text: row.availability_text,
    has_variation: row.has_variation === true,
    variants,
  });
}

/** DTO publico de `/loja` — sem preco em reais, sem estoque bruto, sem dado administrativo. */
export function mapRowToPublicProduct(row) {
  const presentation = presentationOfRow(row);
  const variants = toJsonArray(row.variants_snapshot);

  return {
    // Identidade local do produto na Loja — usada pelo carrinho (FK).
    reward_product_id: row.id ? String(row.id) : null,
    tray_product_id: String(row.tray_product_id),
    name: row.name ?? null,
    description_small: row.description_small ?? null,
    reference: row.reference ?? null,
    brand: row.brand ?? null,
    image_url: row.image_url ?? null,
    images: toJsonArray(row.images_snapshot),
    tray_product_url: row.tray_product_url ?? null,
    nscredits_price: Number(row.nscredits_price),
    display_order: Number(row.display_order ?? 0),
    has_variation: row.has_variation === true,
    is_available: presentation.is_available,
    availability_reason: presentation.reason,
    availability_text: row.availability_text ?? null,
    variants: variants.map((v) => ({
      variant_id: v?.variant_id ?? null,
      reference: v?.reference ?? null,
      values: Array.isArray(v?.values) ? v.values : [],
      is_available: derivePresentation({
        tray_available: v?.tray_available,
        tray_available_in_store: 1,
        stock: v?.stock,
        when_stock_runs_out: row.when_stock_runs_out,
      }).is_available,
    })),
    last_synced_at: toIsoOrNull(row.last_synced_at),
  };
}

/** DTO administrativo — inclui os factuais completos e a referencia de preco Tray. */
export function mapRowToAdminProduct(row) {
  const presentation = presentationOfRow(row);

  return {
    tray_product_id: String(row.tray_product_id),
    nscredits_price: Number(row.nscredits_price),
    is_published: row.is_published === true,
    display_order: Number(row.display_order ?? 0),

    name: row.name ?? null,
    description_small: row.description_small ?? null,
    reference: row.reference ?? null,
    brand: row.brand ?? null,
    image_url: row.image_url ?? null,
    images: toJsonArray(row.images_snapshot),
    tray_product_url: row.tray_product_url ?? null,

    stock: toNumberOrNull(row.stock),
    tray_available: toNumberOrNull(row.tray_available),
    tray_available_in_store: toNumberOrNull(row.tray_available_in_store),
    availability_text: row.availability_text ?? null,
    availability_days: toNumberOrNull(row.availability_days),
    has_variation: row.has_variation === true,
    when_stock_runs_out: row.when_stock_runs_out ?? null,
    order_days_availability: toNumberOrNull(row.order_days_availability),
    variants: toJsonArray(row.variants_snapshot),

    tray_price_snapshot: toNumberOrNull(row.tray_price_snapshot),
    tray_modified_at: toIsoOrNull(row.tray_modified_at),
    last_synced_at: toIsoOrNull(row.last_synced_at),
    published_at: toIsoOrNull(row.published_at),

    is_available: presentation.is_available,
    availability_reason: presentation.reason,
  };
}

const RETURNING_COLUMNS = `
  id, tray_product_id, nscredits_price, is_published, display_order,
  name, description_small, reference, brand, image_url, images_snapshot, tray_product_url,
  stock, tray_available, tray_available_in_store, availability_text, availability_days, has_variation,
  when_stock_runs_out, order_days_availability, variants_snapshot,
  tray_price_snapshot, tray_modified_at, last_synced_at, published_at, published_by,
  created_at, updated_at
`;

/* ─────────────────────────── Publicacao ─────────────────────────── */

const PUBLISH_SQL = `
  insert into public.reward_products (
    tray_product_id, nscredits_price, is_published,
    name, description_small, reference, brand, image_url, images_snapshot, tray_product_url,
    stock, tray_available, tray_available_in_store, availability_text, has_variation,
    when_stock_runs_out, order_days_availability, variants_snapshot,
    tray_price_snapshot, tray_modified_at, last_synced_at, published_at, published_by,
    availability_days
  ) values (
    $1, $2, true,
    $3, $4, $5, $6, $7, $8::jsonb, $9,
    $10, $11, $12, $13, $14,
    $15, $16, $17::jsonb,
    $18, $19, now(), now(), $20,
    $21
  )
  on conflict (tray_product_id) do update set
    nscredits_price = excluded.nscredits_price,
    is_published = true,
    name = excluded.name,
    description_small = excluded.description_small,
    reference = excluded.reference,
    brand = excluded.brand,
    image_url = excluded.image_url,
    images_snapshot = excluded.images_snapshot,
    tray_product_url = excluded.tray_product_url,
    stock = excluded.stock,
    tray_available = excluded.tray_available,
    tray_available_in_store = excluded.tray_available_in_store,
    availability_text = excluded.availability_text,
    availability_days = excluded.availability_days,
    has_variation = excluded.has_variation,
    when_stock_runs_out = excluded.when_stock_runs_out,
    order_days_availability = excluded.order_days_availability,
    variants_snapshot = excluded.variants_snapshot,
    tray_price_snapshot = excluded.tray_price_snapshot,
    tray_modified_at = excluded.tray_modified_at,
    last_synced_at = now(),
    published_at = coalesce(public.reward_products.published_at, now()),
    published_by = coalesce(excluded.published_by, public.reward_products.published_by)
  returning ${RETURNING_COLUMNS}
`;

function publishParams(intent, snapshot, adminUserId) {
  return [
    intent.tray_product_id,
    intent.nscredits_price,
    snapshot.name,
    snapshot.description_small,
    snapshot.reference,
    snapshot.brand,
    snapshot.image_url,
    JSON.stringify(snapshot.images || []),
    snapshot.tray_product_url,
    snapshot.stock,
    snapshot.tray_available,
    snapshot.tray_available_in_store,
    snapshot.availability_text,
    snapshot.has_variation === true,
    snapshot.when_stock_runs_out,
    snapshot.order_days_availability,
    JSON.stringify(snapshot.variants || []),
    snapshot.tray_price,
    snapshot.tray_modified_at,
    adminUserId ?? null,
    snapshot.availability_days ?? null,
  ];
}

/**
 * Publica um lote.
 *
 * Fluxo: valida intencao -> reconsulta CADA produto na Tray -> valida snapshots
 * -> abre transacao -> persiste todos -> commit.
 *
 * Se qualquer produto falhar ANTES da transacao, nada do lote e publicado.
 * A atomicidade e exclusivamente no PostgreSQL da NewStore; nada acontece na Tray.
 */
export async function publishRewardProducts(products, { adminUserId = null } = {}, deps = {}) {
  const d = resolveDeps(deps);
  const intents = validatePublishBatch(products);

  // 1) Reconsulta factual na Tray — o backend nunca confia no snapshot do navegador.
  const snapshots = [];
  for (const intent of intents) {
    const snapshot = await d.getCatalogProduct(intent.tray_product_id, { withVariants: true });
    if (!snapshot || String(snapshot.tray_product_id) !== intent.tray_product_id) {
      throw new RewardStoreError("tray_product_mismatch", {
        status: 502,
        details: { tray_product_id: intent.tray_product_id },
      });
    }
    snapshots.push(snapshot);
  }

  // 2) Persistencia atomica.
  const rows = await d.withTransaction(async (client) => {
    const out = [];
    for (let i = 0; i < intents.length; i += 1) {
      const result = await client.query(PUBLISH_SQL, publishParams(intents[i], snapshots[i], adminUserId));
      out.push(result.rows[0]);
    }
    return out;
  });

  return { published: rows.length, items: rows.filter(Boolean).map(mapRowToAdminProduct) };
}

/* ─────────────────────────── PATCH ─────────────────────────── */

export async function patchRewardProduct(trayProductId, patch, deps = {}) {
  const d = resolveDeps(deps);
  const id = parseTrayProductId(trayProductId);

  if (!patch || typeof patch !== "object" || Array.isArray(patch)) {
    throw new RewardStoreError("empty_patch", { status: 400 });
  }

  const keys = Object.keys(patch);
  if (keys.length === 0) {
    throw new RewardStoreError("empty_patch", { status: 400 });
  }

  const invalidKey = keys.find((k) => !PATCHABLE_FIELDS.has(k));
  if (invalidKey) {
    throw new RewardStoreError("invalid_patch_field", { status: 400, details: { field: invalidKey } });
  }

  const sets = [];
  const params = [];
  const add = (value) => {
    params.push(value);
    return `$${params.length}`;
  };

  if ("nscredits_price" in patch) {
    sets.push(`nscredits_price = ${add(parseNsCreditsPrice(patch.nscredits_price))}`);
  }
  if ("is_published" in patch) {
    if (typeof patch.is_published !== "boolean") {
      throw new RewardStoreError("invalid_is_published", { status: 400 });
    }
    sets.push(`is_published = ${add(patch.is_published)}`);
    if (patch.is_published === true) {
      sets.push("published_at = coalesce(published_at, now())");
    }
  }
  if ("display_order" in patch) {
    const n = Number(patch.display_order);
    if (typeof patch.display_order === "boolean" || !Number.isInteger(n) || n < 0) {
      throw new RewardStoreError("invalid_display_order", { status: 400 });
    }
    sets.push(`display_order = ${add(n)}`);
  }

  const idParam = add(id);
  const { rows } = await d.query(
    `update public.reward_products set ${sets.join(", ")}
      where tray_product_id = ${idParam}
      returning ${RETURNING_COLUMNS}`,
    params
  );

  if (!rows.length) {
    throw new RewardStoreError("reward_product_not_found", { status: 404 });
  }
  return mapRowToAdminProduct(rows[0]);
}

/* ─────────────────────────── Sync ─────────────────────────── */

const SYNC_SQL = `
  update public.reward_products set
    name = $2,
    description_small = $3,
    reference = $4,
    brand = $5,
    image_url = $6,
    images_snapshot = $7::jsonb,
    tray_product_url = $8,
    stock = $9,
    tray_available = $10,
    tray_available_in_store = $11,
    availability_text = $12,
    has_variation = $13,
    when_stock_runs_out = $14,
    order_days_availability = $15,
    variants_snapshot = $16::jsonb,
    tray_price_snapshot = $17,
    tray_modified_at = $18,
    availability_days = $19,
    last_synced_at = now()
  where tray_product_id = $1
  returning ${RETURNING_COLUMNS}
`;

/**
 * Sincroniza a LEITURA da Tray para o snapshot local.
 *
 * O POST em /sync comanda uma operacao interna da NewStore: o backend
 * executa GET na Tray e atualiza o PostgreSQL. Nada e escrito na Tray.
 *
 * Colunas de propriedade da NewStore (nscredits_price, is_published,
 * display_order, published_by, published_at) NAO aparecem no SET.
 */
export async function syncRewardProducts(trayProductIds, deps = {}) {
  const d = resolveDeps(deps);

  let ids;
  if (Array.isArray(trayProductIds) && trayProductIds.length > 0) {
    if (trayProductIds.length > MAX_PUBLISH_BATCH) {
      throw new RewardStoreError("batch_too_large", {
        status: 400,
        details: { max: MAX_PUBLISH_BATCH, received: trayProductIds.length },
      });
    }
    ids = [...new Set(trayProductIds.map(parseTrayProductId))];
  } else {
    // Estrategia segura: os publicados mais desatualizados primeiro, com teto.
    const { rows } = await d.query(
      `select tray_product_id from public.reward_products
        where is_published = true
        order by last_synced_at asc nulls first, display_order asc, id asc
        limit ${MAX_SYNC_WITHOUT_IDS}`
    );
    ids = rows.map((r) => String(r.tray_product_id));
  }

  const results = [];
  for (const id of ids) {
    try {
      const snapshot = await d.getCatalogProduct(id, { withVariants: true });
      const { rows } = await d.query(SYNC_SQL, [
        id,
        snapshot.name,
        snapshot.description_small,
        snapshot.reference,
        snapshot.brand,
        snapshot.image_url,
        JSON.stringify(snapshot.images || []),
        snapshot.tray_product_url,
        snapshot.stock,
        snapshot.tray_available,
        snapshot.tray_available_in_store,
        snapshot.availability_text,
        snapshot.has_variation === true,
        snapshot.when_stock_runs_out,
        snapshot.order_days_availability,
        JSON.stringify(snapshot.variants || []),
        snapshot.tray_price,
        snapshot.tray_modified_at,
        snapshot.availability_days ?? null,
      ]);

      if (!rows.length) {
        results.push({ tray_product_id: id, ok: false, error: "reward_product_not_found" });
        continue;
      }
      results.push({ tray_product_id: id, ok: true, item: mapRowToAdminProduct(rows[0]) });
    } catch (e) {
      results.push({ tray_product_id: id, ok: false, error: e?.code || "sync_failed" });
    }
  }

  return {
    requested: ids.length,
    succeeded: results.filter((r) => r.ok).length,
    failed: results.filter((r) => !r.ok).length,
    results,
  };
}

/* ─────────────────────────── Listagens ─────────────────────────── */

/** `/loja` publico: somente PostgreSQL, somente publicados. Nunca chama a Tray. */
const STORE_PRODUCTS_DEFAULT_LIMIT = 24;
const STORE_PRODUCTS_MAX_LIMIT = 100;

/**
 * Catalogo publico da Loja (GET /api/store/products). Paginado: uma vitrine
 * com centenas de produtos publicados nunca deve devolver tudo de uma vez.
 */
export async function listPublishedRewardProducts({ page = 1, limit = STORE_PRODUCTS_DEFAULT_LIMIT } = {}, deps = {}) {
  const d = resolveDeps(deps);
  const safeLimit = Math.min(Math.max(Number(limit) || STORE_PRODUCTS_DEFAULT_LIMIT, 1), STORE_PRODUCTS_MAX_LIMIT);
  const safePage = Math.max(Number(page) || 1, 1);
  const offset = (safePage - 1) * safeLimit;

  const { rows } = await d.query(
    `select ${RETURNING_COLUMNS}, count(*) over () as total_count
       from public.reward_products
      where is_published = true
      order by display_order asc, created_at asc, id asc
      limit $1 offset $2`,
    [safeLimit, offset]
  );

  const total = rows.length ? Number(rows[0].total_count) : 0;
  return {
    items: rows.map(mapRowToPublicProduct),
    paging: {
      page: safePage,
      limit: safeLimit,
      total,
      pages: total ? Math.ceil(total / safeLimit) : 0,
    },
  };
}

/** Detalhe publico de UM produto publicado. Somente PostgreSQL, nunca chama a Tray. */
export async function getPublishedRewardProduct(trayProductId, deps = {}) {
  const d = resolveDeps(deps);
  const id = String(trayProductId ?? "").trim();
  if (!id) throw new RewardStoreError("invalid_tray_product_id", { status: 400 });

  const { rows } = await d.query(
    `select ${RETURNING_COLUMNS}
       from public.reward_products
      where tray_product_id = $1 and is_published = true
      limit 1`,
    [id]
  );
  if (!rows.length) throw new RewardStoreError("reward_product_not_found", { status: 404 });
  return mapRowToPublicProduct(rows[0]);
}

/** Aba "Publicados na Loja" do admin: somente PostgreSQL. Funciona com a Tray fora do ar. */
export async function listAdminRewardProducts({ page = 1, limit = 50 } = {}, deps = {}) {
  const d = resolveDeps(deps);
  const safeLimit = Math.min(Math.max(Number(limit) || 50, 1), 200);
  const safePage = Math.max(Number(page) || 1, 1);
  const offset = (safePage - 1) * safeLimit;

  const { rows } = await d.query(
    `select ${RETURNING_COLUMNS}, count(*) over () as total_count
       from public.reward_products
      order by display_order asc, created_at asc, id asc
      limit $1 offset $2`,
    [safeLimit, offset]
  );

  const total = rows.length ? Number(rows[0].total_count) : 0;
  return {
    items: rows.map(mapRowToAdminProduct),
    paging: { page: safePage, limit: safeLimit, total },
  };
}

/**
 * Numeros do catalogo curado, para a aba Configuracoes do admin.
 * Le somente o PostgreSQL — o status da Tray e obtido a parte.
 */
export async function getRewardCatalogStats(deps = {}) {
  const d = resolveDeps(deps);
  const { rows } = await d.query(
    `select
       count(*)::int as total,
       count(*) filter (where is_published)::int as published,
       max(last_synced_at) as last_synced_at
     from public.reward_products`
  );

  const row = rows[0] || {};
  const total = Number(row.total) || 0;
  const published = Number(row.published) || 0;
  return {
    total,
    published,
    unpublished: total - published,
    last_synced_at: toIsoOrNull(row.last_synced_at),
  };
}

/** Estado de publicacao NewStore para mesclar na listagem do catalogo Tray. */
export async function getRewardsByTrayIds(trayProductIds, deps = {}) {
  const ids = Array.isArray(trayProductIds)
    ? [...new Set(trayProductIds.map((v) => String(v ?? "").trim()).filter(Boolean))]
    : [];
  if (ids.length === 0) return {};

  const d = resolveDeps(deps);
  const { rows } = await d.query(
    `select tray_product_id, nscredits_price, is_published, display_order, last_synced_at
       from public.reward_products
      where tray_product_id = any($1::text[])`,
    [ids]
  );

  const map = {};
  for (const row of rows) {
    map[String(row.tray_product_id)] = {
      is_published: row.is_published === true,
      nscredits_price: Number(row.nscredits_price),
      display_order: Number(row.display_order ?? 0),
      last_synced_at: toIsoOrNull(row.last_synced_at),
    };
  }
  return map;
}
