// src/services/rewardCart.js
//
// Carrinho da Loja de Premios — INTENCAO DE RESGATE.
//
// O carrinho pertence a NEWSTORE. Nesta fase nao existe carrinho nem pedido
// na Tray: a Tray e consultada SOMENTE para pre-validacao factual (GET).
//
// IMPORTANTE — o carrinho NAO reserva estoque:
//   estoque Tray = 3, cliente coloca 1 no carrinho -> estoque Tray continua 3.
//   Outra pessoa ainda pode comprar o produto fora da Loja NS.
//   Portanto "carrinho valido agora" != "estoque garantido no fechamento".
//   A garantia so pode existir no fechamento transacional da Fase 5.
//
// Este service nao conhece: pedido, endereco, frete, debito de NSCreditos.

import { getPool, query as dbQuery } from "../db.js";
import { getTrayCatalogProduct } from "./trayCatalog.js";

/** Codigos estaveis de problema. Contrato — nao usar texto humano como chave. */
export const CART_ISSUES = {
  CART_EMPTY: "cart_empty",
  PRODUCT_NOT_FOUND: "product_not_found",
  PRODUCT_NOT_PUBLISHED: "product_not_published",
  PRODUCT_NOT_FOUND_TRAY: "product_not_found_tray",
  PRODUCT_UNAVAILABLE: "product_unavailable",
  VARIANT_REQUIRED: "variant_required",
  VARIANT_NOT_FOUND: "variant_not_found",
  VARIANT_NOT_BELONGS_TO_PRODUCT: "variant_not_belongs_to_product",
  VARIANT_UNAVAILABLE: "variant_unavailable",
  INSUFFICIENT_STOCK: "insufficient_stock",
  PRICE_CHANGED: "price_changed",
  INSUFFICIENT_NSCREDITS: "insufficient_nscredits",
  COUPON_EXPIRED: "coupon_expired",
  TRAY_UNAVAILABLE: "tray_unavailable",
};

/** Teto defensivo por item — nao e regra de negocio, e limite de sanidade. */
export const MAX_ITEM_QUANTITY = 999;

export class RewardCartError extends Error {
  constructor(code, { status = 400, details = null } = {}) {
    super(code);
    this.name = "RewardCartError";
    this.code = code;
    this.status = status;
    this.details = details;
  }
}

/* ─────────────────────────── Validacao ─────────────────────────── */

export function parseQuantity(value) {
  const invalid = () => new RewardCartError("invalid_quantity", { status: 400 });

  if (typeof value === "boolean") throw invalid();

  let s;
  if (typeof value === "number") {
    if (!Number.isInteger(value)) throw invalid();
    s = String(value);
  } else if (typeof value === "string") {
    s = value.trim();
  } else {
    throw invalid();
  }

  if (!/^\d+$/.test(s)) throw invalid();
  const n = Number(s);
  if (!Number.isSafeInteger(n) || n < 1 || n > MAX_ITEM_QUANTITY) throw invalid();
  return n;
}

function parseUserId(value) {
  const n = Number(value);
  if (!Number.isSafeInteger(n) || n <= 0) throw new RewardCartError("invalid_user_id", { status: 400 });
  return n;
}

function parseId(value, code) {
  const s = String(value ?? "").trim();
  if (!s) throw new RewardCartError(code, { status: 400 });
  return s;
}

function toSafeInt(value) {
  if (value === null || value === undefined) return 0;
  const n = Number(String(value).trim());
  return Number.isSafeInteger(n) ? n : 0;
}

function toNullableInt(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : null;
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

export function resolveDeps(deps = {}) {
  return {
    query: deps.query || dbQuery,
    withTransaction: deps.withTransaction || defaultWithTransaction,
    getCatalogProduct: deps.getCatalogProduct || ((id, options) => getTrayCatalogProduct(id, options)),
  };
}

/* ─────────────────────────── Pre-validacao factual ─────────────────────────── */

/** Nome legivel da variacao a partir dos valores factuais da Tray. */
export function variantLabel(variant) {
  if (!variant) return null;
  const values = Array.isArray(variant.values) ? variant.values : [];
  const label = values
    .map((v) => [v?.type, v?.value].filter(Boolean).join(": "))
    .filter(Boolean)
    .join(" · ");
  return label || variant.reference || (variant.variant_id ? `#${variant.variant_id}` : null);
}

/**
 * Localiza a variacao pedida dentro do produto factual da Tray.
 * Distingue "nao existe" de "existe mas e de outro produto".
 */
export function resolveVariant(trayProduct, trayVariantId) {
  const variants = Array.isArray(trayProduct?.variants) ? trayProduct.variants : [];
  const wanted = String(trayVariantId);

  const match = variants.find((v) => String(v.variant_id) === wanted);
  if (!match) return { issue: CART_ISSUES.VARIANT_NOT_FOUND, variant: null };

  const belongsTo = match.tray_product_id == null ? null : String(match.tray_product_id);
  if (belongsTo && belongsTo !== String(trayProduct.tray_product_id)) {
    return { issue: CART_ISSUES.VARIANT_NOT_BELONGS_TO_PRODUCT, variant: match };
  }

  // A Tray marca a variacao com `available`; 0 significa desligada.
  if (match.tray_available === 0) {
    return { issue: CART_ISSUES.VARIANT_UNAVAILABLE, variant: match };
  }

  return { issue: null, variant: match };
}

/**
 * Confere a intencao contra o produto factual da Tray.
 * Devolve a lista de problemas — quem chama decide se lanca erro (add/update)
 * ou apenas reporta (validate).
 *
 * `quantity` e a quantidade TOTAL pretendida para o item (nao o incremento).
 */
export function checkAgainstTray({ trayProduct, trayVariantId, quantity }) {
  const issues = [];
  let variant = null;

  if (trayProduct?.presentation?.is_available === false) {
    issues.push(CART_ISSUES.PRODUCT_UNAVAILABLE);
  }

  if (trayProduct?.has_variation) {
    if (!trayVariantId) {
      issues.push(CART_ISSUES.VARIANT_REQUIRED);
      return { issues, variant: null };
    }
    const resolved = resolveVariant(trayProduct, trayVariantId);
    variant = resolved.variant;
    if (resolved.issue) {
      issues.push(resolved.issue);
      return { issues, variant };
    }
  }

  // Estoque: da VARIACAO quando ha variacao, do produto quando e simples.
  //
  // O estoque so limita a quantidade quando a disponibilidade vem DO ESTOQUE.
  // Quando a Tray marca o produto como vendavel sob encomenda (estoque zerado
  // + prazo de entrega, ou when_stock_runs_out permitindo continuar vendendo),
  // travar pelo estoque contradiria a propria Tray.
  const stock = variant ? toNullableInt(variant.stock) : toNullableInt(trayProduct?.stock);
  if (stock !== null && quantity > stock && isStockBacked(trayProduct, variant)) {
    issues.push(CART_ISSUES.INSUFFICIENT_STOCK);
  }

  return { issues, variant, stock };
}

/** Razoes de disponibilidade em que o estoque NAO e o fator limitante. */
const ON_DEMAND_REASONS = new Set(["available_on_demand", "available_extended_lead_time"]);

/**
 * A disponibilidade deste item vem do estoque?
 * Para variacao, o estoque da variacao e sempre o limite: a venda sob
 * encomenda do produto nao diz nada sobre uma variacao especifica.
 */
export function isStockBacked(trayProduct, variant) {
  if (variant) return true;
  return !ON_DEMAND_REASONS.has(trayProduct?.presentation?.reason);
}

/** Erros de leitura da Tray viram codigo estavel de problema. */
export function trayErrorToIssue(error) {
  if (error?.code === "tray_product_not_found" || error?.status === 404) {
    return CART_ISSUES.PRODUCT_NOT_FOUND_TRAY;
  }
  return CART_ISSUES.TRAY_UNAVAILABLE;
}

/* ─────────────────────────── Acesso ao produto local ─────────────────────────── */

const REWARD_PRODUCT_COLUMNS = `
  id, tray_product_id, nscredits_price, is_published, name, image_url, has_variation
`;

async function loadRewardProduct(client, rewardProductId) {
  const { rows } = await client.query(
    `select ${REWARD_PRODUCT_COLUMNS} from public.reward_products where id = $1`,
    [rewardProductId]
  );
  if (!rows.length) throw new RewardCartError(CART_ISSUES.PRODUCT_NOT_FOUND, { status: 404 });
  const row = rows[0];
  if (row.is_published !== true) {
    throw new RewardCartError(CART_ISSUES.PRODUCT_NOT_PUBLISHED, { status: 409 });
  }
  return row;
}

/* ─────────────────────────── Carrinho ─────────────────────────── */

const CART_ITEM_COLUMNS = `
  i.id, i.cart_id, i.reward_product_id, i.tray_product_id, i.tray_variant_id,
  i.quantity, i.nscredits_unit_price_snapshot,
  i.product_name_snapshot, i.variant_name_snapshot, i.image_url_snapshot,
  i.created_at, i.updated_at
`;

async function findActiveCart(client, userId) {
  const { rows } = await client.query(
    "select id, user_id, status, created_at, updated_at from public.reward_carts where user_id = $1 and status = 'active' limit 1",
    [userId]
  );
  return rows[0] || null;
}

/** Cria o carrinho apenas quando ha uma movimentacao de verdade. */
async function ensureActiveCart(client, userId) {
  const { rows } = await client.query(
    `insert into public.reward_carts (user_id) values ($1)
     on conflict (user_id) where status = 'active' do nothing
     returning id, user_id, status`,
    [userId]
  );
  if (rows[0]) return rows[0];
  return await findActiveCart(client, userId);
}

async function loadItems(client, cartId) {
  const { rows } = await client.query(
    `select ${CART_ITEM_COLUMNS},
            p.nscredits_price as current_nscredits_price,
            p.is_published,
            p.has_variation as product_has_variation,
            p.name as current_name
       from public.reward_cart_items i
       join public.reward_products p on p.id = i.reward_product_id
      where i.cart_id = $1
      order by i.created_at asc, i.id asc`,
    [cartId]
  );
  return rows;
}

export function mapCartItem(row) {
  const snapshotPrice = toSafeInt(row.nscredits_unit_price_snapshot);
  const currentPrice = row.current_nscredits_price == null ? snapshotPrice : toSafeInt(row.current_nscredits_price);
  const quantity = toSafeInt(row.quantity);

  return {
    id: String(row.id),
    reward_product_id: String(row.reward_product_id),
    tray_product_id: String(row.tray_product_id),
    tray_variant_id: row.tray_variant_id ?? null,
    quantity,
    // O preco vigente e a autoridade; o snapshot serve para detectar mudanca.
    nscredits_unit_price: currentPrice,
    nscredits_unit_price_snapshot: snapshotPrice,
    nscredits_subtotal: currentPrice * quantity,
    price_changed: currentPrice !== snapshotPrice,
    name: row.current_name ?? row.product_name_snapshot ?? null,
    variant_name: row.variant_name_snapshot ?? null,
    image_url: row.image_url_snapshot ?? null,
    is_published: row.is_published === true,
    created_at: row.created_at instanceof Date ? row.created_at.toISOString() : row.created_at ?? null,
  };
}

function buildCart(cartRow, itemRows) {
  const items = itemRows.map(mapCartItem);
  return {
    id: cartRow ? String(cartRow.id) : null,
    status: cartRow?.status ?? "active",
    items,
    totals: {
      items: items.length,
      units: items.reduce((a, i) => a + i.quantity, 0),
      nscredits: items.reduce((a, i) => a + i.nscredits_subtotal, 0),
    },
  };
}

/**
 * Carrinho do usuario. NAO cria linha no banco apenas por consultar
 * e NAO consulta a Tray — leitura barata para o header e o drawer.
 */
export async function getCart(userId, deps = {}) {
  const d = resolveDeps(deps);
  const id = parseUserId(userId);

  const client = { query: d.query };
  const cart = await findActiveCart(client, id);
  if (!cart) return buildCart(null, []);

  return buildCart(cart, await loadItems(client, cart.id));
}

/**
 * Adiciona um item.
 *
 * O frontend envia SOMENTE a intencao: reward_product_id, tray_variant_id
 * e quantity. Preco, nome, imagem e tray_product_id vem do banco/Tray.
 *
 * Tudo numa transacao: dois cliques simultaneos no mesmo produto/variacao
 * nao criam duas linhas (indices unicos parciais + ON CONFLICT).
 */
export async function addItem({ userId, rewardProductId, trayVariantId = null, quantity = 1 } = {}, deps = {}) {
  const d = resolveDeps(deps);
  const uid = parseUserId(userId);
  const productId = parseId(rewardProductId, "invalid_reward_product_id");
  const qty = parseQuantity(quantity);
  const variantId = trayVariantId == null || String(trayVariantId).trim() === "" ? null : String(trayVariantId).trim();

  return await d.withTransaction(async (client) => {
    const product = await loadRewardProduct(client, productId);

    // Quantidade ja existente do MESMO item: o estoque tem que cobrir o total.
    const existing = await client.query(
      variantId === null
        ? `select i.quantity from public.reward_cart_items i
             join public.reward_carts c on c.id = i.cart_id
            where c.user_id = $1 and c.status = 'active'
              and i.reward_product_id = $2 and i.tray_variant_id is null`
        : `select i.quantity from public.reward_cart_items i
             join public.reward_carts c on c.id = i.cart_id
            where c.user_id = $1 and c.status = 'active'
              and i.reward_product_id = $2 and i.tray_variant_id = $3`,
      variantId === null ? [uid, productId] : [uid, productId, variantId]
    );
    const alreadyInCart = existing.rows.length ? toSafeInt(existing.rows[0].quantity) : 0;
    const totalQuantity = alreadyInCart + qty;

    // Reconsulta factual na Tray — o snapshot local nunca basta para uma acao.
    let trayProduct;
    try {
      trayProduct = await d.getCatalogProduct(product.tray_product_id, { withVariants: true });
    } catch (e) {
      const issue = trayErrorToIssue(e);
      throw new RewardCartError(issue, { status: issue === CART_ISSUES.PRODUCT_NOT_FOUND_TRAY ? 404 : 503 });
    }

    const { issues, variant, stock } = checkAgainstTray({
      trayProduct,
      trayVariantId: variantId,
      quantity: totalQuantity,
    });

    if (issues.length) {
      const code = issues[0];
      throw new RewardCartError(code, {
        status: code === CART_ISSUES.INSUFFICIENT_STOCK ? 409 : 409,
        details: code === CART_ISSUES.INSUFFICIENT_STOCK ? { stock, requested: totalQuantity } : null,
      });
    }

    const cart = await ensureActiveCart(client, uid);

    // O item unico e garantido por DOIS indices parciais complementares
    // (030_reward_carts.sql): produtos com variacao sao unicos por
    // (carrinho, produto, variacao); produtos simples, por (carrinho, produto).
    //
    // O ON CONFLICT so resolve conflito no indice que ele INFERE. Uma linha
    // com tray_variant_id NULL nao entra no indice de variacao, entao apontar
    // para ele no caminho simples nao evitaria nada: a violacao cairia no
    // indice simples e viraria 23505. Por isso o arbitro segue o caminho.
    const conflictTarget =
      variantId === null
        ? "(cart_id, reward_product_id) where tray_variant_id is null"
        : "(cart_id, reward_product_id, tray_variant_id) where tray_variant_id is not null";

    await client.query(
      `insert into public.reward_cart_items
         (cart_id, reward_product_id, tray_product_id, tray_variant_id, quantity,
          nscredits_unit_price_snapshot, product_name_snapshot, variant_name_snapshot, image_url_snapshot)
       values ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       on conflict ${conflictTarget}
       do update set quantity = public.reward_cart_items.quantity + excluded.quantity,
                     nscredits_unit_price_snapshot = excluded.nscredits_unit_price_snapshot,
                     updated_at = now()
       returning ${CART_ITEM_COLUMNS.replace(/i\./g, "")}`,
      [
        cart.id,
        product.id,
        product.tray_product_id,
        variantId,
        qty,
        toSafeInt(product.nscredits_price),
        product.name ?? null,
        variantLabel(variant),
        product.image_url ?? null,
      ]
    );

    return buildCart(cart, await loadItems(client, cart.id));
  });
}

async function loadOwnedItem(client, userId, itemId) {
  const { rows } = await client.query(
    `select ${CART_ITEM_COLUMNS}, c.user_id as cart_user_id, c.id as cart_id_ref
       from public.reward_cart_items i
       join public.reward_carts c on c.id = i.cart_id
      where i.id = $1 and c.user_id = $2 and c.status = 'active'
      limit 1`,
    [itemId, userId]
  );
  if (!rows.length) throw new RewardCartError("cart_item_not_found", { status: 404 });
  return rows[0];
}

/** Alterar quantidade REVALIDA na Tray: o estoque de antes nao vale mais. */
export async function updateItem({ userId, itemId, quantity } = {}, deps = {}) {
  const d = resolveDeps(deps);
  const uid = parseUserId(userId);
  const id = parseId(itemId, "invalid_cart_item_id");
  const qty = parseQuantity(quantity);

  return await d.withTransaction(async (client) => {
    const item = await loadOwnedItem(client, uid, id);
    const product = await loadRewardProduct(client, item.reward_product_id);

    let trayProduct;
    try {
      trayProduct = await d.getCatalogProduct(product.tray_product_id, { withVariants: true });
    } catch (e) {
      const issue = trayErrorToIssue(e);
      throw new RewardCartError(issue, { status: issue === CART_ISSUES.PRODUCT_NOT_FOUND_TRAY ? 404 : 503 });
    }

    const { issues, stock } = checkAgainstTray({
      trayProduct,
      trayVariantId: item.tray_variant_id,
      quantity: qty,
    });

    if (issues.length) {
      const code = issues[0];
      throw new RewardCartError(code, {
        status: 409,
        details: code === CART_ISSUES.INSUFFICIENT_STOCK ? { stock, requested: qty } : null,
      });
    }

    await client.query(
      "update public.reward_cart_items set quantity = $1, updated_at = now() where id = $2",
      [qty, id]
    );

    const cart = await findActiveCart(client, uid);
    return buildCart(cart, await loadItems(client, cart.id));
  });
}

/** Remove SOMENTE da NewStore. Nenhum DELETE na Tray. */
export async function removeItem({ userId, itemId } = {}, deps = {}) {
  const d = resolveDeps(deps);
  const uid = parseUserId(userId);
  const id = parseId(itemId, "invalid_cart_item_id");

  return await d.withTransaction(async (client) => {
    await loadOwnedItem(client, uid, id);
    await client.query("delete from public.reward_cart_items where id = $1", [id]);

    const cart = await findActiveCart(client, uid);
    if (!cart) return buildCart(null, []);
    return buildCart(cart, await loadItems(client, cart.id));
  });
}

/** Limpa SOMENTE o carrinho local. Nenhuma chamada a Tray. */
export async function clearCart({ userId } = {}, deps = {}) {
  const d = resolveDeps(deps);
  const uid = parseUserId(userId);

  return await d.withTransaction(async (client) => {
    const cart = await findActiveCart(client, uid);
    if (!cart) return buildCart(null, []);
    await client.query("delete from public.reward_cart_items where cart_id = $1", [cart.id]);
    return buildCart(cart, []);
  });
}

export { findActiveCart, loadItems, buildCart };
