// src/services/checkoutShipping.js
//
// Orquestra endereco + carrinho + cotacao de frete Tray para a tela de
// revisao do resgate. NAO cria pedido, NAO debita NSCreditos, NAO cobra
// nada — puramente informativo (ver item 39 do pedido: prepare nao debita).

import {
  resolveDeps as resolveCartDeps,
  findActiveCart,
  loadItems,
  buildCart,
} from "./rewardCart.js";
import { getUserAddress, UserAddressError } from "./userAddress.js";
import { fetchShippingCotation } from "./trayShipping.js";
import { TrayCatalogError } from "./trayCatalogClient.js";

export class CheckoutError extends Error {
  constructor(code, { status = 400, details = null } = {}) {
    super(code);
    this.name = "CheckoutError";
    this.code = code;
    this.status = status;
    this.details = details;
  }
}

function trayPriceCentsFor(product, trayVariantId) {
  if (trayVariantId) {
    const variant = (product.variants || []).find((v) => String(v.variant_id) === String(trayVariantId));
    const variantPrice = variant?.price;
    if (Number.isFinite(variantPrice)) return Math.round(variantPrice * 100);
  }
  const productPrice = product?.tray_price;
  if (Number.isFinite(productPrice)) return Math.round(productPrice * 100);
  return null;
}

/**
 * @returns {{ address, items: [{tray_product_id, tray_variant_id, quantity, tray_price_cents}], options }}
 */
export async function getShippingOptionsForCart(userId, addressId, deps = {}) {
  const d = resolveCartDeps(deps);

  const address = await getUserAddress(userId, addressId, deps);
  if (!address) throw new CheckoutError("address_not_found", { status: 404 });

  const cartRow = await findActiveCart(d, userId);
  const itemRows = cartRow ? await loadItems(d, cartRow.id) : [];
  const cart = buildCart(cartRow, itemRows);

  if (cart.items.length === 0) throw new CheckoutError("cart_empty", { status: 409 });

  const productCache = new Map();
  const cotationItems = [];
  const itemsOut = [];

  for (const item of cart.items) {
    if (!productCache.has(item.tray_product_id)) {
      try {
        productCache.set(item.tray_product_id, await d.getCatalogProduct(item.tray_product_id, { withVariants: true }));
      } catch (e) {
        throw new CheckoutError("tray_unavailable", { status: 503, details: { tray_product_id: item.tray_product_id } });
      }
    }
    const product = productCache.get(item.tray_product_id);
    const trayPriceCents = trayPriceCentsFor(product, item.tray_variant_id);
    if (trayPriceCents === null) {
      throw new CheckoutError("tray_price_unavailable", { status: 502, details: { tray_product_id: item.tray_product_id } });
    }

    itemsOut.push({
      tray_product_id: item.tray_product_id,
      tray_variant_id: item.tray_variant_id,
      quantity: item.quantity,
      tray_price_cents: trayPriceCents,
    });
    cotationItems.push({ trayProductId: item.tray_product_id, priceCents: trayPriceCents, quantity: item.quantity });
  }

  let options;
  try {
    options = await fetchShippingCotation({ zipcode: address.zipcode, items: cotationItems }, { deps, timeoutMs: deps.timeoutMs });
  } catch (e) {
    if (e instanceof TrayCatalogError) throw new CheckoutError("shipping_cotation_failed", { status: 502, details: { reason: e.code } });
    throw e;
  }

  return { address, items: itemsOut, options };
}
