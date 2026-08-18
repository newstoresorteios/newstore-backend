// src/services/rewardCartValidator.js
//
// Pre-validacao factual do carrinho.
//
// Combina TRES fontes:
//   1. reward_products      (publicacao e preco vigente em NSCreditos)
//   2. Tray                 (produto, variacao, disponibilidade, estoque) — GET
//   3. coupon_value_cents   (saldo factual do cliente — FASE 5: o cupom
//                            individual do usuario, nao mais nscredit_wallets)
//
// ESTA OPERACAO NAO ALTERA NADA:
//   nao debita NSCreditos, nao grava ledger, nao cria pedido,
//   nao reserva estoque e nao escreve na Tray.
//
// Resultado: `valid` + problemas com CODIGOS ESTAVEIS por item e por carrinho.
// Nunca colapsa tudo num erro generico.

import { getCouponBalance } from "./couponLedger.js";
import {
  CART_ISSUES,
  resolveDeps as resolveCartDeps,
  findActiveCart,
  loadItems,
  buildCart,
  checkAgainstTray,
  trayErrorToIssue,
} from "./rewardCart.js";

/**
 * Saldo em NSCreditos (mesma escala de reward_products.nscredits_price):
 * users.coupon_value_cents / 100. O valor armazenado nunca muda de forma,
 * essa divisao e so para comparar com o total do carrinho.
 */
async function defaultGetWalletBalance(userId, deps) {
  const b = await getCouponBalance(userId, deps);
  return { balance: b.balance_cents / 100, is_expired: b.is_expired };
}

function resolveDeps(deps = {}) {
  return {
    ...resolveCartDeps(deps),
    getWalletBalance: deps.getWalletBalance || ((userId) => defaultGetWalletBalance(userId, deps)),
  };
}

/**
 * Valida o carrinho inteiro do usuario.
 *
 * Consulta a Tray UMA vez por produto distinto (nao por item), para nao
 * multiplicar chamadas quando o mesmo produto aparece em varias variacoes.
 */
export async function validateCart(userId, deps = {}) {
  const d = resolveDeps(deps);
  const client = { query: d.query };

  const cartRow = await findActiveCart(client, userId);
  const itemRows = cartRow ? await loadItems(client, cartRow.id) : [];
  const cart = buildCart(cartRow, itemRows);

  const cartIssues = [];
  if (cart.items.length === 0) cartIssues.push(CART_ISSUES.CART_EMPTY);

  // Uma leitura de catalogo por produto distinto.
  const trayById = new Map();
  const trayErrorById = new Map();
  for (const trayProductId of new Set(cart.items.map((i) => i.tray_product_id))) {
    try {
      trayById.set(trayProductId, await d.getCatalogProduct(trayProductId, { withVariants: true }));
    } catch (e) {
      trayErrorById.set(trayProductId, trayErrorToIssue(e));
    }
  }

  const items = cart.items.map((item) => {
    const issues = [];

    if (!item.is_published) issues.push(CART_ISSUES.PRODUCT_NOT_PUBLISHED);
    if (item.price_changed) issues.push(CART_ISSUES.PRICE_CHANGED);

    const trayError = trayErrorById.get(item.tray_product_id);
    if (trayError) {
      // Tray fora do ar: NAO declaramos valido com base no snapshot antigo.
      issues.push(trayError);
    } else {
      const trayProduct = trayById.get(item.tray_product_id);
      const { issues: trayIssues } = checkAgainstTray({
        trayProduct,
        trayVariantId: item.tray_variant_id,
        quantity: item.quantity,
      });
      issues.push(...trayIssues);
    }

    return {
      id: item.id,
      reward_product_id: item.reward_product_id,
      tray_product_id: item.tray_product_id,
      tray_variant_id: item.tray_variant_id,
      name: item.name,
      variant_name: item.variant_name,
      quantity: item.quantity,
      current_nscredits_price: item.nscredits_unit_price,
      snapshot_nscredits_price: item.nscredits_unit_price_snapshot,
      nscredits_subtotal: item.nscredits_subtotal,
      valid: issues.length === 0,
      issues,
    };
  });

  // Saldo: leitura pura. Nao altera a carteira nem grava ledger.
  let wallet = { balance: 0, sufficient: false, missing: 0 };
  let walletUnavailable = false;
  let couponExpired = false;
  try {
    const { balance, is_expired } = await d.getWalletBalance(userId);
    const total = cart.totals.nscredits;
    couponExpired = !!is_expired;
    const sufficient = !couponExpired && balance >= total;
    wallet = {
      balance,
      sufficient,
      missing: couponExpired ? total : Math.max(0, total - balance),
    };
  } catch {
    walletUnavailable = true;
    wallet = { balance: null, sufficient: false, missing: null };
  }

  if (walletUnavailable) cartIssues.push("wallet_unavailable");
  else if (cart.items.length > 0 && couponExpired) cartIssues.push(CART_ISSUES.COUPON_EXPIRED);
  else if (cart.items.length > 0 && !wallet.sufficient) cartIssues.push(CART_ISSUES.INSUFFICIENT_NSCREDITS);

  const valid = cartIssues.length === 0 && items.every((i) => i.valid);

  return {
    valid,
    cart: {
      id: cart.id,
      total_items: cart.totals.items,
      total_units: cart.totals.units,
      total_nscredits: cart.totals.nscredits,
    },
    wallet,
    issues: cartIssues,
    items,
  };
}
