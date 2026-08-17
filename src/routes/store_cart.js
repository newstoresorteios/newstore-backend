// src/routes/store_cart.js
//
// Carrinho da Loja de Premios — /api/store/cart/*
//
// Todas as rotas exigem requireAuth. O user_id vem SEMPRE do token:
// o navegador nunca informa de quem e o carrinho.
//
// Nenhuma rota aqui cria carrinho ou pedido na Tray, reserva estoque
// ou debita NSCreditos.

import { Router } from "express";
import { requireAuth } from "../middleware/auth.js";
import { getCart, addItem, updateItem, removeItem, clearCart } from "../services/rewardCart.js";
import { validateCart } from "../services/rewardCartValidator.js";

const router = Router();

/** Codigos pg que viram resposta de negocio em vez de 500. */
const PG_ERROR_STATUS = {
  "23505": { status: 409, code: "cart_item_conflict" },
  "23514": { status: 400, code: "cart_constraint_violation" },
  "23503": { status: 404, code: "product_not_found" },
};

function sendError(res, error, context) {
  const pgMapped = error?.code && PG_ERROR_STATUS[error.code];
  const status = pgMapped?.status || Number(error?.status) || 500;
  const code = pgMapped?.code || (typeof error?.code === "string" && !pgMapped ? error.code : null) || "internal_error";

  console.warn("[store.cart] request failed", {
    context,
    status,
    code,
    message: status >= 500 ? String(error?.message || "").slice(0, 200) : undefined,
  });

  const body = { ok: false, error: code };
  if (error?.details) body.details = error.details;
  return res.status(status).json(body);
}

/** GET /api/store/cart — nao cria carrinho e nao consulta a Tray. */
router.get("/", requireAuth, async (req, res) => {
  try {
    return res.json({ cart: await getCart(req.user.id) });
  } catch (e) {
    return sendError(res, e, "get_cart");
  }
});

/**
 * POST /api/store/cart/items
 * Body: { reward_product_id, tray_variant_id?, quantity }
 *
 * Preco, nome, imagem e tray_product_id NAO vem do body — o backend
 * resolve tudo a partir de reward_products e revalida na Tray.
 */
router.post("/items", requireAuth, async (req, res) => {
  try {
    const cart = await addItem({
      userId: req.user.id,
      rewardProductId: req.body?.reward_product_id,
      trayVariantId: req.body?.tray_variant_id ?? null,
      quantity: req.body?.quantity ?? 1,
    });
    return res.status(201).json({ ok: true, cart });
  } catch (e) {
    return sendError(res, e, "add_item");
  }
});

/** PATCH /api/store/cart/items/:itemId — revalida a quantidade na Tray. */
router.patch("/items/:itemId", requireAuth, async (req, res) => {
  try {
    const cart = await updateItem({
      userId: req.user.id,
      itemId: req.params.itemId,
      quantity: req.body?.quantity,
    });
    return res.json({ ok: true, cart });
  } catch (e) {
    return sendError(res, e, "update_item");
  }
});

/** DELETE /api/store/cart/items/:itemId — remove somente da NewStore. */
router.delete("/items/:itemId", requireAuth, async (req, res) => {
  try {
    const cart = await removeItem({ userId: req.user.id, itemId: req.params.itemId });
    return res.json({ ok: true, cart });
  } catch (e) {
    return sendError(res, e, "remove_item");
  }
});

/** DELETE /api/store/cart — limpa somente o carrinho local. */
router.delete("/", requireAuth, async (req, res) => {
  try {
    return res.json({ ok: true, cart: await clearCart({ userId: req.user.id }) });
  } catch (e) {
    return sendError(res, e, "clear_cart");
  }
});

/**
 * POST /api/store/cart/validate
 *
 * O POST comanda uma operacao INTERNA de validacao. Ele NAO cria pedido,
 * NAO altera a Tray, NAO debita a carteira e NAO reserva estoque.
 */
router.post("/validate", requireAuth, async (req, res) => {
  try {
    return res.json(await validateCart(req.user.id));
  } catch (e) {
    return sendError(res, e, "validate_cart");
  }
});

export default router;
