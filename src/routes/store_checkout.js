// src/routes/store_checkout.js
//
// Checkout da Loja de Premios — /api/store/checkout/*
//
// Todas as rotas exigem requireAuth. O user_id vem SEMPRE do token.
//
// FASE 5 (parcial): endereco + cotacao de frete Tray sao informativos.
// NADA aqui cria pedido, debita NSCreditos ou cobra frete — isso e o
// endpoint /redemptions/confirm, que so avanca ate onde o contrato de
// pagamento da Tray permitir (ver relatorio da Fase E).

import { Router } from "express";
import { requireAuth } from "../middleware/auth.js";
import { listUserAddresses, createUserAddress, deleteUserAddress } from "../services/userAddress.js";
import { getShippingOptionsForCart } from "../services/checkoutShipping.js";
import { getCouponBalance } from "../services/couponLedger.js";

const router = Router();

function sendError(res, error, context) {
  const status = Number(error?.status) || 500;
  const code = (typeof error?.code === "string" && error.code) || "internal_error";
  console.warn("[store.checkout] request failed", {
    context,
    status,
    code,
    message: status >= 500 ? String(error?.message || "").slice(0, 200) : undefined,
  });
  const body = { ok: false, error: code };
  if (error?.details) body.details = error.details;
  return res.status(status).json(body);
}

/** GET /api/store/checkout — bootstrap da tela: saldo + enderecos salvos. */
router.get("/", requireAuth, async (req, res) => {
  try {
    const [balance, addresses] = await Promise.all([
      getCouponBalance(req.user.id),
      listUserAddresses(req.user.id),
    ]);
    return res.json({
      wallet: { balance: balance.balance_cents / 100, balance_cents: balance.balance_cents, is_expired: balance.is_expired },
      addresses,
    });
  } catch (e) {
    return sendError(res, e, "bootstrap");
  }
});

router.get("/addresses", requireAuth, async (req, res) => {
  try {
    return res.json({ items: await listUserAddresses(req.user.id) });
  } catch (e) {
    return sendError(res, e, "list_addresses");
  }
});

router.post("/addresses", requireAuth, async (req, res) => {
  try {
    return res.status(201).json({ item: await createUserAddress(req.user.id, req.body || {}) });
  } catch (e) {
    return sendError(res, e, "create_address");
  }
});

router.delete("/addresses/:id", requireAuth, async (req, res) => {
  try {
    const deleted = await deleteUserAddress(req.user.id, req.params.id);
    if (!deleted) return res.status(404).json({ ok: false, error: "address_not_found" });
    return res.json({ ok: true });
  } catch (e) {
    return sendError(res, e, "delete_address");
  }
});

/**
 * POST /api/store/checkout/shipping
 * Body: { address_id }
 * Cotacao real da Tray para os itens do carrinho ativo. NAO cria pedido,
 * NAO debita nada — puramente informativo (ver item 39 do pedido original).
 */
router.post("/shipping", requireAuth, async (req, res) => {
  try {
    const out = await getShippingOptionsForCart(req.user.id, req.body?.address_id);
    return res.json(out);
  } catch (e) {
    return sendError(res, e, "shipping_cotation");
  }
});

export default router;
