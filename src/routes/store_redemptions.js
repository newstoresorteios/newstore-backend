// src/routes/store_redemptions.js
//
// Resgate real da Loja de Premios — /api/store/redemptions/*
//
// FASE E BLOQUEADA (ver relatorio): todo /confirm hoje termina em
// blocked_tray_contract_pending com os creditos devolvidos, porque o
// contrato de pagamento do pedido Tray ainda nao foi decidido. As rotas
// existem e sao seguras (nunca perdem credito do cliente), mas nenhum
// resgate REAL (produto na porta de casa) acontece ainda.
//
// Todas as rotas exigem requireAuth. O user_id vem SEMPRE do token.

import { Router } from "express";
import { requireAuth } from "../middleware/auth.js";
import { prepareRedemption, confirmRedemption, getRedemption, listRedemptions } from "../services/rewardRedemption.js";

const router = Router();

const PG_ERROR_STATUS = {
  "23505": { status: 409, code: "idempotency_conflict" },
  "23514": { status: 409, code: "redemption_constraint_violation" },
  "23503": { status: 404, code: "not_found" },
};

function sendError(res, error, context) {
  const pgMapped = error?.code && PG_ERROR_STATUS[error.code];
  const status = pgMapped?.status || Number(error?.status) || 500;
  const code = pgMapped?.code || (typeof error?.code === "string" && !pgMapped ? error.code : null) || "internal_error";

  console.warn("[store.redemptions] request failed", {
    context,
    status,
    code,
    message: status >= 500 ? String(error?.message || "").slice(0, 200) : undefined,
  });

  const body = { ok: false, error: code };
  if (error?.details) body.details = error.details;
  return res.status(status).json(body);
}

/** POST /api/store/redemptions/prepare — resumo de confirmacao. NUNCA debita. */
router.post("/prepare", requireAuth, async (req, res) => {
  try {
    return res.json(await prepareRedemption(req.user.id, { addressId: req.body?.address_id }));
  } catch (e) {
    return sendError(res, e, "prepare");
  }
});

/**
 * POST /api/store/redemptions/confirm
 * Body: { address_id, shipping_option, idempotency_key }
 * O UNICO endpoint que debita — e so uma vez por idempotency_key.
 */
router.post("/confirm", requireAuth, async (req, res) => {
  try {
    const out = await confirmRedemption(req.user.id, {
      addressId: req.body?.address_id,
      shippingOption: req.body?.shipping_option || null,
      idempotencyKey: req.body?.idempotency_key,
    });
    return res.status(out.replayed ? 200 : 201).json(out);
  } catch (e) {
    return sendError(res, e, "confirm");
  }
});

/** GET /api/store/redemptions — "Meus Pedidos". */
router.get("/", requireAuth, async (req, res) => {
  try {
    return res.json(await listRedemptions(req.user.id, { page: req.query.page, limit: req.query.limit }));
  } catch (e) {
    return sendError(res, e, "list");
  }
});

/** GET /api/store/redemptions/:id */
router.get("/:id", requireAuth, async (req, res) => {
  try {
    const item = await getRedemption(req.user.id, req.params.id);
    if (!item) return res.status(404).json({ ok: false, error: "redemption_not_found" });
    return res.json({ item });
  } catch (e) {
    return sendError(res, e, "detail");
  }
});

export default router;
