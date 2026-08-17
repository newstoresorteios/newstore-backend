// src/routes/admin_nscredits.js
//
// Administracao da carteira de NSCreditos — /api/admin/store/nscredits/*
//
// Todas as rotas exigem requireAuth + requireAdmin.
// O administrador responsavel vem SEMPRE da sessao autenticada; o navegador
// nunca pode informar `created_by`.
//
// Nao existe endpoint que sobrescreva saldo: toda mudanca e uma operacao
// (credit | debit) registrada no ledger.

import { Router } from "express";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import {
  searchUsersForAdmin,
  getAdminWalletDetail,
  applyAdminAdjustment,
} from "../services/nscreditWallet.js";

const router = Router();

/** Codigos pg que viram resposta de negocio em vez de 500. */
const PG_ERROR_STATUS = {
  "23505": { status: 409, code: "idempotency_conflict" },
  "23514": { status: 409, code: "wallet_constraint_violation" },
  "23503": { status: 404, code: "user_not_found" },
};

function sendError(res, error, context) {
  const pgMapped = error?.code && PG_ERROR_STATUS[error.code];
  const status = pgMapped?.status || Number(error?.status) || 500;
  const code = pgMapped?.code || (typeof error?.code === "string" && !pgMapped ? error.code : null) || "internal_error";

  // Nunca logamos o payload completo nem o motivo digitado pelo admin.
  console.warn("[admin.nscredits] request failed", {
    context,
    status,
    code,
    message: status >= 500 ? String(error?.message || "").slice(0, 200) : undefined,
  });

  const body = { ok: false, error: code };
  if (error?.details) body.details = error.details;
  return res.status(status).json(body);
}

function toPositiveInt(value, fallback, max) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 1) return fallback;
  return Math.min(Math.trunc(n), max);
}

/**
 * GET /api/admin/store/nscredits/users?q=&page=&limit=
 * Busca clientes reais (tabela users) com o saldo de NSCreditos.
 */
router.get("/users", requireAuth, requireAdmin, async (req, res) => {
  try {
    const page = toPositiveInt(req.query?.page, 1, 100000);
    const limit = toPositiveInt(req.query?.limit, 20, 100);
    return res.json(await searchUsersForAdmin({ q: req.query?.q, page, limit }));
  } catch (e) {
    return sendError(res, e, "search_users");
  }
});

/**
 * GET /api/admin/store/nscredits/users/:userId?page=&limit=
 * Usuario, saldo e historico paginado.
 */
router.get("/users/:userId", requireAuth, requireAdmin, async (req, res) => {
  try {
    const page = toPositiveInt(req.query?.page, 1, 100000);
    const limit = toPositiveInt(req.query?.limit, 20, 100);
    return res.json(await getAdminWalletDetail(req.params.userId, { page, limit }));
  } catch (e) {
    return sendError(res, e, "wallet_detail");
  }
});

/**
 * POST /api/admin/store/nscredits/users/:userId/transactions
 * Body: { operation: "credit"|"debit", amount, reason, idempotency_key? }
 *
 * O backend recalcula o saldo a partir do valor travado no banco.
 * Nao aceita `balance` nem `created_by` vindos do cliente.
 */
router.post("/users/:userId/transactions", requireAuth, requireAdmin, async (req, res) => {
  try {
    const out = await applyAdminAdjustment({
      userId: req.params.userId,
      operation: req.body?.operation,
      amount: req.body?.amount,
      reason: req.body?.reason,
      idempotencyKey: req.body?.idempotency_key,
      // Autoridade: sessao autenticada, nunca o payload.
      adminUserId: req.user?.id ?? null,
    });

    return res.status(out.replayed ? 200 : 201).json({
      ok: true,
      replayed: out.replayed,
      wallet: { balance: out.balance },
      transaction: out.transaction,
    });
  } catch (e) {
    return sendError(res, e, "apply_adjustment");
  }
});

export default router;
