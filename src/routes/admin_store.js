// src/routes/admin_store.js
//
// Administracao da Loja de Premios — /api/admin/store/*
//
// Todas as rotas exigem requireAuth + requireAdmin.
// A interacao com a Tray e SOMENTE LEITURA (GET de catalogo).
// O POST de /products/sync comanda uma operacao INTERNA da NewStore:
// o backend executa GET na Tray e atualiza o PostgreSQL.

import { Router } from "express";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import { listTrayCatalog, getTrayCatalogProduct, listTrayBrands } from "../services/trayCatalog.js";
import { TRAY_MAX_LIMIT } from "../services/trayCatalogClient.js";
import {
  publishRewardProducts,
  patchRewardProduct,
  syncRewardProducts,
  listAdminRewardProducts,
  getRewardsByTrayIds,
  getRewardCatalogStats,
} from "../services/rewardStore.js";
import { trayTokenHealth } from "../services/tray.js";

const router = Router();

/* ─────────────────────── Normalizacao de erro ─────────────────────── */

/** Codigos pg que viram resposta de negocio em vez de 500. */
const PG_ERROR_STATUS = {
  "23505": { status: 409, code: "reward_product_conflict" },
  "23514": { status: 400, code: "reward_product_constraint_violation" },
  "23502": { status: 400, code: "reward_product_missing_field" },
};

/**
 * Converte qualquer erro interno numa resposta segura.
 * Nunca devolve corpo cru da Tray, URL com token ou mensagem de driver.
 */
function sendError(res, error, context) {
  const pgMapped = error?.code && PG_ERROR_STATUS[error.code];
  const status = pgMapped?.status || Number(error?.status) || 500;
  const code = pgMapped?.code || (typeof error?.code === "string" && !pgMapped ? error.code : null) || "internal_error";

  console.warn("[admin.store] request failed", {
    context,
    status,
    code,
    // message do driver/HTTP nunca vai para o cliente; aqui fica so o codigo.
    message: status >= 500 ? String(error?.message || "").slice(0, 200) : undefined,
  });

  const body = { ok: false, error: code };
  if (error?.details) body.details = error.details;
  if (status === 429 && Number.isFinite(Number(error?.retryAfterSeconds))) {
    res.set("Retry-After", String(error.retryAfterSeconds));
    body.retry_after_seconds = Number(error.retryAfterSeconds);
  }
  return res.status(status).json(body);
}

function toPositiveInt(value, fallback, max) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 1) return fallback;
  return Math.min(Math.trunc(n), max);
}

/* ─────────────────────── Catalogo Tray (leitura) ─────────────────────── */

/**
 * GET /api/admin/store/tray-products
 * Query: page, limit, q, qField (auto|name|reference|id|brand), available (0|1), brand
 *
 * Usa o endpoint de LISTAGEM da Tray com paginacao — uma chamada por pagina,
 * nunca uma chamada por linha da tabela.
 */
router.get("/tray-products", requireAuth, requireAdmin, async (req, res) => {
  try {
    const page = toPositiveInt(req.query?.page, 1, 100000);
    const limit = toPositiveInt(req.query?.limit, 20, TRAY_MAX_LIMIT);

    const { items, paging } = await listTrayCatalog({
      page,
      limit,
      q: req.query?.q,
      qField: req.query?.qField,
      available: req.query?.available,
      brand: req.query?.brand,
    });

    // Uma unica consulta local para todo o lote da pagina (sem N+1).
    const rewards = await getRewardsByTrayIds(items.map((p) => p.tray_product_id));

    return res.json({
      items: items.map((p) => ({
        tray_product_id: p.tray_product_id,
        name: p.name,
        description_small: p.description_small,
        reference: p.reference,
        brand: p.brand,
        image_url: p.image_url,

        // Referencia administrativa apenas — nao e o preco da Loja de Premios.
        tray_price: p.tray_price,
        tray_product_url: p.tray_product_url,

        // Factuais da Tray, preservados (0/1 e texto original).
        stock: p.stock,
        tray_available: p.tray_available,
        tray_available_in_store: p.tray_available_in_store,
        availability_text: p.availability_text,
        availability_days: p.availability_days,
        has_variation: p.has_variation,
        when_stock_runs_out: p.when_stock_runs_out,

        // Booleans de conveniencia, derivados dos factuais acima.
        available: p.tray_available === 1,
        available_in_store: p.tray_available_in_store === 1,
        is_available: p.presentation.is_available,
        availability_reason: p.presentation.reason,

        reward: rewards[p.tray_product_id] || null,
      })),
      paging,
    });
  } catch (e) {
    return sendError(res, e, "list_tray_products");
  }
});

/**
 * GET /api/admin/store/tray-products/:trayProductId
 * Consulta individual — usada ao abrir o detalhe/modal (inclui variacoes).
 */
router.get("/tray-products/:trayProductId", requireAuth, requireAdmin, async (req, res) => {
  try {
    const product = await getTrayCatalogProduct(req.params.trayProductId, { withVariants: true });
    const rewards = await getRewardsByTrayIds([product.tray_product_id]);
    return res.json({ item: { ...product, reward: rewards[product.tray_product_id] || null } });
  } catch (e) {
    return sendError(res, e, "get_tray_product");
  }
});

/** GET /api/admin/store/tray-brands — uma chamada, usada para popular o filtro de marca. */
router.get("/tray-brands", requireAuth, requireAdmin, async (_req, res) => {
  try {
    return res.json({ items: await listTrayBrands() });
  } catch (e) {
    return sendError(res, e, "list_tray_brands");
  }
});

/**
 * GET /api/admin/store/status
 * Aba "Configuracoes": saude da integracao Tray + numeros do catalogo curado.
 * Nunca devolve access_token, refresh_token ou credenciais.
 */
router.get("/status", requireAuth, requireAdmin, async (_req, res) => {
  try {
    const [health, catalog] = await Promise.all([
      trayTokenHealth().catch((e) => ({ ok: false, lastError: e?.code || "tray_status_failed" })),
      getRewardCatalogStats(),
    ]);

    let apiHost = null;
    try {
      apiHost = health.apiBase ? new URL(health.apiBase).host : null;
    } catch {
      apiHost = null;
    }

    return res.json({
      tray: {
        ok: health.ok === true,
        auth_mode: health.authMode ?? null,
        api_host: apiHost,
        access_expires_at: health.expAccessAt ?? null,
        last_error: health.lastError ?? null,
      },
      catalog,
    });
  } catch (e) {
    return sendError(res, e, "store_status");
  }
});

/* ─────────────────────── Catalogo curado (PostgreSQL) ─────────────────────── */

/**
 * GET /api/admin/store/products
 * Somente PostgreSQL. Funciona mesmo com a Tray fora do ar.
 */
router.get("/products", requireAuth, requireAdmin, async (req, res) => {
  try {
    const page = toPositiveInt(req.query?.page, 1, 100000);
    const limit = toPositiveInt(req.query?.limit, 50, 200);
    return res.json(await listAdminRewardProducts({ page, limit }));
  } catch (e) {
    return sendError(res, e, "list_reward_products");
  }
});

/**
 * POST /api/admin/store/products/publish
 * Body: { products: [{ tray_product_id, nscredits_price }] }
 *
 * O backend reconsulta CADA produto na Tray antes de persistir.
 * Publicar aqui NAO reserva estoque nem altera nada na Tray.
 */
router.post("/products/publish", requireAuth, requireAdmin, async (req, res) => {
  try {
    const out = await publishRewardProducts(req.body?.products, { adminUserId: req.user?.id ?? null });
    return res.status(201).json({ ok: true, ...out });
  } catch (e) {
    return sendError(res, e, "publish_reward_products");
  }
});

/**
 * PATCH /api/admin/store/products/:trayProductId
 * Body: { nscredits_price?, is_published?, display_order? }
 * Somente propriedades da NewStore — qualquer atributo Tray e recusado com 400.
 */
router.patch("/products/:trayProductId", requireAuth, requireAdmin, async (req, res) => {
  try {
    const item = await patchRewardProduct(req.params.trayProductId, req.body || {});
    return res.json({ ok: true, item });
  } catch (e) {
    return sendError(res, e, "patch_reward_product");
  }
});

/**
 * POST /api/admin/store/products/sync
 * Body opcional: { tray_product_ids: ["123", "456"] }
 *
 * POST comanda uma operacao interna da NewStore. O backend faz GET na Tray
 * e atualiza o snapshot. Nada e escrito na Tray.
 */
router.post("/products/sync", requireAuth, requireAdmin, async (req, res) => {
  try {
    const ids = Array.isArray(req.body?.tray_product_ids) ? req.body.tray_product_ids : null;
    return res.json({ ok: true, ...(await syncRewardProducts(ids)) });
  } catch (e) {
    return sendError(res, e, "sync_reward_products");
  }
});

export default router;
