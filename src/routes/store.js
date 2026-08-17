// src/routes/store.js
//
// Loja de Premios — endpoint publico de /loja.
//
// NUNCA consulta a Tray. Le exclusivamente o catalogo curado no PostgreSQL,
// para que a pagina publica continue funcionando com a Tray fora do ar
// e para nao estourar o rate limit da integracao.

import { Router } from "express";
import { listPublishedRewardProducts, getPublishedRewardProduct } from "../services/rewardStore.js";

const router = Router();

/** GET /api/store/products — somente produtos publicados, direto do PostgreSQL. Paginado. */
router.get("/products", async (req, res) => {
  try {
    return res.json(
      await listPublishedRewardProducts({ page: req.query.page, limit: req.query.limit })
    );
  } catch (e) {
    console.error("[store] falha ao listar catalogo publicado", {
      code: e?.code || null,
      message: String(e?.message || "").slice(0, 200),
    });
    return res.status(500).json({ ok: false, error: "store_catalog_unavailable" });
  }
});

/**
 * GET /api/store/products/:trayProductId
 * Detalhe publico de um produto publicado. Somente PostgreSQL.
 * As variacoes vem do snapshot local — suficiente para RENDERIZAR.
 * Antes de adicionar ao carrinho o backend revalida na Tray.
 */
router.get("/products/:trayProductId", async (req, res) => {
  try {
    return res.json({ item: await getPublishedRewardProduct(req.params.trayProductId) });
  } catch (e) {
    const status = Number(e?.status) || 500;
    if (status >= 500) {
      console.error("[store] falha ao carregar produto publicado", { code: e?.code || null });
    }
    return res.status(status).json({ ok: false, error: e?.code || "store_product_unavailable" });
  }
});

export default router;
