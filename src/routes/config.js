// src/routes/config.js
import { Router } from "express";
import {
  ensureAppConfig,
  getTicketPriceCents,
  setTicketPriceCents,
  getBannerTitle,
  setBannerTitle,
  getMaxNumbersPerSelection,
  setMaxNumbersPerSelection,
} from "../services/config.js";

const router = Router();

/**
 * GET /api/config
 * Retorna configurações públicas para o front.
 */
router.get("/", async (_req, res) => {
  try {
    await ensureAppConfig(); // garante a tabela/seed
    const [price_cents, banner_title, max_select] = await Promise.all([
      getTicketPriceCents(),
      getBannerTitle(),
      getMaxNumbersPerSelection(),
    ]);
    res.json({
      ticket_price_cents: price_cents,
      banner_title,
      max_numbers_per_selection: max_select,
    });
  } catch (e) {
    console.error("[config][GET] error:", e);
    res.status(500).json({ error: "config_failed" });
  }
});

/**
 * POST /api/config
 * Atualiza chaves (admin deve proteger essa rota via auth/ACL).
 * body: { ticket_price_cents?, banner_title?, max_numbers_per_selection? }
 */
router.post("/", async (req, res) => {
  try {
    const b = req.body || {};

    const writes = [];

    if (b.ticket_price_cents != null) {
      const cents = Math.max(0, Math.floor(Number(b.ticket_price_cents)));
      writes.push(setTicketPriceCents(cents));
    }
    if (b.banner_title != null) {
      writes.push(setBannerTitle(String(b.banner_title)));
    }
    if (b.max_numbers_per_selection != null) {
      const n = Math.max(1, Math.floor(Number(b.max_numbers_per_selection)));
      writes.push(setMaxNumbersPerSelection(n));
    }

    if (writes.length === 0) {
      return res.status(400).json({ error: "no_fields_to_update" });
    }

    await Promise.all(writes);

    const [price_cents, banner_title, max_select] = await Promise.all([
      getTicketPriceCents(),
      getBannerTitle(),
      getMaxNumbersPerSelection(),
    ]);

    res.json({
      ok: true,
      ticket_price_cents: price_cents,
      banner_title,
      max_numbers_per_selection: max_select,
    });
  } catch (e) {
    console.error("[config][POST] error:", e);
    res.status(500).json({ error: "config_update_failed" });
  }
});

export default router;
