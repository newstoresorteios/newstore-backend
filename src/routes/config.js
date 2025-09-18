// src/routes/config.js
import { Router } from "express";
import {
  getTicketPriceCents,
  setTicketPriceCents,
  getBannerTitle,
  getMaxNumbersPerSelection,
  setBannerTitle,
  setMaxNumbersPerSelection,
} from "../services/config.js";

const router = Router();

/** GET /api/config */
router.get("/", async (_req, res) => {
  try {
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
    console.error("[config] GET error:", e);
    res.status(500).json({ error: "config_failed" });
  }
});

/** POST /api/config  (atualiza chaves informadas no body) */
router.post("/", async (req, res) => {
  try {
    const {
      ticket_price_cents,
      banner_title,
      max_numbers_per_selection,
    } = req.body || {};

    const tasks = [];

    if (ticket_price_cents != null) {
      tasks.push(setTicketPriceCents(ticket_price_cents));
    }
    if (banner_title != null) {
      tasks.push(setBannerTitle(String(banner_title)));
    }
    if (max_numbers_per_selection != null) {
      tasks.push(setMaxNumbersPerSelection(max_numbers_per_selection));
    }

    await Promise.all(tasks);

    // responde a config atualizada
    const [price_cents, banner, maxSel] = await Promise.all([
      getTicketPriceCents(),
      getBannerTitle(),
      getMaxNumbersPerSelection(),
    ]);

    res.json({
      ticket_price_cents: price_cents,
      banner_title: banner,
      max_numbers_per_selection: maxSel,
    });
  } catch (e) {
    console.error("[config] POST error:", e);
    res.status(500).json({ error: "config_update_failed" });
  }
});

export default router;
