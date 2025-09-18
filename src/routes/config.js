// src/routes/config.js
import { Router } from 'express';
import {
  getTicketPriceCents,
  getBannerTitle,
  getMaxNumbersPerSelection,
} from '../services/config.js';

const router = Router();

/**
 * GET /api/config
 * Responde chaves de configuração pública usadas no front.
 */
router.get('/', async (_req, res) => {
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
      // espaço pra futuras chaves: current_draw_id, etc.
    });
  } catch (e) {
    console.error('[config] error', e);
    res.status(500).json({ error: 'config_failed' });
  }
});

export default router;
