import { Router } from "express";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import { getTicketPriceCents, setTicketPriceCents } from "../services/config.js";

const router = Router();

// GET /api/admin/config/ticket_price
router.get("/ticket_price", requireAuth, requireAdmin, async (_req, res) => {
  res.json({ ticket_price_cents: await getTicketPriceCents() });
});

// PUT /api/admin/config/ticket_price  { value_cents:number }
router.put("/ticket_price", requireAuth, requireAdmin, async (req, res) => {
  try {
    const value = Number(req.body?.value_cents);
    const ticket_price_cents = await setTicketPriceCents(value);
    res.json({ ok: true, ticket_price_cents });
  } catch (e) {
    console.error("[admin/config/ticket_price] error:", e);
    res.status(500).json({ error: "update_failed" });
  }
});

export default router;
