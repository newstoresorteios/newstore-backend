// backend/src/routes/purchase_limit.js
import { Router } from "express";
import { query } from "../db.js";
import { requireAuth } from "../middleware/auth.js";
import { checkUserLimit } from "../services/purchase_limit.js";

const router = Router();

async function getCurrentOpenDrawId() {
  const { rows } = await query(`
    select id
      from draws
     where status = 'open'
     order by id desc
     limit 1
  `);
  return rows?.[0]?.id ?? null;
}

// GET /api/purchase-limit/check?add=3&draw_id=123
router.get("/check", requireAuth, async (req, res) => {
  let add = parseInt(String(req.query.add ?? "1"), 10);
  if (!Number.isFinite(add) || add <= 0) add = 1;

  let drawId = req.query.draw_id ? Number(req.query.draw_id) : null;
  if (!drawId) drawId = await getCurrentOpenDrawId();
  if (!drawId) return res.status(404).json({ error: "no_open_draw" });

  const out = await checkUserLimit(req.user.id, drawId, add);
  return res.json({ ...out, draw_id: drawId });
});

// POST /api/purchase-limit/check { add, draw_id }
router.post("/check", requireAuth, async (req, res) => {
  let add = parseInt(String(req.body?.add ?? "1"), 10);
  if (!Number.isFinite(add) || add <= 0) add = 1;

  let drawId = req.body?.draw_id ? Number(req.body.draw_id) : null;
  if (!drawId) drawId = await getCurrentOpenDrawId();
  if (!drawId) return res.status(404).json({ error: "no_open_draw" });

  const out = await checkUserLimit(req.user.id, drawId, add);
  return res.json({ ...out, draw_id: drawId });
});

export default router;
