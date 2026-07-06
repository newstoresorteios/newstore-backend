// backend/src/routes/purchase_limit.js
import { Router } from "express";
import { query } from "../db.js";
import { requireAuth } from "../middleware/auth.js";
import { checkUserLimit } from "../services/purchase_limit.js";

const router = Router();

async function getCurrentOpenPrincipalDrawId() {
  const { rows } = await query(`
    select id
      from draws
     where status = 'open'
       and coalesce(draw_type, 'principal') = 'principal'
     order by id desc
     limit 1
  `);
  return rows?.[0]?.id ?? null;
}

async function drawExists(drawId) {
  const { rows } = await query(
    `
    select id
      from draws
     where id = $1
     limit 1
    `,
    [drawId]
  );
  return Boolean(rows?.[0]?.id);
}

function getRequestedDrawId(input) {
  const raw = input?.draw_id ?? input?.drawId;
  if (raw === undefined || raw === null || raw === "") return null;

  const drawId = Number(raw);
  if (!Number.isInteger(drawId) || drawId <= 0) {
    const err = new Error("invalid_draw_id");
    err.status = 400;
    err.code = "invalid_draw_id";
    throw err;
  }

  return drawId;
}

async function resolveDrawId(input) {
  const requestedDrawId = getRequestedDrawId(input);
  if (requestedDrawId) {
    if (!(await drawExists(requestedDrawId))) {
      const err = new Error("draw_not_found");
      err.status = 404;
      err.code = "draw_not_found";
      throw err;
    }
    return requestedDrawId;
  }

  return getCurrentOpenPrincipalDrawId();
}

function sendKnownError(res, err) {
  if (!err?.status || !err?.code) return false;
  res.status(err.status).json({ error: err.code });
  return true;
}

router.get("/check", requireAuth, async (req, res) => {
  try {
    let add = parseInt(String(req.query.add ?? "1"), 10);
    if (!Number.isFinite(add) || add <= 0) add = 1;

    const drawId = await resolveDrawId(req.query);
    if (!drawId) return res.status(404).json({ error: "no_open_draw" });

    const out = await checkUserLimit(req.user.id, drawId, add);
    return res.json({ ...out, draw_id: drawId });
  } catch (e) {
    if (sendKnownError(res, e)) return;
    console.error("[purchase-limit][GET] error:", e);
    return res.status(500).json({
      error: "purchase_limit_error",
      message: e.message,
    });
  }
});

router.post("/check", requireAuth, async (req, res) => {
  try {
    let add = parseInt(String(req.body?.add ?? "1"), 10);
    if (!Number.isFinite(add) || add <= 0) add = 1;

    const drawId = await resolveDrawId(req.body);
    if (!drawId) return res.status(404).json({ error: "no_open_draw" });

    const out = await checkUserLimit(req.user.id, drawId, add);
    return res.json({ ...out, draw_id: drawId });
  } catch (e) {
    if (sendKnownError(res, e)) return;
    console.error("[purchase-limit][POST] error:", e);
    return res.status(500).json({
      error: "purchase_limit_error",
      message: e.message,
    });
  }
});

export default router;
