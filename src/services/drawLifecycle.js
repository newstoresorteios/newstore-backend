import { query as defaultQuery } from "../db.js";

function getQueryRunner(db) {
  return db && typeof db.query === "function" ? db : { query: defaultQuery };
}

export async function closeDrawIfSoldOut(drawId, db) {
  const id = Number(drawId);
  if (!Number.isInteger(id) || id <= 0) {
    return { ok: false, closed: false, error: "invalid_draw_id" };
  }

  const runner = getQueryRunner(db);
  const statsResult = await runner.query(
    `SELECT
        COUNT(*)::int AS total,
        COUNT(*) FILTER (WHERE status = 'sold')::int AS sold,
        COUNT(*) FILTER (WHERE status = 'available')::int AS available,
        COUNT(*) FILTER (WHERE status = 'reserved')::int AS reserved
       FROM public.numbers
      WHERE draw_id = $1`,
    [id]
  );

  const stats = {
    total: Number(statsResult.rows?.[0]?.total || 0),
    sold: Number(statsResult.rows?.[0]?.sold || 0),
    available: Number(statsResult.rows?.[0]?.available || 0),
    reserved: Number(statsResult.rows?.[0]?.reserved || 0),
  };

  const drawResult = await runner.query(
    `SELECT id, status, closed_at
       FROM public.draws
      WHERE id = $1`,
    [id]
  );
  const draw = drawResult.rows?.[0] || null;
  if (!draw) return { ok: false, closed: false, error: "draw_not_found", stats };

  const shouldClose =
    stats.total > 0 &&
    stats.sold === stats.total &&
    stats.available === 0 &&
    stats.reserved === 0 &&
    draw.status === "open";

  if (!shouldClose) {
    return { ok: true, closed: false, stats, draw };
  }

  const updated = await runner.query(
    `UPDATE public.draws
        SET status = 'closed',
            closed_at = COALESCE(closed_at, NOW())
      WHERE id = $1
        AND status = 'open'
      RETURNING id, status, closed_at`,
    [id]
  );

  return {
    ok: true,
    closed: updated.rowCount > 0,
    stats,
    draw: updated.rows?.[0] || draw,
  };
}
