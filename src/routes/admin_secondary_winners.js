import { Router } from "express";
import { getPool, query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";

const router = Router();
const norm = (v, max = 2048) => String(v ?? "").trim().slice(0, max);

router.use(requireAuth, requireAdmin);

router.get("/", async (_req, res) => {
  try {
    const r = await query(
      `
      SELECT
        d.id AS draw_id,
        COALESCE(NULLIF(d.winner_name, ''), u.name, u.email, '-') AS winner_name,
        u.email AS winner_email,
        d.winner_user_id,
        d.winner_number,
        d.realized_at,
        d.closed_at,
        d.status,
        d.product_name,
        d.product_link
      FROM public.draws d
      LEFT JOIN public.users u ON u.id = d.winner_user_id
      WHERE d.draw_type IN ('adicional', 'secundario')
        AND (
          d.realized_at IS NOT NULL
          OR d.winner_user_id IS NOT NULL
          OR d.winner_number IS NOT NULL
        )
      ORDER BY COALESCE(d.realized_at, d.closed_at, d.opened_at, d.created_at) DESC, d.id DESC
      `
    );

    const now = Date.now();
    const winners = (r.rows || []).map((row) => {
      const realized = row.realized_at ? new Date(row.realized_at) : null;
      const daysSince = realized ? Math.max(0, Math.floor((now - realized.getTime()) / 86400000)) : 0;
      const redeemed = !!row.closed_at;
      return {
        type: "secondary",
        draw_id: row.draw_id,
        winner_user_id: row.winner_user_id ?? null,
        winner_name: row.winner_name || "-",
        winner_email: row.winner_email || null,
        winner_number: row.winner_number ?? null,
        realized_at: row.realized_at,
        closed_at: row.closed_at,
        draw_status: row.status,
        product_name: row.product_name || "",
        product_link: row.product_link || "",
        redeemed,
        status: redeemed ? "RESGATADO" : "NAO RESGATADO",
        days_since: daysSince,
      };
    });

    return res.json({ winners });
  } catch (e) {
    console.error("[admin/secondary-winners] error:", e?.code || e?.message || e);
    return res.status(500).json({ error: "list_failed" });
  }
});

router.patch("/:id", async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) {
    return res.status(400).json({ error: "invalid_draw_id" });
  }

  const updates = [];
  const params = [id];
  const addUpdate = (column, value) => {
    params.push(value);
    updates.push(`${column} = $${params.length}`);
  };

  if (req.body?.product_name !== undefined) {
    addUpdate("product_name", req.body.product_name === null ? null : norm(req.body.product_name, 255));
  }

  if (req.body?.product_link !== undefined) {
    addUpdate("product_link", req.body.product_link === null ? null : norm(req.body.product_link, 2048));
  }

  if (req.body?.winner_name !== undefined) {
    addUpdate("winner_name", req.body.winner_name === null ? null : norm(req.body.winner_name, 255));
  }

  if (req.body?.winner_number !== undefined) {
    const winnerNumber = req.body.winner_number === null ? null : Number(req.body.winner_number);
    if (winnerNumber !== null && (!Number.isInteger(winnerNumber) || winnerNumber < 0 || winnerNumber > 99)) {
      return res.status(400).json({ error: "invalid_winner_number" });
    }
    addUpdate("winner_number", winnerNumber);
  }

  if (req.body?.winner_user_id !== undefined) {
    const winnerUserId = req.body.winner_user_id === null ? null : Number(req.body.winner_user_id);
    if (winnerUserId !== null && (!Number.isInteger(winnerUserId) || winnerUserId <= 0)) {
      return res.status(400).json({ error: "invalid_winner_user_id" });
    }
    addUpdate("winner_user_id", winnerUserId);
  }

  if (!updates.length) return res.status(400).json({ error: "no_fields_to_update" });

  const pool = await getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const draw = await client.query(
      `SELECT id
         FROM draws
        WHERE id = $1
          AND draw_type IN ('adicional', 'secundario')
        FOR UPDATE`,
      [id]
    );
    if (!draw.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "draw_not_found" });
    }

    if (req.body?.winner_user_id !== undefined && req.body.winner_user_id !== null) {
      const user = await client.query(`SELECT id FROM users WHERE id = $1`, [Number(req.body.winner_user_id)]);
      if (!user.rowCount) {
        await client.query("ROLLBACK");
        return res.status(404).json({ error: "user_not_found" });
      }
    }

    if (req.body?.winner_number !== undefined && req.body.winner_number !== null) {
      const number = await client.query(
        `SELECT n
           FROM numbers
          WHERE draw_id = $1
            AND n = $2`,
        [id, Number(req.body.winner_number)]
      );
      if (!number.rowCount) {
        await client.query("ROLLBACK");
        return res.status(400).json({ error: "winner_number_not_found" });
      }
    }

    const updated = await client.query(
      `
      UPDATE draws
         SET ${updates.join(", ")}
       WHERE id = $1
         AND draw_type IN ('adicional', 'secundario')
       RETURNING id, product_name, product_link, winner_name,
                 winner_number, winner_user_id, realized_at, closed_at, status
      `,
      params
    );

    await client.query("COMMIT");
    return res.json({
      type: "secondary",
      draw_id: updated.rows[0].id,
      product_name: updated.rows[0].product_name || "",
      product_link: updated.rows[0].product_link || "",
      winner_name: updated.rows[0].winner_name || null,
      winner_number: updated.rows[0].winner_number ?? null,
      winner_user_id: updated.rows[0].winner_user_id ?? null,
      realized_at: updated.rows[0].realized_at,
      closed_at: updated.rows[0].closed_at,
      draw_status: updated.rows[0].status,
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[admin/secondary-winners PATCH] error:", e?.code || e?.message || e);
    return res.status(500).json({ error: "update_failed" });
  } finally {
    client.release();
  }
});

export default router;
