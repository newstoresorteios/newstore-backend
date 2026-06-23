// backend/src/routes/admin_winners.js
import { Router } from "express";
import { query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";

const router = Router();
const norm = (v, max = 2048) => String(v ?? "").trim().slice(0, max);

/**
 * GET /api/admin/winners
 * Lista sorteios realizados (realized_at IS NOT NULL)
 */
router.get("/", requireAuth, requireAdmin, async (req, res) => {
  try {
    const typeRaw = req.query?.draw_type ?? req.query?.type ?? "principal";
    const type = String(typeRaw).toLowerCase();
    if (type === "secondary" || type === "secundario" || type === "additional" || type === "adicional") {
      const r = await query(
        `
        select
          d.id as draw_id,
          coalesce(nullif(d.winner_name,''), u.name, u.email, '-') as winner_name,
          d.winner_user_id,
          d.winner_number,
          d.realized_at,
          d.closed_at,
          d.status as draw_status,
          d.product_name,
          d.product_link
        from public.draws d
        left join public.users u on u.id = d.winner_user_id
        where d.draw_type in ('adicional', 'secundario')
          and (
            d.realized_at is not null
            or d.winner_user_id is not null
            or d.winner_number is not null
          )
        order by coalesce(d.realized_at, d.closed_at, d.opened_at, d.created_at) desc, d.id desc
        `
      );

      const now = Date.now();
      const winners = (r.rows || []).map((row) => {
        const realized = row.realized_at ? new Date(row.realized_at) : null;
        const daysSince = realized ? Math.max(0, Math.floor((now - realized.getTime()) / 86400000)) : 0;
        const redeemed = !!row.closed_at;
        return {
          type: "additional",
          draw_id: row.draw_id,
          winner_user_id: row.winner_user_id ?? null,
          winner_name: row.winner_name || "-",
          winner_number: row.winner_number ?? null,
          realized_at: row.realized_at,
          closed_at: row.closed_at,
          draw_status: row.draw_status,
          product_name: row.product_name || "",
          product_link: row.product_link || "",
          redeemed,
          status: redeemed ? "RESGATADO" : "NAO RESGATADO",
          days_since: daysSince,
        };
      });

      return res.json({ winners });
    }

    const r = await query(
      `
      select
        d.id                                                as draw_id,
        coalesce(nullif(d.winner_name,''), u.name, u.email, '-') as winner_name,
        d.winner_number,
        d.realized_at,
        d.closed_at,
        d.product_name,
        d.product_link
      from public.draws d
      left join public.users u on u.id = d.winner_user_id
      where d.realized_at is not null
        and coalesce(d.draw_type, 'principal') = 'principal'
      order by d.realized_at desc, d.id desc
      `
    );

    const now = Date.now();
    const winners = (r.rows || []).map((row) => {
      const realized = row.realized_at ? new Date(row.realized_at) : null;
      const daysSince = realized ? Math.max(0, Math.floor((now - realized.getTime()) / 86400000)) : 0;
      const redeemed = !!row.closed_at;
      return {
        draw_id: row.draw_id,
        winner_name: row.winner_name || "-",
        winner_number: row.winner_number ?? null,
        realized_at: row.realized_at,
        closed_at: row.closed_at,
        product_name: row.product_name || "",
        product_link: row.product_link || "",
        redeemed,
        status: redeemed ? "RESGATADO" : "NÃO RESGATADO",
        days_since: daysSince,
      };
    });

    return res.json({ winners });
  } catch (e) {
    console.error("[admin/winners] error:", e);
    return res.status(500).json({ error: "list_failed" });
  }
});

/**
 * PATCH /api/admin/winners/:id
 * body: { product_name?, product_link? }
 */
router.patch("/:id", requireAuth, requireAdmin, async (req, res) => {
  try {
    const id = Number(req.params.id);
    const typeRaw = req.query?.draw_type ?? req.query?.type ?? "principal";
    const type = String(typeRaw).toLowerCase();
    const { product_name, product_link, winner_name, winner_number, winner_user_id } = req.body || {};

    if (type === "secondary" || type === "secundario" || type === "additional" || type === "adicional") {
      const updates = [];
      const params = [id];
      const addUpdate = (column, value) => {
        params.push(value);
        updates.push(`${column} = $${params.length}`);
      };

      if (product_name !== undefined) addUpdate("product_name", product_name === null ? null : norm(product_name, 255));
      if (product_link !== undefined) addUpdate("product_link", product_link === null ? null : norm(product_link, 2048));
      if (winner_name !== undefined) addUpdate("winner_name", winner_name === null ? null : norm(winner_name, 255));

      if (winner_number !== undefined) {
        const n = winner_number === null ? null : Number(winner_number);
        if (n !== null && (!Number.isInteger(n) || n < 0 || n > 99)) {
          return res.status(400).json({ error: "invalid_winner_number" });
        }
        addUpdate("winner_number", n);
      }

      if (winner_user_id !== undefined) {
        const userId = winner_user_id === null ? null : Number(winner_user_id);
        if (userId !== null && (!Number.isInteger(userId) || userId <= 0)) {
          return res.status(400).json({ error: "invalid_winner_user_id" });
        }
        addUpdate("winner_user_id", userId);
      }

      if (!updates.length) return res.status(400).json({ error: "no_fields_to_update" });

      const { rows } = await query(
        `
        update public.draws
           set ${updates.join(", ")}
         where id = $1
           and draw_type in ('adicional', 'secundario')
         returning id, product_name, product_link, winner_name, winner_number, winner_user_id
        `,
        params
      );

      if (!rows.length) return res.status(404).json({ error: "not_found" });

      return res.json({
        type: "additional",
        draw_id: rows[0].id,
        product_name: rows[0].product_name || "",
        product_link: rows[0].product_link || "",
        winner_name: rows[0].winner_name || null,
        winner_number: rows[0].winner_number ?? null,
        winner_user_id: rows[0].winner_user_id ?? null,
      });
    }

    const { rows } = await query(
      `
      update public.draws
         set product_name = coalesce($2, product_name),
             product_link = coalesce($3, product_link)
       where id = $1
         and coalesce(draw_type, 'principal') = 'principal'
       returning id, product_name, product_link
      `,
      [
        id,
        product_name != null ? norm(product_name, 255) : null,
        product_link != null ? norm(product_link, 2048) : null,
      ]
    );

    if (!rows.length) return res.status(404).json({ error: "not_found" });

    return res.json({
      draw_id: rows[0].id,
      product_name: rows[0].product_name || "",
      product_link: rows[0].product_link || "",
    });
  } catch (e) {
    console.error("[admin/winners PATCH] error:", e);
    return res.status(500).json({ error: "update_failed" });
  }
});

export default router;
