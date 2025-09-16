import { Router } from "express";
import { query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";

const router = Router();

/**
 * GET /api/admin/clients/active
 * Lista clientes com saldo ativo (última compra aprovada < 6 meses)
 */
router.get("/active", requireAuth, requireAdmin, async (_req, res) => {
  try {
    const r = await query(
      `
      with pays as (
        select
          p.user_id,
          count(*) filter (where lower(trim(coalesce(p.status,''))) = 'approved') as compras,
          coalesce(
            sum(
              case
                when p.amount_cents is not null then p.amount_cents
                when p.total_cents  is not null then p.total_cents
                when p.price_cents  is not null then p.price_cents
                when p.amount       is not null then round(p.amount * 100)
                when p.total        is not null then round(p.total  * 100)
                when p.price        is not null then round(p.price  * 100)
                else 0
              end
            ) filter (where lower(trim(coalesce(p.status,''))) = 'approved'),
            0
          ) as total_cents,
          max(
            coalesce(p.approved_at, p.paid_at, p.created_at)
          ) filter (where lower(trim(coalesce(p.status,''))) = 'approved') as last_buy
        from payments p
        group by p.user_id
      ),
      wins as (
        select winner_user_id as user_id, count(*) as wins
          from draws
         where winner_user_id is not null
         group by winner_user_id
      )
      select
        u.id,
        coalesce(nullif(u.name,''), u.email, '-')      as name,
        u.email,
        u.created_at,
        pa.compras,
        pa.total_cents,
        pa.last_buy,
        coalesce(w.wins, 0)                            as wins,
        (pa.last_buy + interval '6 months')            as expires_at
      from users u
      join pays pa on pa.user_id = u.id
      left join wins w on w.user_id = u.id
      where pa.last_buy >= now() - interval '6 months'
      order by expires_at asc, pa.total_cents desc
      `
    );

    const items = (r.rows || []).map((row) => {
      const expiresAt = row.expires_at ? new Date(row.expires_at) : null;
      const days = expiresAt ? Math.max(0, Math.ceil((expiresAt - new Date()) / 86400000)) : 0;

      return {
        user_id: row.id,
        name: row.name,
        email: row.email,
        created_at: row.created_at,
        purchases_count: row.compras || 0,
        total_brl: Number(((row.total_cents || 0) / 100).toFixed(2)),
        last_buy: row.last_buy,
        wins: row.wins || 0,
        expires_at: row.expires_at,
        days_to_expire: days,
      };
    });

    return res.json({ clients: items });
  } catch (e) {
    console.error("[admin/clients/active] error:", e);
    return res.status(500).json({ error: "list_failed" });
  }
});

export default router;
