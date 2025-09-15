// backend/src/routes/coupons.js
import { Router } from "express";
import { query } from "../db.js";
import { requireAuth } from "../middleware/auth.js";
import { trayCreateCoupon, trayDeleteCoupon } from "../services/tray.js";

const router = Router();

const VALID_DAYS = Number(process.env.TRAY_COUPON_VALID_DAYS || 180);

// backend/src/routes/coupons.js
// ...

// ✅ Código estável e único por usuário (id → NSU-0003-XH)
function codeForUser(userId) {
  const id = Number(userId || 0);
  const base = `NSU-${String(id).padStart(4, '0')}`;
  const salt = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  const tail = salt[(id * 7) % salt.length] + salt[(id * 13) % salt.length];
  return `${base}-${tail}`;
}

function fmtDate(d) {
  // YYYY-MM-DD
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

/**
 * POST /api/coupons/sync
 * - Recalcula o saldo (pagamentos aprovados do usuário)
 * - Se o valor mudou, apaga o cupom antigo na Tray e cria um novo
 * - Persiste coupon_code / tray_coupon_id / coupon_value_cents no users
 */
router.post("/sync", requireAuth, async (req, res) => {
  try {
    const uid = req.user.id;

    // 1) saldo em centavos (pagamentos aprovados)
    const ap = await query(
      `select coalesce(sum(amount_cents),0)::int as cents
         from payments
        where user_id = $1
          and lower(status) in ('approved','paid','pago')`,
      [uid]
    );
    const cents = ap.rows?.[0]?.cents ?? 0;

    // 2) dados atuais do usuário
    const u = await query(
      `select id, email, coupon_code, tray_coupon_id, coalesce(coupon_value_cents,0)::int as coupon_value_cents
         from users
        where id = $1
        limit 1`,
      [uid]
    );
    if (!u.rows.length) return res.status(404).json({ error: "user_not_found" });
    const cur = u.rows[0];

    // Se o valor não mudou, só devolve
    if (cur.coupon_value_cents === cents && cur.coupon_code) {
      return res.json({
        ok: true,
        code: cur.coupon_code,
        value: cents / 100,
        cents,
        id: cur.tray_coupon_id,
        synced: false,
      });
    }

    // 3) apaga cupom anterior (se houver)
    try { await trayDeleteCoupon(cur.tray_coupon_id); } catch {}

    // 4) cria cupom novo na Tray com o novo valor
    const code = codeForUser(uid);
    const startsAt = fmtDate(new Date());
    const endsAt = fmtDate(new Date(Date.now() + VALID_DAYS * 86400000));

    const created = await trayCreateCoupon({
      code,
      value: (cents / 100),
      startsAt,
      endsAt,
      description: `Crédito do cliente ${uid} - New Store`,
    });

    // 5) persiste no users
    await query(
      `update users
          set coupon_code = $2,
              tray_coupon_id = $3,
              coupon_value_cents = $4
        where id = $1`,
      [uid, code, String(created.id), cents]
    );

    return res.json({
      ok: true,
      code,
      value: cents / 100,
      cents,
      id: String(created.id),
      synced: true,
    });
  } catch (e) {
    console.error("[coupons.sync] error:", e?.message || e);
    // Não quebra a página do cliente
    return res.status(200).json({ ok: false, error: "sync_failed" });
  }
});

/**
 * GET /api/coupons/mine
 * - retorna o código/valor atual, *sem* recriar
 */
router.get("/mine", requireAuth, async (req, res) => {
  try {
    const uid = req.user.id;
    const r = await query(
      `select coupon_code, tray_coupon_id, coalesce(coupon_value_cents,0)::int as cents
         from users
        where id = $1 limit 1`,
      [uid]
    );
    if (!r.rows.length) return res.status(404).json({ error: "user_not_found" });
    const row = r.rows[0];
    return res.json({
      ok: true,
      code: row.coupon_code || null,
      id: row.tray_coupon_id || null,
      value: (row.cents || 0) / 100,
      cents: row.cents || 0,
    });
  } catch (e) {
    return res.status(500).json({ error: "read_failed" });
  }
});

export default router;
