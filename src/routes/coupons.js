// backend/src/routes/coupons.js
import { Router } from "express";
import { query } from "../db.js";
import { requireAuth } from "../middleware/auth.js";
import { trayCreateCoupon, trayDeleteCoupon } from "../services/tray.js";

const router = Router();
const VALID_DAYS = Number(process.env.TRAY_COUPON_VALID_DAYS || 180);

function codeForUser(userId) {
  const id = Number(userId || 0);
  const base = `NSU-${String(id).padStart(4, "0")}`;
  const salt = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  const tail =
    salt[(id * 7) % salt.length] + salt[(id * 13) % salt.length];
  return `${base}-${tail}`;
}

function fmtDate(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

/**
 * POST /api/coupons/sync
 */
router.post("/sync", requireAuth, async (req, res) => {
  const rid = Math.random().toString(36).slice(2, 8); // id para amarrar os logs
  try {
    const uid = req.user.id;
    console.log(`[coupons.sync#${rid}] start user=${uid}`);

    // 1) saldo em centavos (pagos)
    const ap = await query(
      `select coalesce(sum(amount_cents),0)::int as cents
         from payments
        where user_id = $1
          and lower(status) in ('approved','paid','pago')`,
      [uid]
    );
    const cents = ap.rows?.[0]?.cents ?? 0;
    console.log(`[coupons.sync#${rid}] saldo cents=`, cents);

    // 2) dados atuais do usuário
    const u = await query(
      `select id, email, coupon_code, tray_coupon_id,
              coalesce(coupon_value_cents,0)::int as coupon_value_cents
         from users
        where id = $1
        limit 1`,
      [uid]
    );
    if (!u.rows.length) {
      console.error(`[coupons.sync#${rid}] user_not_found`);
      return res.status(404).json({ error: "user_not_found" });
    }
    const cur = u.rows[0];
    console.log(`[coupons.sync#${rid}] estado_atual`, {
      coupon_code: cur.coupon_code,
      tray_coupon_id: cur.tray_coupon_id,
      coupon_value_cents: cur.coupon_value_cents,
    });

    // 3) grava SEMPRE o novo valor no banco
    if (cur.coupon_value_cents !== cents) {
      await query(
        `update users set coupon_value_cents=$2 where id=$1`,
        [uid, cents]
      );
      console.log(`[coupons.sync#${rid}] coupon_value_cents atualizado para`, cents);
    }

    // 4) se valor não mudou e já existe cupom na Tray -> só retorna
    if (cur.coupon_value_cents === cents && cur.coupon_code && cur.tray_coupon_id) {
      console.log(`[coupons.sync#${rid}] nada a sincronizar; devolvendo estado atual`);
      return res.json({
        ok: true,
        code: cur.coupon_code,
        value: cents / 100,
        cents,
        id: cur.tray_coupon_id,
        synced: false,
      });
    }

    // 5) apaga cupom anterior na Tray (se existir)
    if (cur.tray_coupon_id) {
      console.log(`[coupons.sync#${rid}] deletando cupom antigo id=${cur.tray_coupon_id}`);
      try { await trayDeleteCoupon(cur.tray_coupon_id); }
      catch (e) { console.warn(`[coupons.sync#${rid}] delete warn:`, e?.message || e); }
    }

    // 6) cria cupom novo na Tray com o novo valor
    const code = (cur.coupon_code && String(cur.coupon_code).trim()) || codeForUser(uid);
    const startsAt = fmtDate(new Date());
    const endsAt = fmtDate(new Date(Date.now() + VALID_DAYS * 86400000));
    console.log(`[coupons.sync#${rid}] criando cupom na Tray`, {
      code, value: cents / 100, startsAt, endsAt
    });

    const created = await trayCreateCoupon({
      code,
      value: (cents / 100),
      startsAt,
      endsAt,
      description: `Crédito do cliente ${uid} - New Store`,
    });
    console.log(`[coupons.sync#${rid}] criado na Tray id=${created.id}`);

    // 7) persiste cupom/ID/valor no users
    const upd = await query(
      `update users
          set coupon_code = $2,
              tray_coupon_id = $3,
              coupon_value_cents = $4,
              coupon_updated_at = now()
        where id = $1`,
      [uid, code, String(created.id), cents]
    );
    console.log(`[coupons.sync#${rid}] persistido no users rowCount=${upd.rowCount}`);

    return res.json({
      ok: true,
      code,
      value: cents / 100,
      cents,
      id: String(created.id),
      synced: true,
    });
  } catch (e) {
    console.error(`[coupons.sync#${rid}] error:`, e?.message || e);
    // Mantém a UI funcionando; o valor já foi gravado no passo 3
    return res.status(200).json({ ok: false, error: "sync_failed" });
  }
});

/**
 * GET /api/coupons/mine
 */
router.get("/mine", requireAuth, async (req, res) => {
  try {
    const uid = req.user.id;
    const r = await query(
      `select coupon_code,
              tray_coupon_id,
              coalesce(coupon_value_cents,0)::int as cents
         from users
        where id = $1
        limit 1`,
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
