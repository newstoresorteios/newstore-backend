// backend/src/routes/coupons.js
import { Router } from "express";
import { query } from "../db.js";
import { requireAuth } from "../middleware/auth.js";
import { trayCreateCoupon, trayDeleteCoupon } from "../services/tray.js";

const router = Router();

const VALID_DAYS = Number(process.env.TRAY_COUPON_VALID_DAYS || 180);

// Código estável e único por usuário (ex.: id 3 -> NSU-0003-XH)
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
 * - Recalcula o saldo (pagamentos aprovados do usuário)
 * - Atualiza SEMPRE coupon_value_cents no banco
 * - Só pula a criação na Tray se já houver tray_coupon_id e o valor não mudou
 * - Caso precise, apaga o cupom anterior na Tray e cria um novo
 */
router.post("/sync", requireAuth, async (req, res) => {
  try {
    const uid = req.user.id;

    // 1) saldo (centavos) com pagamentos aprovados
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
      `select id, email, coupon_code, tray_coupon_id,
              coalesce(coupon_value_cents,0)::int as coupon_value_cents
         from users
        where id = $1
        limit 1`,
      [uid]
    );
    if (!u.rows.length) {
      return res.status(404).json({ error: "user_not_found" });
    }
    const cur = u.rows[0];

    // 3) atualiza SEMPRE o valor no banco (mesmo se a Tray falhar depois)
    if (cur.coupon_value_cents !== cents) {
      try {
        await query(
          `update users set coupon_value_cents=$2 where id=$1`,
          [uid, cents]
        );
      } catch {}
    }

    // 4) se o valor não mudou e já existe cupom criado na Tray -> devolve
    if (
      cur.coupon_value_cents === cents &&
      cur.coupon_code &&
      cur.tray_coupon_id
    ) {
      return res.json({
        ok: true,
        code: cur.coupon_code,
        value: cents / 100,
        cents,
        id: cur.tray_coupon_id,
        synced: false,
      });
    }

    // 5) apaga cupom anterior na Tray (se houver)
    try {
      await trayDeleteCoupon(cur.tray_coupon_id);
    } catch {}

    // 6) cria cupom novo na Tray com o novo valor
    const code =
      (cur.coupon_code && String(cur.coupon_code).trim()) ||
      codeForUser(uid);
    const startsAt = fmtDate(new Date());
    const endsAt = fmtDate(
      new Date(Date.now() + VALID_DAYS * 86400000)
    );

    const created = await trayCreateCoupon({
      code,
      value: cents / 100,
      startsAt,
      endsAt,
      description: `Crédito do cliente ${uid} - New Store`,
    });

    // 7) persiste no users o código/ID da Tray/valor
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
    // Mantém a UI funcionando; o valor já foi gravado no passo 3
    return res.status(200).json({ ok: false, error: "sync_failed" });
  }
});

/**
 * GET /api/coupons/mine
 * - Retorna o código/valor atual sem recriar na Tray
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
    if (!r.rows.length) {
      return res.status(404).json({ error: "user_not_found" });
    }
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
