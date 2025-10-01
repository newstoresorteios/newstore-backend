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
 * Fluxo novo: recebe um delta (add_cents) e SOMA ao valor atual do usuário.
 * Não recalcula mais a partir de payments para evitar sobrescrita.
 */
router.post("/sync", requireAuth, async (req, res) => {
  const rid = Math.random().toString(36).slice(2, 8); // id para logs

  try {
    const uid = req.user.id;

    // aceita add_cents / addCents e carimbo de sincronização do front
    const addRaw =
      req.body?.add_cents ??
      req.body?.addCents ??
      0;

    // inteiro não-negativo
    const addCents = Number.isFinite(Number(addRaw)) ? Math.max(0, parseInt(addRaw, 10)) : 0;

    const lastSyncAtRaw =
      req.body?.last_payment_sync_at ??
      req.body?.lastPaymentSyncAt ??
      null;

    const lastSyncAt = lastSyncAtRaw && !Number.isNaN(Date.parse(lastSyncAtRaw))
      ? new Date(lastSyncAtRaw)
      : new Date();

    console.log(`[coupons.sync#${rid}] start user=${uid} add_cents=${addCents}`);

    // 1) estado atual do usuário
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
    let cur = u.rows[0];

    // 2) soma o delta (se houver)
    let newCents = cur.coupon_value_cents;
    if (addCents > 0) {
      const upd = await query(
        `update users
            set coupon_value_cents = coalesce(coupon_value_cents,0) + $2,
                coupon_updated_at   = $3
          where id = $1
        returning coalesce(coupon_value_cents,0)::int as coupon_value_cents,
                  coupon_code,
                  tray_coupon_id`,
        [uid, addCents, lastSyncAt]
      );
      cur = upd.rows[0];
      newCents = cur.coupon_value_cents;
      console.log(`[coupons.sync#${rid}] incrementado: +${addCents} => ${newCents}`);
    } else {
      console.log(`[coupons.sync#${rid}] nenhum incremento; mantendo ${newCents}`);
    }

    // 3) garante código determinístico (se não houver)
    const code = (cur.coupon_code && String(cur.coupon_code).trim()) || codeForUser(uid);

    // 4) decide se precisa (re)criar na Tray:
    //    - não existe na Tray ainda
    //    - ou o valor mudou (addCents > 0)
    let trayId = cur.tray_coupon_id || null;
    const mustRecreateTray = !trayId || addCents > 0;

    if (mustRecreateTray) {
      // apaga anterior (se houver)
      if (trayId) {
        try {
          console.log(`[coupons.sync#${rid}] deletando cupom antigo id=${trayId}`);
          await trayDeleteCoupon(trayId);
        } catch (e) {
          console.warn(`[coupons.sync#${rid}] delete warn:`, e?.message || e);
        }
      }

      // cria novo cupom com o total atual
      const startsAt = fmtDate(new Date());
      const endsAt = fmtDate(new Date(Date.now() + VALID_DAYS * 86400000));
      console.log(`[coupons.sync#${rid}] criando cupom na Tray`, {
        code, value: newCents / 100, startsAt, endsAt
      });

      const created = await trayCreateCoupon({
        code,
        value: newCents / 100,
        startsAt,
        endsAt,
        description: `Crédito do cliente ${uid} - New Store`,
      });

      trayId = String(created.id);

      // persiste cupom/ID/valor/código e carimbo
      await query(
        `update users
            set coupon_code = $2,
                tray_coupon_id = $3,
                coupon_value_cents = $4,
                coupon_updated_at = $5
          where id = $1`,
        [uid, code, trayId, newCents, lastSyncAt]
      );

      console.log(`[coupons.sync#${rid}] persistido no users (Tray id=${trayId})`);
    } else if (!cur.coupon_code) {
      // sem recriar Tray, mas assegura que o código esteja salvo
      await query(`update users set coupon_code=$2 where id=$1`, [uid, code]);
    }

    return res.json({
      ok: true,
      code,
      value: newCents / 100,
      cents: newCents,
      id: trayId,
      synced: mustRecreateTray,
    });
  } catch (e) {
    console.error(`[coupons.sync#${rid}] error:`, e?.message || e);
    // mantém a UI funcionando
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
