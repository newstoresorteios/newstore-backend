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
  const tail = salt[(id * 7) % salt.length] + salt[(id * 13) % salt.length];
  return `${base}-${tail}`;
}

function fmtDate(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

// ---------- utils ----------

async function ensureUserColumns() {
  try {
    await query(`
      ALTER TABLE IF EXISTS users
        ADD COLUMN IF NOT EXISTS coupon_code text,
        ADD COLUMN IF NOT EXISTS tray_coupon_id text,
        ADD COLUMN IF NOT EXISTS coupon_value_cents int4 DEFAULT 0,
        ADD COLUMN IF NOT EXISTS coupon_updated_at timestamptz,
        ADD COLUMN IF NOT EXISTS last_payment_sync_at timestamptz
    `);
  } catch {}
}

async function hasColumn(table, column, schema = "public") {
  const { rows } = await query(
    `SELECT 1
       FROM information_schema.columns
      WHERE table_schema = $1
        AND table_name   = $2
        AND column_name  = $3
      LIMIT 1`,
    [schema, table, column]
  );
  return !!rows.length;
}

async function buildTimeExpr() {
  const parts = [];
  if (await hasColumn("payments", "paid_at")) parts.push("COALESCE(paid_at, to_timestamp(0))");
  if (await hasColumn("payments", "approved_at")) parts.push("COALESCE(approved_at, to_timestamp(0))");
  if (await hasColumn("payments", "updated_at")) parts.push("COALESCE(updated_at, to_timestamp(0))");
  // sempre deixa created_at como fallback
  parts.push("COALESCE(created_at, to_timestamp(0))");
  const uniq = Array.from(new Set(parts));
  return uniq.length === 1 ? uniq[0] : `GREATEST(${uniq.join(", ")})`;
}

/**
 * Delta por timestamp (quando existe paid_at/approved_at/updated_at)
 */
async function computeServerDeltaByTime(userId, lastSync) {
  const tExpr = await buildTimeExpr();
  const sql = `
    WITH recent AS (
      SELECT COALESCE(amount_cents,0)::int AS cents,
             ${tExpr} AS t
        FROM payments
       WHERE user_id = $1
         AND lower(status) IN ('approved','paid','pago')
         AND ( $2::timestamptz IS NULL OR ${tExpr} > $2::timestamptz )
    )
    SELECT COALESCE(SUM(cents),0)::int AS delta_cents,
           NULLIF(MAX(t), to_timestamp(0)) AS max_t
      FROM recent
  `;
  const { rows } = await query(sql, [userId, lastSync || null]);
  return {
    delta_cents: rows?.[0]?.delta_cents ?? 0,
    max_t: rows?.[0]?.max_t || null,
  };
}

/**
 * Delta por diferença total (fallback quando NÃO existem colunas de carimbo
 * que indiquem o momento da aprovação).
 */
async function computeServerDeltaByDiff(userId, currentCouponCents) {
  const { rows } = await query(
    `SELECT COALESCE(SUM(amount_cents),0)::int AS total
       FROM payments
      WHERE user_id = $1
        AND lower(status) IN ('approved','paid','pago')`,
    [userId]
  );
  const total = rows?.[0]?.total ?? 0;
  const delta = Math.max(0, total - (Number(currentCouponCents) || 0));
  return {
    delta_cents: delta,
    // no fallback não temos "quando aprovou", então usamos NOW()
    max_t: delta > 0 ? new Date().toISOString() : null,
    total_cents: total,
  };
}

/**
 * Escolhe a estratégia de delta:
 *  - se existir pelo menos uma entre paid_at/approved_at/updated_at → usa tempo;
 *  - senão → usa diferença total.
 */
async function computeDeltaSmart(userId, lastSync, currentCouponCents) {
  const hasPaid = await hasColumn("payments", "paid_at");
  const hasApproved = await hasColumn("payments", "approved_at");
  const hasUpdated = await hasColumn("payments", "updated_at");
  if (hasPaid || hasApproved || hasUpdated) {
    return computeServerDeltaByTime(userId, lastSync);
  }
  return computeServerDeltaByDiff(userId, currentCouponCents);
}

// ---------- rotas ----------

/**
 * POST /api/coupons/sync
 */
router.post("/sync", requireAuth, async (req, res) => {
  const rid = Math.random().toString(36).slice(2, 8);
  try {
    await ensureUserColumns();

    const uid = req.user.id;

    // estado atual
    const uQ = await query(
      `SELECT id,
              COALESCE(coupon_value_cents,0)::int AS coupon_value_cents,
              coupon_code,
              tray_coupon_id,
              last_payment_sync_at
         FROM users
        WHERE id = $1
        LIMIT 1`,
      [uid]
    );
    if (!uQ.rows.length) return res.status(404).json({ error: "user_not_found" });
    const cur = uQ.rows[0];

    // calcula delta de forma inteligente (tempo ou diferença)
    const { delta_cents, max_t, total_cents } =
      await computeDeltaSmart(uid, cur.last_payment_sync_at, cur.coupon_value_cents);

    console.log(
      `[coupons.sync#${rid}] user=${uid} lastSync=${cur.last_payment_sync_at || null} ` +
      `coupon=${cur.coupon_value_cents} delta=${delta_cents} ` +
      (total_cents != null ? `total=${total_cents} ` : "") +
      `maxT=${max_t || null}`
    );

    let newCents = cur.coupon_value_cents;
    let trayId   = cur.tray_coupon_id || null;
    let code     = (cur.coupon_code && String(cur.coupon_code).trim()) || codeForUser(uid);

    if (delta_cents > 0 && max_t) {
      const upd = await query(
        `UPDATE users
            SET coupon_value_cents   = COALESCE(coupon_value_cents,0) + $3,
                coupon_updated_at    = NOW(),
                last_payment_sync_at = $4,
                coupon_code          = COALESCE(coupon_code, $2)
          WHERE id = $1
        RETURNING COALESCE(coupon_value_cents,0)::int AS coupon_value_cents,
                  tray_coupon_id,
                  coupon_code,
                  last_payment_sync_at`,
        [uid, code, delta_cents, max_t]
      );
      newCents = upd.rows[0].coupon_value_cents;
      trayId   = upd.rows[0].tray_coupon_id || null;
      code     = upd.rows[0].coupon_code || code;
      console.log(`[coupons.sync#${rid}] increment applied: +${delta_cents} => ${newCents}`);
    } else {
      // se não há delta mas não existe código, garante o code
      if (!cur.coupon_code) {
        await query(`UPDATE users SET coupon_code=$2 WHERE id=$1`, [uid, code]);
      }
      console.log(`[coupons.sync#${rid}] no increment`);
    }

    // Recria cupom na Tray somente quando valor mudou OU não existir ainda
    const mustRecreateTray = !trayId || newCents !== cur.coupon_value_cents;
    if (mustRecreateTray) {
      if (trayId) {
        try {
          console.log(`[coupons.sync#${rid}] deleting old Tray coupon id=${trayId}`);
          await trayDeleteCoupon(trayId);
        } catch (e) {
          console.warn(`[coupons.sync#${rid}] delete warn:`, e?.message || e);
        }
      }
      const startsAt = fmtDate(new Date());
      const endsAt   = fmtDate(new Date(Date.now() + VALID_DAYS * 86400000));
      console.log(`[coupons.sync#${rid}] creating Tray coupon`, {
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
      await query(
        `UPDATE users
            SET tray_coupon_id = $2,
                coupon_updated_at = NOW()
          WHERE id=$1`,
        [uid, trayId]
      );
    }

    return res.json({
      ok: true,
      code,
      value: newCents / 100,
      cents: newCents,
      id: trayId,
      synced: mustRecreateTray,
      last_payment_sync_at: cur.last_payment_sync_at || null,
    });
  } catch (e) {
    console.error(`[coupons.sync] error:`, e?.message || e);
    return res.status(200).json({ ok: false, error: "sync_failed" });
  }
});

/**
 * GET /api/coupons/mine
 */
router.get("/mine", requireAuth, async (req, res) => {
  try {
    await ensureUserColumns();
    const uid = req.user.id;
    const r = await query(
      `SELECT coupon_code,
              tray_coupon_id,
              COALESCE(coupon_value_cents,0)::int AS cents,
              coupon_updated_at,
              last_payment_sync_at
         FROM users
        WHERE id = $1
        LIMIT 1`,
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
      coupon_updated_at: row.coupon_updated_at || null,
      last_payment_sync_at: row.last_payment_sync_at || null,
    });
  } catch (e) {
    return res.status(500).json({ error: "read_failed" });
  }
});

export default router;
