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

// Garante colunas usadas (compat com esquemas legados)
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

/**
 * Calcula, no servidor, o delta de pagamentos aprovados **após** lastSync.
 * Retorna { delta_cents, max_t }.
 */
async function computeServerDelta(userId, lastSync) {
  const { rows } = await query(
    `
    WITH recent AS (
      SELECT
        COALESCE(amount_cents,0)::int AS cents,
        GREATEST(
          COALESCE(paid_at,    to_timestamp(0)),
          COALESCE(updated_at, to_timestamp(0)),
          COALESCE(created_at, to_timestamp(0))
        ) AS t
      FROM payments
      WHERE user_id = $1
        AND lower(status) IN ('approved','paid','pago')
        AND ( $2::timestamptz IS NULL OR
              GREATEST(
                COALESCE(paid_at,    to_timestamp(0)),
                COALESCE(updated_at, to_timestamp(0)),
                COALESCE(created_at, to_timestamp(0))
              ) > $2::timestamptz
        )
    )
    SELECT
      COALESCE(SUM(cents),0)::int AS delta_cents,
      NULLIF(MAX(t), to_timestamp(0)) AS max_t
    FROM recent
    `,
    [userId, lastSync || null]
  );
  return {
    delta_cents: rows?.[0]?.delta_cents ?? 0,
    max_t: rows?.[0]?.max_t || null,
  };
}

/**
 * POST /api/coupons/sync
 * Idempotente e agora **independente do add_cents do cliente**.
 */
router.post("/sync", requireAuth, async (req, res) => {
  const rid = Math.random().toString(36).slice(2, 8);
  try {
    await ensureUserColumns();

    const uid = req.user.id;

    // Recebemos mas NÃO usamos para cap — somente para log / compat.
    const clientAddRaw = req.body?.add_cents ?? req.body?.addCents ?? null;
    const clientAddCents = Number.isFinite(Number(clientAddRaw))
      ? parseInt(clientAddRaw, 10)
      : null;

    // 1) Estado atual do usuário
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

    // 2) Calcula delta real no servidor (ignora o cliente)
    const { delta_cents, max_t } = await computeServerDelta(uid, cur.last_payment_sync_at);

    // ⇩⇩⇩ mudança: SEM "cap"; aplicamos exatamente o delta do servidor
    const applyCents = Math.max(0, delta_cents);

    console.log(
      `[coupons.sync#${rid}] user=${uid} clientAdd=${clientAddCents} serverDelta=${delta_cents} apply=${applyCents} lastSync=${cur.last_payment_sync_at || null} maxT=${max_t || null}`
    );

    let newCents = cur.coupon_value_cents;
    let trayId = cur.tray_coupon_id || null;
    let code = (cur.coupon_code && String(cur.coupon_code).trim()) || codeForUser(uid);

    // 3) Aplica incremento apenas se houver delta **e** max_t for mais novo
    if (applyCents > 0 && max_t) {
      const upd = await query(
        `UPDATE users
            SET coupon_value_cents   = COALESCE(coupon_value_cents,0) + $3,
                coupon_updated_at    = NOW(),
                last_payment_sync_at = $4
          WHERE id = $1
            AND (last_payment_sync_at IS NULL OR $4 > last_payment_sync_at)
        RETURNING COALESCE(coupon_value_cents,0)::int AS coupon_value_cents,
                  coupon_code,
                  tray_coupon_id,
                  last_payment_sync_at`,
        [uid, code, applyCents, max_t]
      );

      if (upd.rowCount > 0) {
        newCents = upd.rows[0].coupon_value_cents;
        trayId   = upd.rows[0].tray_coupon_id || null;
        code     = upd.rows[0].coupon_code || code;
        console.log(`[coupons.sync#${rid}] increment applied: +${applyCents} -> ${newCents}`);
      } else {
        // Outra requisição aplicou antes; recarrega estado
        const ref = await query(
          `SELECT COALESCE(coupon_value_cents,0)::int AS coupon_value_cents,
                  coupon_code, tray_coupon_id, last_payment_sync_at
             FROM users WHERE id=$1 LIMIT 1`,
          [uid]
        );
        newCents = ref.rows[0].coupon_value_cents;
        trayId   = ref.rows[0].tray_coupon_id || null;
        code     = ref.rows[0].coupon_code || code;
        console.log(`[coupons.sync#${rid}] race/no-op; cents=${newCents}`);
      }
    } else {
      console.log(`[coupons.sync#${rid}] no increment (apply=${applyCents})`);
    }

    // 4) Garante código
    if (!cur.coupon_code) {
      await query(`UPDATE users SET coupon_code=$2 WHERE id=$1`, [uid, code]);
    }

    // 5) Atualiza cupom na Tray apenas se o valor mudou OU não existir
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
      const endsAt = fmtDate(new Date(Date.now() + VALID_DAYS * 86400000));
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
