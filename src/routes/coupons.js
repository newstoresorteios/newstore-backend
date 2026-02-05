// backend/src/routes/coupons.js
import { Router } from "express";
import { query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import { ensureTrayCouponForUser } from "../services/trayCouponEnsure.js";
import { creditCouponOnApprovedPayment } from "../services/couponBalance.js";

const router = Router();

function codeForUser(userId) {
  const id = Number(userId || 0);
  const base = `NSU-${String(id).padStart(4, "0")}`;
  const salt = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  const tail = salt[(id * 7) % salt.length] + salt[(id * 13) % salt.length];
  return `${base}-${tail}`;
}

// ---------- helpers de schema/tempo ----------

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
      WHERE table_schema=$1 AND table_name=$2 AND column_name=$3
      LIMIT 1`,
    [schema, table, column]
  );
  return !!rows.length;
}

/**
 * Constrói a expressão de tempo usada para calcular o delta.
 * IMPORTANTE: não usar updated_at para evitar “ressuscitar” pagamentos antigos.
 */
async function buildTimeExpr() {
  const parts = [];
  if (await hasColumn("payments", "paid_at"))     parts.push("COALESCE(paid_at, to_timestamp(0))");
  if (await hasColumn("payments", "approved_at")) parts.push("COALESCE(approved_at, to_timestamp(0))");
  // fallback estável
  parts.push("COALESCE(created_at, to_timestamp(0))");
  const uniq = Array.from(new Set(parts));
  return uniq.length === 1 ? uniq[0] : `GREATEST(${uniq.join(", ")})`;
}

// ---------- rotas ----------

/**
 * POST /api/coupons/sync
 * Idempotente e à prova de corrida.
 */
router.post("/sync", requireAuth, async (req, res) => {
  const rid = Math.random().toString(36).slice(2, 8);
  const uid = req.user.id;
  try {
    await ensureUserColumns();

    // estado atual mínimo (fora da transação, só para saber tray/code)
    const curQ = await query(
      `SELECT id,
              COALESCE(coupon_value_cents,0)::int AS coupon_value_cents,
              coupon_code,
              tray_coupon_id,
              last_payment_sync_at
         FROM users
        WHERE id=$1
        LIMIT 1`,
      [uid]
    );
    if (!curQ.rows.length) return res.status(404).json({ error: "user_not_found" });
    let cur = curQ.rows[0];

    const code = (cur.coupon_code && String(cur.coupon_code).trim()) || codeForUser(uid);
    let trayId = cur.tray_coupon_id || null;
    const beforeCents = cur.coupon_value_cents || 0;

    // --- reconcile credit (idempotente): credita payments aprovados ainda não creditados
    const hasCreditedFlag = await hasColumn("payments", "coupon_credited");
    let reconciledPayments = 0;
    if (hasCreditedFlag) {
      const { rows: pend } = await query(
        `SELECT id,
                COALESCE(provider,'mercadopago') AS provider
           FROM payments
          WHERE user_id = $1
            AND lower(status) IN ('approved','paid','pago')
            AND coupon_credited = false
          ORDER BY id ASC
          LIMIT 500`,
        [uid]
      );

      for (const p of pend) {
        // eslint-disable-next-line no-await-in-loop
        await creditCouponOnApprovedPayment(String(p.id), {
          channel: String(p.provider || "").toLowerCase() === "vindi" ? "VINDI" : "PIX",
          source: "reconcile_sync",
          runTraceId: `coupons.sync#${rid}`,
          meta: { unit_cents: 5500 },
        });
        reconciledPayments++;
      }
    }

    // Atualiza campos auxiliares (sem somar saldo por timestamp)
    const tExpr = await buildTimeExpr();

    await query("BEGIN");
    const auxSql = `
      WITH mx AS (
        SELECT NULLIF(MAX(${tExpr}), to_timestamp(0)) AS max_t
          FROM payments
         WHERE user_id = $1
           AND lower(status) IN ('approved','paid','pago')
      )
      UPDATE users
         SET coupon_code = COALESCE(coupon_code, $2),
             last_payment_sync_at = COALESCE(GREATEST(last_payment_sync_at, (SELECT max_t FROM mx)), last_payment_sync_at, (SELECT max_t FROM mx)),
             coupon_updated_at = NOW()
       WHERE id = $1
       RETURNING last_payment_sync_at;
    `;
    const aux = await query(auxSql, [uid, code]);
    await query("COMMIT");
    const newSync = aux?.rows?.[0]?.last_payment_sync_at || cur.last_payment_sync_at;

    // 🔧 garante coupon_code mesmo com delta=0 (sem alterar lógica de valores)
    if (!cur.coupon_code) {
      try {
        await query(
          `UPDATE users
              SET coupon_code = $2,
                  coupon_updated_at = COALESCE(coupon_updated_at, NOW())
            WHERE id = $1
              AND coupon_code IS NULL`,
          [uid, code]
        );
        cur.coupon_code = code;
      } catch {}
    }

    // Recarrega saldo final após reconcile
    const afterQ = await query(
      `SELECT COALESCE(coupon_value_cents,0)::int AS coupon_value_cents,
              tray_coupon_id
         FROM users
        WHERE id=$1
        LIMIT 1`,
      [uid]
    );
    let finalCents = afterQ.rows?.[0]?.coupon_value_cents ?? beforeCents;
    trayId = afterQ.rows?.[0]?.tray_coupon_id || trayId;

    console.log(
      `[coupons.sync#${rid}] user=${uid} reconciled=${reconciledPayments} coupon_before=${beforeCents} coupon_after=${finalCents} newSync=${newSync || null}`
    );

    // Regra: NÃO deletar/recriar cupom na Tray (mantém o "mesmo cupom do cliente").
    // Sempre chama ensure e deixa ela atualizar por tray_coupon_id ou por code.
    let ensured = null;
    try {
      ensured = await ensureTrayCouponForUser(uid);
      const ensuredTrayId = ensured?.trayId != null ? String(ensured.trayId) : null;
      if (ensuredTrayId && String(trayId || "") !== ensuredTrayId) {
        trayId = ensuredTrayId;
        // Atualiza o id apenas se mudou (evita writes desnecessários)
        await query(
          `UPDATE users
              SET tray_coupon_id = $2,
                  coupon_updated_at = NOW()
            WHERE id = $1
              AND (tray_coupon_id IS DISTINCT FROM $2)`,
          [uid, trayId]
        );
      }
    } catch (e) {
      // ok: mantemos valor no banco mesmo que a Tray falhe
      console.warn(`[tray.coupon.sync#${rid}] tray ensure warn:`, e?.message || e);
    }

    return res.json({
      ok: true,
      code,
      value: finalCents / 100,
      cents: finalCents,
      id: trayId,
      synced: ensured?.status === "SYNCED",
      last_payment_sync_at: newSync || null,
    });
  } catch (e) {
    try { await query("ROLLBACK"); } catch {}
    console.error(`[coupons.sync#${rid}] error:`, e?.message || e);
    // Valor já pode ter sido ajustado dentro da transação; mantém UI funcional.
    return res.status(200).json({ ok: false, error: "sync_failed" });
  }
});

/**
 * POST /api/coupons/ensure
 * Chamado pelo frontend após login (best-effort).
 */
router.post("/ensure", requireAuth, async (req, res) => {
  const userId = req.user?.id;
  console.log(`[api.coupons.ensure] called user=${userId}`);

  // best-effort: nunca retornar erro para UX
  const out = await ensureTrayCouponForUser(userId, { timeoutMs: 5000 });
  return res.json({
    ok: true,
    tray: {
      status: out.status,
      action: out.action,
      code: out.code || null,
      trayId: out.trayId || null,
    },
  });
});

/**
 * POST /api/coupons/sync-tray-pending
 * Admin-only: reprocessa cupons pendentes/failed (consistência eventual).
 */
router.post("/sync-tray-pending", requireAuth, requireAdmin, async (req, res) => {
  const batch = Math.random().toString(36).slice(2, 8);
  const limit = Math.max(1, Math.min(200, Number(req.body?.limit || 50)));
  try {
    const { rows } = await query(
      `select user_id
         from coupon_tray_sync
        where tray_sync_status <> 'SYNCED'
        order by updated_at asc
        limit $1`,
      [limit]
    );
    const pending = rows.length;
    let ok = 0;
    let failed = 0;

    console.log(`[tray.coupon.sync] batch=${batch} pending=${pending}`);
    for (const r of rows) {
      // eslint-disable-next-line no-await-in-loop
      const out = await ensureTrayCouponForUser(r.user_id);
      if (out?.ok) ok++;
      else failed++;
    }

    console.log(`[tray.coupon.sync] batch=${batch} pending=${pending} ok=${ok} failed=${failed}`);
    return res.json({ ok: true, batch, pending, okCount: ok, failedCount: failed });
  } catch (e) {
    console.warn(`[tray.coupon.sync] batch=${batch} error:`, e?.message || e);
    return res.status(200).json({ ok: false, batch, error: "sync_failed" });
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
