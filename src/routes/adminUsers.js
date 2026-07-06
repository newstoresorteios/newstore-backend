// backend/src/routes/admin_users.js
// ESM | CRUD de usuários + atribuição de números (isolado deste router)

import express from "express";
import { query, getPool } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import { creditCouponOnApprovedPayment } from "../services/couponBalance.js";
import { closeDrawIfSoldOut } from "../services/drawLifecycle.js";

const router = express.Router();

// Proteção obrigatória: admin-only
router.use(requireAuth, requireAdmin);

/* =============== helpers =============== */

const mapUser = (r) => ({
  id: Number(r.id),
  name: r.name || "",
  email: r.email || "",
  phone: r.phone || r.celular || "",
  is_admin: !!r.is_admin,
  created_at: r.created_at,
  coupon_code: r.coupon_code || "",
  coupon_value_cents: Number(r.coupon_value_cents || 0),
});

const normStr = (v, max = 255) => String(v ?? "").trim().slice(0, max);
const toInt = (v, def = 0) => {
  const n = Number(v);
  return Number.isFinite(n) ? (n | 0) : def;
};

function safeJsonMeta(meta) {
  try {
    if (!meta || typeof meta !== "object") return "{}";
    return JSON.stringify(meta);
  } catch {
    return "{}";
  }
}

async function getTicketPriceCentsFromAppConfig(q) {
  const r = await q(
    `SELECT value
       FROM public.app_config
      WHERE key = $1
      LIMIT 1`,
    ["ticket_price_cents"]
  );
  const n = Number(r.rows?.[0]?.value);
  if (!Number.isFinite(n) || n <= 0) {
    throw new Error("Invalid or missing app_config.ticket_price_cents");
  }
  return Math.trunc(n);
}

async function hasCouponEventOccurredAtColumn(q) {
  try {
    const r = await q(
      `SELECT 1
         FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name='coupon_balance_history'
          AND column_name='event_occurred_at'
        LIMIT 1`
    );
    return !!r.rows?.length;
  } catch {
    return false;
  }
}

// Normaliza "numbers": aceita array ou CSV e retorna int[] 0..99 (mantém 00 como 0)
function parseNumbers(input) {
  if (Array.isArray(input)) {
    return input
      .map((n) => Number(n))
      .filter((n) => Number.isInteger(n) && n >= 0 && n <= 99);
  }
  const s = String(input || "");
  if (!s) return [];
  return s
    .split(/[,\s;]+/).map((t) => t.trim()).filter(Boolean)
    .map((t) => Number(t))
    .filter((n) => Number.isInteger(n) && n >= 0 && n <= 99);
}

/* =============== LISTAR (com busca/paginação) =============== */
/**
 * GET /api/admin/users
 * Suporta AMBOS:
 *   - ?q=texto&page=1&pageSize=50
 *   - ?q=texto&limit=50&offset=0
 */
router.get("/", async (req, res, next) => {
  try {
    const { q = "" } = req.query;

    // aceita limit/offset OU page/pageSize
    let limit = toInt(req.query.limit, 0);
    let offset = toInt(req.query.offset, 0);

    if (!(limit > 0)) {
      const page = Math.max(1, toInt(req.query.page, 1));
      const pageSize = Math.min(500, Math.max(1, toInt(req.query.pageSize, 50)));
      limit = pageSize;
      offset = (page - 1) * pageSize;
    } else {
      limit = Math.min(500, Math.max(1, limit));
      offset = Math.max(0, offset);
    }

    const like = `%${String(q).trim()}%`;
    const hasQ = String(q).trim().length > 0;

    const cols = `
      id, name, email, phone, is_admin, created_at, coupon_code, coupon_value_cents
    `;
    const base = `FROM public.users`;
    const where = hasQ
      ? ` WHERE (name ILIKE $3
                OR email ILIKE $3
                OR phone ILIKE $3
                OR coupon_code ILIKE $3
                OR CAST(id AS text) ILIKE $3)`
      : ``;
    const order = ` ORDER BY id DESC`;
    const limoff = ` LIMIT $1 OFFSET $2`;

    const params = hasQ ? [limit, offset, like] : [limit, offset];

    // total para paginação
    const totalSql = `SELECT COUNT(1)::int AS total ${base}${where}`;
    const listSql  = `SELECT ${cols} ${base}${where}${order}${limoff}`;

    const [countR, listR] = await Promise.all([
      query(totalSql, hasQ ? [like] : []),
      query(listSql, params),
    ]);

    const total = Number(countR.rows?.[0]?.total || 0);
    const items = (listR.rows || []).map(mapUser);

    res.json({
      users: items,
      total,
      limit,
      offset,
      page: Math.floor(offset / limit) + 1,
      pageSize: limit,
      hasMore: offset + items.length < total,
    });
  } catch (e) {
    next(e);
  }
});

/* =============== OBTER 1 =============== */
/** GET /api/admin/users/:id */
router.get("/:id", async (req, res, next) => {
  try {
    const id = Number(req.params.id);
    const { rows } = await query(
      `SELECT id, name, email, phone, is_admin, created_at, coupon_code, coupon_value_cents
         FROM public.users
        WHERE id = $1`,
      [id]
    );
    if (!rows.length) return res.status(404).json({ error: "not_found" });
    res.json(mapUser(rows[0]));
  } catch (e) {
    next(e);
  }
});

/* =============== CRIAR =============== */
/** POST /api/admin/users
 * body: { name, email, phone, is_admin, coupon_code?, coupon_value_cents? }
 */
router.post("/", async (req, res, next) => {
  const pool = await getPool();
  const client = await pool.connect();
  try {
    const {
      name = "",
      email = "",
      phone = "",
      is_admin = false,
      coupon_code = "",
      coupon_value_cents = 0,
    } = req.body || {};

    const initialBalanceCents = toInt(coupon_value_cents, 0);
    if (initialBalanceCents < 0) {
      return res.status(400).json({ error: "invalid_coupon_balance" });
    }
    const vals = [
      normStr(name, 255),
      normStr(email, 255),
      normStr(phone, 40),
      !!is_admin,
      normStr(coupon_code, 64),
      initialBalanceCents,
    ];

    // Senha padrão "newstore" (hash em bcrypt via pgcrypto)
    const DEFAULT_PASSWORD = "newstore";

    await client.query("BEGIN");
    const { rows } = await client.query(
      `INSERT INTO public.users
         (name, email, phone, is_admin, coupon_code, coupon_value_cents, pass_hash)
       VALUES ($1,$2,$3,$4,$5,$6, crypt($7, gen_salt('bf')))
       RETURNING id, name, email, phone, is_admin, created_at, coupon_code, coupon_value_cents`,
      [...vals, DEFAULT_PASSWORD]
    );

    const created = rows[0];
    const userId = Number(created?.id);
    const hasEventOccurredAt = await hasCouponEventOccurredAtColumn(client.query.bind(client));
    const eventOccurredAtCols = hasEventOccurredAt ? ", event_occurred_at" : "";
    const eventOccurredAtVals = hasEventOccurredAt ? ", now()" : "";

    // Ledger obrigatório: saldo inicial != 0 deve ter histórico correspondente
    if (initialBalanceCents !== 0 && Number.isInteger(userId) && userId > 0) {
      const metaJson = safeJsonMeta({
        previous_balance_cents: 0,
        requested_balance_cents: initialBalanceCents,
        delta_cents: initialBalanceCents,
        source: "adminUsers.create",
        admin_user_id: req.user?.id ?? null,
      });
      await client.query(
        `INSERT INTO public.coupon_balance_history
          (user_id, payment_id, delta_cents, balance_before_cents, balance_after_cents,
           event_type, channel, status, draw_id, reservation_id, run_trace_id, meta${eventOccurredAtCols})
         VALUES
          ($1, NULL, $2, $3, $4,
           'ADMIN_BALANCE_ADJUSTMENT', 'ADMIN', 'approved', NULL, NULL, $5, $6::jsonb${eventOccurredAtVals})`,
        [
          userId,
          initialBalanceCents,
          0,
          initialBalanceCents,
          req.headers["x-request-id"] ? String(req.headers["x-request-id"]) : null,
          metaJson,
        ]
      );
    }

    await client.query("COMMIT");
    res.status(201).json(mapUser(created));
  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    if (e.code === "23505") return res.status(409).json({ error: "duplicated" });
    next(e);
  } finally {
    client.release();
  }
});

/* =============== ATUALIZAR =============== */
/** PUT /api/admin/users/:id
 * body: { name?, email?, phone?, is_admin?, coupon_code?, coupon_value_cents? }
 */
router.put("/:id", async (req, res, next) => {
  const pool = await getPool();
  const client = await pool.connect();
  try {
    const id = Number(req.params.id);
    const { name, email, phone, is_admin, coupon_code, coupon_value_cents } = req.body || {};

    const wantsBalance = coupon_value_cents != null;
    const requestedBalanceCents = wantsBalance ? toInt(coupon_value_cents, 0) : null;
    if (wantsBalance && requestedBalanceCents < 0) {
      return res.status(400).json({ error: "invalid_coupon_balance" });
    }

    await client.query("BEGIN");

    // Lock para garantir ledger + update atômicos
    const cur = await client.query(
      `SELECT id, coupon_value_cents
         FROM public.users
        WHERE id = $1
        FOR UPDATE`,
      [id]
    );
    if (!cur.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "not_found" });
    }
    const previousBalanceCents = Number(cur.rows[0]?.coupon_value_cents || 0) | 0;
    const shouldUpdateBalance = wantsBalance && requestedBalanceCents !== previousBalanceCents;
    const hasEventOccurredAt = await hasCouponEventOccurredAtColumn(client.query.bind(client));
    const eventOccurredAtCols = hasEventOccurredAt ? ", event_occurred_at" : "";
    const eventOccurredAtVals = hasEventOccurredAt ? ", now()" : "";

    const upd = await client.query(
      `UPDATE public.users
          SET name               = COALESCE($2, name),
              email              = COALESCE($3, email),
              phone              = COALESCE($4, phone),
              is_admin           = COALESCE($5, is_admin),
              coupon_code        = COALESCE($6, coupon_code),
              coupon_value_cents = COALESCE($7, coupon_value_cents)
        WHERE id = $1
        RETURNING id, name, email, phone, is_admin, created_at, coupon_code, coupon_value_cents`,
      [
        id,
        name != null ? normStr(name, 255) : null,
        email != null ? normStr(email, 255) : null,
        phone != null ? normStr(phone, 40) : null,
        typeof is_admin === "boolean" ? !!is_admin : null,
        coupon_code != null ? normStr(coupon_code, 64) : null,
        shouldUpdateBalance ? requestedBalanceCents : null,
      ]
    );

    const afterBalanceCents = Number(upd.rows?.[0]?.coupon_value_cents || 0) | 0;
    const deltaCents = afterBalanceCents - previousBalanceCents;

    // Regra obrigatória: qualquer ajuste de saldo pelo admin precisa gerar ledger
    if (shouldUpdateBalance && deltaCents !== 0) {
      const metaJson = safeJsonMeta({
        previous_balance_cents: previousBalanceCents,
        requested_balance_cents: afterBalanceCents,
        delta_cents: deltaCents,
        source: "adminUsers.update",
        admin_user_id: req.user?.id ?? null,
      });

      await client.query(
        `INSERT INTO public.coupon_balance_history
          (user_id, payment_id, delta_cents, balance_before_cents, balance_after_cents,
           event_type, channel, status, draw_id, reservation_id, run_trace_id, meta${eventOccurredAtCols})
         VALUES
          ($1, NULL, $2, $3, $4,
           'ADMIN_BALANCE_ADJUSTMENT', 'ADMIN', 'approved', NULL, NULL, $5, $6::jsonb${eventOccurredAtVals})`,
        [
          id,
          deltaCents,
          previousBalanceCents,
          afterBalanceCents,
          req.headers["x-request-id"] ? String(req.headers["x-request-id"]) : null,
          metaJson,
        ]
      );
    }

    await client.query("COMMIT");
    res.json(mapUser(upd.rows[0]));
  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    if (e.code === "23505") return res.status(409).json({ error: "duplicated" });
    next(e);
  } finally {
    client.release();
  }
});

/* =============== EXCLUIR =============== */
/** DELETE /api/admin/users/:id */
router.delete("/:id", async (req, res, next) => {
  try {
    const id = Number(req.params.id);
    const r = await query("DELETE FROM public.users WHERE id = $1", [id]);
    if (r.rowCount === 0) return res.status(404).json({ error: "not_found" });
    res.status(204).end();
  } catch (e) {
    next(e);
  }
});

/* =============== ATRIBUIR NÚMEROS =============== */
/**
 * POST /api/admin/users/:id/assign-numbers
 * body: {
 *  draw_id: number,
 *  numbers: number[] | "csv",
 *  credit_coupon?: boolean, // default true
 *  no_coupon_credit_reason?: string | null
 * }
 * - Checa conflitos em payments aprovados e reservas ativas
 * - Se ok, cria:
 *    - payments(status='approved')
 *    - reservations(status='paid')
 */
router.post("/:id/assign-numbers", async (req, res, next) => {
  const pool = await getPool();
  const client = await pool.connect();
  try {
    const user_id = Number(req.params.id);
    const draw_id = Number(req.body?.draw_id);
    const numbers = parseNumbers(req.body?.numbers);
    const creditCoupon = req.body?.credit_coupon !== undefined ? !!req.body.credit_coupon : true;
    const noCouponCreditReason = req.body?.no_coupon_credit_reason
      ? String(req.body.no_coupon_credit_reason).slice(0, 240)
      : null;

    if (!Number.isInteger(user_id) || !Number.isInteger(draw_id) || numbers.length === 0) {
      return res.status(400).json({ error: "bad_request" });
    }

    await client.query("BEGIN");

    // garante que o usuário existe
    const u = await client.query("SELECT id FROM public.users WHERE id = $1", [user_id]);
    if (!u.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "user_not_found" });
    }

    // garante sorteio existente
    const d = await client.query("SELECT id FROM public.draws WHERE id = $1", [draw_id]);
    if (!d.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "draw_not_found" });
    }

    // conflitos em payments aprovados
    const payConf = await client.query(
      `SELECT DISTINCT n
         FROM (
           SELECT unnest(p.numbers) AS n
           FROM public.payments p
           WHERE p.draw_id = $1
             AND LOWER(p.status) IN ('approved','paid','pago')
             AND p.numbers && $2::int4[]
         ) s
         WHERE n = ANY ($2::int4[])`,
      [draw_id, numbers]
    );
    if (payConf.rowCount) {
      await client.query("ROLLBACK");
      return res.status(409).json({
        error: "numbers_taken",
        where: "payments",
        conflicts: payConf.rows.map((r) => Number(r.n)).sort((a, b) => a - b),
      });
    }

    // conflitos em reservas ativas (somente pelo array)
    const resvConf = await client.query(
      `SELECT DISTINCT n
         FROM (
           SELECT unnest(r.numbers) AS n
           FROM public.reservations r
           WHERE r.draw_id = $1
             AND LOWER(r.status) IN ('active','pending','paid')
             AND r.numbers && $2::int4[]
         ) x
         WHERE n = ANY ($2::int4[])`,
      [draw_id, numbers]
    );
    if (resvConf.rowCount) {
      await client.query("ROLLBACK");
      return res.status(409).json({
        error: "numbers_reserved",
        where: "reservations",
        conflicts: resvConf.rows.map((r) => Number(r.n)).sort((a, b) => a - b),
      });
    }

    const unit = await getTicketPriceCentsFromAppConfig(client.query.bind(client));
    const amount_cents = numbers.length * unit;
    const payId = `adminassign:${draw_id}:${user_id}:${Date.now()}`;
    const provider = creditCoupon ? "admin_assign" : "admin_assign_no_coupon";
    const nowIso = new Date().toISOString();
    const paymentMeta = safeJsonMeta({
      source: "adminUsers.assignNumbers",
      assignment_source: "adminUsers.assignNumbers",
      admin_user_id: req.user?.id ?? null,
      credit_coupon: creditCoupon,
      no_coupon_credit: !creditCoupon,
      reason: !creditCoupon ? (noCouponCreditReason || "admin_manual_assignment_without_coupon") : null,
      pricing_source: "payments.amount_cents",
      unit_cents: unit,
      qty: numbers.length,
    });

    const pay = await client.query(
      `INSERT INTO public.payments
         (id, user_id, draw_id, numbers, amount_cents, status, created_at, paid_at, provider, coupon_credited, coupon_credited_at, vindi_payload_json)
       VALUES (
         $1, $2, $3, $4::int4[], $5, 'approved', NOW(), NOW(), $6,
         CASE WHEN $7::boolean THEN false ELSE true END,
         CASE WHEN $7::boolean THEN NULL ELSE NOW() END,
         $8::jsonb
       )
       RETURNING id, user_id, draw_id, numbers, amount_cents, status, created_at`,
      [payId, user_id, draw_id, numbers, amount_cents, provider, creditCoupon, paymentMeta]
    );

    // reserva paga; PK uuid gerada pelo banco
    const resv = await client.query(
      `INSERT INTO public.reservations
         (id, user_id, draw_id, numbers, payment_id, status, created_at, expires_at)
       VALUES (gen_random_uuid(), $1, $2, $3::int4[], $4, 'paid', NOW(), NOW() + INTERVAL '30 minutes')
       RETURNING id`,
      [user_id, draw_id, numbers, payId]
    );

    await client.query(
      `UPDATE public.numbers
          SET status = 'sold',
              reservation_id = NULL
        WHERE draw_id = $1
          AND n = ANY($2::int2[])`,
      [draw_id, numbers]
    );

    if (creditCoupon) {
      const creditRes = await creditCouponOnApprovedPayment(String(payId), {
        channel: "ADMIN",
        source: "admin_assign_numbers",
        runTraceId: req.headers["x-request-id"] ? String(req.headers["x-request-id"]) : null,
        pgClient: client,
        meta: {
          pricing_source: "payments.amount_cents",
          assignment_source: "adminUsers.assignNumbers",
          admin_user_id: req.user?.id ?? null,
          credit_coupon: true,
          requested_at: nowIso,
        },
      });
      if (creditRes?.ok === false || ["error", "not_supported", "invalid_amount"].includes(String(creditRes?.action || ""))) {
        throw new Error(`admin_assign_coupon_credit_failed:${creditRes?.action || "unknown"}`);
      }
    }

    await closeDrawIfSoldOut(draw_id, client);

    await client.query("COMMIT");
    res.status(201).json({
      payment: pay.rows[0],
      reservation_id: resv.rows[0]?.id || null,
      credit_coupon: creditCoupon,
      no_coupon_credit_reason: !creditCoupon ? (noCouponCreditReason || "admin_manual_assignment_without_coupon") : null,
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    next(e);
  } finally {
    client.release();
  }
});

export default router;
