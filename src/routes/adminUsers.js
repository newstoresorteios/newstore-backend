// backend/src/routes/admin_users.js
// ESM | CRUD de usuários + atribuição de números (isolado deste router)

import express from "express";
import { query, getPool } from "../db.js";

const router = express.Router();

/* =============== helpers =============== */

const mapUser = (r) => ({
  id: Number(r.id),
  name: r.name || "",
  email: r.email || "",
  phone: r.phone || r.celular || "", // fallback se sua base ainda usar "celular"
  is_admin: !!r.is_admin,
  created_at: r.created_at,
});

const toPgIntArrayText = (arr) =>
  "{" +
  (Array.isArray(arr) ? arr.map((n) => (Number.isFinite(+n) ? (n | 0) : 0)).join(",") : "") +
  "}";

// Se quiser travar por admin, descomente este middleware e o use abaixo
// function requireAdmin(req, res, next) {
//   if (req?.user?.is_admin) return next();
//   return res.status(403).json({ error: "forbidden" });
// }
// router.use(requireAdmin);

/* =============== LISTAR (com busca/paginação) =============== */
/**
 * GET /api/admin/users
 * ?q=texto&page=1&pageSize=50
 */
router.get("/", async (req, res, next) => {
  try {
    const { q = "", page = "1", pageSize = "50" } = req.query;
    const p = Math.max(1, parseInt(page, 10) || 1);
    const ps = Math.min(200, Math.max(1, parseInt(pageSize, 10) || 50));
    const off = (p - 1) * ps;

    const like = `%${String(q).trim()}%`;
    const hasQ = String(q).trim().length > 0;

    const base = "SELECT id, name, email, phone, is_admin, created_at FROM public.users";
    const where = hasQ
      ? " WHERE (name ILIKE $3 OR email ILIKE $3 OR phone ILIKE $3 OR CAST(id AS text) ILIKE $3)"
      : "";
    const order = " ORDER BY id DESC";
    const limit = " LIMIT $1 OFFSET $2";
    const params = hasQ ? [ps, off, like] : [ps, off];

    const { rows } = await query(base + where + order + limit, params);
    res.json({ users: rows.map(mapUser), page: p, pageSize: ps });
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
      "SELECT id, name, email, phone, is_admin, created_at FROM public.users WHERE id = $1",
      [id]
    );
    if (!rows.length) return res.status(404).json({ error: "not_found" });
    res.json(mapUser(rows[0]));
  } catch (e) {
    next(e);
  }
});

/* =============== CRIAR =============== */
/** POST /api/admin/users  { name, email, phone, is_admin } */
router.post("/", async (req, res, next) => {
  try {
    const { name = "", email = "", phone = "", is_admin = false } = req.body || {};
    const { rows } = await query(
      `INSERT INTO public.users (name, email, phone, is_admin)
       VALUES ($1,$2,$3,$4)
       RETURNING id, name, email, phone, is_admin, created_at`,
      [String(name).trim(), String(email).trim(), String(phone).trim(), !!is_admin]
    );
    res.status(201).json(mapUser(rows[0]));
  } catch (e) {
    if (e.code === "23505") return res.status(409).json({ error: "duplicated" });
    next(e);
  }
});

/* =============== ATUALIZAR =============== */
/** PUT /api/admin/users/:id  { name?, email?, phone?, is_admin? } */
router.put("/:id", async (req, res, next) => {
  try {
    const id = Number(req.params.id);
    const { name, email, phone, is_admin } = req.body || {};
    const { rows } = await query(
      `UPDATE public.users
         SET name     = COALESCE($2, name),
             email    = COALESCE($3, email),
             phone    = COALESCE($4, phone),
             is_admin = COALESCE($5, is_admin)
       WHERE id = $1
       RETURNING id, name, email, phone, is_admin, created_at`,
      [
        id,
        name != null ? String(name).trim() : null,
        email != null ? String(email).trim() : null,
        phone != null ? String(phone).trim() : null,
        typeof is_admin === "boolean" ? is_admin : null,
      ]
    );
    if (!rows.length) return res.status(404).json({ error: "not_found" });
    res.json(mapUser(rows[0]));
  } catch (e) {
    if (e.code === "23505") return res.status(409).json({ error: "duplicated" });
    next(e);
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
 * body: { draw_id: number, numbers: number[], amount_cents?: number }
 * - Checa conflitos em payments aprovados e reservas ativas
 * - Se ok, insere um payment 'approved' para o usuário
 */
router.post("/:id/assign-numbers", async (req, res, next) => {
  const pool = await getPool();
  const client = await pool.connect();
  try {
    const user_id = Number(req.params.id);
    const draw_id = Number(req.body?.draw_id);
    const rawNumbers = Array.isArray(req.body?.numbers) ? req.body.numbers : [];
    const amount_cents = Number.isFinite(+req.body?.amount_cents)
      ? Math.max(0, +req.body.amount_cents)
      : 0;

    if (!Number.isInteger(user_id) || !Number.isInteger(draw_id) || rawNumbers.length === 0) {
      return res.status(400).json({ error: "bad_request" });
    }

    const numbers = rawNumbers
      .map((n) => Number(n))
      .filter((n) => Number.isInteger(n) && n >= 0 && n <= 99);
    if (!numbers.length) return res.status(400).json({ error: "no_numbers" });

    const pgArrayText = toPgIntArrayText(numbers);

    await client.query("BEGIN");

    const u = await client.query("SELECT id FROM public.users WHERE id = $1", [user_id]);
    if (!u.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "user_not_found" });
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
      [draw_id, pgArrayText]
    );
    if (payConf.rowCount) {
      await client.query("ROLLBACK");
      return res.status(409).json({
        error: "numbers_taken",
        where: "payments",
        conflicts: payConf.rows.map((r) => Number(r.n)).sort((a, b) => a - b),
      });
    }

    // conflitos em reservas ativas
    const resvConf = await client.query(
      `SELECT DISTINCT n FROM (
         SELECT unnest(r.numbers) AS n
         FROM public.reservations r
         WHERE r.draw_id = $1
           AND LOWER(r.status) IN ('active','pending')
           AND r.numbers && $2::int4[]
         UNION ALL
         SELECT r.n AS n
         FROM public.reservations r
         WHERE r.draw_id = $1
           AND LOWER(r.status) IN ('active','pending')
           AND r.n = ANY($2::int4[])
       ) x
       WHERE n IS NOT NULL`,
      [draw_id, pgArrayText]
    );
    if (resvConf.rowCount) {
      await client.query("ROLLBACK");
      return res.status(409).json({
        error: "numbers_reserved",
        where: "reservations",
        conflicts: resvConf.rows.map((r) => Number(r.n)).sort((a, b) => a - b),
      });
    }

    // grava payment aprovado
    const inserted = await client.query(
      `INSERT INTO public.payments (user_id, draw_id, numbers, amount_cents, status, created_at)
       VALUES ($1, $2, $3::int4[], $4, 'approved', NOW())
       RETURNING id, user_id, draw_id, numbers, amount_cents, status, created_at`,
      [user_id, draw_id, pgArrayText, amount_cents]
    );

    await client.query("COMMIT");
    res.status(201).json(inserted.rows[0]);
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    next(e);
  } finally {
    client.release();
  }
});

export default router;
