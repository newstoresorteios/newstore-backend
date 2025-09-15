// backend/src/routes/auth.js
import express from 'express';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import nodemailer from 'nodemailer';

import { query } from '../db.js';
import { requireAuth } from '../middleware/auth.js';

const router = express.Router();

const JWT_SECRET =
  process.env.JWT_SECRET ||
  process.env.SUPABASE_JWT_SECRET ||
  'change-me-in-env';

const TOKEN_TTL   = process.env.JWT_TTL || '7d';
const COOKIE_NAME = process.env.AUTH_COOKIE_NAME || 'ns_auth';
const IS_PROD     = (process.env.NODE_ENV || 'production') === 'production';

function signToken(payload) {
  return jwt.sign(payload, JWT_SECRET, { expiresIn: TOKEN_TTL });
}

async function verifyPassword(plain, hashed) {
  if (!hashed) return false;
  try {
    const h = String(hashed);
    if (h.startsWith('$2')) return await bcrypt.compare(String(plain), h); // bcrypt
    if (!h.startsWith('$')) return String(plain) === h; // legado texto-plain
    return false;
  } catch {
    return false;
  }
}

// Gera um cupom determinístico por usuário (só se ainda não existir)
function makeUserCouponCode(userId) {
  const id = Number(userId || 0);
  const base = `NSU-${String(id).padStart(4, '0')}`;
  const salt = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  const tail = salt[(id * 7) % salt.length] + salt[(id * 13) % salt.length];
  return `${base}-${tail}`;
}

async function hydrateUserFromDB(id, email) {
  let r = null;
  if (id) {
    r = await query(
      `SELECT id, name, email, is_admin, coupon_code, coupon_updated_at
         FROM users WHERE id=$1 LIMIT 1`, [id]
    );
  }
  if ((!r || !r.rows.length) && email) {
    r = await query(
      `SELECT id, name, email, is_admin, coupon_code, coupon_updated_at
         FROM users WHERE LOWER(email)=LOWER($1) LIMIT 1`, [email]
    );
  }
  if (!r || !r.rows.length) return null;
  let u = r.rows[0];

  // cria o coupon_code se ainda não existir (não mexe em tray aqui)
  if (!u.coupon_code) {
    const code = makeUserCouponCode(u.id);
    const upd = await query(
      `UPDATE users SET coupon_code=$2, coupon_updated_at=NOW()
        WHERE id=$1
        RETURNING id, name, email, is_admin, coupon_code, coupon_updated_at`,
      [u.id, code]
    );
    u = upd.rows[0];
  }

  return {
    id: u.id,
    name: u.name,
    email: u.email,
    role: u.is_admin ? 'admin' : 'user',
    coupon_code: u.coupon_code || null,
    coupon_updated_at: u.coupon_updated_at || null,
  };
}

// Busca usuário por e-mail cobrindo colunas/tabelas legadas
async function findUserByEmail(emailRaw) {
  const email = String(emailRaw).trim();
  try { await query('SELECT 1', []); } catch {}

  const variants = [
    { sql: `SELECT id, email, pass_hash AS hash, CASE WHEN is_admin THEN 'admin' ELSE 'user' END AS role
            FROM users WHERE LOWER(email)=LOWER($1) LIMIT 1`, args: [email] },
    { sql: `SELECT id, email, password_hash AS hash, CASE WHEN is_admin THEN 'admin' ELSE 'user' END AS role
            FROM users WHERE LOWER(email)=LOWER($1) LIMIT 1`, args: [email] },
    { sql: `SELECT id, email, password AS hash, CASE WHEN is_admin THEN 'admin' ELSE 'user' END AS role
            FROM users WHERE LOWER(email)=LOWER($1) LIMIT 1`, args: [email] },
    { sql: `SELECT id, email, password_hash AS hash, role FROM admin_users
            WHERE LOWER(email)=LOWER($1) LIMIT 1`, args: [email] },
    { sql: `SELECT id, email, password AS hash, 'admin' AS role FROM admins
            WHERE LOWER(email)=LOWER($1) LIMIT 1`, args: [email] },
  ];

  for (const v of variants) {
    try {
      const { rows } = await query(v.sql, v.args);
      if (rows && rows.length) return rows[0];
    } catch {/* ignora 42P01/42703 etc. */}
  }
  return null;
}

router.post('/register', async (req, res) => {
  try {
    const { name, email, password } = req.body || {};
    if (!name || !email || !password) {
      return res.status(400).json({ error: 'invalid_payload' });
    }

    const emailNorm = String(email).trim().toLowerCase();
    const dupe = await query('SELECT 1 FROM users WHERE LOWER(email)=LOWER($1)', [emailNorm]);
    if (dupe.rows.length) return res.status(409).json({ error: 'email_in_use' });

    const hash = await bcrypt.hash(String(password), 10);
    const ins = await query(
      `INSERT INTO users (name, email, pass_hash)
       VALUES ($1,$2,$3)
       RETURNING id, name, email, CASE WHEN is_admin THEN 'admin' ELSE 'user' END AS role`,
      [name, emailNorm, hash]
    );

    const u = ins.rows[0];
    const token = signToken({ sub: u.id, email: u.email, name: u.name, role: u.role });

    res.cookie(COOKIE_NAME, token, {
      httpOnly: true,
      secure: IS_PROD,
      sameSite: IS_PROD ? 'none' : 'lax',
      path: '/',
      maxAge: 7 * 24 * 60 * 60 * 1000,
    });

    return res.json({ ok: true, token, user: u });
  } catch (e) {
    console.error('[auth] register error', e.code || e.message || e);
    return res.status(503).json({ error: 'db_unavailable' });
  }
});

router.post('/login', async (req, res) => {
  try {
    const { email, password } = req.body || {};
    if (!email || !password) {
      return res.status(400).json({ error: 'invalid_payload' });
    }

    const user = await findUserByEmail(email);
    if (!user || !user.hash) {
      return res.status(401).json({ error: 'invalid_credentials' });
    }

    const ok = await verifyPassword(password, user.hash);
    if (!ok) return res.status(401).json({ error: 'invalid_credentials' });

    const token = signToken({ sub: user.id, email: user.email, role: user.role || 'user' });

    // usuário “hidratado” (traz coupon_code caso exista/precise criar)
    const full = await hydrateUserFromDB(user.id, user.email) || {
      id: user.id, email: user.email, role: user.role || 'user',
    };

    res.cookie(COOKIE_NAME, token, {
      httpOnly: true,
      secure: IS_PROD,
      sameSite: IS_PROD ? 'none' : 'lax',
      path: '/',
      maxAge: 7 * 24 * 60 * 60 * 1000,
    });

    return res.json({ ok: true, token, user: full });
  } catch (e) {
    console.error('[auth] login error', e.code || e.message || e);
    return res.status(503).json({ error: 'db_unavailable' });
  }
});

router.post('/logout', (_req, res) => {
  res.clearCookie(COOKIE_NAME, {
    httpOnly: true,
    secure: IS_PROD,
    sameSite: IS_PROD ? 'none' : 'lax',
    path: '/',
  });
  return res.json({ ok: true });
});

router.get('/me', requireAuth, async (req, res) => {
  try {
    const u = await hydrateUserFromDB(req.user?.id, req.user?.email);
    // O front já espera o objeto diretamente
    return res.json(u || req.user);
  } catch (e) {
    console.error('[auth] /me error', e?.message || e);
    return res.status(503).json({ error: 'db_unavailable' });
  }
});

/**
 * POST /api/auth/reset-password
 */
router.post('/reset-password', async (req, res) => {
  try {
    let { email, newPassword } = req.body || {};
    email = String(email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ error: 'invalid_email' });

    if (!newPassword) {
      const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789';
      newPassword = Array.from({ length: 6 }, () =>
        alphabet[Math.floor(Math.random() * alphabet.length)]
      ).join('');
    }

    let updated = false;
    try {
      const hash = await bcrypt.hash(String(newPassword), 10);
      const upd = async (sql, vals) => {
        try { const r = await query(sql, vals); if (r.rowCount) updated = true; } catch {}
      };
      await upd(`UPDATE users SET pass_hash=$2        WHERE lower(email)=lower($1)`, [email, hash]);
      await upd(`UPDATE users SET password_hash=$2    WHERE lower(email)=lower($1)`, [email, hash]);
      await upd(`UPDATE users SET password=$2         WHERE lower(email)=lower($1)`, [email, String(newPassword)]);
      await upd(`UPDATE admin_users SET password_hash=$2 WHERE lower(email)=lower($1)`, [email, hash]);
      await upd(`UPDATE admins SET password=$2        WHERE lower(email)=lower($1)`, [email, String(newPassword)]);
    } catch (e) {
      console.warn('[reset-password] hashing/update skipped:', e.message);
    }

    const transporter = nodemailer.createTransport({
      host: process.env.SMTP_HOST,
      port: Number(process.env.SMTP_PORT || 587),
      secure: String(process.env.SMTP_SECURE || 'false') === 'true',
      auth: process.env.SMTP_USER ? { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS } : undefined,
    });

    const mail = {
      from: 'tironinho@hotmail.com',
      to: email,
      subject: 'Reset de senha - New Store Sorteios',
      text: `Sua senha foi resetada.\n\nNova Senha: ${newPassword}\n\nSe você não solicitou, ignore este e-mail.`,
    };

    let delivered = false;
    if (!process.env.SMTP_HOST && !process.env.SMTP_USER) {
      console.log('[reset-password] DEV EMAIL ->', mail);
    } else {
      await transporter.sendMail(mail);
      delivered = true;
    }

    return res.json({ ok: true, delivered, updated });
  } catch (err) {
    console.error('[reset-password] error:', err);
    return res.json({ ok: true, delivered: false, updated: false });
  }
});

export default router;
