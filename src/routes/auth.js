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
    if (h.startsWith('$2')) {
      return await bcrypt.compare(String(plain), h);
    }
    if (!h.startsWith('$')) {
      return String(plain) === h; // legado em texto
    }
    return false;
  } catch {
    return false;
  }
}

// Busca usuário **sem** referenciar colunas inexistentes (evita 42703)
async function findUserByEmail(emailRaw) {
  const email = String(emailRaw || '').trim();
  if (!email) return null;
  try { await query('SELECT 1'); } catch {}

  try {
    const { rows } = await query(
      `SELECT * FROM users WHERE LOWER(email)=LOWER($1) LIMIT 1`,
      [email]
    );
    if (!rows?.length) return null;

    const u = rows[0];
    const hash = u.pass_hash || u.password_hash || u.password || null;
    const role = u.is_admin === true ? 'admin' : (u.role ? String(u.role) : 'user');

    return { id: u.id, email: u.email, hash, role };
  } catch {
    return null;
  }
}

router.post('/register', async (req, res) => {
  try {
    const { name, email, password } = req.body || {};
    if (!name || !email || !password) {
      return res.status(400).json({ error: 'invalid_payload' });
    }

    const emailNorm = String(email).trim().toLowerCase();

    const dupe = await query(
      'SELECT 1 FROM users WHERE LOWER(email)=LOWER($1)',
      [emailNorm]
    );
    if (dupe.rows.length) return res.status(409).json({ error: 'email_in_use' });

    const hash = await bcrypt.hash(String(password), 10);
    const ins = await query(
      `INSERT INTO users (name, email, pass_hash)
       VALUES ($1,$2,$3)
       RETURNING id, name, email,
                 CASE WHEN is_admin THEN 'admin' ELSE 'user' END AS role`,
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

    res.cookie(COOKIE_NAME, token, {
      httpOnly: true,
      secure: IS_PROD,
      sameSite: IS_PROD ? 'none' : 'lax',
      path: '/',
      maxAge: 7 * 24 * 60 * 60 * 1000,
    });

    // devolve no formato que o front já usa
    return res.json({
      ok: true,
      token,
      user: { id: user.id, email: user.email, role: user.role || 'user' },
    });
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
    // tenta enriquecer com coupon_code; se falhar, devolve req.user mesmo
    let out = req.user;
    try {
      const { rows } = await query(
        `SELECT id, name, email, is_admin, coupon_code, coupon_updated_at
           FROM users WHERE id=$1 LIMIT 1`,
        [req.user.id]
      );
      if (rows?.length) {
        const r = rows[0];
        out = {
          id: r.id,
          name: r.name,
          email: r.email,
          role: r.is_admin ? 'admin' : 'user',
          coupon_code: r.coupon_code || null,
          coupon_updated_at: r.coupon_updated_at || null,
        };
      }
    } catch {}
    return res.json(out);
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

      try {
        const r = await query(`UPDATE users SET pass_hash=$2 WHERE lower(email)=lower($1)`, [email, hash]);
        if (r.rowCount) updated = true;
      } catch {}

      try {
        const r = await query(`UPDATE users SET password_hash=$2 WHERE lower(email)=lower($1)`, [email, hash]);
        if (r.rowCount) updated = true;
      } catch {}

      try {
        const r = await query(`UPDATE users SET password=$2 WHERE lower(email)=lower($1)`, [email, String(newPassword)]);
        if (r.rowCount) updated = true;
      } catch {}

      try {
        const r = await query(`UPDATE admin_users SET password_hash=$2 WHERE lower(email)=lower($1)`, [email, hash]);
        if (r.rowCount) updated = true;
      } catch {}

      try {
        const r = await query(`UPDATE admins SET password=$2 WHERE lower(email)=lower($1)`, [email, String(newPassword)]);
        if (r.rowCount) updated = true;
      } catch {}
    } catch (e) {
      console.warn('[reset-password] hashing/update skipped:', e.message);
    }

    const transporter = nodemailer.createTransport({
      host: process.env.SMTP_HOST,
      port: Number(process.env.SMTP_PORT || 587),
      secure: String(process.env.SMTP_SECURE || 'false') === 'true',
      auth: process.env.SMTP_USER
        ? { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS }
        : undefined,
    });

    const mail = {
      from: 'tironinho@hotmail.com',
      to: email,
      subject: 'Reset de senha - New Store Sorteios',
      text:
        `Sua senha foi resetada.\n\n` +
        `Nova Senha: ${newPassword}\n\n` +
        `Se você não solicitou, ignore este e-mail.`,
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
