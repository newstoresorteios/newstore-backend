// src/routes/auth.js
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

const TOKEN_TTL  = process.env.JWT_TTL || '7d';
const COOKIE_NAME = process.env.AUTH_COOKIE_NAME || 'ns_auth';
const IS_PROD = (process.env.NODE_ENV || 'production') === 'production';

function signToken(payload) {
  return jwt.sign(payload, JWT_SECRET, { expiresIn: TOKEN_TTL });
}

async function verifyPassword(plain, hashed) {
  if (!hashed) return false;
  try {
    if (String(hashed).startsWith('$2')) {
      return await bcrypt.compare(String(plain), String(hashed)); // bcrypt
    }
    if (!String(hashed).startsWith('$')) {
      return String(plain) === String(hashed); // fallback (não recomendado)
    }
    return false;
  } catch {
    return false;
  }
}

async function findUserByEmail(emailRaw) {
  const email = String(emailRaw).trim();

  // ping leve (força reconexão se o pool estiver quebrado)
  await query('SELECT 1', []);

  const tries = [
    {
      q: `
        SELECT id, email, pass_hash AS hash,
               CASE WHEN is_admin THEN 'admin' ELSE 'user' END AS role
        FROM users
        WHERE LOWER(email) = LOWER($1)
        LIMIT 1
      `,
      args: [email],
    },
    {
      q: `
        SELECT id, email, password_hash AS hash, role
        FROM admin_users
        WHERE LOWER(email) = LOWER($1)
        LIMIT 1
      `,
      args: [email],
    },
    {
      q: `
        SELECT id, email, password AS hash, 'admin' AS role
        FROM admins
        WHERE LOWER(email) = LOWER($1)
        LIMIT 1
      `,
      args: [email],
    },
  ];

  for (const t of tries) {
    const { rows } = await query(t.q, t.args);
    if (rows.length) return rows[0];
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
    if (!email || !password) return res.status(400).json({ error: 'invalid_payload' });

    const user = await findUserByEmail(email);
    if (!user || !user.hash) return res.status(401).json({ error: 'invalid_credentials' });

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

    return res.json({ ok: true, token, user: { id: user.id, email: user.email, role: user.role || 'user' } });
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

router.get('/me', requireAuth, (req, res) => {
  return res.json({ ok: true, user: req.user });
});


// POST /api/auth/reset-password
// body: { email: string, newPassword?: string }
// - Gera (ou usa) uma senha nova
// - Tenta gravar hash em users.password_hash (best-effort; ignora se não existir)
// - Envia e-mail; se não houver SMTP configurado, apenas loga e retorna ok
router.post('/reset-password', async (req, res) => {
  try {
    let { email, newPassword } = req.body || {};
    email = String(email || '').trim().toLowerCase();
    if (!email) return res.status(400).json({ error: 'invalid_email' });

    // gera senha aleatória de 6 caracteres se não vier pronta
    if (!newPassword) {
      const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789';
      newPassword = Array.from({ length: 6 }, () =>
        alphabet[Math.floor(Math.random() * alphabet.length)]
      ).join('');
    }

    // 1) Tenta atualizar a senha no banco (best-effort)
    try {
      const hash = await bcrypt.hash(String(newPassword), 10);
      await query(
        `UPDATE users
           SET password_hash = $2
         WHERE lower(email) = lower($1)`,
        [email, hash]
      );
    } catch (e) {
      // se a tabela/coluna não existir, seguimos — o e-mail ainda será enviado
      console.warn('[reset-password] DB update skipped:', e.message);
    }

    // 2) Envia o e-mail (ou apenas loga em dev)
    const transporter = nodemailer.createTransport({
      host: process.env.SMTP_HOST,
      port: Number(process.env.SMTP_PORT || 587),
      secure: String(process.env.SMTP_SECURE || 'false') === 'true',
      auth: process.env.SMTP_USER
        ? { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS }
        : undefined,
    });

    const mail = {
      from: 'contato@newstorerj.com.br',
      to: email,
      subject: 'Reset de senha - New Store Sorteios',
      text:
        `Sua senha foi resetada.\n\n` +
        `Nova Senha: ${newPassword}\n\n` +
        `Se você não solicitou, ignore este e-mail.`,
    };

    // Sem SMTP configurado? Não falha; apenas loga e retorna ok.
    if (!process.env.SMTP_HOST && !process.env.SMTP_USER) {
      console.log('[reset-password] DEV EMAIL ->', mail);
      return res.json({ ok: true, delivered: false, dev: true });
    }

    await transporter.sendMail(mail);
    return res.json({ ok: true, delivered: true });
  } catch (err) {
    console.error('[reset-password] error:', err);
    // Mesmo se o e-mail falhar, não travamos o fluxo do front.
    return res.json({ ok: true, delivered: false });
  }
});

export default router;
