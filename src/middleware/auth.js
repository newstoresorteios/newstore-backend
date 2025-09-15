// src/middleware/auth.js
import jwt from 'jsonwebtoken';
import nodemailer from 'nodemailer';
import bcrypt from 'bcryptjs';

const JWT_SECRET =
  process.env.JWT_SECRET ||
  process.env.JWT_SECRET_KEY ||
  process.env.SUPABASE_JWT_SECRET ||
  'dev-secret';

// você pode manter AUTH_COOKIE_NAME, mas também aceitaremos nomes comuns
const COOKIE_NAMES = [
  process.env.AUTH_COOKIE_NAME || 'ns_auth',
  'ns_auth_token',
  'token',
  'jwt',
];

function sanitizeToken(t) {
  if (!t) return '';
  let s = String(t).trim();
  // remove "Bearer " se vier no header
  if (/^Bearer\s+/i.test(s)) s = s.replace(/^Bearer\s+/i, '').trim();
  // remove aspas acidentais
  s = s.replace(/^['"]|['"]$/g, '');
  return s;
}

function extractToken(req) {
  // 1) Authorization
  const auth = req.headers?.authorization;
  if (auth) {
    const tok = sanitizeToken(auth);
    if (tok) return tok;
  }

  // 2) Cookies
  const cookies = req.cookies || {};
  for (const name of COOKIE_NAMES) {
    if (cookies[name]) {
      const tok = sanitizeToken(cookies[name]);
      if (tok) return tok;
    }
  }

  return null;
}

export function requireAuth(req, res, next) {
  try {
    const token = extractToken(req);
    if (!token) return res.status(401).json({ error: 'unauthorized' });

    const payload = jwt.verify(token, JWT_SECRET);

    // anexa um usuário mínimo no req
    req.user = {
      id: payload.id || payload.sub,
      email: payload.email || payload.user?.email,
      role: payload.role || payload.user?.role,
      ...payload,
    };

    return next();
  } catch (e) {
    console.warn('[auth] invalid token:', e?.message || e);
    return res.status(401).json({ error: 'unauthorized' });
  }
}

export function requireAdmin(req, res, next) {
  const u = req.user;
  if (!u || !(u.role === 'admin' || u.is_admin === true)) {
    return res.status(403).json({ error: 'forbidden' });
  }
  return next();
}

// Reset de senha com envio de e-mail.
// Request body: { email, newPassword, from?, subject?, message? }
router.post('/reset-password', async (req, res) => {
  try {
    const { email, newPassword, from, subject, message } = req.body || {};
    if (!email || !newPassword) {
      return res.status(400).json({ error: 'invalid_request' });
    }

    // 1) (Best-effort) Atualiza a senha no banco, se houver coluna password_hash
    try {
      const hash = await bcrypt.hash(String(newPassword), 10);
      await query(
        `UPDATE users
            SET password_hash = $2
          WHERE lower(email) = lower($1)`,
        [email, hash]
      );
    } catch (e) {
      // Se a tabela/coluna não existir, seguimos normalmente (o envio do e-mail ainda acontece)
      console.warn('[reset-password] DB update skipped:', e.message);
    }

    // 2) Envia o e-mail (usa SMTP_* das envs; em dev sem SMTP, apenas loga e retorna ok)
    const transporter = nodemailer.createTransport({
      host: process.env.SMTP_HOST,
      port: Number(process.env.SMTP_PORT || 587),
      secure: String(process.env.SMTP_SECURE || 'false') === 'true',
      auth: process.env.SMTP_USER
        ? { user: process.env.SMTP_USER, pass: process.env.SMTP_PASS }
        : undefined,
    });

    const mail = {
      from: from || 'administracao@newstoresorteios.com.br',
      to: email,
      subject: subject || 'Reset de senha - New Store Sorteios',
      text:
        message ||
        `Sua senha foi resetada.\n\nNova Senha: ${newPassword}\n\nSe você não solicitou, ignore este e-mail.`,
    };

    // Dev fallback: sem SMTP configurado, não falha – apenas loga e retorna ok
    if (!process.env.SMTP_HOST && !process.env.SMTP_USER) {
      console.log('[reset-password] DEV EMAIL ->', mail);
      return res.json({ ok: true, delivered: false, dev: true });
    }

    await transporter.sendMail(mail);
    return res.json({ ok: true, delivered: true });
  } catch (err) {
    console.error('[reset-password] error:', err);
    // Mesmo se o e-mail falhar, não vamos travar o fluxo do front.
    return res.json({ ok: true, delivered: false });
  }
});
