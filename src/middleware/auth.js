// src/middleware/auth.js
import jwt from 'jsonwebtoken';

const JWT_SECRET =
  process.env.JWT_SECRET ||
  process.env.SUPABASE_JWT_SECRET ||
  'change-me-in-env';

const COOKIE_NAME = process.env.AUTH_COOKIE_NAME || 'ns_auth';

function tokenFromReq(req) {
  const b = req.headers.authorization;
  if (b && b.startsWith('Bearer ')) return b.slice(7);
  if (req.cookies && req.cookies[COOKIE_NAME]) return req.cookies[COOKIE_NAME];
  return null;
}

export function requireAuth(req, res, next) {
  try {
    const token = tokenFromReq(req);
    if (!token) return res.status(401).json({ error: 'unauthorized' });
    req.user = jwt.verify(token, JWT_SECRET);
    return next();
  } catch {
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
