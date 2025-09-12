// src/index.js
import 'dotenv/config';
import dns from 'dns';
try { dns.setDefaultResultOrder('ipv4first'); } catch {}

import express from 'express';
import cors from 'cors';
import cookieParser from 'cookie-parser';

import authRoutes from './routes/auth.js';
import numbersRoutes from './routes/numbers.js';
import reservationsRoutes from './routes/reservations.js';
import paymentsRoutes from './routes/payments.js';
import meRoutes from './routes/me.js';
import drawsRoutes from './routes/draws.js';
import drawsExtRoutes from './routes/draws_ext.js';
import adminRoutes from './routes/admin.js';
import { query } from './db/pg.js';

import { getPool } from './db/pg.js';

const app = express();

const PORT = process.env.PORT || 4000;
const ORIGIN = process.env.CORS_ORIGIN || '*';

setInterval(() => {
  query('SELECT 1').catch(e => console.warn('[health] db ping failed', e.code || e.message));
}, 60_000);

app.use(cors({
  origin: ORIGIN === '*' ? true : ORIGIN.split(',').map(s => s.trim()),
  credentials: true,
}));
app.use(express.json({ limit: '1mb' }));
app.use(cookieParser());

// Health
app.get('/health', (_req, res) => {
  res.json({ ok: true, ts: new Date().toISOString() });
});

// Rotas
app.use('/api/auth', authRoutes);
app.use('/api/numbers', numbersRoutes);
app.use('/api/reservations', reservationsRoutes);
app.use('/api/payments', paymentsRoutes);
app.use('/api/me', meRoutes);
app.use('/api/draws', drawsRoutes);
app.use('/api/draws-ext', drawsExtRoutes);
app.use('/api/admin', adminRoutes);

// 404
app.use((req, res) => {
  res.status(404).json({ error: 'not_found', path: req.originalUrl });
});

app.listen(PORT, async () => {
  console.log(`API listening on :${PORT}`);
  try {
    const pool = await getPool();
    await pool.query('SELECT 1');
    console.log('[db] warmup ok');
  } catch (e) {
    console.error('[db] initial check failed:', e);
  }
});
