// src/db/pg.js
// Versão robusta para Supabase (pooler/direct), SNI e SSL "no-verify" quando necessário.
// Mantém retry, IPv4 candidates e reconexão em background.

import pg from 'pg';
import dns from 'dns';
import { URL as NodeURL } from 'url';

const env = process.env;

// 1) coleta URLs (pooler primeiro)
const poolerURL = [
  env.DATABASE_URL_POOLING,
  env.POSTGRES_PRISMA_URL,
  env.POSTGRES_URL,
].find(v => v && v.trim()) || '';

const altPooler = (env.DATABASE_URL_POOLING_ALT || '').trim();
const directURL = [
  env.DATABASE_URL,
  env.POSTGRES_URL_NON_POOLING,
].find(v => v && v.trim()) || '';

const HAS_POOLER = Boolean(poolerURL || altPooler);

// 2) normaliza porta (6543 para pooler, 5432 para supabase direct) e adiciona sslmode=require se não tiver
function normalize(url) {
  if (!url) return null;
  try {
    const u = new NodeURL(url);
    if (/pooler\.supabase\.com$/i.test(u.hostname)) u.port = '6543';
    if (/\.supabase\.co$/i.test(u.hostname) && !u.port) u.port = '5432';
    if (!/[?&]sslmode=/.test(u.search)) u.search += (u.search ? '&' : '?') + 'sslmode=require';
    return u.toString();
  } catch {
    return url;
  }
}

const urlsRaw = (HAS_POOLER ? [poolerURL, altPooler] : [directURL])
  .map(normalize)
  .filter(Boolean);

// remove duplicados mantendo ordem
const seen = new Set();
const urls = urlsRaw.filter(u => (seen.has(u) ? false : (seen.add(u), true)));

if (urls.length === 0) {
  console.error('[pg] nenhuma DATABASE_URL definida nas ENVs');
}

// 3) SSL policy: para supabase usamos rejectUnauthorized:false e passamos servername (SNI)
function sslFor(url, sniHost) {
  try {
    const u = new NodeURL(url);
    if (/\.(supabase\.co|supabase\.com)$/i.test(u.hostname)) {
      return { rejectUnauthorized: false, servername: sniHost || u.hostname };
    }
  } catch {}
  const mode = String(env.PGSSLMODE || 'require').trim().toLowerCase();
  if (mode === 'disable' || mode === 'allow') return false;
  return { rejectUnauthorized: mode !== 'no-verify', servername: sniHost };
}

// 4) DNS helpers
const dnp = dns.promises;
async function resolveAllIPv4(host) {
  try {
    const addrs = await dnp.resolve4(host);
    return Array.isArray(addrs) && addrs.length ? addrs : [];
  } catch {
    try {
      const { address } = await dnp.lookup(host, { family: 4, hints: dns.ADDRCONFIG });
      return address ? [address] : [];
    } catch {
      return [];
    }
  }
}

// Gera candidatos trocando hostname por cada IP e preservando sni=hostname original
async function toIPv4Candidates(url) {
  try {
    const u = new NodeURL(url);
    const host = u.hostname;
    if (!/\.(supabase\.co|supabase\.com)$/i.test(host)) {
      return [{ url, sni: undefined }];
    }
    const ips = await resolveAllIPv4(host);
    if (!ips.length) return [{ url, sni: host }];
    return ips.map(ip => {
      const clone = new NodeURL(url);
      clone.hostname = ip;
      return { url: clone.toString(), sni: host };
    });
  } catch {
    return [{ url, sni: undefined }];
  }
}

// 5) pool config
function cfg(url, sni) {
  return {
    connectionString: url,
    ssl: sslFor(url, sni),
    lookup: (hostname, _opts, cb) => dns.lookup(hostname, { family: 4, hints: dns.ADDRCONFIG }, cb),
    max: Number(env.PG_MAX || 10),
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 10_000,
    keepAlive: true,
  };
}

let pool = null;
let reconnectTimer = null;

function safe(url) {
  return String(url).replace(/:[^@]+@/, '://***:***@');
}

const TRANSIENT_CODES = new Set([
  '57P01','57P02','57P03','08006',
  'ECONNRESET','ETIMEDOUT','EPIPE','ENETUNREACH','ECONNREFUSED',
]);

function isTransient(err) {
  const code = String(err.code || err.errno || '').toUpperCase();
  const msg  = String(err.message || '');
  return TRANSIENT_CODES.has(code) || /Connection terminated|read ECONNRESET/i.test(msg);
}

// tenta conectar uma vez numa URL, experimentando todos os IPs
async function connectOnce(url) {
  const candidates = await toIPv4Candidates(url);
  let lastErr = null;
  for (const c of candidates) {
    const p = new pg.Pool(cfg(c.url, c.sni));
    try {
      await p.query('SELECT 1');
      console.log('[pg] connected on', safe(c.url));
      p.on('error', (e) => {
        console.error('[pg] pool error', e.code || e.message || e);
        pool = null;
        scheduleReconnect();
      });
      return p;
    } catch (e) {
      lastErr = e;
      console.log('[pg] failed on', safe(c.url), '->', e.code || e.errno || e.message || e);
      await p.end().catch(() => {});
      continue;
    }
  }
  throw lastErr || new Error('All IPv4 candidates failed');
}

// connect with retry/backoff across URLs
async function connectWithRetry(urlList) {
  const PER_URL_TRIES = 5;
  const BASE_DELAY = 500;
  let lastErr = null;
  for (const url of urlList) {
    for (let i = 0; i < PER_URL_TRIES; i++) {
      try {
        return await connectOnce(url);
      } catch (e) {
        lastErr = e;
        if (i < PER_URL_TRIES - 1 && isTransient(e)) {
          const delay = BASE_DELAY * Math.pow(2, i);
          console.warn('[pg] transient connect error, retrying', i + 1, 'of', PER_URL_TRIES, 'in', delay, 'ms');
          await new Promise(r => setTimeout(r, delay));
          continue;
        }
        break;
      }
    }
  }
  throw lastErr || new Error('All database URLs failed');
}

function scheduleReconnect() {
  if (reconnectTimer) return;
  reconnectTimer = setInterval(async () => {
    if (pool) { clearInterval(reconnectTimer); reconnectTimer = null; return; }
    try {
      console.warn('[pg] trying background reconnect...');
      pool = await connectWithRetry(urls);
      clearInterval(reconnectTimer);
      reconnectTimer = null;
      console.log('[pg] reconnected');
    } catch (e) {
      console.warn('[pg] background reconnect failed:', e.code || e.message);
    }
  }, 5_000);
}

export async function getPool() {
  if (!pool) {
    console.log('[pg] will try', JSON.stringify(urls, null, 2));
    try {
      pool = await connectWithRetry(urls);
    } catch (e) {
      console.error('[pg] initial connect failed:', e.code || e.message);
      scheduleReconnect();
      throw e;
    }
  }
  return pool;
}

export async function query(text, params) {
  try {
    const p = await getPool();
    return await p.query(text, params);
  } catch (e) {
    if (isTransient(e)) {
      console.warn('[pg] transient query error, recreating pool and retrying once');
      pool = null;
      scheduleReconnect();
      const p = await getPool();
      return await p.query(text, params);
    }
    throw e;
  }
}
