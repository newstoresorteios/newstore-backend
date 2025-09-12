// src/db/pg.js
// Robust pg pool for Supabase (direct/pooler), with SSL no-verify for Supabase,
// SNI preserved when connecting to IP addresses, IPv4 candidate rotation, and retries.
import pg from 'pg';
import dns from 'dns';
import { URL as NodeURL } from 'url';

const env = process.env;

// Helper to safely trim / remove surrounding quotes from env values
function stripQuotes(s) {
  if (!s) return s;
  return String(s).trim().replace(/^['"]+|['"]+$/g, '');
}

// ===== 1) Coleta URLs (prefer direct non-pooling if present)
const poolerURL = stripQuotes(env.DATABASE_URL_POOLING || env.POSTGRES_PRISMA_URL || env.POSTGRES_URL) || '';
const altPooler = stripQuotes(env.DATABASE_URL_POOLING_ALT || '');
const directURL = stripQuotes(env.DATABASE_URL || env.POSTGRES_URL_NON_POOLING || env.POSTGRES_URL) || '';

// Prefer direct URL first (avoids pooler-related SSL issues when possible)
const ordered = [];
if (directURL) ordered.push(directURL);
if (poolerURL) ordered.push(poolerURL);
if (altPooler) ordered.push(altPooler);

const urlsRaw = ordered
  .map(normalizeSafe)
  .filter(Boolean);

// remove duplicadas preservando ordem
const seen = new Set();
const urls = urlsRaw.filter(u => (seen.has(u) ? false : (seen.add(u), true)));

if (urls.length === 0) {
  console.error('[pg] nenhuma DATABASE_URL definida nas ENVs (after normalization)');
}

// ===== normalization helper (ports + sslmode) with safe input handling
function normalizeSafe(url) {
  if (!url) return null;
  try {
    // ensure no surrounding quotes
    url = stripQuotes(url);
    const u = new NodeURL(url);
    if (/pooler\.supabase\.com$/i.test(u.hostname)) u.port = '6543';
    if (/\.supabase\.co$/i.test(u.hostname) && !u.port) u.port = '5432';
    if (!/[?&]sslmode=/.test(u.search)) u.search += (u.search ? '&' : '?') + 'sslmode=require';
    return u.toString();
  } catch (err) {
    console.warn('[pg] normalizeSafe failed for url:', url, err && err.message);
    return url;
  }
}

// ===== 3) SSL policy: for supabase hosts we disable verification but preserve SNI
function sslFor(url, sniHost) {
  try {
    const u = new NodeURL(url);
    if (/\.(supabase\.co|supabase\.com)$/i.test(u.hostname)) {
      // IMPORTANT: rejectUnauthorized:false prevents SELF_SIGNED_CERT_IN_CHAIN errors
      return { rejectUnauthorized: false, servername: sniHost || u.hostname };
    }
  } catch {}
  const mode = String(env.PGSSLMODE || 'require').trim().toLowerCase();
  if (mode === 'disable' || mode === 'allow') return false;
  return { rejectUnauthorized: mode !== 'no-verify', servername: sniHost };
}

// ===== 4) DNS helpers: attempt all IPv4 for a host (helps avoid a "bad" rotated IP)
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

async function toIPv4Candidates(url) {
  try {
    const u = new NodeURL(url);
    const host = u.hostname;
    // Only rewrite hosts that belong to supabase to IPs (others keep hostname)
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

// ===== 5) Pool config builder
function cfg(url, sni) {
  return {
    connectionString: url,
    ssl: sslFor(url, sni),
    // force IPv4 for any internal hostname resolution
    lookup: (hostname, _opts, cb) => dns.lookup(hostname, { family: 4, hints: dns.ADDRCONFIG }, cb),
    max: Number(env.PG_MAX || 10),
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 10_000,
    keepAlive: true,
  };
}

// ===== state & helpers
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

// Connect once to a URL attempting every IPv4 candidate
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

// connect with retry/backoff across provided URLs
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
        break; // try next URL
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
