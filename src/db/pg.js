// src/db/pg.js
// Robust pg pool for Supabase (direct/pooler), with SSL no-verify for Supabase,
// IPv4 rotation, retries and redundancy between POSTGRES_URL, PRISMA_URL, NON_POOLING.
import pg from "pg";
import dns from "dns";
import { URL as NodeURL } from "url";

const env = process.env;

// Helper para limpar variáveis com aspas
function stripQuotes(s) {
  if (!s) return s;
  return String(s).trim().replace(/^['"]+|['"]+$/g, "");
}

// ===== 1) Coleta URLs na ordem de prioridade
const directURL = stripQuotes(env.POSTGRES_URL || "");
const prismaURL = stripQuotes(env.POSTGRES_PRISMA_URL || "");
const nonPoolingURL = stripQuotes(env.POSTGRES_URL_NON_POOLING || "");

const ordered = [];
if (directURL) ordered.push(directURL);
if (prismaURL) ordered.push(prismaURL);
if (nonPoolingURL) ordered.push(nonPoolingURL);

const urlsRaw = ordered.map(normalizeSafe).filter(Boolean);

// Remove duplicadas preservando ordem
const seen = new Set();
const urls = urlsRaw.filter((u) =>
  seen.has(u) ? false : (seen.add(u), true)
);

if (urls.length === 0) {
  console.error("[pg] Nenhuma DATABASE_URL válida encontrada nas ENVs");
}

// ===== normalization helper
function normalizeSafe(url) {
  if (!url) return null;
  try {
    url = stripQuotes(url);
    const u = new NodeURL(url);
    if (/pooler\.supabase\.com$/i.test(u.hostname)) u.port = "6543";
    if (/\.supabase\.co$/i.test(u.hostname) && !u.port) u.port = "5432";
    if (!/[?&]sslmode=/.test(u.search))
      u.search += (u.search ? "&" : "?") + "sslmode=require";
    return u.toString();
  } catch (err) {
    console.warn("[pg] normalizeSafe falhou:", url, err?.message);
    return url;
  }
}

// ===== 2) SSL helper
function sslFor(url, sniHost) {
  try {
    const u = new NodeURL(url);
    if (/\.(supabase\.co|supabase\.com)$/i.test(u.hostname)) {
      return { rejectUnauthorized: false, servername: sniHost || u.hostname };
    }
  } catch {}
  const mode = String(env.PGSSLMODE || "require").trim().toLowerCase();
  if (mode === "disable" || mode === "allow") return false;
  return { rejectUnauthorized: mode !== "no-verify", servername: sniHost };
}

// ===== 3) DNS helpers
const dnp = dns.promises;
async function resolveAllIPv4(host) {
  try {
    const addrs = await dnp.resolve4(host);
    return Array.isArray(addrs) && addrs.length ? addrs : [];
  } catch {
    try {
      const { address } = await dnp.lookup(host, {
        family: 4,
        hints: dns.ADDRCONFIG,
      });
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
    if (!/\.(supabase\.co|supabase\.com)$/i.test(host)) {
      return [{ url, sni: undefined }];
    }
    const ips = await resolveAllIPv4(host);
    if (!ips.length) return [{ url, sni: host }];
    return ips.map((ip) => {
      const clone = new NodeURL(url);
      clone.hostname = ip;
      return { url: clone.toString(), sni: host };
    });
  } catch {
    return [{ url, sni: undefined }];
  }
}

// ===== 4) Config builder
function cfg(url, sni) {
  return {
    connectionString: url,
    ssl: sslFor(url, sni),
    lookup: (hostname, _opts, cb) =>
      dns.lookup(hostname, { family: 4, hints: dns.ADDRCONFIG }, cb),
    max: Number(env.PG_MAX || 10),
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 10_000,
    keepAlive: true,
  };
}

// ===== 5) State
let pool = null;
let reconnectTimer = null;

function safe(url) {
  return String(url).replace(/:[^@]+@/, "://***:***@");
}

const TRANSIENT_CODES = new Set([
  "57P01",
  "57P02",
  "57P03",
  "08006",
  "ECONNRESET",
  "ETIMEDOUT",
  "EPIPE",
  "ENETUNREACH",
  "ECONNREFUSED",
]);

function isTransient(err) {
  const code = String(err.code || err.errno || "").toUpperCase();
  const msg = String(err.message || "");
  return (
    TRANSIENT_CODES.has(code) ||
    /Connection terminated|read ECONNRESET/i.test(msg)
  );
}

// ===== 6) Connection logic
async function connectOnce(url) {
  const candidates = await toIPv4Candidates(url);
  let lastErr = null;

  for (const c of candidates) {
    const p = new pg.Pool(cfg(c.url, c.sni));
    try {
      await p.query("SELECT 1");
      console.log("[pg] conectado em", safe(c.url));
      p.on("error", (e) => {
        console.error("[pg] pool error", e.code || e.message || e);
        pool = null;
        scheduleReconnect();
      });
      return p;
    } catch (e) {
      lastErr = e;
      console.log(
        "[pg] falha em",
        safe(c.url),
        "->",
        e.code || e.errno || e.message
      );
      await p.end().catch(() => {});
      continue;
    }
  }
  throw lastErr || new Error("Todos os candidatos IPv4 falharam");
}

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
          console.warn(
            "[pg] erro transitório, retry",
            i + 1,
            "de",
            PER_URL_TRIES,
            "em",
            delay,
            "ms"
          );
          await new Promise((r) => setTimeout(r, delay));
          continue;
        }
        break;
      }
    }
  }
  throw lastErr || new Error("Todas as URLs de banco falharam");
}

function scheduleReconnect() {
  if (reconnectTimer) return;
  reconnectTimer = setInterval(async () => {
    if (pool) {
      clearInterval(reconnectTimer);
      reconnectTimer = null;
      return;
    }
    try {
      console.warn("[pg] tentando reconectar em background...");
      pool = await connectWithRetry(urls);
      clearInterval(reconnectTimer);
      reconnectTimer = null;
      console.log("[pg] reconectado com sucesso");
    } catch (e) {
      console.warn("[pg] reconexão falhou:", e.code || e.message);
    }
  }, 5_000);
}

export async function getPool() {
  if (!pool) {
    console.log("[pg] tentando conexão com URLs:", JSON.stringify(urls, null, 2));
    try {
      pool = await connectWithRetry(urls);
    } catch (e) {
      console.error("[pg] conexão inicial falhou:", e.code || e.message);
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
      console.warn("[pg] erro transitório em query, recriando pool...");
      pool = null;
      scheduleReconnect();
      const p = await getPool();
      return await p.query(text, params);
    }
    throw e;
  }
}
