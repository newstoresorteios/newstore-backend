// src/services/trayConfig.js
// Centraliza config + KV helpers (compatível com kv_store antigo e novo).

import { query } from "../db.js";

const DEFAULT_API_BASE = "https://www.newstorerj.com.br/web_api";

let kvSchemaCache = null; // { kCol: "k"|"key", vCol: "v"|"value" }

function normalizeApiBase(raw) {
  let s = String(raw || "").trim();
  if (!s) return DEFAULT_API_BASE;
  s = s.replace(/\/+$/, "");
  if (!/^https?:\/\//i.test(s)) s = `https://${s}`;
  // Evita http em produção por acidente (a Tray usa https)
  if (/^http:\/\//i.test(s)) s = s.replace(/^http:\/\//i, "https://");
  return s;
}

export function getTrayEnvConfig() {
  const consumerKey = String(process.env.TRAY_CONSUMER_KEY || "").trim();
  const consumerSecret = String(process.env.TRAY_CONSUMER_SECRET || "").trim();
  const code = String(process.env.TRAY_CODE || "").trim();
  const refreshEnv = String(process.env.TRAY_REFRESH_TOKEN || "").trim();
  const apiBaseEnv = normalizeApiBase(process.env.TRAY_API_BASE || process.env.TRAY_API_ADDRESS || DEFAULT_API_BASE);
  return { consumerKey, consumerSecret, code, refreshEnv, apiBaseEnv };
}

export function validateTrayConfigAtStartup() {
  const { consumerKey, consumerSecret } = getTrayEnvConfig();
  if (!consumerKey || !consumerSecret) {
    console.error("[tray.auth] config error: TRAY_CONSUMER_KEY/TRAY_CONSUMER_SECRET ausentes");
  } else if (consumerKey === consumerSecret) {
    console.warn("[tray.auth] config WARN: TRAY_CONSUMER_KEY === TRAY_CONSUMER_SECRET (provável copy/paste errado)");
  }
}

async function detectKvSchema() {
  if (kvSchemaCache) return kvSchemaCache;

  // Preferência: detectar via information_schema
  try {
    const { rows } = await query(
      `select column_name
         from information_schema.columns
        where table_schema='public'
          and table_name='kv_store'
          and column_name in ('k','key','v','value')`
    );
    const cols = rows.map((r) => r.column_name);
    const kCol = cols.includes("k") ? "k" : (cols.includes("key") ? "key" : null);
    const vCol = cols.includes("v") ? "v" : (cols.includes("value") ? "value" : null);
    if (kCol && vCol) {
      kvSchemaCache = { kCol, vCol };
      return kvSchemaCache;
    }
  } catch {}

  // Fallback: tenta schema novo (k/v), senão antigo (key/value)
  try {
    await query(`select k, v from kv_store limit 1`);
    kvSchemaCache = { kCol: "k", vCol: "v" };
    return kvSchemaCache;
  } catch {}

  kvSchemaCache = { kCol: "key", vCol: "value" };
  return kvSchemaCache;
}

async function ensureKvStoreExistsIfMissing() {
  // Best-effort: cria schema novo se não existir. Se existir com schema antigo, isso não altera.
  try {
    await query(`
      create table if not exists kv_store (
        k text primary key,
        v text,
        updated_at timestamptz default now()
      )
    `);
  } catch {}
}

export async function kvGet(key) {
  const k = String(key);
  await ensureKvStoreExistsIfMissing();
  const { kCol, vCol } = await detectKvSchema();
  const r = await query(`select ${vCol} as v from kv_store where ${kCol}=$1 limit 1`, [k]);
  return r.rows?.[0]?.v ?? null;
}

export async function kvSet(key, value) {
  const k = String(key);
  const v = value == null ? null : String(value);
  await ensureKvStoreExistsIfMissing();
  const { kCol, vCol } = await detectKvSchema();

  // Upsert compatível com ambos os schemas (assumimos PK em k/key)
  if (kCol === "k") {
    await query(
      `insert into kv_store (k, v) values ($1,$2)
       on conflict (k) do update set v=excluded.v, updated_at=now()`,
      [k, v]
    );
  } else {
    await query(
      `insert into kv_store (key, value) values ($1,$2)
       on conflict (key) do update set value=excluded.value, updated_at=now()`,
      [k, v]
    );
  }
}

export async function kvDel(key) {
  const k = String(key);
  await ensureKvStoreExistsIfMissing();
  const { kCol } = await detectKvSchema();
  await query(`delete from kv_store where ${kCol}=$1`, [k]);
}

export async function getTrayApiBase() {
  const fromKv = await kvGet("tray_api_base").catch(() => null);
  if (fromKv) return normalizeApiBase(fromKv);
  return getTrayEnvConfig().apiBaseEnv;
}

export async function setTrayApiBase(apiBase) {
  const norm = normalizeApiBase(apiBase);
  await kvSet("tray_api_base", norm);
  return norm;
}

export async function getTrayRefreshToken() {
  // Ordem exigida: KV primeiro, depois ENV (fallback)
  const kv = await kvGet("tray_refresh_token").catch(() => null);
  if (kv) return { token: String(kv), source: "kv" };
  const env = getTrayEnvConfig().refreshEnv;
  if (env) return { token: env, source: "env" };
  return { token: null, source: "none" };
}

export async function setTrayRefreshToken(rt) {
  if (!rt) return;
  await kvSet("tray_refresh_token", rt);
}

export async function clearTrayRefreshToken() {
  await kvDel("tray_refresh_token").catch(() => {});
}

export async function setTrayAccessToken(at, expAccessAt) {
  if (at) await kvSet("tray_access_token", at);
  if (expAccessAt) await kvSet("tray_access_exp_at", expAccessAt);
}

export async function getTrayCachedAccessToken() {
  const at = await kvGet("tray_access_token").catch(() => null);
  const exp = await kvGet("tray_access_exp_at").catch(() => null);
  return { token: at ? String(at) : null, expAccessAt: exp ? String(exp) : null };
}


