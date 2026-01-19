// backend/src/services/tray.js
import { query } from "../db.js";

const API_BASE = (
  process.env.TRAY_API_BASE ||
  process.env.TRAY_API_ADDRESS ||
  "https://www.newstorerj.com.br/web_api"
).replace(/\/+$/, "");

const CKEY    = process.env.TRAY_CONSUMER_KEY  || "";
const CSECRET = process.env.TRAY_CONSUMER_SECRET || "";
const AUTH_CODE = process.env.TRAY_CODE || ""; // use 1x; preferir TRAY_REFRESH_TOKEN em produção
const LOG_LEVEL = (process.env.LOG_LEVEL || "info").toLowerCase();

function dbg(...a) { if (LOG_LEVEL !== "silent") console.log(...a); }
function warn(...a) { console.warn(...a); }
function err(...a) { console.error(...a); }

let cache = { token: null, exp: 0 };

function form(obj) {
  const p = new URLSearchParams();
  for (const [k, v] of Object.entries(obj)) p.append(k, v ?? "");
  return p.toString();
}

async function readBodySafe(r) {
  // Tray costuma responder JSON; mas em erro pode vir HTML/texto.
  const ct = (r.headers?.get?.("content-type") || "").toLowerCase();
  if (ct.includes("application/json")) {
    const j = await r.json().catch(() => null);
    return { kind: "json", body: j };
  }
  const t = await r.text().catch(() => "");
  try {
    const j = JSON.parse(t);
    return { kind: "json", body: j };
  } catch {
    return { kind: "text", body: t };
  }
}

function computeTtlMs(auth) {
  // Preferir data absoluta (date_expiration_access_token) quando existir.
  const marginMs = 60_000;
  const expStr = auth?.date_expiration_access_token || auth?.date_expiration || null;
  if (expStr) {
    const expMs = new Date(expStr).getTime();
    if (Number.isFinite(expMs)) {
      const ttl = expMs - Date.now() - marginMs;
      return Math.max(0, ttl);
    }
  }
  // Fallback: expires_in em segundos (se existir); senão 3000s com margem.
  const sec = Number.isFinite(Number(auth?.expires_in)) ? Number(auth.expires_in) : 3000;
  return Math.max(0, (sec * 1000) - marginMs);
}

// ---- KV store para refresh_token (persiste entre deploys) ----
async function getSavedRefresh() {
  if (process.env.TRAY_REFRESH_TOKEN) return process.env.TRAY_REFRESH_TOKEN;
  try {
    const r = await query(
      `select v from kv_store where k='tray_refresh_token' limit 1`
    );
    return r.rows?.[0]?.v || null;
  } catch {
    return null;
  }
}
async function saveRefresh(rt) {
  if (!rt) return;
  try {
    await query(
      `insert into kv_store (k, v) values ('tray_refresh_token',$1)
       on conflict (k) do update set v=excluded.v, updated_at=now()`,
      [rt]
    );
    dbg("[tray.auth] refresh_token salvo no kv_store");
  } catch (e) {
    warn("[tray.auth] falha ao salvar refresh_token:", e?.message || e);
  }
}

// ---- token (tenta refresh; se não tiver, usa code 1x) ----
export async function trayToken() {
  if (!CKEY || !CSECRET) throw new Error("tray_env_missing_keys");
  if (cache.token && Date.now() < cache.exp) return cache.token;

  const savedRt = await getSavedRefresh();
  if (savedRt) {
    dbg("[tray.auth] usando refresh_token salvo; API_BASE:", API_BASE);
    // Doc: Refresh via GET /auth?refresh_token=...
    const url = `${API_BASE}/auth?refresh_token=${encodeURIComponent(savedRt)}`;
    const r = await fetch(url, { method: "GET" });
    const parsed = await readBodySafe(r);

    const auth = parsed?.body || null;
    if (!r.ok || !auth?.access_token) {
      err("[tray.auth] refresh fail", { status: r.status, url, body: parsed?.body ?? null });
      throw new Error("tray_auth_failed");
    }

    const ttlMs = computeTtlMs(auth);
    const masked = (auth.access_token || "").slice(0, 8) + "…";
    dbg("[tray.auth] refresh ok", {
      status: r.status,
      token: masked,
      ttlMs,
      date_expiration_access_token: auth?.date_expiration_access_token || null,
    });

    if (auth.refresh_token) await saveRefresh(auth.refresh_token);
    cache = { token: auth.access_token, exp: Date.now() + ttlMs };
    return cache.token;
  } else if (AUTH_CODE) {
    dbg("[tray.auth] sem refresh_token; usando AUTH_CODE 1x; API_BASE:", API_BASE);
    const body = form({
      consumer_key: CKEY,
      consumer_secret: CSECRET,
      code: AUTH_CODE,
    });
    // Doc: 1ª autenticação via POST /auth com consumer_key, consumer_secret, code
    const url = `${API_BASE}/auth`;
    const r = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" },
      body,
    });

    const parsed = await readBodySafe(r);
    const auth = parsed?.body || null;
    if (!r.ok || !auth?.access_token) {
      err("[tray.auth] bootstrap fail", { status: r.status, url, body: parsed?.body ?? null });
      throw new Error("tray_auth_failed");
    }

    const ttlMs = computeTtlMs(auth);
    const masked = (auth.access_token || "").slice(0, 8) + "…";
    dbg("[tray.auth] bootstrap ok", {
      status: r.status,
      token: masked,
      ttlMs,
      date_expiration_access_token: auth?.date_expiration_access_token || null,
    });

    if (auth.refresh_token) await saveRefresh(auth.refresh_token);
    cache = { token: auth.access_token, exp: Date.now() + ttlMs };
    return cache.token;
  }

  err("[tray.auth] falta TRAY_REFRESH_TOKEN (persistido/env) e TRAY_CODE (bootstrap)");
  throw new Error("tray_no_refresh_and_no_code");
}

async function createCouponWithType(params, typeValue) {
  const token = await trayToken();
  const masked = (token || "").slice(0, 8) + "…";
  dbg("[tray.create] tentando criar cupom", {
    code: params.code,
    value: params.value,
    type: typeValue,
    startsAt: params.startsAt,
    endsAt: params.endsAt,
    token: masked,
  });

  const url = `${API_BASE}/discount_coupons/?access_token=${encodeURIComponent(token)}`;

  const body = new URLSearchParams();
  body.append("DiscountCoupon[code]", String(params.code));
  body.append("DiscountCoupon[description]", String(params.description || `Cupom ${params.code}`));
  body.append("DiscountCoupon[starts_at]", String(params.startsAt)); // YYYY-MM-DD
  body.append("DiscountCoupon[ends_at]",   String(params.endsAt));   // YYYY-MM-DD
  body.append("DiscountCoupon[value]",     Number(params.value || 0).toFixed(2));
  body.append("DiscountCoupon[type]",      typeValue);

  // limites para o checkout reconhecer o valor (e não estourar o saldo)
  const money = Number(params.value || 0).toFixed(2);
  body.append("DiscountCoupon[usage_sum_limit]", money);
  body.append("DiscountCoupon[usage_counter_limit]", "1");
  body.append("DiscountCoupon[usage_counter_limit_customer]", "1");
  body.append("DiscountCoupon[cumulative_discount]", "1");

  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" },
    body: body.toString(),
  });

  const parsed = await readBodySafe(r);
  const j = parsed?.body || null;
  if (!r.ok) {
    err("[tray.create] fail", { status: r.status, body: j });
  } else {
    dbg("[tray.create] resp", { status: r.status, bodyKeys: j ? Object.keys(j) : [] });
  }

  return { ok: r.ok && !!j?.DiscountCoupon?.id, status: r.status, body: j };
}

export async function trayCreateCoupon({ code, value, startsAt, endsAt, description }) {
  // 1ª tentativa: type="$"
  const t1 = await createCouponWithType({ code, value, startsAt, endsAt, description }, "$");
  if (t1.ok) {
    const id = t1.body.DiscountCoupon.id;
    dbg("[tray.create] ok com type '$' id:", id);
    return { id, raw: t1.body };
  }

  // 2ª tentativa: type="3"
  const t2 = await createCouponWithType({ code, value, startsAt, endsAt, description }, "3");
  if (t2.ok) {
    const id = t2.body.DiscountCoupon.id;
    dbg("[tray.create] ok com type '3' id:", id);
    return { id, raw: t2.body };
  }

  err("[tray.create] fail (ambas as tentativas)", { first: t1, second: t2 });
  throw new Error("tray_create_coupon_failed");
}

export async function trayDeleteCoupon(id) {
  if (!id) return;
  const token = await trayToken();
  dbg("[tray.delete] deletando cupom id:", id, "token:", (token || "").slice(0, 8) + "…");
  const r = await fetch(
    `${API_BASE}/discount_coupons/${encodeURIComponent(id)}?access_token=${encodeURIComponent(token)}`,
    { method: "DELETE" }
  );
  if (r.ok || r.status === 404) {
    dbg("[tray.delete] ok status:", r.status);
  } else {
    const t = await r.text().catch(() => "");
    warn("[tray.delete] warn", { status: r.status, body: t });
  }
}

/**
 * Healthcheck simples: autentica e tenta listar 1 cupom (para validar access_token).
 * Não altera dados.
 */
export async function trayHealthCheck() {
  const token = await trayToken();
  const url = `${API_BASE}/discount_coupons/?access_token=${encodeURIComponent(token)}`;
  const r = await fetch(url, { method: "GET" });
  const parsed = await readBodySafe(r);
  if (!r.ok) {
    err("[tray.health] fail", { status: r.status, body: parsed?.body ?? null });
    return { ok: false, status: r.status, body: parsed?.body ?? null };
  }
  dbg("[tray.health] ok", { status: r.status });
  return { ok: true, status: r.status, body: parsed?.body ?? null };
}
