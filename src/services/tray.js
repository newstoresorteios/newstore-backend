// backend/src/services/tray.js
import { getTrayEnvConfig, getTrayApiBase, setTrayApiBase, getTrayRefreshToken, setTrayRefreshToken, clearTrayRefreshToken, setTrayAccessToken, getTrayCachedAccessToken } from "./trayConfig.js";

const LOG_LEVEL = (process.env.LOG_LEVEL || "info").toLowerCase();

function dbg(...a) { if (LOG_LEVEL !== "silent") console.log(...a); }
function warn(...a) { console.warn(...a); }
function err(...a) { console.error(...a); }

let cache = { token: null, expMs: 0, expAccessAt: null, mode: null };
let lastError = null;
let codeInvalidUntilMs = 0;

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

function parseTrayDateToMs(s) {
  // Tray costuma retornar "YYYY-MM-DD HH:mm:ss" (sem timezone).
  // Interpretamos como UTC para ter TTL consistente em servidor.
  const str = String(s || "").trim();
  if (!str) return null;
  const iso = str.includes("T") ? str : str.replace(" ", "T") + "Z";
  const ms = new Date(iso).getTime();
  return Number.isFinite(ms) ? ms : null;
}

function computeExpMs(auth) {
  const marginMs = 60_000;
  const expStr = auth?.date_expiration_access_token || null;
  const expMs = parseTrayDateToMs(expStr);
  if (expMs) return Math.max(0, expMs - marginMs);

  const sec = Number.isFinite(Number(auth?.expires_in)) ? Number(auth.expires_in) : 3000;
  return Date.now() + Math.max(0, (sec * 1000) - marginMs);
}

function isTokenInvalidErr(status, body) {
  const ec = body?.error_code;
  const causes = Array.isArray(body?.causes) ? body.causes.join(" | ") : "";
  const msg = `${body?.message || ""} ${causes}`.toLowerCase();
  if (status === 401 && (ec === 1099 || ec === 1000)) return true;
  if (status === 401 && msg.includes("token inválido")) return true;
  return false;
}

function summarizeAuthBody(body) {
  return {
    error_code: body?.error_code ?? null,
    message: body?.message ?? null,
    causes: body?.causes ?? null,
    date_expiration_access_token: body?.date_expiration_access_token ?? null,
    date_expiration_refresh_token: body?.date_expiration_refresh_token ?? null,
  };
}

async function fetchWithRetry(url, options = {}, meta = {}) {
  const { label = "tray.fetch" } = meta;
  const retries = [500, 1500, 3000];
  let lastErr = null;

  for (let i = 0; i <= retries.length; i++) {
    try {
      const r = await fetch(url, options);
      if (r.status === 429 || (r.status >= 500 && r.status <= 599)) {
        const parsed = await readBodySafe(r);
        lastErr = Object.assign(new Error(`${label}_http_${r.status}`), { status: r.status, body: parsed?.body ?? null });
        if (i < retries.length) {
          await new Promise((resolve) => setTimeout(resolve, retries[i]));
          continue;
        }
      }
      return r;
    } catch (e) {
      lastErr = e;
      if (i < retries.length) {
        await new Promise((resolve) => setTimeout(resolve, retries[i]));
        continue;
      }
    }
  }
  throw lastErr || new Error(`${label}_failed`);
}

async function trayTokenWithMeta({ signal, rid = null, forceBootstrap = false, overrideCode = null, overrideApiBase = null } = {}) {
  const { consumerKey, consumerSecret, code: envCode } = getTrayEnvConfig();
  const apiBase = overrideApiBase ? await setTrayApiBase(overrideApiBase) : await getTrayApiBase();

  const hasCKEY = !!consumerKey;
  const hasCSECRET = !!consumerSecret;
  if (!hasCKEY || !hasCSECRET) {
    const e = new Error("tray_env_missing_keys");
    e.code = "tray_env_missing_keys";
    lastError = e.message;
    throw e;
  }
  if (consumerKey === consumerSecret) {
    console.warn("[tray.auth] WARN consumer_key === consumer_secret (provável erro de config)");
  }

  const refresh = await getTrayRefreshToken();
  const hasRefreshKV = refresh.source === "kv";
  const hasRefreshEnv = refresh.source === "env";
  const codeToUse = String(overrideCode || envCode || "").trim();
  const hasCode = !!codeToUse;

  console.log("[tray.auth] env", {
    rid,
    hasCKEY,
    hasCSECRET,
    hasCode,
    hasRefreshEnv,
    hasRefreshKV,
    api_base: apiBase,
  });

  // 4.1 cache memory
  if (cache.token && Date.now() < cache.expMs) {
    return { token: cache.token, authMode: "cache", apiBase, expAccessAt: cache.expAccessAt, hasRefreshKV, lastError };
  }

  // 4.1.1 cache DB opcional
  const cachedDb = await getTrayCachedAccessToken().catch(() => ({ token: null, expAccessAt: null }));
  if (cachedDb?.token && cachedDb?.expAccessAt) {
    const expMs = parseTrayDateToMs(cachedDb.expAccessAt);
    if (expMs && Date.now() < (expMs - 60_000)) {
      cache = { token: cachedDb.token, expMs: expMs - 60_000, expAccessAt: cachedDb.expAccessAt, mode: "cache" };
      return { token: cachedDb.token, authMode: "cache", apiBase, expAccessAt: cachedDb.expAccessAt, hasRefreshKV, lastError };
    }
  }

  // Evita loop infinito quando code está inválido/expirado
  if (Date.now() < codeInvalidUntilMs) {
    const e = new Error("tray_code_invalid_or_expired");
    e.code = "tray_code_invalid_or_expired";
    lastError = e.code;
    throw e;
  }

  // 4.2 refresh (prioridade)
  if (!forceBootstrap && refresh.token) {
    const url = `${apiBase}/auth?refresh_token=${encodeURIComponent(refresh.token)}`;
    console.log("[tray.auth] refresh start", { rid, url });
    const r = await fetchWithRetry(url, { method: "GET", signal }, { label: "tray.auth.refresh" });
    const parsed = await readBodySafe(r);
    const body = parsed?.body || null;

    if (!r.ok || !body?.access_token) {
      console.log("[tray.auth] refresh fail", { rid, status: r.status, body: summarizeAuthBody(body), needReauth: isTokenInvalidErr(r.status, body) });
      lastError = `refresh_fail_${r.status}`;

      // 401 => refresh inválido/expirado: limpa e tenta bootstrap (se tiver code)
      if (isTokenInvalidErr(r.status, body)) {
        await clearTrayRefreshToken().catch(() => {});
        console.log("[tray.auth] refresh invalid/expired; need reauth", { rid });
        // cai para bootstrap abaixo
      } else {
        const e = new Error("tray_auth_failed");
        e.code = "tray_auth_failed";
        e.status = r.status;
        e.body = body;
        throw e;
      }
    } else {
      // ok
      const expMs = computeExpMs(body);
      const expAccessAt = body?.date_expiration_access_token || null;
      const masked = String(body.access_token).slice(0, 8) + "…";
      console.log("[tray.auth] refresh ok", { rid, token: masked, expAccess: expAccessAt, expRefresh: body?.date_expiration_refresh_token || null });
      lastError = null;

      if (body.refresh_token) await setTrayRefreshToken(body.refresh_token).catch(() => {});
      await setTrayAccessToken(body.access_token, expAccessAt).catch(() => {});
      cache = { token: body.access_token, expMs: expMs, expAccessAt, mode: "refresh" };
      return { token: body.access_token, authMode: "refresh", apiBase, expAccessAt, hasRefreshKV: true, lastError };
    }
  }

  // 4.3 bootstrap
  if (hasCode) {
    const url = `${apiBase}/auth`;
    console.log("[tray.auth] bootstrap start", { rid, url });
    const reqBody = form({ consumer_key: consumerKey, consumer_secret: consumerSecret, code: codeToUse });
    const r = await fetchWithRetry(url, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" },
      body: reqBody,
      signal,
    }, { label: "tray.auth.bootstrap" });
    const parsed = await readBodySafe(r);
    const body = parsed?.body || null;

    if (!r.ok || !body?.access_token) {
      console.log("[tray.auth] bootstrap fail", { rid, status: r.status, body: summarizeAuthBody(body) });
      lastError = `bootstrap_fail_${r.status}`;

      if (isTokenInvalidErr(r.status, body)) {
        // CODE inválido/expirado -> instrução operacional e “cooldown”
        console.log("[tray.auth] CODE invalid/expired -> reauthorize app in Tray and generate a new code", { rid });
        console.log("AÇÃO: gere um novo TRAY_CODE na loja (Meus Apps → Acessar) ou reinstale/reauthorize o app; depois rode novamente.");
        codeInvalidUntilMs = Date.now() + 10 * 60_000;
        const e = new Error("tray_code_invalid_or_expired");
        e.code = "tray_code_invalid_or_expired";
        e.status = r.status;
        e.body = body;
        throw e;
      }

      const e = new Error("tray_auth_failed");
      e.code = "tray_auth_failed";
      e.status = r.status;
      e.body = body;
      throw e;
    }

    const expMs = computeExpMs(body);
    const expAccessAt = body?.date_expiration_access_token || null;
    const masked = String(body.access_token).slice(0, 8) + "…";
    console.log("[tray.auth] bootstrap ok", {
      rid,
      token: masked,
      expAccess: expAccessAt,
      expRefresh: body?.date_expiration_refresh_token || null,
    });
    lastError = null;

    if (body.refresh_token) await setTrayRefreshToken(body.refresh_token).catch(() => {});
    await setTrayAccessToken(body.access_token, expAccessAt).catch(() => {});
    cache = { token: body.access_token, expMs: expMs, expAccessAt, mode: "bootstrap" };
    return { token: body.access_token, authMode: "bootstrap", apiBase, expAccessAt, hasRefreshKV: true, lastError };
  }

  const e = new Error("tray_no_refresh_and_no_code");
  e.code = "tray_no_refresh_and_no_code";
  lastError = e.code;
  throw e;
}

// Mantém compatibilidade: retorna apenas o access_token
export async function trayToken({ signal, rid } = {}) {
  const out = await trayTokenWithMeta({ signal, rid });
  return out.token;
}

export async function trayTokenHealth({ signal } = {}) {
  try {
    const out = await trayTokenWithMeta({ signal });
    return { ok: true, ...out };
  } catch (e) {
    return {
      ok: false,
      authMode: cache?.mode || null,
      apiBase: await getTrayApiBase().catch(() => null),
      expAccessAt: cache?.expAccessAt || null,
      hasRefreshKV: (await getTrayRefreshToken().catch(() => ({ source: "none" }))).source === "kv",
      lastError: e?.code || e?.message || "error",
    };
  }
}

export async function trayBootstrap({ code, api_address, signal } = {}) {
  const rid = Math.random().toString(36).slice(2, 8);
  const apiBase = api_address ? await setTrayApiBase(api_address) : await getTrayApiBase();
  return await trayTokenWithMeta({ signal, rid, forceBootstrap: true, overrideCode: code, overrideApiBase: apiBase });
}

function extractCouponsList(body) {
  // Tenta suportar respostas comuns da Tray.
  if (!body || typeof body !== "object") return [];
  if (Array.isArray(body.DiscountCoupons)) return body.DiscountCoupons;
  if (Array.isArray(body.discount_coupons)) return body.discount_coupons;
  if (Array.isArray(body.coupons)) return body.coupons;
  // Às vezes vem como { DiscountCoupon: {...} } no singular
  if (body.DiscountCoupon && typeof body.DiscountCoupon === "object") return [body.DiscountCoupon];
  return [];
}

function normalizeCoupon(c) {
  const obj = c?.DiscountCoupon && typeof c.DiscountCoupon === "object" ? c.DiscountCoupon : c;
  const normDate = (v) => (v ? String(v).slice(0, 10) : null);
  const normNum = (v) => (v == null ? null : String(v));
  return {
    id: obj?.id ?? null,
    code: obj?.code ?? null,
    starts_at: normDate(obj?.starts_at ?? obj?.startsAt ?? null),
    ends_at: normDate(obj?.ends_at ?? obj?.endsAt ?? null),
    value: obj?.value != null ? String(obj.value) : null,
    value_start: normNum(obj?.value_start ?? obj?.valueStart ?? null),
    usage_counter_limit: normNum(obj?.usage_counter_limit ?? null),
    usage_counter_limit_customer: normNum(obj?.usage_counter_limit_customer ?? null),
    raw: obj || c,
  };
}

function buildDiscountCouponFormPayload({
  code,
  description,
  startsAt,
  endsAt,
  valueBRL,
  valueStartBRL,
  type = "$",
}) {
  const c = String(code || "").trim();
  const desc = String(description || "").trim();
  const s = String(startsAt || "").trim(); // YYYY-MM-DD
  const e = String(endsAt || "").trim();   // YYYY-MM-DD
  const v = Number(valueBRL);
  const vs = Number(valueStartBRL);

  if (!s || !e) throw new Error("tray_coupon_missing_dates");
  if (!Number.isFinite(v)) throw new Error("tray_coupon_invalid_value");
  if (!Number.isFinite(vs)) throw new Error("tray_coupon_invalid_value_start");
  if (!(type === "$" || type === "%")) throw new Error("tray_coupon_invalid_type");

  const p = new URLSearchParams();
  // Formato oficial Tray:
  if (c) p.append('["DiscountCoupon"]["code"]', c);
  p.append('["DiscountCoupon"]["description"]', desc);
  p.append('["DiscountCoupon"]["starts_at"]', s);
  p.append('["DiscountCoupon"]["ends_at"]', e);
  p.append('["DiscountCoupon"]["value"]', v.toFixed(2));
  p.append('["DiscountCoupon"]["type"]', type);
  p.append('["DiscountCoupon"]["value_start"]', vs.toFixed(2));
  p.append('["DiscountCoupon"]["value_end"]', "");
  p.append('["DiscountCoupon"]["usage_sum_limit"]', "");
  p.append('["DiscountCoupon"]["usage_counter_limit"]', "1");
  p.append('["DiscountCoupon"]["usage_counter_limit_customer"]', "1");
  p.append('["DiscountCoupon"]["cumulative_discount"]', "1");

  const bodyStr = p.toString();
  const contentLength = Buffer.byteLength(bodyStr);
  return { bodyStr, contentLength };
}

function hasNoDataSent(body) {
  const causes = Array.isArray(body?.causes) ? body.causes.join(" | ") : "";
  return String(causes).toLowerCase().includes("não há dados enviados");
}

function maskAccessTokenInUrl(url, token) {
  return String(url).replace(/(access_token=)[^&]+/i, (_m, p1) => `${p1}${String(token).slice(0, 8)}…`);
}

async function trayHttp({ method, url, token, bodyStr, contentLength, headers, signal }) {
  const maskedUrl = maskAccessTokenInUrl(url, token);
  const safePath = String(maskedUrl).replace(/^https?:\/\/[^/]+/i, "");
  console.log("[tray.http]", { method, url: safePath, contentLength });
  if (!contentLength || contentLength <= 0) {
    throw new Error("tray_http_empty_body");
  }

  const r = await fetchWithRetry(url, { method, headers, body: bodyStr, signal }, { label: `tray.http.${method}` });
  const parsed = await readBodySafe(r);
  const j = parsed?.body ?? null;
  const keys = j && typeof j === "object" ? Object.keys(j) : [];
  console.log("[tray.http.resp]", { method, status: r.status, keys });
  return { r, body: j };
}

function getPagingInfo(body) {
  const paging = body?.paging || body?.Paging || body?.pagination || null;
  const total = Number(paging?.total ?? paging?.Total ?? paging?.total_count ?? paging?.count ?? NaN);
  const limit = Number(paging?.limit ?? paging?.Limit ?? paging?.per_page ?? paging?.page_size ?? 50);
  const safeLimit = Number.isFinite(limit) && limit > 0 ? limit : 50;
  const safeTotal = Number.isFinite(total) && total >= 0 ? total : null;
  const lastPage = safeTotal != null ? Math.max(1, Math.ceil(safeTotal / safeLimit)) : null;
  return { total: safeTotal, limit: safeLimit, lastPage };
}

export async function trayFindCouponByCode(code, { maxPages = 5, signal } = {}) {
  const token = await trayToken({ signal });
  const target = String(code || "").trim();
  if (!target) return { found: false, coupon: null };

  // Tentativa rápida com filtro (se a API suportar):
  try {
    const apiBase = await getTrayApiBase();
    const url = `${apiBase}/discount_coupons/?access_token=${encodeURIComponent(token)}&limit=50&code=${encodeURIComponent(target)}`;
    const r = await fetchWithRetry(url, { method: "GET", signal }, { label: "tray.coupon.find" });
    const parsed = await readBodySafe(r);
    const body = parsed?.body || null;
    if (r.ok) {
      const list = extractCouponsList(body);
      const normalized = list.map(normalizeCoupon);
      const hit = normalized.find((x) => String(x?.code || "").trim() === target);
      console.log("[tray.coupon.find]", { code: target, page: "filtered", found: !!hit, count: normalized.length, total: body?.paging?.total ?? null, lastPage: null });
      if (hit) return { found: true, coupon: hit };
    }
  } catch {}

  // A Tray costuma ordenar por id ASC -> cupons novos ficam no final.
  // Então buscamos nas ÚLTIMAS páginas (até 3 páginas), mas antes pegamos paging via page=1.
  const apiBase = await getTrayApiBase();
  const firstUrl = `${apiBase}/discount_coupons/?access_token=${encodeURIComponent(token)}&limit=50&page=1`;
  const r0 = await fetchWithRetry(firstUrl, { method: "GET", signal }, { label: "tray.coupon.find" });
  const parsed0 = await readBodySafe(r0);
  const body0 = parsed0?.body || null;
  if (!r0.ok) {
    err("[tray.coupon.find] fail", { code: target, page: 1, status: r0.status, body: body0 });
    throw new Error("tray_coupon_find_failed");
  }

  const paging0 = getPagingInfo(body0);
  const fallbackLast = Math.max(1, maxPages);
  const lastPage = paging0.lastPage || fallbackLast;

  const pagesToTry = [];
  for (let i = 0; i < 3; i++) {
    const p = lastPage - i;
    if (p >= 1) pagesToTry.push(p);
  }
  // Se lastPage < 3, garante page=1 no conjunto
  if (!pagesToTry.includes(1)) pagesToTry.push(1);

  for (const page of Array.from(new Set(pagesToTry)).sort((a, b) => b - a)) {
    const url = `${apiBase}/discount_coupons/?access_token=${encodeURIComponent(token)}&limit=50&page=${page}`;
    const r = await fetchWithRetry(url, { method: "GET", signal }, { label: "tray.coupon.find" });
    const parsed = await readBodySafe(r);
    const body = parsed?.body || null;
    if (!r.ok) {
      err("[tray.coupon.find] fail", { code: target, page, status: r.status, body });
      throw new Error("tray_coupon_find_failed");
    }

    const paging = getPagingInfo(body);
    const list = extractCouponsList(body);
    const normalized = list.map(normalizeCoupon);
    const hit = normalized.find((x) => String(x?.code || "").trim() === target);

    console.log("[tray.coupon.find]", {
      code: target,
      page,
      found: !!hit,
      count: normalized.length,
      total: paging.total ?? paging0.total ?? null,
      lastPage: paging.lastPage ?? paging0.lastPage ?? null,
    });

    if (hit) return { found: true, coupon: hit };
    if (!normalized.length) break;
  }
  return { found: false, coupon: null };
}

async function createCouponWithType(params, typeValue) {
  const token = await trayToken({ signal: params?.signal });
  const masked = (token || "").slice(0, 8) + "…";
  dbg("[tray.create] tentando criar cupom", {
    code: params.code,
    value: params.value,
    type: typeValue,
    startsAt: params.startsAt,
    endsAt: params.endsAt,
    token: masked,
  });

  const apiBase = await getTrayApiBase();
  const url = `${apiBase}/discount_coupons/?access_token=${encodeURIComponent(token)}`;

  // TABELA: Cartão Presente -> mínimo de compra permitido (value_start)
  function minPurchaseForGiftCard(giftValueBRL) {
    const v = Number(giftValueBRL);
    if (!Number.isFinite(v)) return null;
    if (v >= 50 && v <= 250) return 1500;
    if (v >= 251 && v <= 600) return 3500;
    if (v >= 601 && v <= 800) return 5500;
    if (v >= 801 && v <= 1100) return 7500;
    if (v >= 1101 && v <= 2100) return 15000;
    if (v >= 2101 && v <= 3100) return 22500;
    if (v >= 3101 && v <= 4200) return 30000;
    return null;
  }

  dbg("[tray.coupon.create]", {
    code: String(params.code),
    value: Number(params.value || 0).toFixed(2),
    starts: params.startsAt,
    ends: params.endsAt,
    type: typeValue,
  });

  const giftValueBRL = Number(params.value || 0);
  let valueStartBRL = minPurchaseForGiftCard(giftValueBRL);
  if (valueStartBRL == null) {
    console.warn(`[tray.coupon.rules] WARN giftValueBRL=${giftValueBRL} fora da tabela -> value_start=30000.00`);
    valueStartBRL = 30000;
  }

  console.log("[tray.coupon.create.req.meta]", {
    code: String(params.code),
    giftValueBRL: Number.isFinite(giftValueBRL) ? Number(giftValueBRL.toFixed(2)) : null,
    value_start: Number(valueStartBRL).toFixed(2),
    usage_counter_limit: 1,
    usage_counter_limit_customer: 1,
    cumulative_discount: 1,
    type: typeValue,
  });

  const { bodyStr, contentLength } = buildDiscountCouponFormPayload({
    code: String(params.code),
    description: String(params.description || `Cupom ${params.code}`),
    startsAt: String(params.startsAt),
    endsAt: String(params.endsAt),
    valueBRL: giftValueBRL,
    valueStartBRL,
    type: typeValue,
  });

  console.log("[tray.coupon.create.req]", { url: maskAccessTokenInUrl(url, token), body: bodyStr });

  const { r, body } = await trayHttp({
    method: "POST",
    url,
    token,
    bodyStr,
    contentLength,
    headers: {
      "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
      Accept: "application/json",
    },
    signal: params?.signal,
  });

  const id = body?.DiscountCoupon?.id ?? body?.discount_coupon?.id ?? null;
  const ok = r.ok && !!id;
  console.log("[tray.coupon.create.resp]", { status: r.status, hasId: !!id, id: id || null, bodyKeys: body && typeof body === "object" ? Object.keys(body) : [] });
  if (!ok) console.log("[tray.coupon.create.resp.body]", body && typeof body === "object" ? body : { body });
  return { ok, status: r.status, body };
}

export async function trayCreateCoupon({ code, value, startsAt, endsAt, description, signal } = {}) {
  // Type deve ser somente "$" ou "%". Mantemos "$" (desconto em reais) e removemos fallback "3".
  const t = await createCouponWithType({ code, value, startsAt, endsAt, description, signal }, "$");
  if (t.ok) {
    const id = t.body?.DiscountCoupon?.id ?? t.body?.discount_coupon?.id ?? null;
    dbg("[tray.create] ok com type '$' id:", id);
    return { id, raw: t.body };
  }

  err("[tray.create] fail", { status: t?.status ?? null, body: t?.body ?? null });
  const e = new Error("tray_create_coupon_failed");
  e.status = t?.status ?? null;
  e.body = t?.body ?? null;
  throw e;
}

export async function trayGetCouponById(id, { signal } = {}) {
  if (!id) throw new Error("tray_coupon_id_missing");
  const token = await trayToken({ signal });
  const apiBase = await getTrayApiBase();
  const url = `${apiBase}/discount_coupons/${encodeURIComponent(id)}/?access_token=${encodeURIComponent(token)}`;
  const urlMasked = String(url).replace(/(access_token=)[^&]+/i, (_m, p1) => `${p1}${String(token).slice(0, 8)}…`);
  console.log("[tray.coupon.confirm.req]", { url: urlMasked, id: String(id) });

  const r = await fetchWithRetry(url, { method: "GET", signal }, { label: "tray.coupon.confirm" });
  const parsed = await readBodySafe(r);
  const j = parsed?.body || null;
  const gotId = j?.DiscountCoupon?.id ?? j?.discount_coupon?.id ?? null;
  const gotCode = j?.DiscountCoupon?.code ?? j?.discount_coupon?.code ?? null;
  const gotStarts = (j?.DiscountCoupon?.starts_at ?? j?.discount_coupon?.starts_at ?? null);
  const gotEnds = (j?.DiscountCoupon?.ends_at ?? j?.discount_coupon?.ends_at ?? null);
  const gotValue = (j?.DiscountCoupon?.value ?? j?.discount_coupon?.value ?? null);
  const gotValueStart = (j?.DiscountCoupon?.value_start ?? j?.discount_coupon?.value_start ?? null);
  const gotUsage1 = (j?.DiscountCoupon?.usage_counter_limit ?? j?.discount_coupon?.usage_counter_limit ?? null);
  const gotUsageCust = (j?.DiscountCoupon?.usage_counter_limit_customer ?? j?.discount_coupon?.usage_counter_limit_customer ?? null);
  const ok = r.ok && !!gotId;
  const keys = j && typeof j === "object" ? Object.keys(j) : [];
  console.log("[tray.coupon.confirm]", {
    id: String(id),
    ok,
    status: r.status,
    code: gotCode || null,
    starts: gotStarts ? String(gotStarts).slice(0, 10) : null,
    ends: gotEnds ? String(gotEnds).slice(0, 10) : null,
    value: gotValue != null ? String(gotValue) : null,
    value_start: gotValueStart != null ? String(gotValueStart) : null,
    usage_counter_limit: gotUsage1 != null ? String(gotUsage1) : null,
    usage_counter_limit_customer: gotUsageCust != null ? String(gotUsageCust) : null,
    keys,
  });
  if (!ok) console.log("[tray.coupon.confirm.body]", j && typeof j === "object" ? j : { body: j });
  return {
    ok,
    status: r.status,
    body: j,
    coupon: normalizeCoupon(j?.DiscountCoupon ? { DiscountCoupon: j.DiscountCoupon } : (j?.discount_coupon ? { DiscountCoupon: j.discount_coupon } : j)),
  };
}

export async function trayUpdateCouponById(id, { startsAt, endsAt, valueBRL, minPurchaseBRL = null, description = null, signal } = {}) {
  if (!id) throw new Error("tray_coupon_id_missing");
  const token = await trayToken({ signal });
  const apiBase = await getTrayApiBase();
  const url = `${apiBase}/discount_coupons/${encodeURIComponent(id)}/?access_token=${encodeURIComponent(token)}`;
  const urlMasked = String(url).replace(/(access_token=)[^&]+/i, (_m, p1) => `${p1}${String(token).slice(0, 8)}…`);

  let valueStartBRL = Number(minPurchaseBRL);
  if (!Number.isFinite(valueStartBRL)) {
    console.warn(`[tray.coupon.rules] WARN minPurchaseBRL inválido -> value_start=30000.00`);
    valueStartBRL = 30000;
  }

  const { bodyStr, contentLength } = buildDiscountCouponFormPayload({
    code: "", // não é obrigatório no PUT, mas a Tray aceita; não enviaremos
    description: String(description || ""),
    startsAt: String(startsAt),
    endsAt: String(endsAt),
    valueBRL: Number(valueBRL || 0),
    valueStartBRL,
    type: "$",
  });
  console.log("[tray.coupon.update]", {
    id: String(id),
    url: urlMasked,
    starts: String(startsAt),
    ends: String(endsAt),
    value: Number(valueBRL || 0).toFixed(2),
    value_start: minPurchaseBRL != null ? Number(minPurchaseBRL).toFixed(2) : "",
  });
  console.log("[tray.coupon.update.req]", { body: bodyStr });

  const urlEncoded = await trayHttp({
    method: "PUT",
    url,
    token,
    bodyStr,
    contentLength,
    headers: {
      "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
      Accept: "application/json",
    },
    signal,
  });

  const id1 = urlEncoded.body?.DiscountCoupon?.id ?? urlEncoded.body?.discount_coupon?.id ?? null;
  if (urlEncoded.r.ok && id1) {
    return { ok: true, status: urlEncoded.r.status, body: urlEncoded.body, id: id1 };
  }

  // Fallback JSON quando vier 400 "Não há dados enviados."
  if (urlEncoded.r.status === 400 && hasNoDataSent(urlEncoded.body)) {
    console.log("[tray.coupon.update] fallback=json attempt=2", { id: String(id) });
    const jsonBody = {
      DiscountCoupon: {
        starts_at: String(startsAt),
        ends_at: String(endsAt),
        value: Number(valueBRL || 0).toFixed(2),
        type: "$",
        description: String(description || ""),
        value_start: Number(valueStartBRL).toFixed(2),
        usage_counter_limit: 1,
        usage_counter_limit_customer: 1,
        cumulative_discount: 1,
      },
    };
    const jsonStr = JSON.stringify(jsonBody);
    const clen = Buffer.byteLength(jsonStr);
    const safePath2 = String(maskAccessTokenInUrl(url, token)).replace(/^https?:\/\/[^/]+/i, "");
    console.log("[tray.http]", { method: "PUT", url: safePath2, contentLength: clen });
    const r2 = await fetchWithRetry(url, {
      method: "PUT",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: jsonStr,
      signal,
    }, { label: "tray.coupon.update.json" });
    const parsed2 = await readBodySafe(r2);
    const j2 = parsed2?.body ?? null;
    const id2 = j2?.DiscountCoupon?.id ?? j2?.discount_coupon?.id ?? null;
    console.log("[tray.http.resp]", { method: "PUT", status: r2.status, keys: j2 && typeof j2 === "object" ? Object.keys(j2) : [] });
    console.log("[tray.coupon.update.resp]", { status: r2.status, hasId: !!id2, id: id2 || null });
    if (!id2) console.log("[tray.coupon.update.resp.body]", j2 && typeof j2 === "object" ? j2 : { body: j2 });
    return { ok: r2.ok && !!id2, status: r2.status, body: j2, id: id2 || null };
  }

  const idFail = urlEncoded.body?.DiscountCoupon?.id ?? urlEncoded.body?.discount_coupon?.id ?? null;
  console.log("[tray.coupon.update.resp]", { status: urlEncoded.r.status, hasId: !!idFail, id: idFail || null });
  if (!idFail) console.log("[tray.coupon.update.resp.body]", urlEncoded.body && typeof urlEncoded.body === "object" ? urlEncoded.body : { body: urlEncoded.body });
  return { ok: false, status: urlEncoded.r.status, body: urlEncoded.body, id: idFail || null };
}
export async function trayDeleteCoupon(id) {
  if (!id) return;
  const token = await trayToken();
  const apiBase = await getTrayApiBase();
  dbg("[tray.delete] deletando cupom id:", id, "token:", (token || "").slice(0, 8) + "…");
  const r = await fetch(
    `${apiBase}/discount_coupons/${encodeURIComponent(id)}?access_token=${encodeURIComponent(token)}`,
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
  const apiBase = await getTrayApiBase();
  const url = `${apiBase}/discount_coupons/?access_token=${encodeURIComponent(token)}`;
  const r = await fetch(url, { method: "GET" });
  const parsed = await readBodySafe(r);
  if (!r.ok) {
    err("[tray.health] fail", { status: r.status, body: parsed?.body ?? null });
    return { ok: false, status: r.status, body: parsed?.body ?? null };
  }
  dbg("[tray.health] ok", { status: r.status });
  return { ok: true, status: r.status, body: parsed?.body ?? null };
}
