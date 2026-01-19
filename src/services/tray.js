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
  return {
    id: obj?.id ?? null,
    code: obj?.code ?? null,
    raw: obj || c,
  };
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

  const maskUrlToken = (u) => String(u).replace(/(access_token=)[^&]+/i, (_m, p1) => `${p1}${String(token).slice(0, 8)}…`);

  const buildCouponBody = (style) => {
    const p = new URLSearchParams();
    const code = String(params.code);
    const description = String(params.description || `Cupom ${params.code}`);
    const startsAt = String(params.startsAt);
    const endsAt = String(params.endsAt);
    const value = Number(params.value || 0).toFixed(2);

    // Doc: keys no formato ["DiscountCoupon"]["campo"]
    const k = (field) => (style === "doc" ? `["DiscountCoupon"]["${field}"]` : `DiscountCoupon[${field}]`);

    p.append(k("code"), code);
    p.append(k("description"), description);
    p.append(k("starts_at"), startsAt);
    p.append(k("ends_at"), endsAt);
    p.append(k("value"), value);
    p.append(k("type"), typeValue); // somente "$" ou "%"

    // Campos opcionais (enviar vazio = sem limite)
    p.append(k("value_start"), "");
    p.append(k("value_end"), "");

    // Se quiser 1 uso por cliente, alinhar os dois:
    p.append(k("usage_sum_limit"), "");
    p.append(k("usage_counter_limit"), "1");
    p.append(k("usage_counter_limit_customer"), "1");
    p.append(k("cumulative_discount"), "1");

    return p;
  };

  dbg("[tray.coupon.create]", {
    code: String(params.code),
    value: Number(params.value || 0).toFixed(2),
    starts: params.startsAt,
    ends: params.endsAt,
    type: typeValue,
  });

  const send = async (style) => {
    const body = buildCouponBody(style);
    const bodyStr = body.toString();
    console.log("[tray.coupon.create.req]", {
      url: maskUrlToken(url),
      style,
      body: bodyStr,
    });

    const r = await fetchWithRetry(url, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" },
      body: bodyStr,
      signal: params?.signal,
    }, { label: "tray.coupon.create" });

    const parsed = await readBodySafe(r);
    const j = parsed?.body || null;
    const id = j?.DiscountCoupon?.id ?? j?.discount_coupon?.id ?? null;
    const hasId = !!id;
    const keys = j && typeof j === "object" ? Object.keys(j) : [];
    console.log("[tray.coupon.create.resp]", { status: r.status, style, hasId, id: id || null, keys });
    if (!hasId) {
      console.log("[tray.coupon.create.resp.body]", j && typeof j === "object" ? j : { body: j });
    }
    return { r, j, id, hasId };
  };

  // Tentativa principal: doc-style. Se a Tray responder "Não há dados enviados.", tentamos o legacy-style.
  const first = await send("doc");
  if (first.r.ok && first.hasId) {
    return { ok: true, status: first.r.status, body: first.j };
  }

  const causes = Array.isArray(first.j?.causes) ? first.j.causes.join(" | ") : "";
  const noData = String(causes || "").toLowerCase().includes("não há dados enviados");
  if (!first.r.ok && first.r.status === 400 && noData) {
    console.log("[tray.coupon.create.retry]", { reason: "no_data_sent", trying: "legacy" });
    const second = await send("legacy");
    if (second.r.ok && second.hasId) {
      return { ok: true, status: second.r.status, body: second.j };
    }
    if (!second.r.ok) err("[tray.create] fail", { status: second.r.status, body: second.j });
    return { ok: false, status: second.r.status, body: second.j };
  }

  if (!first.r.ok) err("[tray.create] fail", { status: first.r.status, body: first.j });
  return { ok: false, status: first.r.status, body: first.j };
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
  const ok = r.ok && !!gotId;
  const keys = j && typeof j === "object" ? Object.keys(j) : [];
  console.log("[tray.coupon.confirm]", { id: String(id), ok, status: r.status, hasId: !!gotId, keys });
  if (!ok) console.log("[tray.coupon.confirm.body]", j && typeof j === "object" ? j : { body: j });
  return { ok, status: r.status, body: j };
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
