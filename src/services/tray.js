// backend/src/services/tray.js
import { query } from "../db.js";

const API_BASE = (
  process.env.TRAY_API_BASE ||
  process.env.TRAY_API_ADDRESS ||
  "https://www.newstorerj.com.br/web_api"
).replace(/\/+$/, "");

const CKEY    = process.env.TRAY_CONSUMER_KEY  || "";
const CSECRET = process.env.TRAY_CONSUMER_SECRET || "";

// ⚠ TRAY_CODE é o *authorization code* de uso único.
// Em produção, prefira definir TRAY_REFRESH_TOKEN direto na ENV.
const AUTH_CODE = process.env.TRAY_CODE || "";

let cache = { token: null, exp: 0 };

function form(obj) {
  const p = new URLSearchParams();
  for (const [k, v] of Object.entries(obj)) p.append(k, v ?? "");
  return p.toString();
}

// ---- KV store para refresh_token (sobrevive a deploy) ----
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
  } catch {}
}

// ---- token (tenta refresh; se não tiver, usa code 1x) ----
export async function trayToken() {
  if (!CKEY || !CSECRET) throw new Error("tray_env_missing_keys");
  if (cache.token && Date.now() < cache.exp) return cache.token;

  let body = null;
  const savedRt = await getSavedRefresh();
  if (savedRt) {
    body = form({
      consumer_key: CKEY,
      consumer_secret: CSECRET,
      refresh_token: savedRt,
      grant_type: "refresh_token",
    });
  } else if (AUTH_CODE) {
    body = form({
      consumer_key: CKEY,
      consumer_secret: CSECRET,
      code: AUTH_CODE,
    });
  } else {
    throw new Error("tray_no_refresh_and_no_code");
  }

  const r = await fetch(`${API_BASE}/auth`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8" },
    body,
  });

  const j = await r.json().catch(() => ({}));
  if (!r.ok || !j?.access_token) {
    console.error("[tray.auth] fail", { status: r.status, body: j });
    throw new Error("tray_auth_failed");
  }

  if (j.refresh_token) await saveRefresh(j.refresh_token);

  const ttlMs = ((j.expires_in ? j.expires_in - 60 : 3000) * 1000);
  cache = { token: j.access_token, exp: Date.now() + ttlMs };
  return cache.token;
}

// ---- criação/remoção de cupom ----
async function createCouponWithType(params, typeValue) {
  const token = await trayToken();
  const url = `${API_BASE}/discount_coupons/?access_token=${encodeURIComponent(token)}`;

  const body = new URLSearchParams();
  body.append("DiscountCoupon[code]", String(params.code));
  body.append("DiscountCoupon[description]", String(params.description || `Cupom ${params.code}`));
  body.append("DiscountCoupon[starts_at]", String(params.startsAt)); // YYYY-MM-DD
  body.append("DiscountCoupon[ends_at]",   String(params.endsAt));   // YYYY-MM-DD
  body.append("DiscountCoupon[value]",     Number(params.value || 0).toFixed(2));
  body.append("DiscountCoupon[type]",      typeValue);

  // Limites para o checkout reconhecer o valor e não permitir “passar do saldo”
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

  const j = await r.json().catch(() => ({}));
  return { ok: r.ok && !!j?.DiscountCoupon?.id, status: r.status, body: j };
}

export async function trayCreateCoupon({ code, value, startsAt, endsAt, description }) {
  // 1ª tentativa: type = "$" (algumas lojas aceitam assim)
  let t1 = await createCouponWithType({ code, value, startsAt, endsAt, description }, "$");
  if (t1.ok) return { id: t1.body.DiscountCoupon.id, raw: t1.body };

  // 2ª tentativa: type = "3" (valor em R$ em outras instalações)
  let t2 = await createCouponWithType({ code, value, startsAt, endsAt, description }, "3");
  if (t2.ok) return { id: t2.body.DiscountCoupon.id, raw: t2.body };

  console.error("[tray.create] fail", { first: t1, second: t2 });
  throw new Error("tray_create_coupon_failed");
}

export async function trayDeleteCoupon(id) {
  if (!id) return;
  const token = await trayToken();
  const r = await fetch(
    `${API_BASE}/discount_coupons/${encodeURIComponent(id)}?access_token=${encodeURIComponent(token)}`,
    { method: "DELETE" }
  );
  if (!r.ok && r.status !== 404) {
    const t = await r.text().catch(() => "");
    console.warn("[tray.delete] warn", { status: r.status, body: t });
  }
}
