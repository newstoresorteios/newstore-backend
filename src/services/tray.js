// backend/src/services/tray.js
let cache = { token: null, exp: 0 };

const API_BASE   = process.env.TRAY_API_BASE;           // https://loja/web_api
const CKEY       = process.env.TRAY_CONSUMER_KEY;
const CSECRET    = process.env.TRAY_CONSUMER_SECRET;
const STORE_CODE = process.env.TRAY_STORE_CODE;

function form(obj) {
  const p = new URLSearchParams();
  for (const [k, v] of Object.entries(obj)) p.append(k, v ?? "");
  return p.toString();
}

export async function trayToken() {
  if (!API_BASE || !CKEY || !CSECRET || !STORE_CODE) {
    throw new Error("tray_env_missing");
  }
  if (cache.token && Date.now() < cache.exp) return cache.token;

  const r = await fetch(`${API_BASE}/auth`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: form({
      consumer_key: CKEY,
      consumer_secret: CSECRET,
      code: STORE_CODE,
    }),
  });
  if (!r.ok) throw new Error("tray_auth_failed");
  const j = await r.json().catch(() => ({}));
  const token = j?.access_token || j?.accessToken || j?.token;
  if (!token) throw new Error("tray_auth_no_token");
  // 50 min de vida se não vier expires_in
  const ttl = (j?.expires_in ? (j.expires_in - 60) : 3000) * 1000;
  cache = { token, exp: Date.now() + ttl };
  return token;
}

export async function trayCreateCoupon({ code, value, startsAt, endsAt, description }) {
  const token = await trayToken();
  const url = `${API_BASE}/discount_coupons/?access_token=${encodeURIComponent(token)}`;

  const body = new URLSearchParams();
  body.append("DiscountCoupon[code]", code);
  body.append("DiscountCoupon[description]", description || "Crédito New Store");
  body.append("DiscountCoupon[starts_at]", startsAt);
  body.append("DiscountCoupon[ends_at]", endsAt);
  body.append("DiscountCoupon[value]", Number(value).toFixed(2)); // R$ fixo
  body.append("DiscountCoupon[type]", "3");                       // valor (fixo)
  body.append("DiscountCoupon[cumulative_discount]", "1");
  body.append("DiscountCoupon[usage_counter_limit_customer]", "999999"); // sem limite por cliente

  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: body.toString(),
  });
  if (!r.ok) {
    const txt = await r.text().catch(() => "");
    throw new Error(`tray_create_failed ${r.status} ${txt}`);
  }
  const j = await r.json().catch(() => ({}));
  // normaliza id
  const id =
    j?.DiscountCoupon?.id ??
    j?.id ??
    j?.data?.id ??
    j?.discount_coupon?.id ??
    null;
  if (!id) throw new Error("tray_create_no_id");
  return { id, raw: j };
}

export async function trayDeleteCoupon(id) {
  if (!id) return;
  const token = await trayToken();
  const url = `${API_BASE}/discount_coupons/${encodeURIComponent(id)}?access_token=${encodeURIComponent(token)}`;
  const r = await fetch(url, { method: "DELETE" });
  // Tray pode devolver 200/204/201; se 404, ignora
  if (!r.ok && r.status !== 404) {
    const txt = await r.text().catch(() => "");
    throw new Error(`tray_delete_failed ${r.status} ${txt}`);
  }
}
