// backend/src/services/tray.js
const API = (process.env.TRAY_API_ADDRESS || 'https://newstorerj.com.br/web_api').replace(/\/+$/, '');
const CK  = process.env.TRAY_CONSUMER_KEY  || '';
const CS  = process.env.TRAY_CONSUMER_SECRET || '';
const CODE = process.env.TRAY_CODE || '';

async function readJsonSafe(res) {
  try { return await res.json(); } catch { return null; }
}

async function trayAuth() {
  const body = new URLSearchParams({
    consumer_key: CK,
    consumer_secret: CS,
    code: CODE,
  });

  const r = await fetch(`${API}/auth`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
    body,
  });

  const j = await readJsonSafe(r);
  if (!r.ok || !j?.access_token) {
    console.error('[tray.auth] fail', { status: r.status, body: j });
    throw new Error('tray_auth_failed');
  }
  return j.access_token;
}

/**
 * Cria cupom na Tray.
 * Campos principais (docs Tray):
 * - DiscountCoupon[code]           -> string (sem espaços)
 * - DiscountCoupon[value]          -> decimal "10.00"
 * - DiscountCoupon[type]           -> "$" (valor em reais)
 * - DiscountCoupon[starts_at]      -> "YYYY-MM-DD"
 * - DiscountCoupon[ends_at]        -> "YYYY-MM-DD"
 * Recomendado em muitas lojas:
 * - DiscountCoupon[usage_sum_limit]            -> limite total em R$
 * - DiscountCoupon[usage_counter_limit]        -> qtd. total de usos
 * - DiscountCoupon[usage_counter_limit_customer] -> qtd. por cliente
 * - DiscountCoupon[cumulative_discount]        -> 1 (permite acumular)
 */
export async function trayCreateCoupon({ code, value, startsAt, endsAt, description }) {
  const token = await trayAuth();

  const body = new URLSearchParams();
  body.append('DiscountCoupon[code]', String(code));
  body.append('DiscountCoupon[description]', String(description || `Cupom ${code}`));
  body.append('DiscountCoupon[starts_at]', String(startsAt)); // YYYY-MM-DD
  body.append('DiscountCoupon[ends_at]',   String(endsAt));   // YYYY-MM-DD
  body.append('DiscountCoupon[value]',     Number(value || 0).toFixed(2)); // "10.00"
  body.append('DiscountCoupon[type]',      '$');

  // parâmetros que muitas lojas exigem para o cupom funcionar no checkout
  const money = Number(value || 0).toFixed(2);
  body.append('DiscountCoupon[usage_sum_limit]', money);
  body.append('DiscountCoupon[usage_counter_limit]', '1');
  body.append('DiscountCoupon[usage_counter_limit_customer]', '1');
  body.append('DiscountCoupon[cumulative_discount]', '1');

  const r = await fetch(`${API}/discount_coupons/?access_token=${encodeURIComponent(token)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
    body,
  });

  const j = await readJsonSafe(r);
  if (!r.ok || !j?.DiscountCoupon?.id) {
    console.error('[tray.create] fail', { status: r.status, body: j });
    throw new Error('tray_create_coupon_failed');
  }
  return j.DiscountCoupon; // { id, code, ... }
}

export async function trayDeleteCoupon(couponId) {
  if (!couponId) return;
  const token = await trayAuth();
  const r = await fetch(
    `${API}/discount_coupons/${encodeURIComponent(couponId)}?access_token=${encodeURIComponent(token)}`,
    { method: 'DELETE' }
  );
  // 200/204 ok. 404 ignoramos (já não existe).
  if (!r.ok && r.status !== 404) {
    const t = await r.text().catch(() => '');
    console.warn('[tray.delete] warn', { status: r.status, body: t });
  }
}

export default { trayAuth, trayCreateCoupon, trayDeleteCoupon };
