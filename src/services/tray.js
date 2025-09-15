// backend/src/services/tray.js
const API  = (process.env.TRAY_API_ADDRESS || 'https://newstorerj.com.br/web_api').replace(/\/+$/, '');
const CK   = process.env.TRAY_CONSUMER_KEY || '';
const CS   = process.env.TRAY_CONSUMER_SECRET || '';
const CODE = process.env.TRAY_CODE || ''; // "code" enviado pela Tray (autorização)

async function trayAuth() {
  const body = new URLSearchParams({
    consumer_key: CK,
    consumer_secret: CS,
    code: CODE,
  });

  const r = await fetch(`${API}/auth`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body,
  });

  const j = await r.json().catch(() => ({}));
  if (!r.ok || !j?.access_token) {
    console.error('[tray.auth] fail', r.status, j);
    throw new Error('tray_auth_failed');
  }
  return j.access_token;
}

// Cria cupom (valor decimal em R$, ex.: 110.00). type="$" = valor fixo em reais.
export async function trayCreateCoupon({ code, value, startsAt, endsAt, description }) {
  const token = await trayAuth();

  const body = new URLSearchParams();
  body.append('DiscountCoupon[code]', String(code));
  body.append('DiscountCoupon[description]', String(description || `Cupom ${code}`));
  body.append('DiscountCoupon[starts_at]', String(startsAt));         // YYYY-MM-DD
  body.append('DiscountCoupon[ends_at]',   String(endsAt));           // YYYY-MM-DD
  body.append('DiscountCoupon[value]',     Number(value).toFixed(2)); // "110.00"
  body.append('DiscountCoupon[type]',      '$');                      // desconto em R$

  const r = await fetch(`${API}/discount_coupons/?access_token=${encodeURIComponent(token)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body,
  });

  const j = await r.json().catch(() => ({}));
  if (!r.ok || !j?.DiscountCoupon?.id) {
    console.error('[tray.create] fail', r.status, j);
    throw new Error('tray_create_coupon_failed');
  }

  return j.DiscountCoupon; // { id, ... }
}

export async function trayDeleteCoupon(couponId) {
  if (!couponId) return;
  const token = await trayAuth();
  const r = await fetch(`${API}/discount_coupons/${couponId}?access_token=${encodeURIComponent(token)}`, {
    method: 'DELETE',
  });
  if (!r.ok && r.status !== 404) {
    const t = await r.text().catch(() => '');
    console.warn('[tray.delete] warn', r.status, t);
  }
}
