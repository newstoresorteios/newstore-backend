// src/services/trayCoupons.js
// Mantido por compatibilidade, mas usando o mesmo fluxo de token do serviço principal (tray.js)
import { trayToken } from "./tray.js";

const API = (
  process.env.TRAY_API_BASE ||
  process.env.TRAY_API_ADDRESS ||
  "https://www.newstorerj.com.br/web_api"
).replace(/\/+$/, "");

async function readJsonSafe(r) {
  return await r.json().catch(async () => {
    const t = await r.text().catch(() => "");
    return { _raw: t };
  });
}

/**
 * Cria ou atualiza um cupom na Tray
 * @param {object} p
 * @param {string} p.code - código do cupom (ex.: NSU-0003-XH)
 * @param {number} p.value_cents - valor em centavos do desconto
 * @param {string|number} [p.coupon_id] - id do cupom na Tray (se já existir)
 */
export async function upsertTrayCoupon({ code, value_cents = 0, coupon_id }) {
  const token = await trayToken();
  const value = (Number(value_cents) / 100).toFixed(2); // "10.00"

  const payload = new URLSearchParams();
  // Formato padrão PHP (compatível com backend Tray)
  payload.append("DiscountCoupon[code]", code);
  payload.append("DiscountCoupon[description]", `Cupom New Store - ${code}`);
  payload.append("DiscountCoupon[value]", value);
  payload.append("DiscountCoupon[type]", "$"); // desconto em R$
  payload.append("DiscountCoupon[usage_counter_limit]", "1");
  payload.append("DiscountCoupon[usage_counter_limit_customer]", "1");
  payload.append("DiscountCoupon[cumulative_discount]", "1");
  // Demais campos são opcionais, manter simples.

  const path   = coupon_id ? `/discount_coupons/${coupon_id}` : `/discount_coupons`;
  const method = coupon_id ? 'PUT' : 'POST';

  const bodyStr = payload.toString();
  const contentLength = Buffer.byteLength(bodyStr);
  const r = await fetch(`${API}${path}?access_token=${encodeURIComponent(token)}`, {
    method,
    headers: {
      Accept: "application/json",
      "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
      "Content-Length": String(contentLength),
    },
    body: bodyStr,
  });

  const j = await readJsonSafe(r);
  if (!r.ok) {
    // Ajuda no debug
    console.error('[tray.upsert] fail', { status: r.status, body: j });
    throw new Error('tray_coupon_upsert_failed');
  }

  const dc = j?.DiscountCoupon || {};
  return {
    id: dc.id || coupon_id,
    value_cents: Math.round(parseFloat(dc.value || value) * 100),
  };
}
