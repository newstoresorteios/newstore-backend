// src/services/trayShipping.js
//
// Cotacao de frete — SOMENTE LEITURA (GET /shippings/cotation/), contrato
// oficial da Tray auditado na Fase A do relatorio de resgate real.
//
// Peso/dimensao vem do catalogo Tray, nao do request. Um array `Shipping`
// vazio e uma resposta valida (indisponibilidade regional), nao falha.
//
// Esta camada NAO cria pedido, NAO cria carrinho Tray, NAO cobra nada.
// E puramente informativa para a tela de revisao do resgate.

import { trayCatalogGet, TrayCatalogError } from "./trayCatalogClient.js";

function toCents(brlString) {
  const n = Number(String(brlString ?? "0").replace(",", "."));
  return Number.isFinite(n) ? Math.round(n * 100) : 0;
}

function toIntSafe(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

/**
 * @param {object} params
 * @param {string} params.zipcode 8 digitos, sem formatacao
 * @param {Array<{trayProductId:string, priceCents:number, quantity:number}>} params.items
 *   priceCents = preco REAL do produto na Tray (nunca o preco em NSCreditos).
 */
export async function fetchShippingCotation({ zipcode, items }, options = {}) {
  const zip = String(zipcode ?? "").replace(/\D/g, "");
  if (zip.length !== 8) throw new TrayCatalogError("invalid_zipcode", { status: 400 });

  const list = Array.isArray(items) ? items : [];
  if (!list.length) throw new TrayCatalogError("shipping_cotation_no_items", { status: 400 });

  const params = { zipcode: zip };
  list.forEach((item, i) => {
    params[`products[${i}][product_id]`] = String(item.trayProductId);
    params[`products[${i}][price]`] = (Number(item.priceCents || 0) / 100).toFixed(2);
    params[`products[${i}][quantity]`] = String(Math.max(1, Number(item.quantity) || 1));
  });

  const body = await trayCatalogGet("/shippings/cotation/", params, options);
  const raw = Array.isArray(body?.Shipping) ? body.Shipping : [];

  return raw.map((entry) => ({
    id: String(entry?.id ?? ""),
    name: String(entry?.name ?? ""),
    price_cents: toCents(entry?.price),
    delivery_time_days: toIntSafe(entry?.delivery_time),
    delivery_time_text: entry?.delivery_time_text ? String(entry.delivery_time_text) : null,
  })).filter((o) => o.id);
}
