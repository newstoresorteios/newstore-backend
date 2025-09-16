// src/config.js
let _ticketPriceCents = Number(process.env.PRICE_CENTS || 5500);

export function getTicketPriceCents() {
  return _ticketPriceCents;
}

export function setTicketPriceCents(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) {
    throw new Error("invalid_price");
  }
  _ticketPriceCents = Math.round(n);
  console.log("[config] ticket price set to", _ticketPriceCents, "cents");
  return _ticketPriceCents;
}

export {
  ensureAppConfig,
  getTicketPriceCents,
  setTicketPriceCents,
} from './services/config.js';