// src/services/config.js
import { query } from "../db.js";

/** Cria a tabela e semeia o preço (em centavos) se não existir */
export async function ensureAppConfig() {
  await query(`
    create table if not exists app_config (
      key text primary key,
      value text not null,
      updated_at timestamptz default now()
    )
  `);
  const def = String(process.env.PRICE_CENTS ?? "5500");
  await query(
    `insert into app_config(key,value)
       values('ticket_price_cents', $1)
     on conflict (key) do nothing`,
    [def]
  );
}

// util genérica
async function getConfigValue(key) {
  const r = await query(`select value from app_config where key=$1`, [key]);
  return r.rows?.[0]?.value ?? null;
}
async function setConfigValue(key, value) {
  await query(
    `insert into app_config(key,value,updated_at)
       values($1,$2, now())
     on conflict (key) do update
       set value = excluded.value,
           updated_at = now()`,
    [key, String(value)]
  );
}

/* ------ ticket_price_cents ------ */
let priceCache = { v: null, ts: 0 };
export async function getTicketPriceCents() {
  if (Date.now() - priceCache.ts < 10_000 && Number.isFinite(priceCache.v)) {
    return priceCache.v;
  }
  const v =
    Number(await getConfigValue("ticket_price_cents")) ??
    Number(process.env.PRICE_CENTS ?? 5500);
  const n = Number.isFinite(v) ? v : 5500;
  priceCache = { v: n, ts: Date.now() };
  return n;
}
export async function setTicketPriceCents(v) {
  const n = Math.max(0, Math.floor(Number(v || 0)));
  await setConfigValue("ticket_price_cents", n);
  priceCache = { v: n, ts: Date.now() };
  return n;
}

/* ------ banner_title ------ */
export async function getBannerTitle() {
  return (await getConfigValue("banner_title")) || "";
}
export async function setBannerTitle(title) {
  await setConfigValue("banner_title", String(title ?? ""));
  return String(title ?? "");
}

/* ------ max_numbers_per_selection ------ */
export async function getMaxNumbersPerSelection() {
  const v = Number(await getConfigValue("max_numbers_per_selection"));
  return Number.isFinite(v) && v > 0 ? Math.floor(v) : 5;
}
export async function setMaxNumbersPerSelection(n) {
  const v = Math.max(1, Math.floor(Number(n || 1)));
  await setConfigValue("max_numbers_per_selection", v);
  return v;
}
