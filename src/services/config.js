import { query } from "../db.js";

/**
 * Garante a existência da tabela app_config e injeta ticket_price_cents padrão.
 */
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

/* ---------- Ticket price (com cache leve de 10s) ---------- */
let priceCache = { v: null, ts: 0 };

export async function getTicketPriceCents() {
  if (Date.now() - priceCache.ts < 10_000 && Number.isFinite(priceCache.v)) {
    return priceCache.v;
  }
  const r = await query(
    `select value from app_config where key = 'ticket_price_cents'`
  );
  const n = Number(r.rows?.[0]?.value ?? process.env.PRICE_CENTS ?? 5500);
  priceCache = { v: n, ts: Date.now() };
  return n;
}

export async function setTicketPriceCents(v) {
  const n = Math.max(0, Math.floor(Number(v || 0)));
  await query(
    `insert into app_config(key,value,updated_at)
       values('ticket_price_cents', $1, now())
     on conflict (key) do update
       set value = excluded.value,
           updated_at = now()`,
    [String(n)]
  );
  priceCache = { v: n, ts: Date.now() };
  return n;
}

/* ---------- Utilitários genéricos ---------- */
export async function getConfigValue(key) {
  const r = await query(`select value from app_config where key = $1`, [key]);
  return r.rows?.[0]?.value ?? null;
}

export async function setConfigValue(key, value) {
  await query(
    `insert into app_config(key,value,updated_at)
       values($1, $2, now())
     on conflict (key) do update
       set value = excluded.value,
           updated_at = now()`,
    [String(key), String(value ?? "")]
  );
}

/* ---------- Banner (frase promocional) ---------- */
export async function getBannerTitle() {
  return (await getConfigValue("banner_title")) || "";
}

export async function setBannerTitle(v) {
  await setConfigValue("banner_title", String(v ?? ""));
}

/* ---------- Máximo de números por seleção ---------- */
export async function getMaxNumbersPerSelection() {
  const v = await getConfigValue("max_numbers_per_selection");
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : 5; // fallback 5
}

export async function setMaxNumbersPerSelection(v) {
  const n = Math.max(1, Math.floor(Number(v || 1)));
  await setConfigValue("max_numbers_per_selection", String(n));
}
