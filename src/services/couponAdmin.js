// src/services/couponAdmin.js
//
// Administracao do saldo de NSCreditos — FASE 5: o mesmo cupom individual
// do usuario (users.coupon_value_cents), nao mais nscredit_wallets.
//
// Toda mudanca e uma operacao (credit | debit) registrada em
// coupon_balance_history, nunca uma sobrescrita direta de saldo.

import { query as defaultQuery } from "../db.js";
import { getCouponBalance, applyCouponLedgerEntry, parseUserId, CouponLedgerError } from "./couponLedger.js";

const MAX_PAGE_SIZE = 100;

function resolveDeps(deps = {}) {
  return { query: deps.query || defaultQuery, withTransaction: deps.withTransaction };
}

function parsePaging({ page = 1, limit = 20 } = {}) {
  const safeLimit = Math.min(Math.max(Number(limit) || 20, 1), MAX_PAGE_SIZE);
  const safePage = Math.max(Number(page) || 1, 1);
  return { page: safePage, limit: safeLimit, offset: (safePage - 1) * safeLimit };
}

function parseMetaReason(meta) {
  if (!meta) return null;
  try {
    const parsed = typeof meta === "string" ? JSON.parse(meta) : meta;
    return typeof parsed?.reason === "string" ? parsed.reason : null;
  } catch {
    return null;
  }
}

/**
 * Formato compativel com o admin ja publicado (NsCreditsTab.jsx), que
 * historicamente consumia nscredit_transactions (operation/amount/
 * balance_before/balance_after/source_type/reason). O ledger canonico usa
 * delta_cents/event_type/channel/meta — mapeado aqui para nao exigir
 * mudanca de frontend so por causa da troca de fonte de saldo.
 */
function mapHistoryRow(row) {
  const deltaCents = Number(row.delta_cents);
  return {
    id: row.id,
    operation: deltaCents >= 0 ? "credit" : "debit",
    amount: Math.abs(deltaCents) / 100,
    balance_before: Number(row.balance_before_cents) / 100,
    balance_after: Number(row.balance_after_cents) / 100,
    source_type: row.channel || row.event_type,
    reason: parseMetaReason(row.meta),
    event_type: row.event_type,
    redemption_id: row.redemption_id || null,
    created_at: row.created_at instanceof Date ? row.created_at.toISOString() : row.created_at,
  };
}

/** Busca de clientes reais com o saldo de cupom (mesma fonte da Loja). */
export async function searchUsersForCouponAdmin({ q = "", page = 1, limit = 20 } = {}, deps = {}) {
  const d = resolveDeps(deps);
  const paging = parsePaging({ page, limit });

  const term = String(q ?? "").trim();
  const params = [];
  const where = [];

  if (term) {
    params.push(`%${term}%`);
    const like = `$${params.length}`;
    const digits = term.replace(/\D/g, "");
    const clauses = [`u.name ILIKE ${like}`, `u.email ILIKE ${like}`, `coalesce(u.phone,'') ILIKE ${like}`, `coalesce(u.coupon_code,'') ILIKE ${like}`];
    if (/^\d+$/.test(term)) {
      params.push(Number(term));
      clauses.push(`u.id = $${params.length}`);
    }
    if (digits && digits !== term) {
      params.push(`%${digits}%`);
      clauses.push(`regexp_replace(coalesce(u.phone,''), '\\D', '', 'g') ILIKE $${params.length}`);
    }
    where.push(`(${clauses.join(" OR ")})`);
  }

  params.push(paging.limit);
  const limitParam = `$${params.length}`;
  params.push(paging.offset);
  const offsetParam = `$${params.length}`;

  const { rows } = await d.query(
    `select u.id,
            coalesce(nullif(u.name,''), u.email, '-') as name,
            u.email,
            coalesce(u.coupon_value_cents,0)::int as balance_cents,
            u.coupon_code,
            u.coupon_expires_at,
            count(*) over () as total
       from public.users u
      ${where.length ? `where ${where.join(" and ")}` : ""}
      order by u.id asc
      limit ${limitParam} offset ${offsetParam}`,
    params
  );

  return {
    items: rows.map((r) => ({
      id: Number(r.id),
      name: r.name,
      email: r.email,
      balance: Number(r.balance_cents) / 100,
      balance_cents: Number(r.balance_cents),
      coupon_code: r.coupon_code || null,
      coupon_expires_at: r.coupon_expires_at || null,
    })),
    paging: { page: paging.page, limit: paging.limit, total: rows.length ? Number(rows[0].total) : 0 },
  };
}

/** Usuario + saldo + historico paginado do ledger canonico (coupon_balance_history). */
export async function getUserCouponDetail(userId, paging = {}, deps = {}) {
  const d = resolveDeps(deps);
  const id = parseUserId(userId);

  const user = await d.query("select id, name, email from public.users where id = $1", [id]);
  if (!user.rows.length) throw new CouponLedgerError("user_not_found", { status: 404 });

  const p = parsePaging(paging);
  const [balance, list, count] = await Promise.all([
    getCouponBalance(id, deps),
    d.query(
      `select id, event_type, delta_cents, balance_before_cents, balance_after_cents, channel, redemption_id, meta, created_at
         from public.coupon_balance_history
        where user_id = $1
        order by created_at desc, id desc
        limit $2 offset $3`,
      [id, p.limit, p.offset]
    ),
    d.query("select count(*) as total from public.coupon_balance_history where user_id = $1", [id]),
  ]);

  const u = user.rows[0];
  return {
    user: { id: Number(u.id), name: u.name ?? null, email: u.email ?? null },
    wallet: {
      balance: balance.balance_cents / 100,
      balance_cents: balance.balance_cents,
      coupon_code: balance.coupon_code,
      expires_at: balance.expires_at,
      is_expired: balance.is_expired,
    },
    transactions: list.rows.map(mapHistoryRow),
    paging: { page: p.page, limit: p.limit, total: Number(count.rows[0]?.total || 0) },
  };
}

/**
 * Ajuste administrativo de credito/debito. `amount` chega em NSCreditos
 * (mesma unidade exibida no admin); convertido para centavos antes de
 * gravar, pois o dado canonico continua sendo coupon_value_cents.
 */
export async function applyAdminCouponAdjustment(
  { userId, operation, amount, reason, adminUserId, idempotencyKey = null } = {},
  deps = {}
) {
  if (adminUserId === null || adminUserId === undefined) {
    throw new CouponLedgerError("admin_required", { status: 403 });
  }
  const trimmedReason = String(reason ?? "").trim();
  if (!trimmedReason) throw new CouponLedgerError("reason_required", { status: 400 });

  const n = Number(amount);
  if (!Number.isFinite(n) || n <= 0) throw new CouponLedgerError("invalid_amount", { status: 400 });
  const amountCents = Math.round(n * 100);

  const out = await applyCouponLedgerEntry(
    {
      userId,
      operation,
      amountCents,
      eventType: "ADMIN_BALANCE_ADJUSTMENT",
      channel: "ADMIN",
      idempotencyKey,
      meta: { reason: trimmedReason, admin_user_id: parseUserId(adminUserId) },
    },
    deps
  );

  return {
    replayed: out.replayed,
    balance: out.balance_cents / 100,
    balance_cents: out.balance_cents,
    transaction: out.entry,
  };
}
