// src/services/nscreditWallet.js
//
// LEGADO / INATIVO NA LOJA a partir da FASE 5 (decisao de negocio: o cupom
// individual do usuario passou a ser a fonte de NSCreditos, ver
// src/services/couponLedger.js e src/services/couponAdmin.js).
//
// Este arquivo NAO foi apagado nem alterado em comportamento — nscredit_wallets
// e nscredit_transactions continuam existindo em producao e este service
// continua funcional, so que nada na Loja o chama mais. Mantido por
// compatibilidade/auditoria; nao usar em codigo novo da Loja de Premios.
//
// --- comentario original, ainda valido para o dado ja existente abaixo ---
// Carteira de NSCreditos.
//
// NSCreditos sao uma moeda propria da Loja de Premios. NAO tem relacao com
// coupon_value_cents, saldo em reais, Mercado Pago, Vindi ou preco Tray.
// Nada aqui le ou escreve nessas estruturas.
//
// Arquitetura:
//   nscredit_wallets      -> saldo materializado (performance)
//   nscredit_transactions -> ledger imutavel (fonte auditavel)
//
// O saldo NUNCA e sobrescrito diretamente: toda mudanca e uma operacao
// (credit | debit) aplicada atomicamente com lock da linha da carteira.

import { getPool, query as dbQuery } from "../db.js";

/** NSCreditos nao tem centavos e a API so trafega inteiros seguros em JS. */
export const MAX_NSCREDIT_AMOUNT = Number.MAX_SAFE_INTEGER;

/** Teto de paginacao para historico e busca administrativa. */
export const MAX_PAGE_SIZE = 100;

/** Tamanho maximo do motivo registrado no ledger. */
export const MAX_REASON_LENGTH = 300;

/** Origem padrao desta fase. A coluna aceita outras origens no futuro. */
export const SOURCE_ADMIN = "admin";

/**
 * Sinal interno: o INSERT do ledger bateu no unique de idempotency_key porque
 * outra requisicao concorrente gravou primeiro. Precisa atravessar o
 * ROLLBACK para ser tratado fora da transacao.
 */
class IdempotencyRace extends Error {
  constructor(key) {
    super("idempotency_race");
    this.name = "IdempotencyRace";
    this.key = key;
  }
}

export class NsCreditError extends Error {
  constructor(code, { status = 400, details = null } = {}) {
    super(code);
    this.name = "NsCreditError";
    this.code = code;
    this.status = status;
    this.details = details;
  }
}

/* ─────────────────────────── Validacao ─────────────────────────── */

/**
 * O banco guarda BIGINT, que pode ultrapassar a faixa segura do Number do JS.
 * Convertemos explicitamente e falhamos alto em vez de devolver numero errado.
 */
function toSafeInt(value, { code = "balance_out_of_range", status = 500 } = {}) {
  if (value === null || value === undefined) return 0;
  const s = String(value).trim();
  if (!/^-?\d+$/.test(s)) throw new NsCreditError(code, { status });
  const n = Number(s);
  if (!Number.isSafeInteger(n)) throw new NsCreditError(code, { status });
  return n;
}

export function parseNsCreditAmount(value) {
  const invalid = () => new NsCreditError("invalid_amount", { status: 400 });

  if (typeof value === "boolean") throw invalid();

  let s;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw invalid();
    if (!Number.isInteger(value)) throw invalid();
    s = String(value);
  } else if (typeof value === "string") {
    s = value.trim();
  } else {
    throw invalid();
  }

  if (!/^\d+$/.test(s)) throw invalid();

  // Fora da faixa segura antes mesmo de virar Number: compara como string.
  if (s.replace(/^0+/, "").length > String(MAX_NSCREDIT_AMOUNT).length) {
    throw new NsCreditError("amount_out_of_range", { status: 400 });
  }

  const n = Number(s);
  if (!Number.isSafeInteger(n)) throw new NsCreditError("amount_out_of_range", { status: 400 });
  if (n <= 0) throw invalid();
  return n;
}

export function parseOperation(value) {
  if (value === "credit" || value === "debit") return value;
  throw new NsCreditError("invalid_operation", { status: 400 });
}

export function parseReason(value) {
  if (typeof value !== "string") throw new NsCreditError("reason_required", { status: 400 });
  const trimmed = value.trim();
  if (!trimmed) throw new NsCreditError("reason_required", { status: 400 });
  if (trimmed.length > MAX_REASON_LENGTH) throw new NsCreditError("reason_too_long", { status: 400 });
  return trimmed;
}

export function parseUserId(value) {
  if (typeof value === "boolean" || value === null || value === undefined) {
    throw new NsCreditError("invalid_user_id", { status: 400 });
  }
  const s = String(value).trim();
  if (!/^\d+$/.test(s)) throw new NsCreditError("invalid_user_id", { status: 400 });
  const n = Number(s);
  if (!Number.isSafeInteger(n) || n <= 0) throw new NsCreditError("invalid_user_id", { status: 400 });
  return n;
}

function parsePaging({ page = 1, limit = 20 } = {}) {
  const safeLimit = Math.min(Math.max(Number(limit) || 20, 1), MAX_PAGE_SIZE);
  const safePage = Math.max(Number(page) || 1, 1);
  return { page: safePage, limit: safeLimit, offset: (safePage - 1) * safeLimit };
}

/* ─────────────────────────── Deps ─────────────────────────── */

async function defaultWithTransaction(fn) {
  const pool = await getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const out = await fn(client);
    await client.query("COMMIT");
    return out;
  } catch (e) {
    await client.query("ROLLBACK").catch(() => {});
    throw e;
  } finally {
    client.release();
  }
}

function resolveDeps(deps = {}) {
  return {
    query: deps.query || dbQuery,
    withTransaction: deps.withTransaction || defaultWithTransaction,
  };
}

/* ─────────────────────────── Mapeamento ─────────────────────────── */

/** DTO do ledger. Nao expoe idempotency_key nem o admin responsavel. */
function mapTransaction(row) {
  return {
    id: Number(row.id),
    operation: row.operation,
    amount: toSafeInt(row.amount),
    balance_before: toSafeInt(row.balance_before),
    balance_after: toSafeInt(row.balance_after),
    source_type: row.source_type,
    source_id: row.source_id ?? null,
    reason: row.reason ?? null,
    created_at: row.created_at instanceof Date ? row.created_at.toISOString() : row.created_at ?? null,
  };
}

const TRANSACTION_COLUMNS = `
  id, user_id, operation, amount, balance_before, balance_after,
  source_type, source_id, reason, created_by, idempotency_key, created_at
`;

/* ─────────────────────────── Leitura ─────────────────────────── */

/**
 * Saldo factual. Usuario sem carteira tem saldo 0 — e isso NAO cria linha,
 * para nao gerar milhoes de carteiras vazias.
 */
export async function getBalance(userId, deps = {}) {
  const d = resolveDeps(deps);
  const id = parseUserId(userId);

  const { rows } = await d.query(
    "select balance from public.nscredit_wallets where user_id = $1",
    [id]
  );

  return { balance: rows.length ? toSafeInt(rows[0].balance) : 0 };
}

export async function getTransactionHistory(userId, paging = {}, deps = {}) {
  const d = resolveDeps(deps);
  const id = parseUserId(userId);
  const { page, limit, offset } = parsePaging(paging);

  const [list, count] = await Promise.all([
    d.query(
      `select ${TRANSACTION_COLUMNS}
         from public.nscredit_transactions
        where user_id = $1
        order by created_at desc, id desc
        limit $2 offset $3`,
      [id, limit, offset]
    ),
    d.query("select count(*) as total from public.nscredit_transactions where user_id = $1", [id]),
  ]);

  return {
    items: list.rows.map(mapTransaction),
    paging: { page, limit, total: toSafeInt(count.rows[0]?.total) },
  };
}

/* ─────────────────────────── Movimentacao ─────────────────────────── */

async function findByIdempotencyKey(client, key) {
  if (!key) return null;
  const { rows } = await client.query(
    `select ${TRANSACTION_COLUMNS} from public.nscredit_transactions where idempotency_key = $1 limit 1`,
    [key]
  );
  return rows[0] || null;
}

/**
 * Aplica UMA movimentacao na carteira, atomicamente.
 *
 * Fluxo dentro da transacao:
 *   1. valida que o usuario existe
 *   2. se ja existe movimentacao com a mesma idempotency_key -> devolve a original
 *   3. garante a carteira (INSERT ... ON CONFLICT DO NOTHING)
 *   4. SELECT balance ... FOR UPDATE  (serializa operacoes concorrentes)
 *   5. valida saldo suficiente no debito
 *   6. UPDATE do saldo
 *   7. INSERT no ledger
 *
 * Qualquer erro derruba a transacao inteira: nunca sobra saldo sem ledger
 * nem ledger sem saldo.
 *
 * `sourceType` fica aberto para origens futuras (ex.: "redemption"), mas
 * NENHUMA origem automatica esta implementada nesta fase.
 */
export async function applyTransaction(
  { userId, operation, amount, sourceType = SOURCE_ADMIN, sourceId = null, reason = null, createdBy = null, idempotencyKey = null } = {},
  deps = {}
) {
  const d = resolveDeps(deps);

  const id = parseUserId(userId);
  const op = parseOperation(operation);
  const value = parseNsCreditAmount(amount);
  const source = String(sourceType || SOURCE_ADMIN).trim() || SOURCE_ADMIN;

  // Motivo e obrigatorio para movimentacao administrativa.
  const finalReason = source === SOURCE_ADMIN ? parseReason(reason) : (reason == null ? null : parseReason(reason));

  const key = idempotencyKey == null ? null : String(idempotencyKey).trim() || null;

  try {
    return await runTransaction();
  } catch (e) {
    // Corrida na mesma idempotency_key: outra requisicao gravou primeiro.
    // A transacao JA foi revertida (o UPDATE do saldo nao valeu), entao aqui
    // so lemos a movimentacao vencedora e devolvemos ela.
    if (e instanceof IdempotencyRace) {
      const { rows } = await d.query(
        `select ${TRANSACTION_COLUMNS} from public.nscredit_transactions where idempotency_key = $1 limit 1`,
        [e.key]
      );
      if (rows[0]) {
        return {
          replayed: true,
          balance: toSafeInt(rows[0].balance_after),
          transaction: mapTransaction(rows[0]),
        };
      }
      throw new NsCreditError("idempotency_conflict", { status: 409 });
    }
    throw e;
  }

  async function runTransaction() {
    return await d.withTransaction(async (client) => {
    const user = await client.query("select id from public.users where id = $1", [id]);
    if (!user.rows.length) throw new NsCreditError("user_not_found", { status: 404 });

    const replay = await findByIdempotencyKey(client, key);
    if (replay) {
      return {
        replayed: true,
        balance: toSafeInt(replay.balance_after),
        transaction: mapTransaction(replay),
      };
    }

    await client.query(
      "insert into public.nscredit_wallets (user_id) values ($1) on conflict (user_id) do nothing",
      [id]
    );

    // FOR UPDATE: duas operacoes concorrentes na mesma carteira ficam em fila.
    const locked = await client.query(
      "select balance from public.nscredit_wallets where user_id = $1 for update",
      [id]
    );
    const balanceBefore = toSafeInt(locked.rows[0]?.balance);

    if (op === "debit" && value > balanceBefore) {
      throw new NsCreditError("insufficient_balance", {
        status: 409,
        details: { balance: balanceBefore, requested: value },
      });
    }

    const balanceAfter = op === "credit" ? balanceBefore + value : balanceBefore - value;
    if (!Number.isSafeInteger(balanceAfter)) {
      throw new NsCreditError("balance_out_of_range", { status: 409 });
    }
    if (balanceAfter < 0) {
      // Defesa redundante: o CHECK do banco tambem barra.
      throw new NsCreditError("insufficient_balance", {
        status: 409,
        details: { balance: balanceBefore, requested: value },
      });
    }

    await client.query(
      "update public.nscredit_wallets set balance = $1, updated_at = now() where user_id = $2",
      [balanceAfter, id]
    );

    let inserted;
    try {
      inserted = await client.query(
        `insert into public.nscredit_transactions
           (user_id, operation, amount, balance_before, balance_after,
            source_type, source_id, reason, created_by, idempotency_key)
         values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
         returning ${TRANSACTION_COLUMNS}`,
        [id, op, value, balanceBefore, balanceAfter, source, sourceId, finalReason, createdBy, key]
      );
    } catch (e) {
      // Sinaliza a corrida para FORA da transacao. Precisa ser um throw:
      // so assim o ROLLBACK desfaz o UPDATE de saldo feito acima, evitando
      // que o valor seja aplicado duas vezes.
      if (e?.code === "23505" && key) throw new IdempotencyRace(key);
      throw e;
    }

      return {
        replayed: false,
        balance: balanceAfter,
        transaction: mapTransaction(inserted.rows[0]),
      };
    });
  }
}

/**
 * Movimentacao manual feita por um administrador.
 * O `created_by` vem SEMPRE da sessao autenticada — nunca do payload.
 */
export async function applyAdminAdjustment(
  { userId, operation, amount, reason, adminUserId, idempotencyKey = null } = {},
  deps = {}
) {
  if (adminUserId === null || adminUserId === undefined) {
    throw new NsCreditError("admin_required", { status: 403 });
  }
  const admin = parseUserId(adminUserId);

  return await applyTransaction(
    {
      userId,
      operation,
      amount,
      sourceType: SOURCE_ADMIN,
      sourceId: null,
      reason,
      createdBy: admin,
      idempotencyKey,
    },
    deps
  );
}

/* ─────────────────────────── Administracao ─────────────────────────── */

/**
 * Busca de clientes reais (tabela `users`) com o saldo de NSCreditos.
 * Nao cria segundo cadastro e nao devolve campos privados desnecessarios.
 */
export async function searchUsersForAdmin({ q = "", page = 1, limit = 20 } = {}, deps = {}) {
  const d = resolveDeps(deps);
  const paging = parsePaging({ page, limit });

  const term = String(q ?? "").trim();
  const params = [];
  const where = [];

  if (term) {
    params.push(`%${term}%`);
    const like = `$${params.length}`;
    const digits = term.replace(/\D/g, "");

    const clauses = [
      `u.name ILIKE ${like}`,
      `u.email ILIKE ${like}`,
      `coalesce(u.phone,'') ILIKE ${like}`,
    ];
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
            coalesce(w.balance, 0) as balance,
            count(*) over () as total
       from public.users u
       left join public.nscredit_wallets w on w.user_id = u.id
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
      balance: toSafeInt(r.balance),
    })),
    paging: {
      page: paging.page,
      limit: paging.limit,
      total: rows.length ? toSafeInt(rows[0].total) : 0,
    },
  };
}

/** Detalhe da carteira para o admin: usuario, saldo e historico paginado. */
export async function getAdminWalletDetail(userId, paging = {}, deps = {}) {
  const d = resolveDeps(deps);
  const id = parseUserId(userId);

  const user = await d.query(
    "select id, name, email from public.users where id = $1",
    [id]
  );
  if (!user.rows.length) throw new NsCreditError("user_not_found", { status: 404 });

  const [{ balance }, history] = await Promise.all([
    getBalance(id, deps),
    getTransactionHistory(id, paging, deps),
  ]);

  const u = user.rows[0];
  return {
    user: { id: Number(u.id), name: u.name ?? null, email: u.email ?? null },
    wallet: { balance },
    transactions: history.items,
    paging: history.paging,
  };
}
