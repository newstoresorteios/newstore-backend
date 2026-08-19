// src/services/trayCustomerResolver.js
//
// Resolve o customer_id Tray de um usuario NewStore, com cache
// (users.tray_customer_id) e protecao contra criacao duplicada (item 14
// do pedido: "dois confirms concorrentes nao podem criar Customer Tray A
// e Customer Tray B pro mesmo user").
//
// Ordem:
//   1. users.tray_customer_id ja preenchido? usa direto, nenhuma chamada Tray.
//   2. GET /customers?email= (idempotente por natureza -- duas buscas
//      concorrentes acham o MESMO cliente, sem risco de duplicar).
//      Achou? persiste (UPDATE condicional, tolera corrida) e usa.
//   3. Nao achou: perfil precisa estar completo (birth_date). Se nao
//      estiver, bloqueio isolado (TrayCustomerProfileIncompleteError) --
//      nunca inventa nem bloqueia o resgate inteiro.
//   4. Completo: adquire lock de sessao Postgres (pg_advisory_lock, NAO
//      uma transacao -- nao segura BEGIN/COMMIT durante a chamada Tray,
//      so uma conexao dedicada) por userId, RE-CHECA cache+busca dentro
//      do lock (outra tentativa pode ja ter terminado), e SO ENTAO cria
//      via POST /customers. Libera o lock sempre, mesmo em erro.

import { getPool, query as defaultQuery } from "../db.js";
import { findTrayCustomerByEmail, createTrayCustomer } from "./trayCustomerClient.js";
import { TrayCatalogError } from "./trayCatalogClient.js";

export class TrayCustomerProfileIncompleteError extends Error {
  constructor(missingFields = []) {
    super("tray_customer_profile_incomplete");
    this.name = "TrayCustomerProfileIncompleteError";
    this.code = "tray_customer_profile_incomplete";
    this.missingFields = missingFields;
    // Deterministico: nenhuma mutacao Tray ocorreu, seguro compensar.
    this.ambiguous = false;
  }
}

function resolveDeps(deps = {}) {
  return {
    query: deps.query || defaultQuery,
    getPool: deps.getPool || getPool,
    findTrayCustomerByEmail: deps.findTrayCustomerByEmail || findTrayCustomerByEmail,
    createTrayCustomer: deps.createTrayCustomer || createTrayCustomer,
  };
}

async function persistTrayCustomerIdIfAbsent(query, userId, trayCustomerId) {
  // So grava se ainda estiver vazio -- nunca sobrescreve um valor ja
  // presente (pode ter sido preenchido por uma tentativa concorrente
  // enquanto esta rodava).
  await query(`update public.users set tray_customer_id = $2 where id = $1 and tray_customer_id is null`, [userId, trayCustomerId]);
}

async function withUserCreationLock(pool, userId, fn) {
  const client = await pool.connect();
  try {
    await client.query("select pg_advisory_lock($1)", [userId]);
    try {
      return await fn();
    } finally {
      await client.query("select pg_advisory_unlock($1)", [userId]).catch(() => {});
    }
  } finally {
    client.release();
  }
}

/**
 * @param {number} userId
 * @param {{name:string, email:string, birthDate:string|null, phone:string|null}} profile
 * @returns {Promise<string>} tray_customer_id
 * @throws {TrayCustomerProfileIncompleteError} perfil sem birth_date e nenhum Customer existente foi achado
 * @throws {TrayCatalogError} code="tray_customer_ambiguous" mais de um Customer com o mesmo e-mail
 */
export async function resolveTrayCustomerId(userId, profile, deps = {}) {
  const d = resolveDeps(deps);

  const cached = await d.query(`select tray_customer_id from public.users where id = $1`, [userId]);
  const existing = cached.rows[0]?.tray_customer_id;
  if (existing) return String(existing);

  const found = await d.findTrayCustomerByEmail(profile.email, deps);
  if (found) {
    await persistTrayCustomerIdIfAbsent(d.query, userId, found.id);
    return found.id;
  }

  if (!profile.birthDate) {
    throw new TrayCustomerProfileIncompleteError(["birth_date"]);
  }

  const pool = await d.getPool();
  return withUserCreationLock(pool, userId, async () => {
    // Re-checa DENTRO do lock -- outra tentativa concorrente pode ja ter
    // resolvido (cache ou criacao) enquanto esperavamos o lock.
    const recheck = await d.query(`select tray_customer_id from public.users where id = $1`, [userId]);
    const recheckId = recheck.rows[0]?.tray_customer_id;
    if (recheckId) return String(recheckId);

    const foundAgain = await d.findTrayCustomerByEmail(profile.email, deps);
    if (foundAgain) {
      await persistTrayCustomerIdIfAbsent(d.query, userId, foundAgain.id);
      return foundAgain.id;
    }

    const created = await d.createTrayCustomer(
      { name: profile.name, email: profile.email, birthDate: profile.birthDate, phone: profile.phone },
      deps
    );
    await persistTrayCustomerIdIfAbsent(d.query, userId, created.id);
    return created.id;
  });
}

export { TrayCatalogError };
