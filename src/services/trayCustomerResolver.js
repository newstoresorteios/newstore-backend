// src/services/trayCustomerResolver.js
//
// Resolve o customer_id Tray de um usuario NewStore, com cache
// (users.tray_customer_id) e protecao contra criacao duplicada (item 14
// do pedido: "dois confirms concorrentes nao podem criar Customer Tray A
// e Customer Tray B pro mesmo user").
//
// M7.1: a Tray exige cpf pra criar um Customer nesta loja (prova real via
// 400 controlado). Isso significa que agora ha DOIS sinais de identidade
// possiveis do lado da Tray (e-mail e cpf), que podem apontar pra
// Customers diferentes -- a resolucao precisa reconciliar os dois, nunca
// escolher as cegas.
//
// Ordem:
//   1. users.tray_customer_id ja preenchido? usa direto, nenhuma chamada Tray.
//   2. GET /customers?email= e GET /customers?cpf= (ambas idempotentes por
//      natureza -- buscas concorrentes acham os MESMOS clientes, sem risco
//      de duplicar). Cada uma bate com no maximo 1 Customer (a propria
//      busca ja lanca tray_customer_ambiguous se achar mais de um).
//   3. Reconcilia os dois resultados (reconcileCustomerMatches):
//        A/C — os dois acham o MESMO Customer (ou so um dos dois acha, e o
//              outro sinal nao contradiz) -- mapeia.
//        B    — so e-mail acha, e o Customer nao tem cpf cadastrado na
//              Tray (nao ha dado conflitante) -- mapeia, nunca escreve o
//              cpf nesse Customer.
//        D/E  — e-mail e cpf apontam pra Customers DIFERENTES, ou o
//              Customer achado por um sinal tem o outro campo preenchido
//              com um valor DIFERENTE do nosso -- bloqueio isolado
//              (TrayCustomerIdentityConflictError), nunca escolhe
//              arbitrariamente, nunca sobrescreve o Customer real.
//        F    — nenhum dos dois acha nada -- segue pra criacao.
//   4. Sem match algum: perfil precisa estar completo (birth_date + cpf).
//      Se nao estiver, bloqueio isolado (TrayCustomerProfileIncompleteError)
//      -- nunca inventa nem bloqueia o resgate inteiro.
//   5. Completo: adquire lock de sessao Postgres (pg_advisory_lock, NAO
//      uma transacao -- nao segura BEGIN/COMMIT durante a chamada Tray,
//      so uma conexao dedicada) por userId, RE-CHECA cache+buscas dentro
//      do lock (outra tentativa pode ja ter terminado), e SO ENTAO cria
//      via POST /customers. Libera o lock sempre, mesmo em erro.

import { getPool, query as defaultQuery } from "../db.js";
import { findTrayCustomerByEmail, findTrayCustomerByCpf, createTrayCustomer } from "./trayCustomerClient.js";
import { TrayCatalogError } from "./trayCatalogClient.js";
import { normalizeCPF } from "./rewardProfile.js";

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

/**
 * E-mail e cpf apontam pra identidades incompatíveis do lado da Tray.
 * Nunca escolhe arbitrariamente, nunca sobrescreve o Customer real --
 * bloqueio isolado, deterministico (so leituras GET ocorreram).
 */
export class TrayCustomerIdentityConflictError extends Error {
  constructor(reason, details = {}) {
    super("tray_customer_identity_conflict");
    this.name = "TrayCustomerIdentityConflictError";
    this.code = "tray_customer_identity_conflict";
    this.reason = reason;
    this.details = details;
    // Deterministico: nenhuma mutacao Tray ocorreu (so GETs), seguro compensar.
    this.ambiguous = false;
  }
}

function resolveDeps(deps = {}) {
  return {
    query: deps.query || defaultQuery,
    getPool: deps.getPool || getPool,
    findTrayCustomerByEmail: deps.findTrayCustomerByEmail || findTrayCustomerByEmail,
    findTrayCustomerByCpf: deps.findTrayCustomerByCpf || findTrayCustomerByCpf,
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
 * Reconcilia o resultado das duas buscas (por e-mail e por cpf) num
 * unico veredito: mapear pra um Customer existente, bloquear por
 * conflito de identidade, ou seguir pra criacao.
 *
 * @returns {{type:'map', id:string} | {type:'conflict', reason:string, details:object} | {type:'create'}}
 */
export function reconcileCustomerMatches({ emailResult, cpfResult, profileEmail }) {
  if (!emailResult && !cpfResult) {
    // Caso F: nenhum sinal encontrou nada -- segue pra criacao.
    return { type: "create" };
  }

  if (emailResult && cpfResult) {
    if (String(emailResult.id) === String(cpfResult.id)) {
      // Casos A/C: os dois sinais confirmam o MESMO Customer.
      return { type: "map", id: emailResult.id };
    }
    // Caso E: e-mail e cpf apontam pra Customers DIFERENTES -- nunca escolher.
    return {
      type: "conflict",
      reason: "email_and_cpf_different_customers",
      details: { emailCustomerId: emailResult.id, cpfCustomerId: cpfResult.id },
    };
  }

  if (emailResult && !cpfResult) {
    const trayCpf = normalizeCPF(emailResult.cpf);
    if (!trayCpf) {
      // Caso B: e-mail unico bate, Customer sem cpf cadastrado na Tray --
      // nenhum dado conflitante, seguro mapear (nunca escrevemos cpf nele).
      return { type: "map", id: emailResult.id };
    }
    // O Customer achado por e-mail tem um cpf cadastrado que NAO e o
    // nosso (senao a busca por cpf teria achado o mesmo Customer) --
    // conflito de identidade, pode ser outra pessoa.
    return {
      type: "conflict",
      reason: "email_match_cpf_mismatch",
      details: { customerId: emailResult.id },
    };
  }

  // !emailResult && cpfResult
  const trayEmail = String(cpfResult.email || "").trim().toLowerCase();
  const ourEmail = String(profileEmail || "").trim().toLowerCase();
  if (!trayEmail || trayEmail === ourEmail) {
    // Caso C: cpf unico bate, mesmo e-mail (ou Customer sem e-mail
    // cadastrado, nenhum dado conflitante) -- mapeia.
    return { type: "map", id: cpfResult.id };
  }
  // Caso D: cpf bate mas o e-mail e DIFERENTE -- pode ser outra pessoa,
  // nunca linkar automaticamente.
  return {
    type: "conflict",
    reason: "cpf_match_email_mismatch",
    details: { customerId: cpfResult.id },
  };
}

async function lookupBothSignals(d, profile, deps) {
  const email = String(profile.email || "").trim();
  const cpf = normalizeCPF(profile.cpf);
  const emailResult = email ? await d.findTrayCustomerByEmail(email, deps) : null;
  const cpfResult = cpf ? await d.findTrayCustomerByCpf(cpf, deps) : null;
  return { emailResult, cpfResult };
}

/**
 * @param {number} userId
 * @param {{name:string, email:string, birthDate:string|null, cpf?:string|null, phone:string|null}} profile
 * @returns {Promise<string>} tray_customer_id
 * @throws {TrayCustomerProfileIncompleteError} nenhum Customer existente encontrado e perfil sem birth_date/cpf
 * @throws {TrayCustomerIdentityConflictError} e-mail e cpf apontam pra identidades incompativeis
 * @throws {TrayCatalogError} code="tray_customer_ambiguous" mais de um Customer com o mesmo e-mail/cpf
 */
export async function resolveTrayCustomerId(userId, profile, deps = {}) {
  const d = resolveDeps(deps);

  const cached = await d.query(`select tray_customer_id from public.users where id = $1`, [userId]);
  const existing = cached.rows[0]?.tray_customer_id;
  if (existing) return String(existing);

  const { emailResult, cpfResult } = await lookupBothSignals(d, profile, deps);
  const resolution = reconcileCustomerMatches({ emailResult, cpfResult, profileEmail: profile.email });

  if (resolution.type === "map") {
    await persistTrayCustomerIdIfAbsent(d.query, userId, resolution.id);
    return String(resolution.id);
  }
  if (resolution.type === "conflict") {
    throw new TrayCustomerIdentityConflictError(resolution.reason, resolution.details);
  }

  const missing = [];
  if (!profile.birthDate) missing.push("birth_date");
  if (!normalizeCPF(profile.cpf)) missing.push("cpf");
  if (missing.length) throw new TrayCustomerProfileIncompleteError(missing);

  const pool = await d.getPool();
  return withUserCreationLock(pool, userId, async () => {
    // Re-checa DENTRO do lock -- outra tentativa concorrente pode ja ter
    // resolvido (cache ou criacao) enquanto esperavamos o lock.
    const recheck = await d.query(`select tray_customer_id from public.users where id = $1`, [userId]);
    const recheckId = recheck.rows[0]?.tray_customer_id;
    if (recheckId) return String(recheckId);

    const again = await lookupBothSignals(d, profile, deps);
    const resolutionAgain = reconcileCustomerMatches({ emailResult: again.emailResult, cpfResult: again.cpfResult, profileEmail: profile.email });
    if (resolutionAgain.type === "map") {
      await persistTrayCustomerIdIfAbsent(d.query, userId, resolutionAgain.id);
      return String(resolutionAgain.id);
    }
    if (resolutionAgain.type === "conflict") {
      throw new TrayCustomerIdentityConflictError(resolutionAgain.reason, resolutionAgain.details);
    }

    const created = await d.createTrayCustomer(
      { name: profile.name, email: profile.email, birthDate: profile.birthDate, cpf: normalizeCPF(profile.cpf), phone: profile.phone },
      deps
    );
    await persistTrayCustomerIdIfAbsent(d.query, userId, created.id);
    return String(created.id);
  });
}

export { TrayCatalogError };
