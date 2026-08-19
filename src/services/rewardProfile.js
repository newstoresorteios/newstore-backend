// src/services/rewardProfile.js
//
// Completude de perfil exigida para o resgate real da Loja NS: a Tray
// exige, para criar um Customer (POST /customers), name + email +
// birth_date (required no schema oficial curado pela propria Tray,
// tray-tecnologia/tray-api-ai-plugin, skills/clientes/schemas/cliente.create.json).
//
// M7.1: o teste controlado real (rodada anterior) provou que, para ESTA
// loja Tray especifica, cpf tambem e obrigatorio na criacao de Customer
// (HTTP 400 real: "Este campo nao pode ser deixado em branco"). Isso
// prevalece sobre o schema curado, que o classificava como opcional --
// e por isso cpf passa a fazer parte do perfil exigido, com o MESMO
// tratamento de PII: nunca devolvido cru fora da tela de edicao que
// realmente precisa, sempre mascarado em qualquer outro contexto.
//
// name/email a NewStore ja coleta no cadastro (NOT NULL em users). rg/gender
// continuam OPCIONAIS no schema e deliberadamente NAO coletados (item 8:
// nao coletar PII que a Tray nao exige, YAGNI).
//
// Mesmo padrao do telefone (users.phone): pede uma vez quando falta, valida,
// salva no perfil, reutiliza depois -- nunca pede de novo se ja completo.

import { query as defaultQuery } from "../db.js";

export class RewardProfileError extends Error {
  constructor(code, { status = 400 } = {}) {
    super(code);
    this.name = "RewardProfileError";
    this.code = code;
    this.status = status;
  }
}

function resolveDeps(deps = {}) {
  return { query: deps.query || defaultQuery };
}

function parseUserId(value) {
  const n = Number(value);
  if (!Number.isSafeInteger(n) || n <= 0) throw new RewardProfileError("invalid_user_id", { status: 400 });
  return n;
}

const MIN_AGE_YEARS = 13;
const MAX_AGE_YEARS = 120;

/**
 * Valida uma data de nascimento no formato YYYY-MM-DD.
 * Nunca infere idade -- exige o valor explicito do usuario, so recusa
 * datas fisicamente impossiveis (futuro, > 120 anos) ou implausiveis
 * (< 13 anos), mesma logica que qualquer cadastro de e-commerce real usa.
 */
export function validateBirthDate(raw) {
  const s = String(raw || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) throw new RewardProfileError("invalid_birth_date", { status: 400 });

  const [y, m, d] = s.split("-").map(Number);
  const date = new Date(Date.UTC(y, m - 1, d));
  const isRealCalendarDate = date.getUTCFullYear() === y && date.getUTCMonth() === m - 1 && date.getUTCDate() === d;
  if (!isRealCalendarDate) throw new RewardProfileError("invalid_birth_date", { status: 400 });

  const now = new Date();
  const ageMs = now.getTime() - date.getTime();
  const ageYears = ageMs / (365.25 * 24 * 3600 * 1000);
  if (ageMs < 0) throw new RewardProfileError("birth_date_in_future", { status: 400 });
  if (ageYears < MIN_AGE_YEARS) throw new RewardProfileError("birth_date_too_recent", { status: 400 });
  if (ageYears > MAX_AGE_YEARS) throw new RewardProfileError("birth_date_implausible", { status: 400 });

  return s;
}

/** Somente os digitos -- nunca usar Number (perde zeros a esquerda). */
export function normalizeCPF(raw) {
  return String(raw ?? "").replace(/\D/g, "");
}

/**
 * Validacao real de CPF: 11 digitos, rejeita sequencias repetidas
 * (000.000.000-00 .. 999.999.999-99, todas matematicamente "validas" pelo
 * digito verificador mas nunca CPFs reais), e confere os dois digitos
 * verificadores pelo algoritmo oficial.
 */
export function validateCPF(raw) {
  const digits = normalizeCPF(raw);
  if (digits.length !== 11) throw new RewardProfileError("invalid_cpf", { status: 400 });
  if (/^(\d)\1{10}$/.test(digits)) throw new RewardProfileError("invalid_cpf", { status: 400 });

  const nums = digits.split("").map(Number);

  let sum = 0;
  for (let i = 0; i < 9; i++) sum += nums[i] * (10 - i);
  let rem = sum % 11;
  const d10 = rem < 2 ? 0 : 11 - rem;
  if (d10 !== nums[9]) throw new RewardProfileError("invalid_cpf", { status: 400 });

  sum = 0;
  for (let i = 0; i < 10; i++) sum += nums[i] * (11 - i);
  rem = sum % 11;
  const d11 = rem < 2 ? 0 : 11 - rem;
  if (d11 !== nums[10]) throw new RewardProfileError("invalid_cpf", { status: 400 });

  return digits;
}

/** ***.***.***-35 -- so os dois digitos verificadores ficam visiveis. */
export function maskCPF(digits) {
  const s = normalizeCPF(digits);
  if (s.length !== 11) return null;
  return `***.***.***-${s.slice(9)}`;
}

/**
 * Campos que faltam para a Tray conseguir criar um Customer para este
 * usuario. birth_date e cpf -- name/email sao NOT NULL em users, sempre
 * presentes.
 */
export function computeMissingRewardProfileFields(user) {
  const missing = [];
  if (!user?.birth_date) missing.push("birth_date");
  if (!user?.cpf) missing.push("cpf");
  return missing;
}

export function isRewardProfileComplete(user) {
  return computeMissingRewardProfileFields(user).length === 0;
}

function mapProfileRow(u) {
  const missing = computeMissingRewardProfileFields(u);
  return {
    name: u.name || null,
    email: u.email || null,
    phone: u.phone || null,
    birth_date: u.birth_date || null,
    has_cpf: !!u.cpf,
    cpf_masked: u.cpf ? maskCPF(u.cpf) : null,
    tray_customer_id: u.tray_customer_id || null,
    profile_complete_for_reward: missing.length === 0,
    missing_fields: missing,
  };
}

/** Le o perfil do usuario autenticado com os campos usados pelo resgate. Nunca devolve cpf cru. */
export async function getRewardProfile(userId, deps = {}) {
  const d = resolveDeps(deps);
  const id = parseUserId(userId);
  const { rows } = await d.query(
    `select id, name, email, phone, birth_date, cpf, tray_customer_id from public.users where id = $1`,
    [id]
  );
  if (!rows.length) throw new RewardProfileError("user_not_found", { status: 404 });
  return mapProfileRow(rows[0]);
}

/** Atualiza somente birth_date do usuario autenticado. JWT decide o user_id, nunca o body. */
export async function updateBirthDate(userId, rawBirthDate, deps = {}) {
  const d = resolveDeps(deps);
  const id = parseUserId(userId);
  const birthDate = validateBirthDate(rawBirthDate);

  const { rows } = await d.query(
    `update public.users set birth_date = $2 where id = $1 returning id, name, email, phone, birth_date, cpf, tray_customer_id`,
    [id, birthDate]
  );
  if (!rows.length) throw new RewardProfileError("user_not_found", { status: 404 });
  return mapProfileRow(rows[0]);
}

/**
 * Atualiza somente cpf do usuario autenticado. JWT decide o user_id, nunca
 * o body. Nunca sobrescreve silenciosamente um cpf ja salvo em outro
 * usuario -- a UNIQUE parcial do banco (migration 035) e a garantia final,
 * mas verificamos aqui primeiro para devolver um erro claro em vez de um
 * erro de constraint cru.
 */
export async function updateCpf(userId, rawCpf, deps = {}) {
  const d = resolveDeps(deps);
  const id = parseUserId(userId);
  const cpf = validateCPF(rawCpf);

  const existing = await d.query(`select id from public.users where cpf = $1 and id <> $2 limit 1`, [cpf, id]);
  if (existing.rows.length) throw new RewardProfileError("cpf_already_in_use", { status: 409 });

  const { rows } = await d.query(
    `update public.users set cpf = $2 where id = $1 returning id, name, email, phone, birth_date, cpf, tray_customer_id`,
    [id, cpf]
  );
  if (!rows.length) throw new RewardProfileError("user_not_found", { status: 404 });
  return mapProfileRow(rows[0]);
}
