// src/services/rewardProfile.js
//
// Completude de perfil exigida para o resgate real da Loja NS: a Tray
// exige, para criar um Customer (POST /customers), name + email +
// birth_date (required no schema oficial curado pela propria Tray,
// tray-tecnologia/tray-api-ai-plugin, skills/clientes/schemas/cliente.create.json).
//
// name/email a NewStore ja coleta no cadastro (NOT NULL em users). O UNICO
// campo genuinamente faltante e birth_date. cpf/rg/gender sao OPCIONAIS
// nesse schema -- deliberadamente NAO coletados aqui (item 8 do pedido:
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

/**
 * Campos que faltam para a Tray conseguir criar um Customer para este
 * usuario. Hoje so birth_date -- name/email sao NOT NULL em users, sempre
 * presentes.
 */
export function computeMissingRewardProfileFields(user) {
  const missing = [];
  if (!user?.birth_date) missing.push("birth_date");
  return missing;
}

export function isRewardProfileComplete(user) {
  return computeMissingRewardProfileFields(user).length === 0;
}

/** Le o perfil do usuario autenticado com os campos usados pelo resgate. */
export async function getRewardProfile(userId, deps = {}) {
  const d = resolveDeps(deps);
  const id = parseUserId(userId);
  const { rows } = await d.query(
    `select id, name, email, phone, birth_date, tray_customer_id from public.users where id = $1`,
    [id]
  );
  if (!rows.length) throw new RewardProfileError("user_not_found", { status: 404 });
  const u = rows[0];
  const missing = computeMissingRewardProfileFields(u);
  return {
    name: u.name || null,
    email: u.email || null,
    phone: u.phone || null,
    birth_date: u.birth_date || null,
    tray_customer_id: u.tray_customer_id || null,
    profile_complete_for_reward: missing.length === 0,
    missing_fields: missing,
  };
}

/** Atualiza somente birth_date do usuario autenticado. JWT decide o user_id, nunca o body. */
export async function updateBirthDate(userId, rawBirthDate, deps = {}) {
  const d = resolveDeps(deps);
  const id = parseUserId(userId);
  const birthDate = validateBirthDate(rawBirthDate);

  const { rows } = await d.query(
    `update public.users set birth_date = $2 where id = $1 returning id, name, email, phone, birth_date, tray_customer_id`,
    [id, birthDate]
  );
  if (!rows.length) throw new RewardProfileError("user_not_found", { status: 404 });
  const u = rows[0];
  const missing = computeMissingRewardProfileFields(u);
  return {
    name: u.name || null,
    email: u.email || null,
    phone: u.phone || null,
    birth_date: u.birth_date || null,
    tray_customer_id: u.tray_customer_id || null,
    profile_complete_for_reward: missing.length === 0,
    missing_fields: missing,
  };
}
