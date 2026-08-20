// src/services/trayCustomerClient.js
//
// Resolução de identidade do cliente Tray.
//
// O pedido Tray exige customer_id (ID interno da Tray, nunca o nosso
// users.id). Este arquivo PROCURA um cliente Tray existente por e-mail
// (GET, sempre seguro) e, se realmente necessário e autorizado pelo
// chamador, CRIA um novo (POST /customers) — nunca sem antes procurar.
//
// Contrato de busca: GET /customers?email=<email> — documentação oficial
// Tray (skills/clientes, tray-tecnologia/tray-api-ai-plugin, mesma
// convenção de envelope já usada pelo catálogo: coleção no plural, item
// singular dentro).
//
// Contrato de criação (skills/clientes/schemas/cliente.create.json,
// curado pela própria Tray, required=["name","email","birth_date"]):
// name/email a NewStore já tem sempre; birth_date é coletado no perfil
// (ver rewardProfile.js) especificamente para isso.
//
// M7.1: o teste controlado real (POST /customers contra a conta Tray real
// desta loja) provou que cpf TAMBÉM é obrigatório para esta loja
// especificamente — prevalece sobre o schema curado, que o classificava
// como opcional. cpf agora faz parte do perfil exigido (rewardProfile.js).
// rg/gender continuam OPCIONAIS e deliberadamente NÃO enviados (item 8).

import { trayCatalogGet, TrayCatalogError } from "./trayCatalogClient.js";
import { trayMutationRequest } from "./trayMutationClient.js";

function unwrapCustomers(body) {
  if (!body || typeof body !== "object") return null;
  const list = body.Customers;
  if (!Array.isArray(list)) return null;
  return list.map((entry) =>
    entry && typeof entry === "object" && entry.Customer && typeof entry.Customer === "object" ? entry.Customer : entry
  );
}

/**
 * @returns {Promise<{id: string, name: string|null, email: string} | null>}
 *   null quando nenhum cliente Tray tem esse e-mail — nunca inventa um.
 * @throws {TrayCatalogError} code="tray_customer_ambiguous" quando MAIS DE UM
 *   cliente Tray tem exatamente esse e-mail — nunca escolhe arbitrariamente.
 */
export async function findTrayCustomerByEmail(email, options = {}) {
  const normalized = String(email || "").trim().toLowerCase();
  if (!normalized) throw new TrayCatalogError("email_missing", { status: 400 });

  const body = await trayCatalogGet("/customers", { email: normalized }, options);
  const rows = unwrapCustomers(body);
  if (!rows) throw new TrayCatalogError("tray_invalid_response", { status: 502 });
  if (!rows.length) return null;

  // Correspondência EXATA de e-mail — a Tray pode devolver resultados
  // parciais/case-insensitive; nunca usar o primeiro item às cegas.
  const matches = rows.filter((r) => String(r?.email || "").trim().toLowerCase() === normalized && r?.id != null);
  if (!matches.length) return null;
  if (matches.length > 1) {
    throw new TrayCatalogError("tray_customer_ambiguous", { status: 409, publicDetails: { count: matches.length } });
  }

  const match = matches[0];
  return { id: String(match.id), name: match.name || null, email: normalized, cpf: match.cpf ? String(match.cpf) : null };
}

/**
 * Mesmo contrato de findTrayCustomerByEmail, buscando por cpf
 * (GET /customers?cpf=<11 digitos>). cpf ja deve vir normalizado (sem
 * pontuacao) -- ver rewardProfile.js.
 *
 * @returns {Promise<{id: string, name: string|null, email: string|null, cpf: string} | null>}
 * @throws {TrayCatalogError} code="tray_customer_ambiguous" quando MAIS DE UM
 *   cliente Tray tem exatamente esse cpf — nunca escolhe arbitrariamente.
 */
/**
 * Customer CANONICO por id — a fonte de identidade do POST /orders quando
 * users.tray_customer_id ja existe.
 *
 * Motivo (M7, prova real): o pedido valida Order.Customer como cadastro de
 * cliente. Remontar essa identidade com os dados da NewStore faz a Tray achar
 * que e um cliente novo (o e-mail diverge do cadastro dela) e recusar com
 * cpf "Está em uso em outro cadastro.". Usando a identidade que a propria
 * Tray tem, o cadastro descrito e o mesmo que ja existe.
 *
 * Devolve os campos crus da Tray (sem normalizar/mascarar): quem monta o DTO
 * decide o que enviar. Nunca logar este retorno -- e PII.
 */
export async function getTrayCustomerById(customerId, options = {}) {
  const id = String(customerId ?? "").trim();
  if (!id) throw new TrayCatalogError("customer_id_invalid", { status: 400 });

  const body = await trayCatalogGet(`/customers/${encodeURIComponent(id)}`, {}, options);
  const c = body?.Customer ?? body?.customer ?? null;
  if (!c || c.id == null) throw new TrayCatalogError("tray_invalid_response", { status: 502 });

  return {
    id: String(c.id),
    type: c.type != null ? String(c.type) : null,
    name: c.name ? String(c.name).trim() : null,
    email: c.email ? String(c.email).trim() : null,
    cpf: String(c.cpf || "").replace(/\D/g, "") || null,
    birth_date: c.birth_date ? String(c.birth_date).slice(0, 10) : null,
    phone: String(c.phone || "").replace(/\D/g, "") || null,
    cellphone: String(c.cellphone || "").replace(/\D/g, "") || null,
    rg: c.rg ? String(c.rg).trim() : null,
    gender: c.gender ? String(c.gender).trim() : null,
  };
}

export async function findTrayCustomerByCpf(cpf, options = {}) {
  const normalized = String(cpf || "").replace(/\D/g, "");
  if (normalized.length !== 11) throw new TrayCatalogError("cpf_missing", { status: 400 });

  const body = await trayCatalogGet("/customers", { cpf: normalized }, options);
  const rows = unwrapCustomers(body);
  if (!rows) throw new TrayCatalogError("tray_invalid_response", { status: 502 });
  if (!rows.length) return null;

  const matches = rows.filter((r) => String(r?.cpf || "").replace(/\D/g, "") === normalized && r?.id != null);
  if (!matches.length) return null;
  if (matches.length > 1) {
    throw new TrayCatalogError("tray_customer_ambiguous", { status: 409, publicDetails: { count: matches.length } });
  }

  const match = matches[0];
  return {
    id: String(match.id),
    name: match.name || null,
    email: match.email ? String(match.email).trim().toLowerCase() : null,
    cpf: normalized,
  };
}

/**
 * Cria um Customer Tray novo. Chamador e responsavel por ja ter feito o
 * lookup (findTrayCustomerByEmail) e confirmado que nao existe — esta
 * funcao NUNCA verifica duplicidade sozinha, so cria.
 *
 * @param {object} profile
 * @param {string} profile.name
 * @param {string} profile.email
 * @param {string} profile.birthDate formato YYYY-MM-DD
 * @param {string} profile.cpf 11 digitos, sem pontuacao (ja normalizado -- ver rewardProfile.js)
 * @param {string|null} [profile.phone]
 * @returns {Promise<{id: string}>}
 */
export async function createTrayCustomer({ name, email, birthDate, cpf, phone = null }, options = {}) {
  const cleanName = String(name || "").trim();
  const cleanEmail = String(email || "").trim().toLowerCase();
  const cleanBirthDate = String(birthDate || "").trim();
  const cleanCpf = String(cpf || "").replace(/\D/g, "");

  if (!cleanName) throw new TrayCatalogError("customer_name_missing", { status: 400 });
  if (!cleanEmail) throw new TrayCatalogError("customer_email_missing", { status: 400 });
  if (!/^\d{4}-\d{2}-\d{2}$/.test(cleanBirthDate)) throw new TrayCatalogError("customer_birth_date_missing", { status: 400 });
  // M7.1: provado obrigatorio via 400 real desta loja -- nunca inventar, nunca enviar vazio.
  if (cleanCpf.length !== 11) throw new TrayCatalogError("customer_cpf_missing", { status: 400 });

  const body = {
    Customer: {
      name: cleanName,
      email: cleanEmail,
      birth_date: cleanBirthDate,
      cpf: cleanCpf,
      // rg/gender deliberadamente ausentes -- opcionais no schema oficial, YAGNI (item 8).
      ...(phone ? { phone: String(phone) } : {}),
    },
  };

  const result = await trayMutationRequest("TRAY_CUSTOMER_CREATE", "POST", "/customers", body, options);
  const id = result?.id ?? result?.Customer?.id ?? result?.customer?.id ?? null;
  if (!id) throw new TrayCatalogError("tray_customer_id_missing", { status: 502, publicDetails: { tray_body: result } });

  return { id: String(id) };
}
