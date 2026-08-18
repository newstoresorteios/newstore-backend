// src/services/userAddress.js
//
// Endereco de entrega do usuario. Necessario para cotacao de frete e,
// futuramente, para o pedido Tray real (Fase 5, ainda bloqueada — ver
// relatorio: contrato de pagamento pendente).
//
// Campos determinados pelo contrato de endereco de cliente da Tray
// (recipient, street, number, neighborhood, city, state, zipcode).

import { query as defaultQuery } from "../db.js";

export class UserAddressError extends Error {
  constructor(code, { status = 400, details = null } = {}) {
    super(code);
    this.name = "UserAddressError";
    this.code = code;
    this.status = status;
    this.details = details;
  }
}

function resolveDeps(deps = {}) {
  return { query: deps.query || defaultQuery };
}

function parseUserId(value) {
  const n = Number(value);
  if (!Number.isSafeInteger(n) || n <= 0) throw new UserAddressError("invalid_user_id", { status: 400 });
  return n;
}

function requiredString(value, field, { maxLength = 200 } = {}) {
  const s = String(value ?? "").trim();
  if (!s) throw new UserAddressError(`${field}_required`, { status: 400 });
  if (s.length > maxLength) throw new UserAddressError(`${field}_too_long`, { status: 400 });
  return s;
}

function parseZipcode(value) {
  const digits = String(value ?? "").replace(/\D/g, "");
  if (digits.length !== 8) throw new UserAddressError("invalid_zipcode", { status: 400 });
  return digits;
}

function parseState(value) {
  const s = String(value ?? "").trim().toUpperCase();
  if (!/^[A-Z]{2}$/.test(s)) throw new UserAddressError("invalid_state", { status: 400 });
  return s;
}

function mapRow(row) {
  return {
    id: row.id,
    recipient_name: row.recipient_name,
    zipcode: row.zipcode,
    street: row.street,
    number: row.number,
    complement: row.complement || null,
    neighborhood: row.neighborhood,
    city: row.city,
    state: row.state,
    country: row.country,
    is_default: !!row.is_default,
    created_at: row.created_at,
  };
}

/** Enderecos do usuario autenticado. Nunca aceita user_id do corpo/query. */
export async function listUserAddresses(userId, deps = {}) {
  const d = resolveDeps(deps);
  const id = parseUserId(userId);
  const { rows } = await d.query(
    `select * from public.user_addresses where user_id = $1 order by is_default desc, created_at desc`,
    [id]
  );
  return rows.map(mapRow);
}

export async function createUserAddress(userId, input = {}, deps = {}) {
  const d = resolveDeps(deps);
  const id = parseUserId(userId);

  const recipient_name = requiredString(input.recipient_name, "recipient_name");
  const zipcode = parseZipcode(input.zipcode);
  const street = requiredString(input.street, "street");
  const number = requiredString(input.number, "number", { maxLength: 20 });
  const complement = input.complement ? requiredString(input.complement, "complement", { maxLength: 200 }) : null;
  const neighborhood = requiredString(input.neighborhood, "neighborhood");
  const city = requiredString(input.city, "city");
  const state = parseState(input.state);
  const isDefault = !!input.is_default;

  if (isDefault) {
    await d.query(`update public.user_addresses set is_default = false where user_id = $1`, [id]);
  }

  const { rows } = await d.query(
    `insert into public.user_addresses
       (user_id, recipient_name, zipcode, street, number, complement, neighborhood, city, state, is_default)
     values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
     returning *`,
    [id, recipient_name, zipcode, street, number, complement, neighborhood, city, state, isDefault]
  );
  return mapRow(rows[0]);
}

/** Devolve null se o endereco nao existe OU nao pertence ao usuario — nunca vaza a existencia. */
export async function getUserAddress(userId, addressId, deps = {}) {
  const d = resolveDeps(deps);
  const id = parseUserId(userId);
  const { rows } = await d.query(
    `select * from public.user_addresses where id = $1 and user_id = $2`,
    [String(addressId), id]
  );
  return rows.length ? mapRow(rows[0]) : null;
}

export async function deleteUserAddress(userId, addressId, deps = {}) {
  const d = resolveDeps(deps);
  const id = parseUserId(userId);
  const { rowCount } = await d.query(
    `delete from public.user_addresses where id = $1 and user_id = $2`,
    [String(addressId), id]
  );
  return rowCount > 0;
}
