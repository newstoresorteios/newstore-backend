// src/services/trayCustomerClient.js
//
// Resolução de identidade do cliente Tray — SOMENTE LEITURA (GET).
//
// O pedido Tray exige customer_id (ID interno da Tray, nunca o nosso
// users.id). Este arquivo só PROCURA um cliente Tray existente por e-mail —
// nunca cria um novo (ver trayRedemptionOrder.js: criar cliente novo exige
// birth_date, que a NewStore não coleta — bloqueio isolado ali, não aqui).
//
// Contrato: GET /customers?email=<email> — documentação oficial Tray
// (skills/clientes, tray-tecnologia/tray-api-ai-plugin, mesma convenção de
// envelope já usada pelo catálogo: coleção no plural, item singular dentro).

import { trayCatalogGet, TrayCatalogError } from "./trayCatalogClient.js";

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
 */
export async function findTrayCustomerByEmail(email, options = {}) {
  const normalized = String(email || "").trim().toLowerCase();
  if (!normalized) throw new TrayCatalogError("email_missing", { status: 400 });

  const body = await trayCatalogGet("/customers", { email: normalized }, options);
  const rows = unwrapCustomers(body);
  if (!rows) throw new TrayCatalogError("tray_invalid_response", { status: 502 });
  if (!rows.length) return null;

  // Correspondência exata de e-mail — a Tray pode devolver resultados
  // parciais/case-insensitive; nunca usar o primeiro item às cegas.
  const match = rows.find((r) => String(r?.email || "").trim().toLowerCase() === normalized) || null;
  if (!match?.id) return null;

  return { id: String(match.id), name: match.name || null, email: normalized };
}
