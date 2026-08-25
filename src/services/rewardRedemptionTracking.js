// src/services/rewardRedemptionTracking.js
//
// Acompanhamento logístico do resgate para o CLIENTE (/loja/pedidos).
//
// SOMENTE LEITURA e financeiramente side-effect-free: nenhuma escrita no
// PostgreSQL, nenhum lançamento no ledger, nenhuma sincronização de cupom,
// nenhuma mutação na Tray. Um GET aqui nunca move dinheiro.
//
// SEGURANÇA (itens 10 e 11): o navegador informa apenas o ID do RESGATE.
// O tray_order_id é lido do banco depois de confirmar a posse:
//
//   redemption_id -> busca o resgate -> confirma user_id == usuário do token
//                 -> lê tray_order_id DO BANCO -> consulta a Tray
//
// Assim ninguém consegue apontar a consulta para o pedido de outra pessoa,
// e um resgate de terceiro responde 404 (nunca 403), para não permitir
// enumeração.

import { query as defaultQuery } from "../db.js";
import { getTrayOrder } from "./trayOrderClient.js";
import { buildCustomerTrayStatus } from "./trayOrderLogistics.js";

export class RedemptionTrackingError extends Error {
  constructor(code, { status = 400 } = {}) {
    super(code);
    this.name = "RedemptionTrackingError";
    this.code = code;
    this.status = status;
  }
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function resolveDeps(deps = {}) {
  return {
    query: deps.query || defaultQuery,
    getTrayOrder: deps.getTrayOrder || getTrayOrder,
  };
}

function parseUserId(value) {
  const n = Number(value);
  if (!Number.isSafeInteger(n) || n <= 0) throw new RedemptionTrackingError("unauthorized", { status: 401 });
  return n;
}

/**
 * Acompanhamento factual do pedido Tray de UM resgate do próprio usuário.
 *
 * Só chega a chamar a Tray quando o resgate existe, é do usuário e já tem
 * tray_order_id. Falha de consulta NUNCA vira falha do resgate (item 21):
 * devolve `temporarily_unavailable` e o pedido local segue intacto.
 */
export async function getRedemptionTrayStatus(userId, redemptionId, deps = {}) {
  const d = resolveDeps(deps);
  const uid = parseUserId(userId);

  const id = String(redemptionId ?? "").trim();
  // ID malformado nem chega ao banco — e responde igual a "não existe".
  if (!UUID_RE.test(id)) throw new RedemptionTrackingError("redemption_not_found", { status: 404 });

  // A posse entra na PRÓPRIA cláusula WHERE: um resgate de outra pessoa é
  // indistinguível de um resgate inexistente.
  const { rows } = await d.query(
    `select id, status, tray_order_id
       from public.reward_redemptions
      where id = $1::uuid and user_id = $2`,
    [id, uid]
  );
  if (!rows.length) throw new RedemptionTrackingError("redemption_not_found", { status: 404 });

  const trayOrderId = rows[0].tray_order_id || null;
  if (!trayOrderId) {
    // Sem pedido na Tray não há o que consultar — e nenhuma chamada externa
    // é feita. Isso não é erro: é o estado factual do resgate.
    return {
      redemption_id: rows[0].id,
      available: false,
      reason: "tray_order_not_created",
      checked_at: new Date().toISOString(),
    };
  }

  try {
    const { raw } = await d.getTrayOrder(trayOrderId);
    const status = buildCustomerTrayStatus(raw);
    if (!status) {
      return {
        redemption_id: rows[0].id,
        available: false,
        temporarily_unavailable: true,
        reason: "tray_invalid_response",
        checked_at: new Date().toISOString(),
      };
    }
    return { redemption_id: rows[0].id, ...status, checked_at: new Date().toISOString() };
  } catch (e) {
    // Observabilidade sem PII: só identificadores e o código do erro.
    console.warn("[store.tracking] consulta do pedido Tray falhou", {
      redemption_id: rows[0].id,
      tray_order_id: String(trayOrderId),
      code: e?.code || "tray_request_failed",
    });
    // A Tray fora do ar não pode derrubar /loja/pedidos nem mudar nada local.
    return {
      redemption_id: rows[0].id,
      available: false,
      temporarily_unavailable: true,
      reason: "tray_unavailable",
      checked_at: new Date().toISOString(),
    };
  }
}
