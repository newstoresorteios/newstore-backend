// backend/src/services/purchase_limit.js
import { query } from "../db.js";

const MAX = Number(process.env.MAX_NUMBERS_PER_USER || 20);

// Status aceitos (cubra nomes em pt e en, conforme seu schema)
const STATUSES = ['reservado','pago','reserved','paid','taken','sold'];

export async function getUserCountInDraw(userId, drawId) {
  const { rows } = await query(
    `
    select count(*)::int as cnt
      from numbers
     where draw_id = $1
       and user_id = $2
       and status = ANY($3)
    `,
    [drawId, userId, STATUSES]
  );
  return rows?.[0]?.cnt ?? 0;
}

// Versão "read-only" para a rota de checagem
export async function checkUserLimit(userId, drawId, addingCount = 1) {
  const current = await getUserCountInDraw(userId, drawId);
  const blocked = current >= MAX || current + addingCount > MAX;
  return { blocked, current, max: MAX };
}

// Versão "hard" para ser usada nos fluxos de reserva/checkout
export async function assertUserUnderLimit(userId, drawId, addingCount = 1) {
  const { blocked, current, max } = await checkUserLimit(userId, drawId, addingCount);
  if (blocked) {
    const err = new Error("max_numbers_reached");
    err.status = 409;
    err.code = "max_numbers_reached";
    err.payload = { current, max };
    throw err;
  }
  return { current, max };
}
