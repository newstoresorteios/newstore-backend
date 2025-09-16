// backend/src/services/purchase_limit.js
import { query } from "../db.js";

const MAX = Number(process.env.MAX_NUMBERS_PER_USER || 20);
// status aceitos (ajuste se seus nomes forem outros)
const STATUSES = ["reservado","pago","reserved","paid","taken","sold"];

// tenta achar uma coluna de usuário na tabela informada
async function resolveUserColumn(table) {
  const { rows } = await query(
    `
    select column_name
      from information_schema.columns
     where table_schema = 'public'
       and table_name   = $1
    `,
    [table]
  );
  const cols = rows.map(r => r.column_name);

  // nomes mais comuns
  const candidates = [
    "user_id", "client_id", "customer_id", "account_id",
    "buyer_id", "participant_id", "owner_id"
  ];

  return candidates.find(c => cols.includes(c)) || null;
}

// conta via tabela numbers (caso tenha coluna de usuário)
async function countViaNumbers(userId, drawId, userCol) {
  const sql = `
    select count(*)::int as cnt
      from numbers
     where draw_id = $1
       and ${userCol} = $2
       and lower(status) = ANY($3)
  `;
  const { rows } = await query(sql, [drawId, userId, STATUSES.map(s => s.toLowerCase())]);
  return rows?.[0]?.cnt ?? 0;
}

// fallback: conta via reservations (se numbers não guarda o usuário)
async function countViaReservations(userId, drawId) {
  // descobre coluna de usuário em reservations
  const userCol = await resolveUserColumn("reservations");
  if (!userCol) return 0;

  // tenta achar coluna que aponte para numbers.id (number_id, num_id, etc.)
  const { rows } = await query(
    `
    select column_name
      from information_schema.columns
     where table_schema = 'public'
       and table_name   = 'reservations'
       and column_name in ('number_id','numbers_id','num_id','n_id')
    `
  );
  const numCol = rows?.[0]?.column_name || null;

  // se não tiver FK explícita, ainda dá pra contar por draw_id se existir em reservations
  const { rows: drawColRows } = await query(
    `
    select column_name
      from information_schema.columns
     where table_schema = 'public'
       and table_name   = 'reservations'
       and column_name in ('draw_id','sorteio_id')
    `
  );
  const drawCol = drawColRows?.[0]?.column_name || null;

  if (numCol) {
    // join com numbers
    const sql = `
      select count(*)::int as cnt
        from reservations r
        join numbers n on n.id = r.${numCol}
       where n.draw_id = $1
         and r.${userCol} = $2
         and lower(coalesce(n.status, r.status, '')) = ANY($3)
    `;
    const { rows: R } = await query(sql, [drawId, userId, STATUSES.map(s => s.toLowerCase())]);
    return R?.[0]?.cnt ?? 0;
  }

  if (drawCol) {
    // conta direto por reservations se ela já tiver draw_id
    const sql = `
      select count(*)::int as cnt
        from reservations r
       where r.${drawCol} = $1
         and r.${userCol} = $2
         and lower(coalesce(r.status, '')) = ANY($3)
    `;
    const { rows: R } = await query(sql, [drawId, userId, STATUSES.map(s => s.toLowerCase())]);
    return R?.[0]?.cnt ?? 0;
  }

  // último fallback: 0 (não conseguimos determinar)
  return 0;
}

export async function getUserCountInDraw(userId, drawId) {
  // tenta via numbers primeiro
  const userCol = await resolveUserColumn("numbers");

  if (userCol) {
    return countViaNumbers(userId, drawId, userCol);
  }

  // fallback via reservations
  return countViaReservations(userId, drawId);
}

export async function checkUserLimit(userId, drawId, addingCount = 1) {
  const current = await getUserCountInDraw(userId, drawId);
  const blocked = current >= MAX || current + addingCount > MAX;
  return { blocked, current, max: MAX };
}

// opção "hard" p/ usar em rotas que não podem passar do limite
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
