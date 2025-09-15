// backend/src/routes/numbers.js
import { Router } from 'express';
import { query } from '../db.js';

const router = Router();

/**
 * GET /api/numbers
 * - Pega o draw aberto
 * - Lê todos os números do draw (0..99) a partir da tabela numbers
 * - Marca como "sold" (indisponível) os números que têm pagamento aprovado
 * - Marca como "reserved" os números com reserva ativa (não expirada)
 * - Faz lazy-expire das reservas vencidas (best-effort)
 * - Retorna o status final para cada número
 */
router.get('/', async (_req, res) => {
  try {
    // 1) draw aberto
    const dr = await query(
      `SELECT id FROM draws WHERE status = 'open' ORDER BY id DESC LIMIT 1`
    );
    if (!dr.rows.length) return res.json({ drawId: null, numbers: [] });
    const drawId = dr.rows[0].id;

    // 2) lista base de números 0..99
    const base = await query(
      `SELECT n FROM numbers WHERE draw_id = $1 ORDER BY n ASC`,
      [drawId]
    );

    // 3) pagos => SOLD (UI espera "sold")
    const pays = await query(
      `SELECT numbers
         FROM payments
        WHERE draw_id = $1
          AND lower(status) IN ('approved','paid','pago')`,
      [drawId]
    );
    const sold = new Set();
    for (const p of pays.rows || []) {
      for (const n of (p.numbers || [])) sold.add(Number(n));
    }

    // 4) reservas ativas; ignora expiradas (e tenta expirar em background)
    const resvs = await query(
      `SELECT id, numbers, status, expires_at
         FROM reservations
        WHERE draw_id = $1
          AND lower(coalesce(status,'')) IN ('active','pending','reserved','')`,
      [drawId]
    );

    const now = Date.now();
    const reserved = new Set();

    for (const r of resvs.rows || []) {
      const exp = r.expires_at ? new Date(r.expires_at).getTime() : null;
      const isExpired = exp && !Number.isNaN(exp) && exp < now;

      if (isExpired) {
        // best-effort: não bloqueia a resposta
        query(`UPDATE reservations SET status = 'expired' WHERE id = $1`, [r.id])
          .catch(() => {});
        continue;
      }

      // reserva só se ainda não foi vendida
      for (const n of (r.numbers || [])) {
        const num = Number(n);
        if (!sold.has(num)) reserved.add(num);
      }
    }

    // 5) status final por número
    const numbers = base.rows.map(({ n }) => {
      if (sold.has(n))     return { n, status: 'sold' };      // <<< ajuste aqui
      if (reserved.has(n)) return { n, status: 'reserved' };
      return { n, status: 'available' };
    });

    res.json({ drawId, numbers });
  } catch (err) {
    console.error('GET /api/numbers failed', err);
    res.status(500).json({ error: 'failed_to_list_numbers' });
  }
});

export default router;
