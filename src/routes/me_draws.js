import { Router } from "express";
import { query } from "../db.js";
import { requireAuth } from "../middleware/auth.js";

const router = Router();

export async function loadDrawForBoard(drawId, queryFn = query) {
  const result = await queryFn(
    `SELECT d.id,
            d.status,
            d.realized_at,
            d.winner_user_id,
            d.winner_number,
            d.product_name,
            d.product_link,
            COALESCE(NULLIF(d.winner_name, ''), NULLIF(u.name, '')) AS winner_name
       FROM public.draws d
  LEFT JOIN public.users u
         ON u.id = d.winner_user_id
      WHERE d.id = $1
      LIMIT 1`,
    [drawId]
  );
  return result.rows[0] || null;
}

export async function loadBoardNumbers(drawId, queryFn = query) {
  const result = await queryFn(
    `SELECT n::int AS n
       FROM public.numbers
      WHERE draw_id = $1
      ORDER BY n ASC`,
    [drawId]
  );
  return (result.rows || []).map((row) => Number(row.n));
}

// Apenas formatacao visual: 0..99 -> "00".."99"; 0..499/0..999 -> "000"...
export function boardLabelWidth(numbers) {
  const max = numbers.reduce((acc, n) => (n > acc ? n : acc), 0);
  return Math.max(2, String(max).length);
}

// Representa os numeros reais do sorteio; winner_number = 0 e vencedor valido.
export function buildBoard({ numbers, taken, reserved, mine, winner }) {
  const sorted = [...numbers].sort((a, b) => a - b);
  const width = boardLabelWidth(sorted);
  const winnerNumber = winner === null || winner === undefined ? null : Number(winner);
  return sorted.map((n) => {
    const isMine = mine.has(n);
    const state = isMine || taken.has(n) ? "taken" : reserved.has(n) ? "reserved" : "available";
    return {
      n,
      label: String(n).padStart(width, "0"),
      state, // available | reserved | taken
      isMine,
      isWinner: winnerNumber !== null && winnerNumber === n, // usado no UI para estilizar e mostrar o nome
    };
  });
}

/**
 * GET /api/me/draws/:id/board
 * Retorna o tabuleiro com os numeros reais de public.numbers (ex.: 00..99) com:
 * - isMine: números do usuário logado (payments aprovados/pagos)
 * - state: available | reserved | taken
 * - isWinner: número sorteado
 * Também retorna product_name/product_link e o nome do vencedor (se houver).
 */
router.get("/:id/board", requireAuth, async (req, res) => {
  try {
    const userId = Number(req.user?.id);
    const drawId = Number(req.params.id);
    if (!Number.isInteger(drawId) || drawId <= 0) {
      return res.status(400).json({ error: "bad_draw_id" });
    }

    // dados do sorteio + produto + nome do vencedor
    const draw = await loadDrawForBoard(drawId);
    if (!draw) return res.status(404).json({ error: "draw_not_found" });

    // números comprados por QUALQUER pessoa (indisponíveis)
    const takenR = await query(
      `SELECT unnest(p.numbers)::int AS n
         FROM public.payments p
        WHERE p.draw_id = $1
          AND LOWER(p.status) IN ('approved','paid','pago')`,
      [drawId]
    );

    // reservas ativas/pending/paid (marcamos como "reserved")
    const resvR = await query(
      `SELECT unnest(r.numbers)::int AS n
         FROM public.reservations r
        WHERE r.draw_id = $1
          AND LOWER(r.status) IN ('active','pending','paid')`,
      [drawId]
    );

    // números do usuário logado
    const mineR = await query(
      `SELECT unnest(p.numbers)::int AS n
         FROM public.payments p
        WHERE p.draw_id = $1
          AND p.user_id = $2
          AND LOWER(p.status) IN ('approved','paid','pago')`,
      [drawId, userId]
    );

    const setTaken = new Set((takenR.rows || []).map(r => Number(r.n)));
    const setResv  = new Set((resvR.rows  || []).map(r => Number(r.n)));
    const setMine  = new Set((mineR.rows  || []).map(r => Number(r.n)));
    const winner   = (draw.winner_number ?? null);

    // monta a grade a partir dos registros reais de public.numbers
    const board = buildBoard({
      numbers: await loadBoardNumbers(drawId),
      taken: setTaken,
      reserved: setResv,
      mine: setMine,
      winner,
    });

    return res.json({
      draw: {
        id: draw.id,
        status: draw.status,
        realized_at: draw.realized_at,
        winner_number: winner,
        product_name: draw.product_name || null,
        product_link: draw.product_link || null,
        winner_name: draw.winner_name || null,
      },
      my_numbers: Array.from(setMine).sort((a,b)=>a-b),
      board
    });
  } catch (e) {
    console.error("[me/draws/:id/board] error:", e);
    return res.status(500).json({ error: "board_failed" });
  }
});

export default router;
