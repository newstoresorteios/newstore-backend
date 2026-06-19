// backend/src/routes/autopay.js
import express from "express";
import { query, getPool } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";

// Helpers de Mercado Pago (SDK/tokenização feita no front)
// mpEnsureCustomer({ user, doc_number, name }) -> { customerId }
// mpSaveCard({ customerId, card_token }) -> { cardId, brand, last4 }
// mpChargeCard({ customerId, cardId, amount_cents, description, metadata }) -> { status, paymentId }
import {
  getMercadoPagoAccessToken,
  mpEnsureCustomer,
  mpSaveCard,
  mpChargeCard,
} from "../services/mercadopago.js";
import { creditCouponOnApprovedPayment } from "../services/couponBalance.js";

const router = express.Router();

/* ------------------------------------------------------------------ *
 * Utils
 * ------------------------------------------------------------------ */

function parseNumbers(input) {
  // Dedup, valida (00..99) e aplica um limite de segurança no backend (20)
  const arr = Array.isArray(input)
    ? input
    : String(input || "")
        .split(/[,\s;]+/)
        .map((t) => t.trim())
        .filter(Boolean);

  const nums = [...new Set(arr.map(Number))] // dedupe
    .filter((n) => Number.isInteger(n) && n >= 0 && n <= 99)
    .slice(0, 20); // limite de segurança

  // (opcional) manter ordenado para UX mais previsível
  nums.sort((a, b) => a - b);
  return nums;
}

async function getTicketPriceCents(client) {
  const r = await client.query(
    `SELECT value
       FROM public.app_config
      WHERE key = $1
      LIMIT 1`,
    ["ticket_price_cents"]
  );
  const n = Number(r.rows?.[0]?.value);
  if (!Number.isFinite(n) || n <= 0) {
    throw new Error("Invalid or missing app_config.ticket_price_cents");
  }
  return Math.trunc(n);
}

async function isNumberFree(client, draw_id, n) {
  // livre se NÃO está em payments aprovados e NÃO está em reservas ativas/pagas
  const q = `
    with
    p as (
      select 1 from public.payments
       where draw_id=$1
         and lower(status) in ('approved','paid','pago')
         and $2 = any(numbers) limit 1
    ),
    r as (
      select 1 from public.reservations
       where draw_id=$1
         and lower(status) in ('active','pending','paid')
         and (
           $2 = any(numbers)
           or n = $2
         )
       limit 1
    )
    select
      coalesce((select 1 from p),0) as taken_pay,
      coalesce((select 1 from r),0) as taken_resv
  `;
  const r = await client.query(q, [draw_id, n]);
  return !(r.rows[0].taken_pay || r.rows[0].taken_resv);
}

/* ------------------------------------------------------------------ *
 * ME: carregar/salvar perfil
 * ------------------------------------------------------------------ */

// GET /api/me/autopay
router.get("/me/autopay", requireAuth, async (req, res) => {
  try {
    const { rows } = await query(
      `select ap.*, array(
         select n from public.autopay_numbers an where an.autopay_id = ap.id order by n
       ) as numbers
       from public.autopay_profiles ap
      where ap.user_id = $1
      limit 1`,
      [req.user.id]
    );
    if (!rows.length) return res.json(null);
    const p = rows[0];
    res.json({
      id: p.id,
      active: !!p.active,
      brand: p.brand || null,
      last4: p.last4 || null,
      holder_name: p.holder_name || null,
      doc_number: p.doc_number || null,
      numbers: p.numbers || [],
    });
  } catch (e) {
    console.error("[autopay] GET error:", e?.message || e);
    res.status(500).json({ error: "load_failed" });
  }
});

// **NOVO** — GET /api/autopay/claims
// Números cativos ocupados (globais) e os do usuário logado
router.get("/autopay/claims", requireAuth, async (req, res) => {
  try {
    const userId = req.user.id;

    // todos os números com perfil ativo
    const all = await query(
      `select array(
         select distinct n
           from public.autopay_numbers an
           join public.autopay_profiles ap on ap.id = an.autopay_id
          where ap.active = true
          order by n
       ) as taken`
    );

    // números cativos do usuário logado (se tiver perfil)
    const mine = await query(
      `select array(
         select n from public.autopay_numbers an
          where an.autopay_id = (
            select id from public.autopay_profiles where user_id=$1 limit 1
          )
          order by n
       ) as mine`,
      [userId]
    );

    res.json({
      taken: all.rows?.[0]?.taken || [],
      mine: mine.rows?.[0]?.mine || [],
    });
  } catch (e) {
    console.error("[autopay/claims] error:", e?.message || e);
    res.status(500).json({ error: "claims_failed" });
  }
});

// POST /api/me/autopay
// body: { active?:bool, numbers?:[]|csv, card_token?:string, holder_name?, doc_number? }
router.post("/me/autopay", requireAuth, async (req, res) => {
  const pool = await getPool();
  const client = await pool.connect();
  try {
    const user_id = req.user.id;
    const active =
      req.body?.active !== undefined ? !!req.body.active : true;
    const holder_name = String(req.body?.holder_name || "").slice(0, 120);
    const doc_number = String(req.body?.doc_number || "")
      .replace(/\D+/g, "")
      .slice(0, 18);
    const numbers = parseNumbers(req.body?.numbers);
    const card_token = req.body?.card_token
      ? String(req.body.card_token)
      : null;

    // Se for atualizar/salvar cartão, exigir dados mínimos do titular
    if (card_token && (!holder_name || !doc_number)) {
      return res
        .status(400)
        .json({ error: "missing_holder_or_doc" });
    }

    // Se tentará salvar cartão mas o servidor não tem MP token, avisa claramente
    if (card_token && !getMercadoPagoAccessToken()) {
      console.error("[autopay] missing MP_ACCESS_TOKEN on server");
      return res.status(503).json({ error: "mp_token_missing" });
    }

    await client.query("BEGIN");

    // upsert perfil
    let r = await client.query(
      `insert into public.autopay_profiles (user_id, active, holder_name, doc_number)
       values ($1,$2,$3,$4)
       on conflict (user_id) do update
         set active = excluded.active,
             holder_name = excluded.holder_name,
             doc_number = excluded.doc_number,
             updated_at = now()
       returning *`,
      [user_id, active, holder_name || null, doc_number || null]
    );
    const profile = r.rows[0];

    // atualiza números (substitui todos)
    await client.query(
      `delete from public.autopay_numbers where autopay_id=$1`,
      [profile.id]
    );
    if (numbers.length) {
      const args = numbers.map((_, i) => `($1,$${i + 2})`).join(",");
      await client.query(
        `insert into public.autopay_numbers(autopay_id, n) values ${args}`,
        [profile.id, ...numbers]
      );
    }

    // cartão (opcional) — salvar no MP e gravar ids (não logar dados sensíveis)
    let cardMeta = {
      brand: profile.brand,
      last4: profile.last4,
      mp_card_id: profile.mp_card_id,
      mp_customer_id: profile.mp_customer_id,
    };

    if (card_token) {
      const customer = await mpEnsureCustomer({
        user: req.user,
        doc_number,
        name: holder_name || req.user?.name || "Cliente",
      });

      const saved = await mpSaveCard({
        customerId: customer.customerId,
        card_token,
      });

      const up = await client.query(
        `update public.autopay_profiles
            set mp_customer_id = $2,
                mp_card_id = $3,
                brand = $4,
                last4 = $5,
                updated_at = now()
          where id=$1
          returning *`,
        [
          profile.id,
          customer.customerId,
          saved.cardId,
          saved.brand,
          saved.last4,
        ]
      );

      cardMeta = {
        brand: up.rows[0].brand,
        last4: up.rows[0].last4,
        mp_customer_id: up.rows[0].mp_customer_id,
        mp_card_id: up.rows[0].mp_card_id,
      };
    }

    await client.query("COMMIT");
    res.json({
      ok: true,
      active,
      numbers,
      card: {
        brand: cardMeta.brand || null,
        last4: cardMeta.last4 || null,
        has_card: !!cardMeta.mp_card_id,
      },
    });
  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    console.error("[autopay] save error:", e?.message || e);
    res.status(500).json({ error: "save_failed" });
  } finally {
    client.release();
  }
});

/* ------------------ NOVO: cancelar perfil/limpar cartão e números ------------------ */
// POST /api/me/autopay/cancel
router.post("/me/autopay/cancel", requireAuth, async (req, res) => {
  const pool = await getPool();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // Obtém (ou cria) perfil do usuário
    const { rows } = await client.query(
      `select * from public.autopay_profiles where user_id=$1 limit 1`,
      [req.user.id]
    );

    if (!rows.length) {
      // nenhum perfil: nada a cancelar, mas respondemos ok
      await client.query("COMMIT");
      return res.json({
        ok: true,
        canceled: true,
        active: false,
        numbers: [],
        card: { has_card: false, brand: null, last4: null },
      });
    }

    const profile = rows[0];

    // limpa números
    await client.query(
      `delete from public.autopay_numbers where autopay_id=$1`,
      [profile.id]
    );

    // desativa + apaga cartão (mantém holder/doc)
    const up = await client.query(
      `update public.autopay_profiles
          set active=false,
              mp_card_id=null,
              brand=null,
              last4=null,
              updated_at=now()
        where id=$1
        returning *`,
      [profile.id]
    );

    await client.query("COMMIT");
    return res.json({
      ok: true,
      canceled: true,
      active: false,
      numbers: [],
      card: { has_card: false, brand: null, last4: null },
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("[autopay/cancel] error:", e?.message || e);
    res.status(500).json({ error: "cancel_failed" });
  } finally {
    client.release();
  }
});

/* ------------------------------------------------------------------ *
 * ADMIN: rodar cobrança automática em um sorteio aberto
 * ------------------------------------------------------------------ */

// POST /api/admin/draws/:id/autopay-run
router.post(
  "/admin/draws/:id/autopay-run",
  requireAuth,
  requireAdmin,
  async (_req, res) => {
    return res.status(410).json({
      error: "legacy_mp_autopay_disabled",
      message: "Mercado Pago autopay is disabled. Use Vindi autopay runner.",
    });
  }
);

export default router;
