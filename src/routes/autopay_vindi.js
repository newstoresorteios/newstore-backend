// backend/src/routes/autopay_vindi.js
// Rotas para autopay usando Vindi
import express from "express";
import { query, getPool } from "../db.js";
import { requireAuth } from "../middleware/auth.js";
import {
  ensureCustomer,
  createPaymentProfile,
} from "../services/vindi.js";

const router = express.Router();

// Helper para parse de números (mesmo do autopay.js)
function parseNumbers(input) {
  const arr = Array.isArray(input)
    ? input
    : String(input || "")
        .split(/[,\s;]+/)
        .map((t) => t.trim())
        .filter(Boolean);

  const nums = [...new Set(arr.map(Number))]
    .filter((n) => Number.isInteger(n) && n >= 0 && n <= 99)
    .slice(0, 20);

  nums.sort((a, b) => a - b);
  return nums;
}

/**
 * POST /api/autopay/vindi/setup
 * Configura autopay com Vindi
 * Body: { gateway_token, holder_name, doc_number?, numbers?, active? }
 */
router.post("/vindi/setup", requireAuth, async (req, res) => {
  const pool = await getPool();
  const client = await pool.connect();

  try {
    const user_id = req.user.id;
    const gateway_token = req.body?.gateway_token ? String(req.body.gateway_token) : null;
    const holder_name = String(req.body?.holder_name || "").slice(0, 120);
    const doc_number = String(req.body?.doc_number || "")
      .replace(/\D+/g, "")
      .slice(0, 18);
    const numbers = parseNumbers(req.body?.numbers);
    const active = req.body?.active !== undefined ? !!req.body.active : true;

    // Verifica se Vindi está configurado
    if (!process.env.VINDI_API_KEY) {
      console.error("[autopay/vindi] VINDI_API_KEY não configurado");
      return res.status(503).json({ error: "vindi_not_configured" });
    }

    await client.query("BEGIN");

    // 1) Busca perfil existente (se houver)
    let existingProfile = null;
    const existingResult = await client.query(
      `select * from public.autopay_profiles where user_id=$1 limit 1`,
      [user_id]
    );
    if (existingResult.rows.length) {
      existingProfile = existingResult.rows[0];
    }

    // 2) Validações: gateway_token é obrigatório apenas se não tem Vindi configurado
    const hasVindiProfile = !!(existingProfile?.vindi_payment_profile_id);
    
    if (!hasVindiProfile && !gateway_token) {
      await client.query("ROLLBACK");
      return res.status(400).json({
        error: "gateway_token_required",
        code: "GATEWAY_TOKEN_REQUIRED",
        message: "gateway_token é obrigatório quando não há cartão Vindi salvo",
      });
    }

    // Se tem Vindi mas veio gateway_token, valida dados do titular
    if (hasVindiProfile && gateway_token) {
      if (!holder_name || !doc_number) {
        await client.query("ROLLBACK");
        return res.status(400).json({
          error: "holder_name e doc_number são obrigatórios ao atualizar cartão",
        });
      }
    }

    // 3) Upsert perfil no DB
    let profileResult = await client.query(
      `insert into public.autopay_profiles (user_id, active, holder_name, doc_number)
       values ($1,$2,$3,$4)
       on conflict (user_id) do update
         set active = excluded.active,
             holder_name = COALESCE(excluded.holder_name, autopay_profiles.holder_name),
             doc_number = COALESCE(excluded.doc_number, autopay_profiles.doc_number),
             updated_at = now()
       returning *`,
      [user_id, active, holder_name || null, doc_number || null]
    );
    const profile = profileResult.rows[0];

    // 4) Atualiza números (substitui todos)
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

    // 5) Se já tem Vindi configurado e não veio gateway_token, apenas atualiza preferências
    let vindiCustomerId = profile.vindi_customer_id;
    let paymentProfileId = profile.vindi_payment_profile_id;
    let lastFour = profile.vindi_last4;

    if (hasVindiProfile && !gateway_token) {
      // Apenas atualiza preferências, não recria payment_profile
      await client.query("COMMIT");
      
      return res.json({
        ok: true,
        active,
        numbers,
        vindi: {
          customer_id: vindiCustomerId,
          payment_profile_id: paymentProfileId,
          last_four: lastFour,
        },
        card: {
          last4: lastFour || null,
          has_card: true,
        },
      });
    }

    // 6) Integração Vindi: ensureCustomer (se necessário)
    if (!vindiCustomerId) {
      const customer = await ensureCustomer({
        email: req.user.email,
        name: holder_name || req.user?.name || "Cliente",
        code: `user_${user_id}`,
      });
      vindiCustomerId = customer.customerId;
    }

    // 7) Cria/atualiza payment_profile na Vindi (apenas se veio gateway_token)
    if (gateway_token) {
      if (!holder_name || !doc_number) {
        await client.query("ROLLBACK");
        return res.status(400).json({
          error: "holder_name e doc_number são obrigatórios ao salvar cartão",
        });
      }

      try {
        const paymentProfile = await createPaymentProfile({
          customerId: vindiCustomerId,
          gatewayToken: gateway_token,
          holderName: holder_name,
          docNumber: doc_number,
          phone: req.user.phone || null,
        });

        paymentProfileId = paymentProfile.paymentProfileId;
        lastFour = paymentProfile.lastFour;
      } catch (e) {
        await client.query("ROLLBACK");
        console.error("[autopay/vindi] createPaymentProfile falhou", {
          user_id,
          msg: e?.message,
          status: e?.status,
        });
        return res.status(500).json({
          error: "payment_profile_failed",
          message: e?.message || "Falha ao salvar cartão na Vindi",
        });
      }
    }

    // 8) Atualiza perfil com dados Vindi e limpa campos MP
    const updateResult = await client.query(
      `update public.autopay_profiles
          set vindi_customer_id = $2,
              vindi_payment_profile_id = $3,
              vindi_last4 = $4,
              mp_customer_id = NULL,
              mp_card_id = NULL,
              brand = NULL,
              last4 = NULL,
              updated_at = now()
        where id=$1
        returning *`,
      [profile.id, vindiCustomerId, paymentProfileId, lastFour]
    );

    await client.query("COMMIT");

    res.json({
      ok: true,
      active,
      numbers,
      vindi: {
        customer_id: vindiCustomerId,
        payment_profile_id: paymentProfileId,
        last_four: lastFour,
      },
      card: {
        last4: lastFour || null,
        has_card: !!paymentProfileId,
      },
    });
  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    console.error("[autopay/vindi] setup error:", e?.message || e);
    res.status(500).json({ error: "setup_failed", message: e?.message });
  } finally {
    client.release();
  }
});

/**
 * GET /api/autopay/vindi/status
 * Retorna status do autopay Vindi do usuário
 */
router.get("/vindi/status", requireAuth, async (req, res) => {
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

    if (!rows.length) {
      return res.json({
        active: false,
        has_vindi: false,
        numbers: [],
        card: null,
      });
    }

    const p = rows[0];
    const hasVindi = !!(p.vindi_customer_id && p.vindi_payment_profile_id);

    res.json({
      active: !!p.active && hasVindi,
      has_vindi: hasVindi,
      numbers: p.numbers || [],
      vindi: hasVindi
        ? {
            customer_id: p.vindi_customer_id,
            payment_profile_id: p.vindi_payment_profile_id,
            last_four: p.vindi_last4,
          }
        : null,
      card: hasVindi
        ? {
            last4: p.vindi_last4 || null,
            has_card: true,
          }
        : {
            last4: null,
            has_card: false,
          },
    });
  } catch (e) {
    console.error("[autopay/vindi] status error:", e?.message || e);
    res.status(500).json({ error: "status_failed" });
  }
});

/**
 * POST /api/autopay/vindi/cancel
 * Cancela autopay Vindi (remove payment_profile, mantém customer)
 */
router.post("/vindi/cancel", requireAuth, async (req, res) => {
  const pool = await getPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const { rows } = await client.query(
      `select * from public.autopay_profiles where user_id=$1 limit 1`,
      [req.user.id]
    );

    if (!rows.length) {
      await client.query("COMMIT");
      return res.json({
        ok: true,
        canceled: true,
        active: false,
      });
    }

    const profile = rows[0];

    // Remove números
    await client.query(
      `delete from public.autopay_numbers where autopay_id=$1`,
      [profile.id]
    );

    // Desativa e limpa payment_profile (mantém customer_id)
    await client.query(
      `update public.autopay_profiles
          set active=false,
              vindi_payment_profile_id=null,
              vindi_last4=null,
              updated_at=now()
        where id=$1
        returning *`,
      [profile.id]
    );

    await client.query("COMMIT");

    res.json({
      ok: true,
      canceled: true,
      active: false,
      numbers: [],
    });
  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    console.error("[autopay/vindi] cancel error:", e?.message || e);
    res.status(500).json({ error: "cancel_failed" });
  } finally {
    client.release();
  }
});

export default router;

