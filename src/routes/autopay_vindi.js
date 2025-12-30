// backend/src/routes/autopay_vindi.js
// Rotas para autopay usando Vindi
import express from "express";
import { query, getPool } from "../db.js";
import { requireAuth } from "../middleware/auth.js";
import {
  ensureCustomer,
  createPaymentProfile,
} from "../services/vindi.js";
import { tokenizeCardPublic } from "../services/vindi_public.js";

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
 * POST /api/autopay/vindi/tokenize
 * Tokeniza cartão via Vindi Public API e retorna gateway_token
 * Body: { holder_name, card_number, card_expiration_month, card_expiration_year, card_cvv, payment_method_code?, document_number? }
 */
router.post("/vindi/tokenize", requireAuth, async (req, res) => {
  try {
    // Verifica se Vindi Public está configurado
    if (!process.env.VINDI_PUBLIC_KEY) {
      console.error("[autopay/vindi/tokenize] VINDI_PUBLIC_KEY não configurado");
      return res.status(503).json({
        error: "VINDI_PUBLIC_KEY não configurado no servidor",
        status: 503,
      });
    }

    // Extrai e valida campos obrigatórios
    const holder_name = req.body?.holder_name;
    const card_number = req.body?.card_number;
    const card_expiration_month = req.body?.card_expiration_month;
    const card_expiration_year = req.body?.card_expiration_year;
    const card_cvv = req.body?.card_cvv;
    const payment_method_code = req.body?.payment_method_code || "credit_card";
    const document_number = req.body?.document_number;

    // Validações obrigatórias (sem logar dados sensíveis)
    if (!holder_name || !card_number || !card_expiration_month || !card_expiration_year || !card_cvv) {
      console.warn("[autopay/vindi/tokenize] campos obrigatórios faltando", {
        has_holder_name: !!holder_name,
        has_card_number: !!card_number,
        has_month: !!card_expiration_month,
        has_year: !!card_expiration_year,
        has_cvv: !!card_cvv,
      });
      return res.status(422).json({
        error: "Campos obrigatórios: holder_name, card_number, card_expiration_month, card_expiration_year, card_cvv",
        status: 422,
      });
    }

    // Prepara payload
    const payload = {
      holder_name,
      card_number,
      card_expiration_month,
      card_expiration_year,
      card_cvv,
      payment_method_code,
    };

    if (document_number) {
      payload.document_number = document_number;
    }

    // Tokeniza cartão
    try {
      const result = await tokenizeCardPublic(payload);

      // Retorna gateway_token e payment_profile
      res.json({
        gateway_token: result.gatewayToken,
        payment_profile: result.paymentProfile || {},
      });
    } catch (e) {
      // Se Vindi retornou erro com errors[0].message, usar essa mensagem
      const errorMessage = e?.response?.errors?.[0]?.message || e?.message || "Falha ao tokenizar cartão na Vindi";
      
      // Status original da Vindi quando possível (401/422 etc), senão 500
      const status = e?.status && e.status >= 400 && e.status < 600 ? e.status : 500;
      
      console.error("[autopay/vindi/tokenize] tokenize falhou", {
        status: e?.status,
        msg: errorMessage,
        has_errors: !!e?.response?.errors,
        errors_count: e?.response?.errors?.length || 0,
        // NÃO logar dados do cartão
      });

      return res.status(status).json({
        error: errorMessage,
        status: status,
      });
    }
  } catch (e) {
    console.error("[autopay/vindi/tokenize] erro inesperado:", e?.message || e);
    res.status(500).json({
      error: e?.message || "Erro interno ao tokenizar cartão",
      status: 500,
    });
  }
});

/**
 * POST /api/autopay/vindi/setup
 * Configura autopay com Vindi
 * Body: { gateway_token?, holder_name?, doc_number?, numbers?, active? }
 * gateway_token é obrigatório apenas se não há vindi_payment_profile_id salvo
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
    
    // Se não tem Vindi e quer ativar ou tem números, exige gateway_token
    if (!hasVindiProfile && !gateway_token) {
      if (active || numbers.length > 0) {
        await client.query("ROLLBACK");
        return res.status(400).json({
          error: "gateway_token_required",
          code: "GATEWAY_TOKEN_REQUIRED",
          message: "gateway_token é obrigatório quando não há cartão Vindi salvo e você quer ativar autopay",
        });
      }
      // Se active=false e numbers vazio, permite salvar "desativado" sem cartão
    }

    // Se tem Vindi mas veio gateway_token, valida dados do titular
    if (hasVindiProfile && gateway_token) {
      if (!holder_name || !doc_number) {
        await client.query("ROLLBACK");
        return res.status(400).json({
          error: "holder_name e doc_number são obrigatórios ao atualizar cartão",
          code: "MISSING_HOLDER_DATA",
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
    let vindiCustomerId = existingProfile?.vindi_customer_id || profile.vindi_customer_id;
    let paymentProfileId = existingProfile?.vindi_payment_profile_id || profile.vindi_payment_profile_id;
    let lastFour = existingProfile?.vindi_last4 || profile.vindi_last4;
    let brand = existingProfile?.vindi_brand || profile.vindi_brand || null;

    if (hasVindiProfile && !gateway_token) {
      // Apenas atualiza preferências, não recria payment_profile
      // Atualiza campos básicos se vieram no request
      const updateProfileResult = await client.query(
        `update public.autopay_profiles
            set active = $2,
                holder_name = COALESCE($3, holder_name),
                doc_number = COALESCE($4, doc_number),
                updated_at = now()
          where id=$1
          returning *`,
        [profile.id, active, holder_name || null, doc_number || null]
      );
      
      const updatedProfile = updateProfileResult.rows[0];
      await client.query("COMMIT");
      
      return res.json({
        ok: true,
        active,
        numbers,
        holder_name: updatedProfile.holder_name || null,
        doc_number: updatedProfile.doc_number || null,
        vindi: {
          customer_id: vindiCustomerId,
          payment_profile_id: paymentProfileId,
          last_four: lastFour,
        },
        card: {
          brand: brand,
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
          code: "MISSING_HOLDER_DATA",
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
        brand = paymentProfile.cardType || paymentProfile.brand || null;
      } catch (e) {
        await client.query("ROLLBACK");
        console.error("[autopay/vindi] createPaymentProfile falhou", {
          user_id,
          msg: e?.message,
          status: e?.status,
        });
        return res.status(500).json({
          error: "payment_profile_failed",
          code: "VINDI_PAYMENT_PROFILE_FAILED",
          message: e?.message || "Falha ao salvar cartão na Vindi",
        });
      }
    }

    // 8) Atualiza perfil com dados Vindi e limpa campos MP
    const updateResult = await client.query(
      `update public.autopay_profiles
          set vindi_customer_id = COALESCE($2, vindi_customer_id),
              vindi_payment_profile_id = COALESCE($3, vindi_payment_profile_id),
              vindi_last4 = COALESCE($4, vindi_last4),
              vindi_brand = COALESCE($5, vindi_brand),
              mp_customer_id = NULL,
              mp_card_id = NULL,
              brand = NULL,
              last4 = NULL,
              updated_at = now()
        where id=$1
        returning *`,
      [profile.id, vindiCustomerId, paymentProfileId, lastFour, brand]
    );

    await client.query("COMMIT");

    res.json({
      ok: true,
      active,
      numbers,
      holder_name: updateResult.rows[0]?.holder_name || holder_name || null,
      doc_number: updateResult.rows[0]?.doc_number || doc_number || null,
      vindi: {
        customer_id: vindiCustomerId,
        payment_profile_id: paymentProfileId,
        last_four: lastFour,
      },
      card: {
        brand: updateResult.rows[0]?.vindi_brand || null,
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
      holder_name: p.holder_name || null,
      doc_number: p.doc_number || null,
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
            brand: p.vindi_brand || null,
            last4: p.vindi_last4 || null,
            has_card: true,
          }
        : {
            brand: null,
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

