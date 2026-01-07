// backend/src/routes/autopay_vindi.js
// Rotas para autopay usando Vindi
import express from "express";
import { query, getPool } from "../db.js";
import { requireAuth } from "../middleware/auth.js";
import {
  ensureCustomer,
  createPaymentProfile,
  createPaymentProfileWithCardData,
  associateGatewayToken,
} from "../services/vindi.js";
import { detectPaymentCompanyCode } from "../services/vindi_public.js";

const router = express.Router();

/**
 * Mapeia erros da Vindi para códigos HTTP apropriados
 * Evita que erros de autenticação da Vindi (401/403) sejam interpretados como erro de JWT
 * @param {Error} error - Erro da Vindi (deve ter error.provider === "VINDI")
 * @returns {object} - { httpStatus, code, message, providerStatus, details }
 */
function mapVindiError(error) {
  // Se não for erro do provider Vindi, retorna erro genérico
  if (error?.provider !== "VINDI") {
    return {
      httpStatus: 500,
      code: "INTERNAL_ERROR",
      message: error?.message || "Erro interno",
      providerStatus: null,
      details: [{ message: error?.message || "Erro interno" }],
    };
  }
  
  const vindiStatus = error?.status;
  const errorResponse = error?.response || {};
  const errors = errorResponse?.errors || [];
  
  // Extrai mensagens de erro da Vindi (limite 300 chars)
  const errorMessages = errors.map(e => e?.message || "").filter(Boolean);
  const errorSummary = errorMessages.length > 0
    ? errorMessages.join("; ").slice(0, 300)
    : error?.message || "Erro na integração com Vindi";
  
  // Mapeia status da Vindi para HTTP status apropriado
  if (vindiStatus === 401 || vindiStatus === 403) {
    // Erro de autenticação da Vindi → 502 Bad Gateway (não 401 para não confundir com JWT)
    return {
      httpStatus: 502,
      code: "VINDI_AUTH_ERROR",
      message: "Falha de autenticação na Vindi (verifique VINDI_API_KEY/VINDI_API_BASE_URL).",
      providerStatus: vindiStatus,
      details: errors.length > 0 ? errors : [{ message: errorSummary }],
    };
  }
  
  if (vindiStatus === 422) {
    // Erro de validação → 400 Bad Request (conforme solicitado)
    return {
      httpStatus: 400,
      code: "VINDI_VALIDATION_ERROR",
      message: errorSummary,
      providerStatus: vindiStatus,
      details: errors.length > 0 ? errors : [{ message: errorSummary }],
    };
  }
  
  if (vindiStatus === 400) {
    // Bad Request → manter 400
    return {
      httpStatus: 400,
      code: "VINDI_BAD_REQUEST",
      message: errorSummary,
      providerStatus: vindiStatus,
      details: errors.length > 0 ? errors : [{ message: errorSummary }],
    };
  }
  
  if (vindiStatus >= 500 && vindiStatus < 600) {
    // Erro 5xx da Vindi → 502 Bad Gateway
    return {
      httpStatus: 502,
      code: "VINDI_UPSTREAM_ERROR",
      message: `Erro no servidor da Vindi (${vindiStatus})`,
      providerStatus: vindiStatus,
      details: errors.length > 0 ? errors : [{ message: errorSummary }],
    };
  }
  
  if (vindiStatus && vindiStatus >= 400 && vindiStatus < 500) {
    // Outros 4xx → 502 (não queremos confundir com nossos erros)
    return {
      httpStatus: 502,
      code: "VINDI_CLIENT_ERROR",
      message: errorSummary,
      providerStatus: vindiStatus,
      details: errors.length > 0 ? errors : [{ message: errorSummary }],
    };
  }
  
  // Sem status ou erro desconhecido → 500
  return {
    httpStatus: 500,
    code: "INTERNAL_ERROR",
    message: errorSummary,
    providerStatus: vindiStatus || null,
    details: errors.length > 0 ? errors : [{ message: errorSummary }],
  };
}

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
 * Cria customer e payment_profile na Vindi usando API privada (fluxo recomendado)
 * Body: { holder_name, card_number, card_expiration (MM/YY ou MM/YYYY), card_cvv, payment_company_code? (opcional), card_doc_number? (cpf/cnpj opcional) }
 * Retorna: { ok: true, customer_id, payment_profile_id, card_last4, payment_company_code? }
 */
router.post("/vindi/tokenize", requireAuth, async (req, res) => {
  const user_id = req.user?.id;
  
  try {
    // Verifica se Vindi API está configurada
    if (!process.env.VINDI_API_KEY) {
      console.error("[autopay/vindi/tokenize] VINDI_API_KEY não configurado");
      return res.status(500).json({
        error: "vindi_not_configured",
        message: "VINDI_API_KEY não configurado no servidor",
      });
    }

    if (!user_id) {
      return res.status(401).json({
        error: "Usuário não autenticado",
      });
    }

    // Extrai campos do body (aceita camelCase e snake_case)
    const holderName = req.body?.holderName || req.body?.holder_name;
    const cardNumber = req.body?.cardNumber || req.body?.card_number;
    const expMonth = req.body?.expMonth || req.body?.card_expiration_month;
    const expYear = req.body?.expYear || req.body?.card_expiration_year;
    const cardExpiration = req.body?.card_expiration || req.body?.cardExpiration; // MM/YY ou MM/YYYY
    const cvv = req.body?.cvv || req.body?.card_cvv;
    const card_doc_number = req.body?.card_doc_number || req.body?.document_number || req.body?.documentNumber;
    const payment_company_code = req.body?.payment_company_code || 
                                 req.body?.paymentCompanyCode || 
                                 req.body?.brand || 
                                 req.body?.brandCode || 
                                 null;

    // Normaliza e limpa campos
    const cleanHolderName = holderName ? String(holderName).trim() : "";
    const cleanCardNumber = cardNumber ? String(cardNumber).replace(/\D+/g, "") : "";
    const cleanCvv = cvv ? String(cvv).replace(/\D+/g, "") : "";
    const cleanDocNumber = card_doc_number ? String(card_doc_number).replace(/\D+/g, "") : null;
    
    // Normaliza expiração para MM/YYYY sempre
    let normalizedCardExpiration = null;
    
    if (cardExpiration) {
      // Formato MM/YY ou MM/YYYY
      const parts = String(cardExpiration).trim().split("/");
      if (parts.length === 2) {
        const month = parts[0].replace(/\D+/g, "").padStart(2, "0");
        let yearPart = parts[1].replace(/\D+/g, "");
        
        // Normaliza ano para 4 dígitos
        if (yearPart.length === 2) {
          // MM/YY: assume 20YY se YY <= 79, senão 19YY
          const yy = parseInt(yearPart, 10);
          const fullYear = yy <= 79 ? `20${yearPart.padStart(2, "0")}` : `19${yearPart.padStart(2, "0")}`;
          normalizedCardExpiration = `${month}/${fullYear}`;
        } else if (yearPart.length === 4) {
          // MM/YYYY: já está correto
          normalizedCardExpiration = `${month}/${yearPart}`;
        }
      }
    } else if (expMonth && expYear) {
      // Campos separados: monta MM/YYYY
      const month = String(expMonth).replace(/\D+/g, "").padStart(2, "0");
      let year = String(expYear).replace(/\D+/g, "");
      
      // Normaliza ano para 4 dígitos
      if (year.length === 2) {
        const yy = parseInt(year, 10);
        year = yy <= 79 ? `20${year.padStart(2, "0")}` : `19${year.padStart(2, "0")}`;
      } else if (year.length === 4) {
        // Já está correto
      } else {
        year = null; // Inválido
      }
      
      if (year) {
        normalizedCardExpiration = `${month}/${year}`;
      }
    }

    // Validação mínima: verifica se campos obrigatórios estão vazios após normalização
    const validationErrors = [];
    if (!cleanHolderName) {
      validationErrors.push({ field: "holder_name", message: "holder_name não pode ficar em branco" });
    }
    if (!cleanCardNumber) {
      validationErrors.push({ field: "card_number", message: "card_number não pode ficar em branco" });
    }
    if (!normalizedCardExpiration) {
      validationErrors.push({ field: "card_expiration", message: "card_expiration não pode ficar em branco (formato MM/YY ou MM/YYYY)" });
    }
    if (!cleanCvv) {
      validationErrors.push({ field: "card_cvv", message: "card_cvv não pode ficar em branco" });
    }

    if (validationErrors.length > 0) {
      console.warn("[autopay/vindi/tokenize] validação falhou", {
        user_id,
        validation_errors: validationErrors,
      });
      return res.status(422).json({
        error: "Campos obrigatórios não podem ficar em branco",
        details: validationErrors,
      });
    }

    const last4 = cleanCardNumber.length >= 4 ? cleanCardNumber.slice(-4) : "****";

    // Determina payment_company_code final: prioriza frontend, senão detecta
    const validCodes = ["visa", "mastercard", "elo", "american_express", "diners_club", "hipercard", "hiper"];
    let finalPaymentCompanyCode = null;
    
    // PRIORIDADE 1: payment_company_code do frontend
    if (payment_company_code) {
      const cleanPcc = String(payment_company_code).trim().toLowerCase();
      if (validCodes.includes(cleanPcc)) {
        finalPaymentCompanyCode = cleanPcc;
      }
    }
    
    // PRIORIDADE 2: Detecção automática se não veio do frontend
    if (!finalPaymentCompanyCode) {
      const detected = detectPaymentCompanyCode(cleanCardNumber);
      if (detected?.brandCode && validCodes.includes(detected.brandCode)) {
        finalPaymentCompanyCode = detected.brandCode;
      }
    }

    // Busca dados do usuário no DB se não vierem no token
    let userEmail = req.user?.email;
    let userName = req.user?.name;
    let userPhone = req.user?.phone;
    let userCpfCnpj = cleanDocNumber;

    if (!userEmail || !userName) {
      try {
        const userResult = await query(
          `SELECT id, name, email, phone FROM users WHERE id = $1 LIMIT 1`,
          [user_id]
        );
        if (userResult.rows.length > 0) {
          const dbUser = userResult.rows[0];
          if (!userEmail) userEmail = dbUser.email;
          if (!userName) userName = dbUser.name || cleanHolderName || "Cliente";
          if (!userPhone) userPhone = dbUser.phone;
        }
      } catch (dbError) {
        console.warn("[autopay/vindi/tokenize] erro ao buscar usuário no DB", {
          user_id,
          msg: dbError?.message,
        });
      }
    }

    if (!userEmail) {
      return res.status(422).json({
        error: "Email do usuário não encontrado",
        details: [{ field: "email", message: "Email é obrigatório para criar customer na Vindi" }],
      });
    }

    // Mascara cartão para log
    const maskCardForLog = (num) => {
      if (!num || num.length < 4) return "****";
      if (num.length <= 8) return `****${num.slice(-4)}`;
      return `${num.slice(0, 4)}${"*".repeat(Math.max(0, num.length - 8))}${num.slice(-4)}`;
    };
    const maskedCardLog = maskCardForLog(cleanCardNumber);

    console.log("[autopay/vindi/tokenize] iniciando criação de customer e payment_profile", {
      user_id,
      holder_name: cleanHolderName,
      card_masked: maskedCardLog,
      card_expiration: normalizedCardExpiration,
      payment_company_code: finalPaymentCompanyCode || null,
    });

    // PASSO 1: Garantir customer na Vindi
    let customerId;
    try {
      const customer = await ensureCustomer({
        email: userEmail,
        name: userName,
        code: `user_${user_id}`,
        cpfCnpj: userCpfCnpj,
      });
      customerId = customer.customerId;
      console.log("[autopay/vindi/tokenize] customer garantido", {
        user_id,
        customer_id: customerId,
      });
    } catch (ensureError) {
      const mappedError = mapVindiError(ensureError);
      
      console.error("[autopay/vindi/tokenize] falha ao garantir customer", {
        user_id,
        code: mappedError.code,
        provider_status: mappedError.providerStatus,
        error_message: mappedError.message,
        errors_count: mappedError.details?.length || 0,
      });
      
      return res.status(mappedError.httpStatus).json({
        error: mappedError.code === "INTERNAL_ERROR" ? "vindi_error" : mappedError.code.toLowerCase(),
        code: mappedError.code,
        message: mappedError.message,
        provider_status: mappedError.providerStatus,
        details: mappedError.details,
      });
    }

    // PASSO 2: Criar payment_profile com dados do cartão (API privada)
    try {
      const paymentProfile = await createPaymentProfileWithCardData({
        customerId,
        holderName: cleanHolderName,
        cardNumber: cleanCardNumber,
        cardExpiration: normalizedCardExpiration,
        cardCvv: cleanCvv,
        paymentCompanyCode: finalPaymentCompanyCode,
        docNumber: userCpfCnpj,
        phone: userPhone,
      });

      console.log("[autopay/vindi/tokenize] payment_profile criado", {
        user_id,
        customer_id: customerId,
        payment_profile_id: paymentProfile.paymentProfileId,
        card_last4: paymentProfile.lastFour || last4,
      });

      // Retorna resposta
      const response = {
        ok: true,
        customer_id: customerId,
        payment_profile_id: paymentProfile.paymentProfileId,
        card_last4: paymentProfile.lastFour || last4,
      };

      if (paymentProfile.paymentCompanyCode) {
        response.payment_company_code = paymentProfile.paymentCompanyCode;
      }

      res.status(200).json(response);
    } catch (e) {
      const mappedError = mapVindiError(e);
      
      console.error("[autopay/vindi/tokenize] falha ao criar payment_profile", {
        user_id,
        customer_id: customerId,
        code: mappedError.code,
        provider_status: mappedError.providerStatus,
        error_message: mappedError.message,
        errors_count: mappedError.details?.length || 0,
      });

      return res.status(mappedError.httpStatus).json({
        error: mappedError.code === "INTERNAL_ERROR" ? "vindi_error" : mappedError.code.toLowerCase(),
        code: mappedError.code,
        message: mappedError.message,
        provider_status: mappedError.providerStatus,
        details: mappedError.details,
      });
    }
  } catch (e) {
    console.error("[autopay/vindi/tokenize] erro inesperado:", {
      user_id: req.user?.id,
      msg: e?.message || e,
    });
    res.status(500).json({
      error: e?.message || "Erro interno ao tokenizar cartão",
    });
  }
});

/**
 * POST /api/autopay/vindi/setup
 * Configura autopay com Vindi
 * Body: { payment_profile_id? (modo novo), gateway_token? (modo legado), customer_id?, card_last4?, payment_company_code?, holder_name?, doc_number?, numbers?, active? }
 * Modo novo: se vier payment_profile_id, apenas persiste e ativa (não cria novo payment_profile)
 * Modo legado: se vier gateway_token, cria payment_profile usando gateway_token
 */
router.post("/vindi/setup", requireAuth, async (req, res) => {
  const pool = await getPool();
  const client = await pool.connect();

  try {
    const user_id = req.user.id;
    const payment_profile_id = req.body?.payment_profile_id ? String(req.body.payment_profile_id) : null;
    const customer_id = req.body?.customer_id ? String(req.body.customer_id) : null;
    const card_last4 = req.body?.card_last4 ? String(req.body.card_last4).slice(0, 4) : null;
    const payment_company_code = req.body?.payment_company_code ? String(req.body.payment_company_code).trim() : null;
    const gateway_token = req.body?.gateway_token ? String(req.body.gateway_token) : null; // Modo legado
    const holder_name = String(req.body?.holder_name || "").slice(0, 120);
    const doc_number = String(req.body?.doc_number || "")
      .replace(/\D+/g, "")
      .slice(0, 18);
    const numbers = parseNumbers(req.body?.numbers);
    const active = req.body?.active !== undefined ? !!req.body.active : true;

    // Verifica se Vindi está configurado (apenas se precisar criar payment_profile)
    if (!process.env.VINDI_API_KEY && gateway_token) {
      console.error("[autopay/vindi/setup] VINDI_API_KEY não configurado");
      return res.status(500).json({ 
        error: "vindi_not_configured",
        message: "VINDI_API_KEY não configurado no servidor",
      });
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

    // 2) Modo novo: se veio payment_profile_id, apenas persiste (não cria novo)
    if (payment_profile_id) {
      if (!customer_id) {
        await client.query("ROLLBACK");
        return res.status(400).json({
          error: "customer_id é obrigatório quando payment_profile_id é fornecido",
          code: "MISSING_CUSTOMER_ID",
        });
      }

      // Upsert perfil no DB
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

      // Atualiza números
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

      // Atualiza dados Vindi
      const updateResult = await client.query(
        `update public.autopay_profiles
            set vindi_customer_id = $2,
                vindi_payment_profile_id = $3,
                vindi_last4 = COALESCE($4, vindi_last4),
                vindi_brand = COALESCE($5, vindi_brand),
                active = $6,
                updated_at = now()
          where id=$1
          returning *`,
        [profile.id, customer_id, payment_profile_id, card_last4, payment_company_code, active]
      );

      await client.query("COMMIT");

      return res.json({
        ok: true,
        active,
        numbers,
        holder_name: updateResult.rows[0]?.holder_name || holder_name || null,
        doc_number: updateResult.rows[0]?.doc_number || doc_number || null,
        vindi: {
          customer_id,
          payment_profile_id,
          last_four: card_last4 || updateResult.rows[0]?.vindi_last4 || null,
        },
        card: {
          brand: payment_company_code || updateResult.rows[0]?.vindi_brand || null,
          last4: card_last4 || updateResult.rows[0]?.vindi_last4 || null,
          has_card: true,
        },
      });
    }

    // 3) Modo legado: gateway_token (mantém compatibilidade)
    const hasVindiProfile = !!(existingProfile?.vindi_payment_profile_id);
    
    // Se não tem Vindi e quer ativar ou tem números, exige gateway_token
    if (!hasVindiProfile && !gateway_token) {
      if (active || numbers.length > 0) {
        await client.query("ROLLBACK");
        return res.status(400).json({
          error: "payment_profile_id ou gateway_token é obrigatório quando não há cartão Vindi salvo",
          code: "PAYMENT_PROFILE_REQUIRED",
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
      try {
        const customer = await ensureCustomer({
          email: req.user.email,
          name: holder_name || req.user?.name || "Cliente",
          code: `user_${user_id}`,
        });
        vindiCustomerId = customer.customerId;
      } catch (ensureError) {
        await client.query("ROLLBACK");
        const mappedError = mapVindiError(ensureError);
        
        console.error("[autopay/vindi/setup] falha ao garantir customer", {
          user_id,
          code: mappedError.code,
          provider_status: mappedError.providerStatus,
        });
        
        return res.status(mappedError.httpStatus).json({
          error: mappedError.code === "INTERNAL_ERROR" ? "vindi_error" : mappedError.code.toLowerCase(),
          code: mappedError.code,
          message: mappedError.message,
          provider_status: mappedError.providerStatus,
          details: mappedError.details,
        });
      }
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
        const mappedError = mapVindiError(e);
        
        console.error("[autopay/vindi/setup] createPaymentProfile falhou", {
          user_id,
          code: mappedError.code,
          provider_status: mappedError.providerStatus,
        });
        
        return res.status(mappedError.httpStatus).json({
          error: mappedError.code === "INTERNAL_ERROR" ? "vindi_error" : mappedError.code.toLowerCase(),
          code: mappedError.code,
          message: mappedError.message,
          provider_status: mappedError.providerStatus,
          details: mappedError.details,
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
              active = $6,
              mp_customer_id = NULL,
              mp_card_id = NULL,
              brand = NULL,
              last4 = NULL,
              updated_at = now()
        where id=$1
        returning *`,
      [profile.id, vindiCustomerId, paymentProfileId, lastFour, brand, active]
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

