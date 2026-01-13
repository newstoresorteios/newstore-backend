// backend/src/routes/autopay_vindi.js
// Rotas para autopay usando Vindi
import express from "express";
import crypto from "node:crypto";
import { query, getPool } from "../db.js";
import { requireAuth } from "../middleware/auth.js";
import {
  ensureCustomer,
  createPaymentProfile,
  createPaymentProfileWithCardData,
  associateGatewayToken,
} from "../services/vindi.js";
import { tokenizeCardPublic, detectPaymentCompanyCode } from "../services/vindi_public.js";

const router = express.Router();

/**
 * Mapeia erros da Vindi para códigos HTTP apropriados
 * Evita que erros de autenticação da Vindi (401/403) sejam interpretados como erro de JWT
 * @param {Error} error - Erro da Vindi (pode ter error.provider === "VINDI" ou error.code)
 * @returns {object} - { httpStatus, code, message, providerStatus, details }
 */
function mapVindiError(error) {
  // Se o erro já tem um code definido (vindo de vindi.js ou vindi_public.js), usa diretamente
  if (error?.code) {
    // Para erros de autenticação (401/403), preserva o status original
    const httpStatus = error?.status || (error?.code === "VINDI_AUTH_ERROR" ? 401 : 502);
    const providerStatus = error?.provider_status || error?.status || null;
    const errorResponse = error?.response || {};
    const errors = errorResponse?.errors || [];
    const details = error?.details || (errors.length > 0 ? errors : []);

    return {
      httpStatus,
      code: error.code,
      message: error.message || "Erro na integração com Vindi",
      providerStatus,
      details,
    };
  }

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
    // Erro de autenticação da Vindi → retornar 401/403 com code VINDI_AUTH_ERROR
    // Mensagem específica para 401: "Chave da API inválida"
    const authMessage = vindiStatus === 401 
      ? "Chave da API inválida"
      : "Falha de autenticação na Vindi (verifique VINDI_API_KEY/VINDI_API_BASE_URL).";
    
    return {
      httpStatus: vindiStatus, // Preserva 401/403
      code: "VINDI_AUTH_ERROR",
      message: authMessage,
      providerStatus: vindiStatus,
      details: errors.length > 0 ? errors : [{ message: errorSummary }],
    };
  }
  
  if (vindiStatus === 422) {
    // Erro de validação → 422 (mantém status original)
    return {
      httpStatus: 422,
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
 * Tokeniza cartão via Vindi Public API (somente tokenização, não cria customer nem payment_profile)
 * Body: { holder_name, card_number, card_expiration (MM/YY ou MM/YYYY), card_cvv, payment_company_code? (opcional), document_number? (cpf/cnpj opcional) }
 * Retorna: { ok: true, gateway_token, brand?, last4? }
 */
router.post("/vindi/tokenize", requireAuth, async (req, res) => {
  const user_id = req.user?.id;
  // Aceita requestId do header x-request-id do frontend, senão gera UUID
  const requestId = req.headers["x-request-id"] || crypto.randomUUID();
  
  try {
    if (!user_id) {
      return res.status(401).json({
        ok: false,
        code: "UNAUTHORIZED",
        error_message: "Usuário não autenticado",
        requestId,
      });
    }

    // Extrai campos do body (aceita camelCase e snake_case)
    const holderName = req.body?.holderName || req.body?.holder_name;
    const cardNumber = req.body?.cardNumber || req.body?.card_number;
    const expMonth = req.body?.expMonth || req.body?.card_expiration_month;
    const expYear = req.body?.expYear || req.body?.card_expiration_year;
    const cardExpiration = req.body?.card_expiration || req.body?.cardExpiration; // MM/YY ou MM/YYYY
    const cvv = req.body?.cvv || req.body?.card_cvv;
    const document_number = req.body?.document_number || req.body?.documentNumber || req.body?.card_doc_number;
    const payment_company_code = req.body?.payment_company_code || 
                                 req.body?.paymentCompanyCode || 
                                 req.body?.brand || 
                                 req.body?.brandCode || 
                                 null;

    // Normaliza e limpa campos
    const cleanHolderName = holderName ? String(holderName).trim() : "";
    const cleanCardNumber = cardNumber ? String(cardNumber).replace(/\D+/g, "") : "";
    const cleanCvv = cvv ? String(cvv).replace(/\D+/g, "") : "";
    const cleanDocNumber = document_number ? String(document_number).replace(/\D+/g, "") : null;
    
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
        requestId,
        validation_errors: validationErrors,
      });
      return res.status(422).json({
        ok: false,
        code: "VALIDATION_ERROR",
        error_message: "Campos obrigatórios não podem ficar em branco",
        details: validationErrors,
        requestId,
      });
    }

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

    // Mascara cartão para log
    const maskCardForLog = (num) => {
      if (!num || num.length < 4) return "****";
      if (num.length <= 8) return `****${num.slice(-4)}`;
      return `${num.slice(0, 4)}${"*".repeat(Math.max(0, num.length - 8))}${num.slice(-4)}`;
    };
    const maskedCardLog = maskCardForLog(cleanCardNumber);

    console.log("[autopay/vindi/tokenize] iniciando tokenização pública", {
      user_id,
      requestId,
      holder_name: cleanHolderName,
      card_masked: maskedCardLog,
      card_expiration: normalizedCardExpiration,
      payment_company_code: finalPaymentCompanyCode || null,
    });

    // Tokeniza via Vindi Public API
    try {
      const tokenizePayload = {
        holder_name: cleanHolderName,
        card_number: cleanCardNumber,
        card_expiration: normalizedCardExpiration,
        card_cvv: cleanCvv,
        payment_company_code: finalPaymentCompanyCode,
        document_number: cleanDocNumber,
        user_id,
      };

      const result = await tokenizeCardPublic(tokenizePayload);
      const gatewayToken = result.gatewayToken;
      const paymentProfile = result.paymentProfile || {};

      if (!gatewayToken) {
        console.error("[autopay/vindi/tokenize] tokenização retornou sem gateway_token", {
          user_id,
          requestId,
        });
      return res.status(500).json({
        ok: false,
        code: "TOKENIZATION_ERROR",
        error_message: "Tokenização não retornou gateway_token",
        requestId,
      });
      }

      console.log("[autopay/vindi/tokenize] tokenização bem-sucedida", {
        user_id,
        requestId,
        has_gateway_token: !!gatewayToken,
        has_last4: !!paymentProfile.last_four,
        has_brand: !!paymentProfile.card_type,
      });

      // Retorna resposta padronizada
      const response = {
        ok: true,
        gateway_token: gatewayToken,
      };

      if (paymentProfile.last_four) {
        response.last4 = paymentProfile.last_four;
      }

      if (paymentProfile.card_type) {
        response.brand = paymentProfile.card_type;
      } else if (finalPaymentCompanyCode) {
        response.brand = finalPaymentCompanyCode;
      }

      res.status(200).json(response);
    } catch (e) {
      // Mapeia erros da tokenização
      let httpStatus = 500;
      let code = "TOKENIZATION_ERROR";
      let message = e?.message || "Erro ao tokenizar cartão";
      let providerStatus = null;
      let details = [];

      if (e?.code === "VINDI_PUBLIC_CONFIG_ERROR") {
        httpStatus = 502;
        code = "VINDI_PUBLIC_CONFIG_ERROR";
        message = "Configuração Vindi Public inválida (verifique VINDI_PUBLIC_KEY/BASE_URL).";
      } else if (e?.code === "VINDI_AUTH_ERROR") {
        httpStatus = 502;
        code = "VINDI_AUTH_ERROR";
        message = "Falha de autenticação na Vindi (verifique VINDI_PUBLIC_KEY/BASE_URL).";
        providerStatus = e?.provider_status || 401;
      } else if (e?.code === "VINDI_VALIDATION_ERROR") {
        httpStatus = 422;
        code = "VINDI_VALIDATION_ERROR";
        message = e?.message || "Erro de validação na tokenização";
        providerStatus = e?.provider_status || 422;
        details = e?.details || e?.response?.errors || [];
      } else if (e?.code === "VINDI_UPSTREAM_ERROR") {
        httpStatus = 502;
        code = "VINDI_UPSTREAM_ERROR";
        message = e?.message || "Erro no servidor da Vindi";
        providerStatus = e?.provider_status || null;
      } else if (e?.status === 422) {
        httpStatus = 422;
        code = "VINDI_VALIDATION_ERROR";
        message = e?.message || "Erro de validação na tokenização";
        providerStatus = 422;
        details = e?.details || e?.response?.errors || [];
      }

      console.error("[autopay/vindi/tokenize] falha na tokenização", {
        user_id,
        requestId,
        code,
        provider_status: providerStatus,
        error_message: message,
        errors_count: details?.length || 0,
      });

      return res.status(httpStatus).json({
        ok: false,
        code,
        error_message: message,
        provider_status: providerStatus,
        details: details.length > 0 ? details : undefined,
        requestId,
      });
    }
  } catch (e) {
    console.error("[autopay/vindi/tokenize] erro inesperado:", {
      user_id: req.user?.id,
      msg: e?.message || e,
    });
    res.status(500).json({
      ok: false,
      code: "INTERNAL_ERROR",
      error_message: e?.message || "Erro interno ao tokenizar cartão",
      requestId: crypto.randomUUID(),
    });
  }
});

/**
 * POST /api/autopay/vindi/setup
 * Configura autopay com Vindi: ensureCustomer + createPaymentProfile com customer_id
 * Body: { gateway_token (obrigatório), holder_name, doc_number, card_last4?, payment_company_code?, numbers?, active? }
 * Fluxo: ensureCustomer(email, name) -> customerId -> createPaymentProfile({ customer_id, gateway_token, ... })
 */
router.post("/vindi/setup", requireAuth, async (req, res) => {
  const pool = await getPool();
  const client = await pool.connect();
  // Aceita requestId do header x-request-id do frontend, senão gera UUID
  const requestId = req.headers["x-request-id"] || crypto.randomUUID();

  try {
    const user_id = req.user.id;
    const gateway_token = req.body?.gateway_token ? String(req.body.gateway_token) : null;
    const payment_profile_id = req.body?.payment_profile_id ? String(req.body.payment_profile_id) : null;
    const customer_id = req.body?.customer_id ? String(req.body.customer_id) : null;
    const card_last4 = req.body?.card_last4 ? String(req.body.card_last4).slice(0, 4) : null;
    const payment_company_code = req.body?.payment_company_code ? String(req.body.payment_company_code).trim() : null;
    const holder_name = String(req.body?.holder_name || "").slice(0, 120);
    const doc_number = String(req.body?.doc_number || "")
      .replace(/\D+/g, "")
      .slice(0, 18);
    const numbers = parseNumbers(req.body?.numbers);
    const active = req.body?.active !== undefined ? !!req.body.active : true;

    // Verifica se Vindi está configurado
    if (!process.env.VINDI_API_KEY) {
      console.error("[autopay/vindi/setup] VINDI_API_KEY não configurado", {
        user_id,
        requestId,
      });
      await client.query("ROLLBACK");
      return res.status(502).json({ 
        ok: false,
        code: "VINDI_CONFIG_ERROR",
        error_message: "VINDI_API_KEY não configurado no servidor",
        requestId,
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
          ok: false,
          code: "MISSING_CUSTOMER_ID",
          error_message: "customer_id é obrigatório quando payment_profile_id é fornecido",
          requestId,
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

    // 3) Se veio gateway_token, valida dados obrigatórios
    if (gateway_token) {
      if (!holder_name || !doc_number) {
        await client.query("ROLLBACK");
        return res.status(400).json({
          ok: false,
          code: "MISSING_HOLDER_DATA",
          error_message: "holder_name e doc_number são obrigatórios ao salvar cartão",
          requestId,
        });
      }
    }

    // Se não veio gateway_token nem payment_profile_id, e não tem perfil existente, permite apenas desativar
    const hasVindiProfile = !!(existingProfile?.vindi_payment_profile_id);
    if (!gateway_token && !payment_profile_id && !hasVindiProfile) {
      if (active || numbers.length > 0) {
        await client.query("ROLLBACK");
        return res.status(400).json({
          ok: false,
          code: "PAYMENT_PROFILE_REQUIRED",
          error_message: "gateway_token é obrigatório quando não há cartão Vindi salvo",
          requestId,
        });
      }
      // Se active=false e numbers vazio, permite salvar "desativado" sem cartão
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


    // 6) Integração Vindi: ensureCustomer + createPaymentProfile (se veio gateway_token)
    let vindiCustomerId = existingProfile?.vindi_customer_id || profile.vindi_customer_id || customer_id;
    let paymentProfileId = existingProfile?.vindi_payment_profile_id || profile.vindi_payment_profile_id || null;
    let lastFour = existingProfile?.vindi_last4 || profile.vindi_last4 || card_last4;
    let brand = existingProfile?.vindi_brand || profile.vindi_brand || payment_company_code || null;

    if (gateway_token) {
      // PASSO 1: Garantir customer (sempre, mesmo se já existir customer_id)
      // Isso garante que o customer existe na Vindi antes de criar payment_profile
      try {
        // Busca email do usuário se não vier no token
        let userEmail = req.user.email;
        if (!userEmail) {
          const userResult = await query(
            `SELECT email, name FROM users WHERE id = $1 LIMIT 1`,
            [user_id]
          );
          if (userResult.rows.length > 0) {
            userEmail = userResult.rows[0].email;
          }
        }

        if (!userEmail) {
          await client.query("ROLLBACK");
          return res.status(422).json({
            ok: false,
            code: "MISSING_EMAIL",
            error_message: "Email do usuário não encontrado",
            requestId,
          });
        }

        // Sempre chama ensureCustomer para garantir que existe (busca ou cria)
        const customer = await ensureCustomer({
          email: userEmail,
          name: holder_name || req.user?.name || "Cliente",
          code: `user_${user_id}`,
          cpfCnpj: doc_number || null,
        });
        vindiCustomerId = customer.customerId;
        
        console.log("[autopay/vindi/setup] customer garantido", {
          user_id,
          requestId,
          customer_id: vindiCustomerId,
        });
      } catch (ensureError) {
        await client.query("ROLLBACK");
        const mappedError = mapVindiError(ensureError);
        
        console.error("[autopay/vindi/setup] falha ao garantir customer", {
          user_id,
          requestId,
          code: mappedError.code,
          provider_status: mappedError.providerStatus,
        });
        
        return res.status(mappedError.httpStatus).json({
          ok: false,
          code: mappedError.code,
          error_message: mappedError.message,
          provider_status: mappedError.providerStatus,
          details: mappedError.details,
          requestId,
        });
      }

      // PASSO 2: createPaymentProfile com customer_id (garantido no passo anterior)
      try {
        const paymentProfile = await createPaymentProfile({
          customerId: vindiCustomerId,
          gatewayToken: gateway_token,
          holderName: holder_name,
          docNumber: doc_number,
          phone: req.user.phone || null,
        });

        paymentProfileId = paymentProfile.paymentProfileId;
        lastFour = paymentProfile.lastFour || card_last4;
        brand = paymentProfile.cardType || payment_company_code || null;
        
        console.log("[autopay/vindi/setup] payment_profile criado", {
          user_id,
          requestId,
          customer_id: vindiCustomerId,
          payment_profile_id: paymentProfileId,
          card_last4: lastFour,
        });
      } catch (e) {
        await client.query("ROLLBACK");
        const mappedError = mapVindiError(e);
        
        console.error("[autopay/vindi/setup] createPaymentProfile falhou", {
          user_id,
          requestId,
          code: mappedError.code,
          provider_status: mappedError.providerStatus,
        });
        
        return res.status(mappedError.httpStatus).json({
          ok: false,
          code: mappedError.code,
          error_message: mappedError.message,
          provider_status: mappedError.providerStatus,
          details: mappedError.details,
          requestId,
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
    console.error("[autopay/vindi] setup error:", {
      user_id: req.user?.id,
      requestId,
      msg: e?.message || e,
    });
    res.status(500).json({ 
      ok: false,
      code: "INTERNAL_ERROR",
      error_message: e?.message || "Erro interno ao configurar autopay",
      requestId,
    });
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
    res.status(500).json({ 
      ok: false,
      code: "INTERNAL_ERROR",
      error_message: "Erro ao buscar status do autopay",
    });
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
    res.status(500).json({ 
      ok: false,
      code: "INTERNAL_ERROR",
      error_message: "Erro ao cancelar autopay",
    });
  } finally {
    client.release();
  }
});

export default router;

