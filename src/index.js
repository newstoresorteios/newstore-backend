// backend/src/index.js
import "dotenv/config";
import dns from "dns";
try { dns.setDefaultResultOrder("ipv4first"); } catch {}

import express from "express";
import cors from "cors";
import cookieParser from "cookie-parser";

import authRoutes from "./routes/auth.js";
import numbersRoutes from "./routes/numbers.js";
import reservationsRoutes from "./routes/reservations.js";
import paymentsRoutes from "./routes/payments.js";
import paymentsVindiRoutes from "./routes/payments_vindi.js";
import meRoutes from "./routes/me.js";
import drawsRoutes from "./routes/draws.js";
import drawsExtRoutes from "./routes/draws_ext.js";

// Routers ADMIN específicos (monte ANTES do /api/admin genérico)
import adminDrawsRouter from "./routes/admin_draws.js";
import adminClientsRouter from "./routes/admin_clients.js";
import adminWinnersRouter from "./routes/admin_winners.js";
import adminDashboardRouter from "./routes/admin_dashboard.js";

// ✅ Config pública (GET/POST completo) e admin
//    ATENÇÃO: usamos APENAS ESTE router para /api/config para evitar duplicidade.
import configRouter from "./routes/config.js";
import adminConfigRouter from "./routes/admin_config.js";

// Router admin genérico (DEIXAR POR ÚLTIMO entre /api/admin/*)
import adminRoutes from "./routes/admin.js";

import purchaseLimitRouter from "./routes/purchase_limit.js";
import couponsRouter from "./routes/coupons.js";
import trayRouter from "./routes/tray.js";

import adminUsersRouter from "./routes/adminUsers.js";

import autopayRouter from "./routes/autopay.js";
import autopayVindiRouter from "./routes/autopay_vindi.js";

import meDraws from "./routes/me_draws.js";

import autopayRunnerRoute from "./routes/autopay_runner.js";

import adminAnalyticsRouter from "./routes/analytics.js";

import { autoReconcile } from './middleware/autoReconcile.js';

import { query, getPool } from "./db.js";
import { ensureSchema } from "./seed.js";
import { ensureAppConfig } from "./services/config.js";
import { validateTrayConfigAtStartup } from "./services/trayConfig.js";

const app = express();

// Validação de config Tray no boot (sem imprimir segredos)
try { validateTrayConfigAtStartup(); } catch {}

const PORT = process.env.PORT || 4000;

// Se não setar CORS_ORIGIN, usamos esta allowlist padrão
const ORIGIN =
  process.env.CORS_ORIGIN ||
  "http://localhost:3000,https://newstore-frontend-ten.vercel.app,https://newstorerj.com.br,https://www.newstorerj.com.br";

// ⚠️ CORS_ORIGIN deve conter SOMENTE origens (sem /api, sem paths)
const ORIGINS = ORIGIN.split(",").map((s) => s.trim()).filter(Boolean);

// Saúde do DB (mantém conexão viva em hosts free)
setInterval(() => {
  query("SELECT 1").catch((e) =>
    console.warn("[health] db ping failed:", e.code || e.message)
  );
}, 60_000);

// ── Middlewares ─────────────────────────────────────────────
const corsOptions = {
  origin: ORIGINS, // array de origens permitidas
  credentials: true,
  methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
  // Deixe allowedHeaders indefinido para refletir os headers solicitados no preflight
  optionsSuccessStatus: 204,
};

app.use(cors(corsOptions));
app.options("*", cors(corsOptions)); // responde TODOS os preflights
app.use(express.json({ limit: "1mb" }));
app.use(cookieParser());

// Healthcheck
app.get("/health", (_req, res) => {
  res.json({ ok: true, ts: new Date().toISOString() });
});

app.use(autoReconcile);

// ── Rotas públicas/gerais ───────────────────────────────────
app.use("/api/auth", authRoutes);
app.use("/api/numbers", numbersRoutes);
app.use("/api/reservations", reservationsRoutes);

// Pagamentos
app.use("/api/payments", paymentsRoutes);
app.use("/api/payments", paymentsVindiRoutes);
app.use("/api/orders", paymentsRoutes); // aliases
app.use("/api/participations", paymentsRoutes); // aliases

app.use("/api/me", meRoutes);
app.use("/api/draws", drawsRoutes);
app.use("/api/draws-ext", drawsExtRoutes);

// ── Rotas ADMIN específicas (antes do genérico) ────────────
app.use("/api/admin/draws", adminDrawsRouter);
app.use("/api/admin/clients", adminClientsRouter);
app.use("/api/admin/winners", adminWinnersRouter);
app.use("/api/admin/dashboard", adminDashboardRouter);

// ✅ Config (pública e admin) — rota pública MONTADA UMA ÚNICA VEZ
app.use("/api/config", configRouter);           // GET: preço, banner, max_select | POST: atualiza
app.use("/api/admin/config", adminConfigRouter);

app.use("/api/admin/analytics", adminAnalyticsRouter);

// ── Router ADMIN genérico (DEIXAR POR ÚLTIMO) ──────────────
app.use("/api/admin", adminRoutes);

// ✅ Limite de compras
app.use("/api/purchase-limit", purchaseLimitRouter);

// Cupons
app.use("/api/coupons", couponsRouter);

// Tray OAuth callback e health
app.use("/tray", trayRouter);       // /tray/callback/auth
app.use("/api/tray", trayRouter);   // /api/tray/health

app.use("/api/admin/users", adminUsersRouter);

app.use("/api", autopayRouter);
app.use("/api/autopay", autopayVindiRouter);

app.use("/api/me/draws", meDraws);

app.use("/api/admin/autopay", autopayRunnerRoute);

// 404 padrão
app.use((req, res) => {
  res.status(404).json({ error: "not_found", path: req.originalUrl });
});

// Handler de erros global (sempre retorna JSON, nunca HTML)
app.use((err, req, res, next) => {
  // Se já foi enviada resposta, delegar para handler padrão do Express
  if (res.headersSent) {
    return next(err);
  }

  // Log do erro (sem dados sensíveis)
  console.error("[express] erro não tratado:", {
    path: req.originalUrl,
    method: req.method,
    message: err?.message || String(err),
    code: err?.code,
    status: err?.status,
  });

  // Sempre retorna JSON padronizado
  const status = err?.status || err?.statusCode || 500;
  const code = err?.code || "INTERNAL_ERROR";
  const message = err?.message || "Erro interno do servidor";

  res.status(status).json({
    ok: false,
    code,
    error_message: message,
    ...(err?.provider_status && { provider_status: err.provider_status }),
  });
});

// ── Validação de env Vindi no boot (FAIL-FAST) ────────────────
function validateVindiConfig() {
  const errors = [];
  const warnings = [];

  const rawBaseUrl = process.env.VINDI_API_BASE_URL || process.env.VINDI_API_URL;
  const rawApiKey = process.env.VINDI_API_KEY || "";
  const rawPublicBaseUrl = process.env.VINDI_PUBLIC_BASE_URL || process.env.VINDI_PUBLIC_URL;
  const rawPublicKey = process.env.VINDI_PUBLIC_KEY || "";

  const trimmedApiKey = String(rawApiKey).trim();
  const trimmedPublicKey = String(rawPublicKey).trim();

  // Detecta ambiente (produção vs sandbox)
  const isSandbox = process.env.VINDI_SANDBOX === "true" || process.env.NODE_ENV === "development";
  const environment = isSandbox ? "SANDBOX" : "PRODUÇÃO";
  const expectedBaseUrl = isSandbox 
    ? "https://sandbox-app.vindi.com.br/api/v1"
    : "https://app.vindi.com.br/api/v1";

  // Helper para mascarar chaves (primeiros 4 + últimos 4)
  const maskKey = (key) => {
    if (!key || key.length < 8) return "****";
    const first4 = key.substring(0, 4);
    const last4 = key.substring(key.length - 4);
    return `${first4}...${last4}`;
  };

  // Validação 1: VINDI_API_BASE_URL - se existir e não começar com http, logar WARNING e ignorar (usar fallback)
  if (rawBaseUrl) {
    const trimmed = String(rawBaseUrl).trim();
    if (!trimmed.startsWith("http://") && !trimmed.startsWith("https://")) {
      warnings.push(`VINDI_API_BASE_URL inválida (não começa com http): "${maskKey(trimmed)}". Usando fallback: ${expectedBaseUrl}`);
    } else {
      try {
        const url = new URL(trimmed);
        if (url.host !== new URL(expectedBaseUrl).host) {
          warnings.push(`VINDI_API_BASE_URL aponta para ${url.host}, mas esperado é ${new URL(expectedBaseUrl).host} para ${environment.toLowerCase()}`);
        }
      } catch (e) {
        warnings.push(`VINDI_API_BASE_URL inválida (erro ao parsear): ${e.message}. Usando fallback: ${expectedBaseUrl}`);
      }
    }
  }

  // Validação 2: VINDI_API_KEY não pode estar vazia
  if (!trimmedApiKey) {
    errors.push("VINDI_API_KEY não configurada ou vazia. Configure a chave de API privada da Vindi.");
  }

  // Validação 3: VINDI_PUBLIC_KEY não pode estar vazia
  if (!trimmedPublicKey) {
    errors.push("VINDI_PUBLIC_KEY não configurada ou vazia. Configure a chave de API pública da Vindi.");
  }

  // Validação 4: VINDI_API_KEY e VINDI_PUBLIC_KEY não podem ser iguais
  if (trimmedApiKey && trimmedPublicKey && trimmedApiKey === trimmedPublicKey) {
    errors.push("VINDI_API_KEY e VINDI_PUBLIC_KEY são iguais. Elas devem ser diferentes (chave privada vs pública).");
  }

  // Validação 5: VINDI_PUBLIC_BASE_URL - se existir e não começar com http, logar WARNING e ignorar (usar fallback)
  if (rawPublicBaseUrl) {
    const trimmed = String(rawPublicBaseUrl).trim();
    if (!trimmed.startsWith("http://") && !trimmed.startsWith("https://")) {
      warnings.push(`VINDI_PUBLIC_BASE_URL inválida (não começa com http): "${maskKey(trimmed)}". Usando fallback: ${expectedBaseUrl}`);
    } else {
      try {
        const url = new URL(trimmed);
        if (url.host !== new URL(expectedBaseUrl).host) {
          warnings.push(`VINDI_PUBLIC_BASE_URL aponta para ${url.host}, mas esperado é ${new URL(expectedBaseUrl).host} para ${environment.toLowerCase()}`);
        }
      } catch (e) {
        warnings.push(`VINDI_PUBLIC_BASE_URL inválida (erro ao parsear): ${e.message}. Usando fallback: ${expectedBaseUrl}`);
      }
    }
  }

  // Log warnings (não bloqueiam)
  if (warnings.length > 0) {
    console.warn(`[boot] Vindi Config Warnings (${environment}):`);
    warnings.forEach(w => console.warn(`  ⚠️  ${w}`));
  }

  // Se houver erros, interrompe o start do serviço
  if (errors.length > 0) {
    console.error(`[boot] ERRO CRÍTICO: Configuração Vindi inválida (${environment}):`);
    errors.forEach(e => console.error(`  ❌ ${e}`));
    console.error(`[boot] Serviço não será iniciado. Corrija as variáveis de ambiente e tente novamente.`);
    throw new Error(`Configuração Vindi inválida: ${errors.join("; ")}`);
  }

  // Log sucesso (sem expor secrets - mascarar chaves)
  let baseUrlHost = "N/A";
  if (rawBaseUrl) {
    try {
      const trimmed = String(rawBaseUrl).trim();
      if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
        baseUrlHost = new URL(trimmed).host;
      } else {
        baseUrlHost = `INVÁLIDA (${maskKey(trimmed)}) - usando fallback`;
      }
    } catch {
      baseUrlHost = "INVÁLIDA - usando fallback";
    }
  } else {
    baseUrlHost = "não configurada (usando fallback)";
  }

  let publicBaseUrlHost = "N/A";
  if (rawPublicBaseUrl) {
    try {
      const trimmed = String(rawPublicBaseUrl).trim();
      if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
        publicBaseUrlHost = new URL(trimmed).host;
      } else {
        publicBaseUrlHost = `INVÁLIDA (${maskKey(trimmed)}) - usando fallback`;
      }
    } catch {
      publicBaseUrlHost = "INVÁLIDA - usando fallback";
    }
  } else {
    publicBaseUrlHost = "não configurada (usando fallback)";
  }
  
  console.log(`[boot] ✅ Vindi Config validada (${environment}):`);
  console.log(`  VINDI_API_BASE_URL: ${baseUrlHost}`);
  console.log(`  VINDI_API_KEY: ${maskKey(trimmedApiKey)} (${trimmedApiKey.length} caracteres)`);
  console.log(`  VINDI_PUBLIC_BASE_URL: ${publicBaseUrlHost}`);
  console.log(`  VINDI_PUBLIC_KEY: ${maskKey(trimmedPublicKey)} (${trimmedPublicKey.length} caracteres)`);
}

// ── Bootstrap ───────────────────────────────────────────────
async function bootstrap() {
  try {
    // Validação de env Vindi
    validateVindiConfig();

    await ensureSchema(); // cria o schema base/tabelas
    await ensureAppConfig(); // garante app_config e ticket_price_cents

    const pool = await getPool();
    await pool.query("SELECT 1");
    console.log("[db] warmup ok");

    app.listen(PORT, () => {
      console.log(`API listening on :${PORT}`);
      console.log(`[cors] origins = ${ORIGINS.join(", ")}`);
    });
  } catch (e) {
    console.error("[bootstrap] falha ao iniciar backend:", e);
    process.exit(1);
  }
}
bootstrap();
