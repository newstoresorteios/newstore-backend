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
import meRoutes from "./routes/me.js";
import drawsRoutes from "./routes/draws.js";
import drawsExtRoutes from "./routes/draws_ext.js";

// Routers ADMIN específicos (monte ANTES do /api/admin genérico)
import adminDrawsRouter from "./routes/admin_draws.js";
import adminClientsRouter from "./routes/admin_clients.js";
import adminWinnersRouter from "./routes/admin_winners.js";
import adminDashboardRouter from "./routes/admin_dashboard.js";

// Config pública (front lê o preço) e admin (atualiza o preço)
import configPublicRouter from "./routes/config_public.js";
import adminConfigRouter from "./routes/admin_config.js";

// Router admin genérico (DEIXAR POR ÚLTIMO entre /api/admin/*)
import adminRoutes from "./routes/admin.js";

import purchaseLimitRouter from "./routes/purchase_limit.js";
import couponsRouter from "./routes/coupons.js";

import { query, getPool } from "./db.js";
import { ensureSchema } from "./seed.js";
import { ensureAppConfig } from "./services/config.js";

const app = express();

const PORT = process.env.PORT || 4000;

// Se não setar CORS_ORIGIN, usamos uma allowlist padrão
const ORIGIN = process.env.CORS_ORIGIN
  || "http://localhost:3000,https://newstore-frontend-ten.vercel.app,https://newstorerj.com.br,https://www.newstorerj.com.br";

const ORIGINS = ORIGIN.split(",").map(s => s.trim()).filter(Boolean);

// Ping de saúde no DB (mantém a conexão acordada em hosts free)
setInterval(() => {
  query("SELECT 1").catch((e) =>
    console.warn("[health] db ping failed:", e.code || e.message)
  );
}, 60_000);

// ── Middlewares ─────────────────────────────────────────────
const corsOptions = {
  origin(origin, cb) {
    if (!origin) return cb(null, true); // server-to-server, curl, healthchecks
    const ok = ORIGINS.includes("*") || ORIGINS.includes(origin);
    return cb(ok ? null : new Error("Not allowed by CORS"), ok);
  },
  credentials: true,
  methods: ["GET","POST","PUT","PATCH","DELETE","OPTIONS"],
  allowedHeaders: ["Content-Type","Authorization","X-Requested-With"],
  optionsSuccessStatus: 204,
};

app.use(cors(corsOptions));
app.options("*", cors(corsOptions));   // responde preflight
app.use(express.json({ limit: "1mb" }));
app.use(cookieParser());

// Healthcheck simples
app.get("/health", (_req, res) => {
  res.json({ ok: true, ts: new Date().toISOString() });
});

// ── Rotas públicas/gerais ───────────────────────────────────
app.use("/api/auth", authRoutes);
app.use("/api/numbers", numbersRoutes);
app.use("/api/reservations", reservationsRoutes);

// Pagamentos
app.use("/api/payments", paymentsRoutes);
app.use("/api/orders", paymentsRoutes);          // aliases
app.use("/api/participations", paymentsRoutes);  // aliases

app.use("/api/me", meRoutes);
app.use("/api/draws", drawsRoutes);
app.use("/api/draws-ext", drawsExtRoutes);

// ── Rotas ADMIN específicas (antes do genérico) ────────────
app.use("/api/admin/draws", adminDrawsRouter);
app.use("/api/admin/clients", adminClientsRouter);
app.use("/api/admin/winners", adminWinnersRouter);
app.use("/api/admin/dashboard", adminDashboardRouter);

// Config (pública e admin)
app.use("/api/config", configPublicRouter);
app.use("/api/admin/config", adminConfigRouter);

// ── Router ADMIN genérico (DEIXAR POR ÚLTIMO) ──────────────
app.use("/api/admin", adminRoutes);

// ✅ Limite de compras
app.use("/api/purchase-limit", purchaseLimitRouter);

// Cupons
app.use("/api/coupons", couponsRouter);

// 404 padrão
app.use((req, res) => {
  res.status(404).json({ error: "not_found", path: req.originalUrl });
});

// ── Bootstrap ───────────────────────────────────────────────
async function bootstrap() {
  try {
    await ensureSchema();       // cria o schema base/tabelas
    await ensureAppConfig();    // garante app_config e ticket_price_cents

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
