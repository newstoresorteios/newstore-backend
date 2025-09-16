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

// Routers admin ESPECÍFICOS (antes do /api/admin genérico)
import adminDrawsRouter from "./routes/admin_draws.js";
import adminClientsRouter from "./routes/admin_clients.js";
import adminWinnersRouter from "./routes/admin_winners.js";
import adminDashboardRouter from "./routes/admin_dashboard.js";

import configPublicRouter from "./routes/config_public.js";
import adminConfigRouter from "./routes/admin_config.js";
import adminRoutes from "./routes/admin.js"; // genérico (deixe por último entre /api/admin/*)

import couponsRouter from "./routes/coupons.js";

import { query, getPool } from "./db.js";
import { ensureSchema } from "./seed.js";
import { ensureAppConfig } from "./services/config.js";

const app = express();

const PORT = process.env.PORT || 4000;
const ORIGIN = process.env.CORS_ORIGIN || "*";

// ping de saúde no DB
setInterval(() => {
  query("SELECT 1").catch((e) =>
    console.warn("[health] db ping failed", e.code || e.message)
  );
}, 60_000);

// ── middlewares ─────────────────────────────────────────────
app.use(
  cors({
    origin: ORIGIN === "*" ? true : ORIGIN.split(",").map((s) => s.trim()),
    credentials: true,
  })
);
app.use(express.json({ limit: "1mb" }));
app.use(cookieParser());

// Health
app.get("/health", (_req, res) => {
  res.json({ ok: true, ts: new Date().toISOString() });
});

// ── rotas públicas/gerais ───────────────────────────────────
app.use("/api/auth", authRoutes);
app.use("/api/numbers", numbersRoutes);
app.use("/api/reservations", reservationsRoutes);

// monta uma única vez o payments **correto**
app.use("/api/payments", paymentsRoutes);

// aliases para compatibilidade com o front
app.use("/api/orders", paymentsRoutes);
app.use("/api/participations", paymentsRoutes);

app.use("/api/me", meRoutes);
app.use("/api/draws", drawsRoutes);
app.use("/api/draws-ext", drawsExtRoutes);

// ── rotas admin ESPECÍFICAS (antes do genérico) ────────────
app.use("/api/admin/draws", adminDrawsRouter);
app.use("/api/admin/clients", adminClientsRouter);
app.use("/api/admin/winners", adminWinnersRouter);
app.use("/api/admin/dashboard", adminDashboardRouter);

// preço (pública para exibir no site) e admin para atualizar
app.use("/api/config", configPublicRouter);
app.use("/api/admin/config", adminConfigRouter);

// ── router admin GENÉRICO (deixa por último) ───────────────
app.use("/api/admin", adminRoutes);

app.use("/api/coupons", couponsRouter);

// 404
app.use((req, res) => {
  res.status(404).json({ error: "not_found", path: req.originalUrl });
});

// ── bootstrap ───────────────────────────────────────────────
async function bootstrap() {
  try {
    await ensureSchema();
    await ensureAppConfig(); // <— garante que o preço existe
    const pool = await getPool();
    await pool.query("SELECT 1");
    console.log("[db] warmup ok");

    app.listen(PORT, () => {
      console.log(`API listening on :${PORT}`);
    });
  } catch (e) {
    console.error("[bootstrap] falha ao iniciar backend:", e);
    process.exit(1);
  }
}

bootstrap();
