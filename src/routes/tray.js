// src/routes/tray.js
import { Router } from "express";
import { trayBootstrap, trayTokenHealth } from "../services/tray.js";

const router = Router();

/**
 * Callback de autorização da Tray.
 * GET /tray/callback/auth?code=...&api_address=...&store=...
 */
router.get("/callback/auth", async (req, res) => {
  const rid = Math.random().toString(36).slice(2, 8);
  const code = String(req.query?.code || "").trim();
  const api_address = String(req.query?.api_address || "").trim();
  const store = req.query?.store ? String(req.query.store) : null;

  console.log("[tray.auth] callback received", { rid, hasCode: !!code, hasApiAddress: !!api_address, store });

  if (!code || !api_address) {
    return res.status(400).json({ ok: false, error: "missing_code_or_api_address" });
  }

  try {
    const out = await trayBootstrap({ code, api_address });
    return res.json({
      ok: true,
      rid,
      apiBase: out.apiBase,
      authMode: out.authMode,
      expAccessAt: out.expAccessAt,
    });
  } catch (e) {
    console.log("[tray.auth] callback bootstrap fail", { rid, code: e?.code, msg: e?.message });
    return res.status(200).json({ ok: false, rid, error: e?.code || "bootstrap_failed" });
  }
});

/**
 * Health do OAuth.
 * GET /api/tray/health
 */
router.get("/health", async (_req, res) => {
  const out = await trayTokenHealth();
  return res.json(out);
});

export default router;


