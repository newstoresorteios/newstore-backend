import { Router } from "express";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import {
  createCaptivePreAuthorizationsForDraw,
  isCaptivePreauthEnabled,
} from "../services/autopay/captivePreauthService.js";

const router = Router();

router.post("/draws/:drawId/create", requireAuth, requireAdmin, async (req, res) => {
  if (!isCaptivePreauthEnabled()) {
    console.warn("[captive-preauth] feature_disabled", {
      admin_user_id: req.user?.id || null,
      draw_id: req.params?.drawId || null,
    });
    return res.status(403).json({ ok: false, error: "captive_preauth_disabled" });
  }

  try {
    const result = await createCaptivePreAuthorizationsForDraw(req.params.drawId, {
      adminUserId: req.user?.id ?? null,
    });
    return res.json(result);
  } catch (error) {
    console.error("[captive-preauth] failed", {
      admin_user_id: req.user?.id || null,
      draw_id: req.params?.drawId || null,
      message: error?.message || null,
      code: error?.code || null,
    });
    return res.status(error?.status || 500).json({
      ok: false,
      error: error?.message || "captive_preauth_create_failed",
    });
  }
});

export default router;
