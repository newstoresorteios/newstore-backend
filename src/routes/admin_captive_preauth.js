import { Router } from "express";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import {
  createCaptivePreAuthorizationsForDraw,
  getCurrentCaptiveDrawContext,
  isCaptivePreauthEnabled,
  reissueAndResendPendingCaptivePreauths,
} from "../services/autopay/captivePreauthService.js";

const router = Router();

router.post("/current-draw/reissue-and-resend", requireAuth, requireAdmin, async (req, res) => {
  if (req.body?.confirmation !== "REEMITIR") {
    return res.status(400).json({ ok: false, error: "confirmation_required" });
  }

  try {
    const currentDraw = await getCurrentCaptiveDrawContext();
    if (!currentDraw.draw_id) {
      return res.status(404).json({ ok: false, error: "current_principal_draw_not_found" });
    }
    const result = await reissueAndResendPendingCaptivePreauths({
      drawId: currentDraw.draw_id,
      adminUserId: req.user?.id ?? null,
    });
    console.log("[captive-preauth] admin_current_draw_reissue_route", {
      admin_user_id: req.user?.id || null,
      draw_id: result.draw_id,
      pending_found: result.pending_found,
      failed_recoverable_found: result.failed_recoverable_found,
      failed_recovered: result.failed_recovered,
      amount_corrected: result.amount_corrected,
      sent: result.sent,
      failed: result.failed,
    });
    return res.json(result);
  } catch (error) {
    console.error("[captive-preauth] admin_current_draw_reissue_failed", {
      admin_user_id: req.user?.id || null,
      message: error?.message || null,
      code: error?.code || null,
    });
    return res.status(error?.status || 500).json({
      ok: false,
      error: error?.message || "captive_preauth_reissue_failed",
    });
  }
});

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

router.post("/draws/:drawId/reissue-and-resend", requireAuth, requireAdmin, async (req, res) => {
  if (req.body?.confirmation !== "REEMITIR") {
    return res.status(400).json({ ok: false, error: "confirmation_required" });
  }

  try {
    const result = await reissueAndResendPendingCaptivePreauths({
      drawId: req.params.drawId,
      adminUserId: req.user?.id ?? null,
    });
    console.log("[captive-preauth] admin_reissue_route", {
      admin_user_id: req.user?.id || null,
      draw_id: Number(req.params.drawId),
      pending_found: result.pending_found,
      amount_corrected: result.amount_corrected,
      failed_recovered: result.failed_recovered,
      sent: result.sent,
      skipped:
        Number(result.skipped_consent || 0) +
        Number(result.skipped_notifications_disabled || 0) +
        Number(result.skipped_invalid_phone || 0) +
        Number(result.skipped_near_expiration || 0) +
        Number(result.skipped_reservation_unavailable || 0) +
        Number(result.skipped_already_charged || 0) +
        Number(result.skipped_whatsapp_disabled || 0) +
        Number(result.skipped_template_missing || 0) +
        Number(result.skipped_other || 0),
      failed: result.failed,
    });
    return res.json(result);
  } catch (error) {
    console.error("[captive-preauth] admin_reissue_failed", {
      admin_user_id: req.user?.id || null,
      draw_id: req.params?.drawId || null,
      message: error?.message || null,
      code: error?.code || null,
    });
    return res.status(error?.status || 500).json({
      ok: false,
      error: error?.message || "captive_preauth_reissue_failed",
    });
  }
});

export default router;
