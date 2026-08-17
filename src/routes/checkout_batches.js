import { Router } from "express";
import { requireAuth } from "../middleware/auth.js";
import { CheckoutBatchError, reserveCheckoutBatch } from "../services/checkoutBatchService.js";
import {
  createCheckoutBatchPix,
  getCheckoutBatchStatus,
  resolveBatchNotificationUrl,
} from "../services/checkoutBatchPaymentService.js";

const router = Router();
const ALLOWED_ROOT_FIELDS = new Set(["items"]);

function sendError(res, error) {
  if (error instanceof CheckoutBatchError || (error?.status && error?.code)) {
    return res.status(error.status || 400).json({ error: error.code, ...(error.payload || {}) });
  }
  console.error("[checkout-batches] error", {
    code: error?.code,
    message: error?.message,
  });
  return res.status(500).json({ error: "checkout_batch_failed" });
}

router.post("/reserve", requireAuth, async (req, res) => {
  try {
    const forbidden = Object.keys(req.body || {}).filter((key) => !ALLOWED_ROOT_FIELDS.has(key));
    if (forbidden.length) {
      throw new CheckoutBatchError("forbidden_checkout_fields", 400, { fields: forbidden });
    }
    const result = await reserveCheckoutBatch({
      userId: req.user.id,
      items: req.body?.items,
      idempotencyKey: req.get("Idempotency-Key"),
    });
    return res.status(201).json(result);
  } catch (error) {
    return sendError(res, error);
  }
});

router.post("/:batchId/pix", requireAuth, async (req, res) => {
  try {
    const result = await createCheckoutBatchPix({
      batchId: req.params.batchId,
      userId: req.user.id,
      payerEmail: req.user.email,
      notificationUrl: resolveBatchNotificationUrl(),
    });
    return res.status(result.action === "creating" ? 202 : 200).json(result.response);
  } catch (error) {
    return sendError(res, error);
  }
});

router.get("/:batchId/status", requireAuth, async (req, res) => {
  try {
    const result = await getCheckoutBatchStatus(req.params.batchId, req.user.id);
    return res.json(result);
  } catch (error) {
    return sendError(res, error);
  }
});

export default router;
