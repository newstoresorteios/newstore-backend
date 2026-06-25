import express from "express";
import { requireAuth, requireAdmin } from "../middleware/auth.js";
import {
  createPushRule,
  getAllowedPushRuleEvents,
  listPushRules,
  seedDefaultPushRules,
  updatePushRule,
} from "../services/notifications/pushRules.js";
import { getAuthenticatedUserId } from "../services/notifications/pushAccessGuard.js";

const router = express.Router();

router.use(requireAuth, requireAdmin);

function statusFor(code) {
  if (code === "push_rule_not_found") return 404;
  if (String(code || "").startsWith("push_rule_")) return 400;
  return 500;
}

function sendError(res, error) {
  const code = error?.code || "push_rules_failed";
  return res.status(statusFor(code)).json({ ok: false, error: code });
}

function adminUserId(req) {
  return getAuthenticatedUserId({ user: req.user, auth: req.auth });
}

router.get("/", async (_req, res) => {
  try {
    const items = await listPushRules();
    return res.json({
      ok: true,
      allowed_events: getAllowedPushRuleEvents(),
      items,
    });
  } catch (error) {
    return sendError(res, error);
  }
});

router.post("/", async (req, res) => {
  try {
    const item = await createPushRule(req.body || {}, { adminUserId: adminUserId(req) });
    return res.status(201).json({ ok: true, item });
  } catch (error) {
    if (error?.code === "23505") {
      error.code = "push_rule_event_key_exists";
    }
    return sendError(res, error);
  }
});

router.patch("/:id", async (req, res) => {
  try {
    const item = await updatePushRule(req.params.id, req.body || {}, {
      adminUserId: adminUserId(req),
    });
    return res.json({ ok: true, item });
  } catch (error) {
    return sendError(res, error);
  }
});

router.post("/seed-defaults", async (req, res) => {
  try {
    const created = await seedDefaultPushRules({ adminUserId: adminUserId(req) });
    return res.json({
      ok: true,
      created_count: created.length,
      created,
      allowed_events: getAllowedPushRuleEvents(),
    });
  } catch (error) {
    return sendError(res, error);
  }
});

export default router;
