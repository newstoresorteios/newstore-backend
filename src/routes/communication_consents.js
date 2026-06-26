import { Router } from "express";
import { requireAuth } from "../middleware/auth.js";
import {
  createWhatsAppCommunicationConsent,
  getWhatsappConsentStatusForUser,
  isAllowedWhatsAppConsentCategory,
  normalizeWhatsAppConsentCategory,
} from "../services/notifications/communicationConsent.js";

const router = Router();

function getRequestIp(req) {
  const forwarded = String(req.headers?.["x-forwarded-for"] || "").split(",")[0].trim();
  return forwarded || req.ip || req.socket?.remoteAddress || null;
}

function getRequestUserAgent(req) {
  return req.headers?.["user-agent"] ? String(req.headers["user-agent"]) : null;
}

function getRequestedCategory(req, fallback = "all") {
  const raw = req.body?.category ?? req.query?.category ?? fallback;
  return String(raw || fallback).trim().toLowerCase();
}

router.use(requireAuth);

router.get("/", async (req, res) => {
  try {
    const category = normalizeWhatsAppConsentCategory(req.query?.category || "all");
    const consent = await getWhatsappConsentStatusForUser({
      userId: req.user.id,
      category,
    });

    return res.json({
      ok: true,
      whatsapp: {
        channel: "whatsapp",
        status: consent.whatsapp_consent_status || "missing",
        can_send: consent.whatsapp_can_send === true,
        category: consent.whatsapp_consent_category || category,
        source: consent.whatsapp_consent_source || null,
        created_at: consent.whatsapp_consent_at || null,
      },
    });
  } catch (error) {
    console.error("[communication-consents] get error", {
      message: error?.message || null,
      code: error?.code || null,
    });
    return res.status(500).json({ ok: false, error: "communication_consents_failed" });
  }
});

router.post("/whatsapp", async (req, res) => {
  try {
    const requestedCategory = getRequestedCategory(req, "all");
    if (!isAllowedWhatsAppConsentCategory(requestedCategory)) {
      return res.status(400).json({ ok: false, error: "invalid_category" });
    }

    const result = await createWhatsAppCommunicationConsent({
      userId: req.user.id,
      category: requestedCategory,
      status: "granted",
      source: "account_page",
      ip: getRequestIp(req),
      userAgent: getRequestUserAgent(req),
      meta: { action: "opt_in" },
    });

    if (!result.ok) {
      return res.status(503).json({ ok: false, error: result.error });
    }

    return res.json({
      ok: true,
      whatsapp: {
        channel: "whatsapp",
        status: "granted",
        can_send: true,
        category: result.consent?.category || requestedCategory,
        source: result.consent?.source || "account_page",
        created_at: result.consent?.created_at || null,
      },
    });
  } catch (error) {
    console.error("[communication-consents] whatsapp opt-in error", {
      message: error?.message || null,
      code: error?.code || null,
    });
    return res.status(500).json({ ok: false, error: "communication_consents_failed" });
  }
});

router.delete("/whatsapp", async (req, res) => {
  try {
    const requestedCategory = getRequestedCategory(req, "all");
    if (!isAllowedWhatsAppConsentCategory(requestedCategory)) {
      return res.status(400).json({ ok: false, error: "invalid_category" });
    }

    const result = await createWhatsAppCommunicationConsent({
      userId: req.user.id,
      category: requestedCategory,
      status: "revoked",
      source: "account_page",
      ip: getRequestIp(req),
      userAgent: getRequestUserAgent(req),
      meta: { action: "opt_out" },
    });

    if (!result.ok) {
      return res.status(503).json({ ok: false, error: result.error });
    }

    return res.json({
      ok: true,
      whatsapp: {
        channel: "whatsapp",
        status: "revoked",
        can_send: false,
        category: result.consent?.category || requestedCategory,
        source: result.consent?.source || "account_page",
        created_at: result.consent?.created_at || null,
      },
    });
  } catch (error) {
    console.error("[communication-consents] whatsapp opt-out error", {
      message: error?.message || null,
      code: error?.code || null,
    });
    return res.status(500).json({ ok: false, error: "communication_consents_failed" });
  }
});

export default router;
