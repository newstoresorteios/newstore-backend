import { query } from "../../db.js";

export const WHATSAPP_CONSENT_CATEGORY_DEFAULT = "manual";

const GRANTED_STATUSES = new Set([
  "granted",
  "active",
  "opt_in",
  "allowed",
  "subscribed",
  "accepted",
]);

const REVOKED_STATUSES = new Set([
  "revoked",
  "opt_out",
  "denied",
  "blocked",
  "unsubscribed",
]);

function runQuery(pgClient, text, params) {
  if (pgClient) return pgClient.query(text, params);
  return query(text, params);
}

function envBool(name, defaultValue) {
  const raw = process.env[name];
  if (raw == null || raw === "") return defaultValue;
  return String(raw).trim() === "true";
}

function cleanText(value, fallback = "") {
  return String(value ?? fallback).trim();
}

function normalizeCategory(category) {
  const raw = cleanText(category, WHATSAPP_CONSENT_CATEGORY_DEFAULT).toLowerCase();
  return raw || WHATSAPP_CONSENT_CATEGORY_DEFAULT;
}

function maskPhone(phone) {
  const digits = String(phone || "").replace(/\D/g, "");
  if (!digits) return null;
  if (digits.length <= 4) return "****";
  return `${digits.slice(0, 2)}****${digits.slice(-4)}`;
}

export function isWhatsAppConsentRequired() {
  return envBool("NOTIFICATION_WHATSAPP_REQUIRE_CONSENT", true);
}

export function isUnlinkedWhatsAppPhoneAllowed() {
  return envBool("NOTIFICATION_WHATSAPP_ALLOW_UNLINKED_PHONE", false);
}

export function mapCommunicationConsentStatus(status) {
  const normalized = cleanText(status).toLowerCase();
  if (!normalized) {
    return {
      whatsapp_consent_status: "missing",
      whatsapp_can_send: false,
      reason: "whatsapp_consent_missing",
    };
  }
  if (GRANTED_STATUSES.has(normalized)) {
    return {
      whatsapp_consent_status: normalized,
      whatsapp_can_send: true,
      reason: null,
    };
  }
  if (REVOKED_STATUSES.has(normalized)) {
    return {
      whatsapp_consent_status: normalized,
      whatsapp_can_send: false,
      reason: "whatsapp_consent_revoked",
    };
  }
  return {
    whatsapp_consent_status: normalized || "unknown",
    whatsapp_can_send: false,
    reason: "whatsapp_consent_unknown",
  };
}

export async function getUserCommunicationConsent({
  pgClient,
  userId,
  channel = "whatsapp",
  category = WHATSAPP_CONSENT_CATEGORY_DEFAULT,
} = {}) {
  const uid = Number(userId);
  const safeCategory = normalizeCategory(category);
  const safeChannel = cleanText(channel, "whatsapp").toLowerCase();

  if (!Number.isInteger(uid) || uid <= 0) {
    return {
      user_id: null,
      channel: safeChannel,
      category: safeCategory,
      status: "missing",
      source: null,
      created_at: null,
      whatsapp_can_send: false,
      reason: "whatsapp_consent_missing",
    };
  }

  try {
    const result = await runQuery(
      pgClient,
      `SELECT user_id, channel, category, status, source, created_at
         FROM public.communication_consents
        WHERE user_id = $1
          AND LOWER(channel) = LOWER($2)
          AND LOWER(category) = LOWER($3)
        ORDER BY created_at DESC
        LIMIT 1`,
      [uid, safeChannel, safeCategory]
    );
    const row = result.rows?.[0] || null;
    const mapped = mapCommunicationConsentStatus(row?.status);
    return {
      user_id: uid,
      channel: safeChannel,
      category: safeCategory,
      status: mapped.whatsapp_consent_status,
      source: row?.source || null,
      created_at: row?.created_at || null,
      whatsapp_can_send: mapped.whatsapp_can_send,
      reason: mapped.reason,
    };
  } catch (error) {
    if (error?.code === "42P01" || error?.code === "42703") {
      console.warn("[whatsapp-consent] check:blocked", {
        user_id: uid,
        category: safeCategory,
        reason: "whatsapp_consent_unknown",
        schema_error: true,
      });
      return {
        user_id: uid,
        channel: safeChannel,
        category: safeCategory,
        status: "unknown",
        source: null,
        created_at: null,
        whatsapp_can_send: false,
        reason: "whatsapp_consent_unknown",
        schema_error: true,
      };
    }
    throw error;
  }
}

export async function getWhatsappConsentStatusForUser({
  pgClient,
  userId,
  category = WHATSAPP_CONSENT_CATEGORY_DEFAULT,
} = {}) {
  const consent = await getUserCommunicationConsent({
    pgClient,
    userId,
    channel: "whatsapp",
    category,
  });
  return {
    whatsapp_consent_status: consent.status || "missing",
    whatsapp_can_send: consent.whatsapp_can_send === true,
    whatsapp_consent_category: consent.category || normalizeCategory(category),
    whatsapp_consent_source: consent.source || null,
    whatsapp_consent_at: consent.created_at || null,
    whatsapp_consent_reason: consent.reason || null,
  };
}

export async function assertWhatsAppConsent({
  pgClient,
  userId,
  phone = null,
  category = WHATSAPP_CONSENT_CATEGORY_DEFAULT,
  source = "manual",
  recipientForced = false,
} = {}) {
  const safeCategory = normalizeCategory(category);
  console.log("[whatsapp-consent] check:start", {
    user_id: userId || null,
    category: safeCategory,
    source: source || null,
    recipient_forced: recipientForced === true,
    has_phone: Boolean(phone),
  });

  if (!isWhatsAppConsentRequired()) {
    console.log("[whatsapp-consent] check:allowed", {
      user_id: userId || null,
      category: safeCategory,
      reason: "consent_not_required",
    });
    return {
      ok: true,
      reason: null,
      whatsapp_consent_status: "not_required",
      whatsapp_can_send: true,
      whatsapp_consent_category: safeCategory,
    };
  }

  if (recipientForced === true) {
    console.log("[whatsapp-consent] check:allowed", {
      user_id: userId || null,
      category: safeCategory,
      reason: "forced_test_recipient",
    });
    return {
      ok: true,
      reason: null,
      whatsapp_consent_status: "test_recipient",
      whatsapp_can_send: true,
      whatsapp_consent_category: safeCategory,
    };
  }

  if (!userId) {
    const allowed = isUnlinkedWhatsAppPhoneAllowed();
    const reason = allowed ? null : "whatsapp_unlinked_phone_blocked";
    console.warn("[whatsapp-consent] check:blocked", {
      user_id: null,
      category: safeCategory,
      source: source || null,
      reason,
      phone_masked: maskPhone(phone),
    });
    return {
      ok: allowed,
      reason,
      whatsapp_consent_status: allowed ? "unlinked_allowed" : "unknown",
      whatsapp_can_send: allowed,
      whatsapp_consent_category: safeCategory,
    };
  }

  const consent = await getWhatsappConsentStatusForUser({
    pgClient,
    userId,
    category: safeCategory,
  });

  if (consent.whatsapp_can_send) {
    console.log("[whatsapp-consent] check:allowed", {
      user_id: Number(userId),
      category: safeCategory,
      status: consent.whatsapp_consent_status,
    });
    return { ok: true, reason: null, ...consent };
  }

  console.warn("[whatsapp-consent] check:blocked", {
    user_id: Number(userId),
    category: safeCategory,
    source: source || null,
    reason: consent.whatsapp_consent_reason || "whatsapp_consent_missing",
  });
  return {
    ok: false,
    reason: consent.whatsapp_consent_reason || "whatsapp_consent_missing",
    ...consent,
  };
}

export async function countWhatsAppConsentForAudience({
  pgClient,
  whereSql = "",
  params = [],
  category = WHATSAPP_CONSENT_CATEGORY_DEFAULT,
} = {}) {
  const safeCategory = normalizeCategory(category);
  const accepted = Array.from(GRANTED_STATUSES);
  try {
    const result = await runQuery(
      pgClient,
      `WITH latest_consent AS (
         SELECT DISTINCT ON (user_id)
                user_id, LOWER(status) AS status
           FROM public.communication_consents
          WHERE LOWER(channel) = 'whatsapp'
            AND LOWER(category) = LOWER($1)
          ORDER BY user_id, created_at DESC
       )
       SELECT
         COUNT(*)::int AS total_candidates,
         COUNT(*) FILTER (WHERE lc.status = ANY($2::text[]))::int AS allowed_by_whatsapp_consent
       FROM public.users u
       LEFT JOIN latest_consent lc ON lc.user_id = u.id
       ${whereSql}`,
      [safeCategory, accepted, ...params]
    );
    const row = result.rows?.[0] || {};
    const total = Number(row.total_candidates || 0);
    const allowed = Number(row.allowed_by_whatsapp_consent || 0);
    return {
      total_candidates: total,
      allowed_by_whatsapp_consent: allowed,
      blocked_by_whatsapp_consent: Math.max(0, total - allowed),
      whatsapp_consent_category: safeCategory,
    };
  } catch (error) {
    if (error?.code === "42P01" || error?.code === "42703") {
      return {
        total_candidates: 0,
        allowed_by_whatsapp_consent: 0,
        blocked_by_whatsapp_consent: 0,
        whatsapp_consent_category: safeCategory,
        whatsapp_consent_error: "whatsapp_consent_unknown",
      };
    }
    throw error;
  }
}
