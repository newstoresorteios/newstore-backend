import webpush from "web-push";
import { query } from "../../db.js";
import {
  assertPushSendAllowed,
  isPushTestMode,
} from "./pushSafetyGuard.js";

const TITLE_MAX = 80;
const BODY_MAX = 180;
const DEFAULT_URL = "/me";

let vapidConfigured = false;

function truncate(str, max) {
  const s = String(str ?? "").trim();
  if (s.length <= max) return s;
  return s.slice(0, max);
}

function safePayload(payload) {
  if (!payload || typeof payload !== "object") return {};
  const out = {};
  const allowed = ["event_key", "category", "url", "tag", "icon", "badge"];
  for (const key of allowed) {
    if (payload[key] != null) out[key] = payload[key];
  }
  return out;
}

function parseBrowserName(userAgent) {
  const ua = String(userAgent || "");
  if (!ua) return null;
  if (/Edg\//i.test(ua)) return "Edge";
  if (/Chrome\//i.test(ua)) return "Chrome";
  if (/Firefox\//i.test(ua)) return "Firefox";
  if (/Safari\//i.test(ua) && !/Chrome/i.test(ua)) return "Safari";
  return null;
}

function parseDeviceType(userAgent) {
  const ua = String(userAgent || "");
  if (!ua) return null;
  if (/Mobile|Android|iPhone|iPad/i.test(ua)) return "mobile";
  return "desktop";
}

function currentMode() {
  return isPushTestMode() ? "test" : "production";
}

export function configureWebPush() {
  const publicKey = process.env.PUSH_VAPID_PUBLIC_KEY;
  const privateKey = process.env.PUSH_VAPID_PRIVATE_KEY;
  const subject =
    process.env.PUSH_VAPID_SUBJECT || "mailto:suporte@newstore.com.br";

  if (!publicKey || !privateKey) {
    console.warn("[push] VAPID keys missing");
    vapidConfigured = false;
    return false;
  }

  webpush.setVapidDetails(subject, publicKey, privateKey);
  vapidConfigured = true;
  return true;
}

export function getVapidPublicKey() {
  return process.env.PUSH_VAPID_PUBLIC_KEY || null;
}

async function getUserPushPrefs(userId) {
  const r = await query(
    `SELECT push_operational_opt_in,
            push_marketing_opt_in,
            push_opt_out,
            push_opt_in_at,
            push_opt_in_source,
            push_opt_out_at
       FROM public.users
      WHERE id = $1
      LIMIT 1`,
    [userId]
  );
  return r.rows[0] || null;
}

function consentAllowsSend({
  prefs,
  category,
  isAdminTest,
  isSelfTest,
  eventKey,
}) {
  if (isSelfTest || eventKey === "PUSH_SELF_TEST") return true;

  if (isAdminTest) {
    return true;
  }

  if (!prefs) return false;
  if (prefs.push_opt_out) return false;

  if (category === "marketing") {
    return prefs.push_marketing_opt_in === true;
  }

  if (category === "operational") {
    return prefs.push_operational_opt_in === true;
  }

  return prefs.push_operational_opt_in === true || prefs.push_marketing_opt_in === true;
}

async function recordConsent({
  userId,
  category,
  status,
  source,
  ip,
  userAgent,
  meta = {},
}) {
  await query(
    `INSERT INTO public.communication_consents
       (user_id, channel, category, status, source, ip, user_agent, meta)
     VALUES ($1, 'push', $2, $3, $4, $5, $6, $7::jsonb)`,
    [
      userId,
      category,
      status,
      source,
      ip || null,
      userAgent || null,
      JSON.stringify(meta),
    ]
  );
}

export async function savePushSubscription({
  userId,
  subscription,
  userAgent,
  deviceLabel,
}) {
  const endpoint = subscription?.endpoint;
  const p256dh = subscription?.keys?.p256dh;
  const auth = subscription?.keys?.auth;

  if (!endpoint || !p256dh || !auth) {
    const err = new Error("invalid_subscription");
    err.code = "invalid_subscription";
    throw err;
  }

  const browserName = parseBrowserName(userAgent);
  const deviceType = parseDeviceType(userAgent);

  const r = await query(
    `INSERT INTO public.push_subscriptions
       (user_id, endpoint, p256dh, auth, user_agent, device_label, browser_name, device_type, is_active, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, true, NOW())
     ON CONFLICT (endpoint) DO UPDATE SET
       user_id = EXCLUDED.user_id,
       p256dh = EXCLUDED.p256dh,
       auth = EXCLUDED.auth,
       user_agent = EXCLUDED.user_agent,
       device_label = EXCLUDED.device_label,
       browser_name = EXCLUDED.browser_name,
       device_type = EXCLUDED.device_type,
       is_active = true,
       updated_at = NOW()
     RETURNING *`,
    [
      userId,
      endpoint,
      p256dh,
      auth,
      userAgent || null,
      deviceLabel || null,
      browserName,
      deviceType,
    ]
  );

  return r.rows[0];
}

export async function deactivatePushSubscription({ userId, endpoint }) {
  const r = await query(
    `UPDATE public.push_subscriptions
        SET is_active = false,
            updated_at = NOW()
      WHERE user_id = $1
        AND endpoint = $2
      RETURNING id`,
    [userId, endpoint]
  );
  return r.rows[0] || null;
}

export async function getPushPreferences({ userId }) {
  const prefs = await getUserPushPrefs(userId);
  if (!prefs) {
    const err = new Error("user_not_found");
    err.code = "user_not_found";
    throw err;
  }

  const subs = await query(
    `SELECT id, device_label, browser_name, device_type, is_active, last_success_at, created_at
       FROM public.push_subscriptions
      WHERE user_id = $1
      ORDER BY created_at DESC`,
    [userId]
  );

  return {
    push_operational_opt_in: prefs.push_operational_opt_in,
    push_marketing_opt_in: prefs.push_marketing_opt_in,
    push_opt_out: prefs.push_opt_out,
    push_opt_in_at: prefs.push_opt_in_at,
    push_opt_in_source: prefs.push_opt_in_source,
    push_opt_out_at: prefs.push_opt_out_at,
    subscriptions: subs.rows || [],
  };
}

export async function updatePushPreferences({
  userId,
  operationalOptIn,
  marketingOptIn,
  source,
  ip,
  userAgent,
}) {
  const operational = operationalOptIn === true;
  const marketing = marketingOptIn === true;
  const anyOptIn = operational || marketing;

  await query(
    `UPDATE public.users
        SET push_operational_opt_in = $2,
            push_marketing_opt_in = $3,
            push_opt_in_at = CASE
              WHEN $4 THEN COALESCE(push_opt_in_at, NOW())
              ELSE push_opt_in_at
            END,
            push_opt_in_source = CASE
              WHEN $4 THEN $5
              ELSE push_opt_in_source
            END,
            push_opt_out = CASE
              WHEN $4 THEN false
              ELSE true
            END,
            push_opt_out_at = CASE
              WHEN $4 THEN NULL
              ELSE NOW()
            END
      WHERE id = $1`,
    [userId, operational, marketing, anyOptIn, source || "preferences_update"]
  );

  if (operational) {
    await recordConsent({
      userId,
      category: "operational",
      status: "opt_in",
      source: source || "preferences_update",
      ip,
      userAgent,
    });
  } else {
    await recordConsent({
      userId,
      category: "operational",
      status: "opt_out",
      source: source || "preferences_update",
      ip,
      userAgent,
    });
  }

  if (marketing) {
    await recordConsent({
      userId,
      category: "marketing",
      status: "opt_in",
      source: source || "preferences_update",
      ip,
      userAgent,
    });
  } else {
    await recordConsent({
      userId,
      category: "marketing",
      status: "opt_out",
      source: source || "preferences_update",
      ip,
      userAgent,
    });
  }

  return getPushPreferences({ userId });
}

export async function sendPushToSubscription({
  subscriptionRow,
  title,
  body,
  url,
  eventKey,
  category,
  payload,
  source,
  mode,
  userId,
}) {
  if (!vapidConfigured) {
    configureWebPush();
  }

  const safeTitle = truncate(title, TITLE_MAX);
  const safeBody = truncate(body, BODY_MAX);
  const safeUrl = url || DEFAULT_URL;
  const sendMode = mode || currentMode();

  const dispatchInsert = await query(
    `INSERT INTO public.notification_push_dispatches
       (user_id, subscription_id, event_key, category, title, body, url, payload, mode, source, status)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, 'pending')
     RETURNING id`,
    [
      userId || subscriptionRow.user_id,
      subscriptionRow.id,
      eventKey || null,
      category || null,
      safeTitle,
      safeBody,
      safeUrl,
      JSON.stringify(safePayload(payload)),
      sendMode,
      source || null,
    ]
  );
  const dispatchId = dispatchInsert.rows[0]?.id;

  const pushSubscription = {
    endpoint: subscriptionRow.endpoint,
    keys: {
      p256dh: subscriptionRow.p256dh,
      auth: subscriptionRow.auth,
    },
  };

  const notificationPayload = JSON.stringify({
    title: safeTitle,
    body: safeBody,
    url: safeUrl,
    ...safePayload(payload),
  });

  try {
    await webpush.sendNotification(pushSubscription, notificationPayload);

    await query(
      `UPDATE public.notification_push_dispatches
          SET status = 'sent',
              sent_at = NOW()
        WHERE id = $1`,
      [dispatchId]
    );

    await query(
      `UPDATE public.push_subscriptions
          SET last_success_at = NOW(),
              updated_at = NOW()
        WHERE id = $1`,
      [subscriptionRow.id]
    );

    return { ok: true, dispatchId, status: "sent" };
  } catch (e) {
    const statusCode = e?.statusCode || e?.body?.statusCode;
    const errorMessage = String(e?.message || e?.body || "send_failed").slice(0, 500);

    await query(
      `UPDATE public.notification_push_dispatches
          SET status = 'failed',
              error_message = $2
        WHERE id = $1`,
      [dispatchId, errorMessage]
    );

    await query(
      `UPDATE public.push_subscriptions
          SET last_error_at = NOW(),
              last_error = $2,
              updated_at = NOW()
        WHERE id = $1`,
      [subscriptionRow.id, errorMessage]
    );

    if (statusCode === 404 || statusCode === 410) {
      await query(
        `UPDATE public.push_subscriptions
            SET is_active = false,
                updated_at = NOW()
          WHERE id = $1`,
        [subscriptionRow.id]
      );
    }

    return { ok: false, dispatchId, status: "failed", error: errorMessage, statusCode };
  }
}

export async function sendPushToUser({
  userId,
  title,
  body,
  url,
  eventKey,
  category,
  payload,
  source,
  isAdminTest = false,
  isSelfTest = false,
}) {
  assertPushSendAllowed({
    userId,
    source,
    isAudience: false,
    isAdminTest,
    isSelfTest,
    category,
  });

  const prefs = await getUserPushPrefs(userId);

  if (!consentAllowsSend({
    prefs,
    category,
    isAdminTest,
    isSelfTest,
    eventKey,
  })) {
    console.warn("[push] send:blocked", {
      user_id: userId,
      event_key: eventKey,
      reason: "consent_denied",
    });
    return { sent: 0, failed: 0, skipped: 1, reason: "consent_denied" };
  }

  const subs = await query(
    `SELECT *
       FROM public.push_subscriptions
      WHERE user_id = $1
        AND is_active = true`,
    [userId]
  );

  if (!subs.rows.length) {
    console.warn("[push] send:blocked", {
      user_id: userId,
      event_key: eventKey,
      reason: "no_active_subscriptions",
    });
    return { sent: 0, failed: 0, skipped: 1, reason: "no_active_subscriptions" };
  }

  console.log("[push] send:start", {
    user_id: userId,
    event_key: eventKey,
    category,
    source,
    test_mode: isPushTestMode(),
    title_length: truncate(title, TITLE_MAX).length,
    body_length: truncate(body, BODY_MAX).length,
  });

  let sent = 0;
  let failed = 0;
  let skipped = 0;
  const mode = currentMode();

  for (const sub of subs.rows) {
    const result = await sendPushToSubscription({
      subscriptionRow: sub,
      title,
      body,
      url,
      eventKey,
      category,
      payload,
      source,
      mode,
      userId,
    });

    if (result.ok) sent += 1;
    else if (result.status === "failed") failed += 1;
    else skipped += 1;
  }

  console.log("[push] send:done", {
    user_id: userId,
    event_key: eventKey,
    sent,
    failed,
    skipped,
  });

  return { sent, failed, skipped };
}

async function resolveAudienceUserIds(audience, entityId) {
  switch (audience) {
    case "all_push_operational_opt_in":
      const r1 = await query(
        `SELECT id FROM public.users
          WHERE push_operational_opt_in = true
            AND push_opt_out = false`
      );
      return (r1.rows || []).map((row) => Number(row.id));

    case "all_push_marketing_opt_in":
      const r2 = await query(
        `SELECT id FROM public.users
          WHERE push_marketing_opt_in = true
            AND push_opt_out = false`
      );
      return (r2.rows || []).map((row) => Number(row.id));

    case "draw_buyers":
      if (!entityId) {
        const err = new Error("entity_id_required_for_draw_buyers");
        err.code = "entity_id_required";
        throw err;
      }
      const r3 = await query(
        `SELECT DISTINCT user_id AS id
           FROM public.payments
          WHERE draw_id = $1
            AND LOWER(status) IN ('approved', 'paid', 'pago')`,
        [Number(entityId)]
      );
      return (r3.rows || []).map((row) => Number(row.id));

    case "active_draw_buyers":
      const r4 = await query(
        `SELECT DISTINCT p.user_id AS id
           FROM public.payments p
           JOIN public.draws d ON d.id = p.draw_id
          WHERE d.status = 'open'
            AND LOWER(p.status) IN ('approved', 'paid', 'pago')`
      );
      return (r4.rows || []).map((row) => Number(row.id));

    default:
      const err = new Error("unknown_audience");
      err.code = "unknown_audience";
      throw err;
  }
}

export async function sendPushToAudience({
  audience,
  title,
  body,
  url,
  eventKey,
  category,
  payload,
  source,
  entityId,
}) {
  assertPushSendAllowed({
    userId: null,
    source,
    isAudience: true,
    isAdminTest: false,
    isSelfTest: false,
    category,
  });

  const userIds = await resolveAudienceUserIds(audience, entityId);

  let sent = 0;
  let failed = 0;
  let skipped = 0;

  for (const uid of userIds) {
    const result = await sendPushToUser({
      userId: uid,
      title,
      body,
      url,
      eventKey,
      category,
      payload,
      source,
      isAdminTest: false,
      isSelfTest: false,
    });
    sent += result.sent || 0;
    failed += result.failed || 0;
    skipped += result.skipped || 0;
  }

  console.log("[push] send:done", {
    user_id: null,
    event_key: eventKey,
    audience,
    sent,
    failed,
    skipped,
  });

  return { sent, failed, skipped, audience_count: userIds.length };
}
