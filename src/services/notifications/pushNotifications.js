import webpush from "web-push";
import { query } from "../../db.js";
import {
  assertAllowedTestSubscription,
  assertPushSingleDeviceMode,
  coded,
} from "./pushSingleDeviceGuard.js";

const DEFAULT_URL = "/me";
const DEFAULT_TEST_LABEL = "43998640480";
let configuredSignature = "";

function trimmed(value, maxLength) {
  return String(value || "").trim().slice(0, maxLength);
}

function validateMessage({ title, body, url }) {
  const cleanTitle = String(title || "").trim();
  const cleanBody = String(body || "").trim();
  if (!cleanTitle) throw coded("push_title_required");
  if (!cleanBody) throw coded("push_body_required");
  if (cleanTitle.length > 80) throw coded("push_title_too_long");
  if (cleanBody.length > 180) throw coded("push_body_too_long");

  const cleanUrl = String(url || DEFAULT_URL).trim() || DEFAULT_URL;
  if (!cleanUrl.startsWith("/") || cleanUrl.startsWith("//") || cleanUrl.length > 500) {
    throw coded("push_url_invalid");
  }
  return { title: cleanTitle, body: cleanBody, url: cleanUrl };
}

function safePayload(value) {
  if (value == null) return {};
  if (typeof value !== "object" || Array.isArray(value)) throw coded("push_payload_invalid");
  let json;
  try {
    json = JSON.stringify(value);
  } catch {
    throw coded("push_payload_invalid");
  }
  if (json.length > 2048) throw coded("push_payload_too_large");
  if (/(password|secret|token|authorization|phone|cpf|document)/i.test(json)) {
    throw coded("push_payload_sensitive_data_blocked");
  }
  return JSON.parse(json);
}

function safeErrorMessage(error) {
  return trimmed(error?.body || error?.message || error?.code || "push_send_failed", 500);
}

export function configureWebPush() {
  const publicKey = String(process.env.PUSH_VAPID_PUBLIC_KEY || "").trim();
  const privateKey = String(process.env.PUSH_VAPID_PRIVATE_KEY || "").trim();
  const subject = String(process.env.PUSH_VAPID_SUBJECT || "").trim();
  if (!publicKey || !privateKey || !subject) throw coded("push_vapid_configuration_missing");

  const signature = `${subject}:${publicKey.length}:${privateKey.length}`;
  if (configuredSignature !== signature) {
    webpush.setVapidDetails(subject, publicKey, privateKey);
    configuredSignature = signature;
  }
  return true;
}

export function getVapidPublicKey() {
  return String(process.env.PUSH_VAPID_PUBLIC_KEY || "").trim() || null;
}

export async function savePushSubscription({
  userId,
  subscription,
  userAgent,
  deviceLabel,
}) {
  const endpoint = String(subscription?.endpoint || "").trim();
  const p256dh = String(subscription?.keys?.p256dh || "").trim();
  const auth = String(subscription?.keys?.auth || "").trim();
  if (!userId) throw coded("push_user_not_authenticated");
  if (!endpoint || endpoint.length > 4096 || !p256dh || !auth) {
    throw coded("push_subscription_invalid");
  }

  const result = await query(
    `INSERT INTO public.push_subscriptions (
       user_id, endpoint, p256dh, auth, user_agent, device_label,
       is_active, test_label, operational_opt_in, marketing_opt_in,
       last_error_at, last_error, updated_at
     ) VALUES ($1, $2, $3, $4, $5, $6, true, $7, true, false, NULL, NULL, now())
     ON CONFLICT (endpoint) DO UPDATE SET
       user_id = EXCLUDED.user_id,
       p256dh = EXCLUDED.p256dh,
       auth = EXCLUDED.auth,
       user_agent = EXCLUDED.user_agent,
       device_label = EXCLUDED.device_label,
       is_active = true,
       test_label = EXCLUDED.test_label,
       updated_at = now()
     RETURNING id, operational_opt_in, marketing_opt_in`,
    [
      userId,
      endpoint,
      p256dh,
      auth,
      trimmed(userAgent, 1000) || null,
      trimmed(deviceLabel, 240) || null,
      process.env.PUSH_TEST_PHONE_LABEL || DEFAULT_TEST_LABEL,
    ]
  );

  console.log("[push.single-device] subscribe:ok");
  return {
    subscription_id: result.rows[0].id,
    push_operational_opt_in: result.rows[0].operational_opt_in,
    push_marketing_opt_in: result.rows[0].marketing_opt_in,
  };
}

export async function deactivatePushSubscription({ userId, endpoint }) {
  if (!userId) throw coded("push_user_not_authenticated");
  const cleanEndpoint = String(endpoint || "").trim();
  if (!cleanEndpoint) throw coded("push_endpoint_required");
  const result = await query(
    `UPDATE public.push_subscriptions
        SET is_active = false, updated_at = now()
      WHERE user_id = $1 AND endpoint = $2
      RETURNING id`,
    [userId, cleanEndpoint]
  );
  return { ok: true, deactivated: result.rowCount > 0 };
}

export async function getPushPreferences({ userId }) {
  if (!userId) throw coded("push_user_not_authenticated");
  const result = await query(
    `SELECT operational_opt_in, marketing_opt_in
       FROM public.push_subscriptions
      WHERE user_id = $1 AND is_active = true
      ORDER BY updated_at DESC, created_at DESC
      LIMIT 1`,
    [userId]
  );
  const row = result.rows[0];
  const operational = row ? row.operational_opt_in === true : true;
  const marketing = row ? row.marketing_opt_in === true : false;
  return {
    push_operational_opt_in: operational,
    push_marketing_opt_in: marketing,
    push_opt_out: !operational && !marketing,
  };
}

export async function updatePushPreferences({
  userId,
  operationalOptIn,
  marketingOptIn,
}) {
  if (!userId) throw coded("push_user_not_authenticated");
  if (typeof operationalOptIn !== "boolean" || typeof marketingOptIn !== "boolean") {
    throw coded("push_preferences_invalid");
  }
  const result = await query(
    `UPDATE public.push_subscriptions
        SET operational_opt_in = $2,
            marketing_opt_in = $3,
            updated_at = now()
      WHERE id = (
        SELECT id
          FROM public.push_subscriptions
         WHERE user_id = $1 AND is_active = true
         ORDER BY updated_at DESC, created_at DESC
         LIMIT 1
      )
      RETURNING operational_opt_in, marketing_opt_in`,
    [userId, operationalOptIn, marketingOptIn]
  );
  if (!result.rows[0]) throw coded("push_active_subscription_not_found");
  return {
    push_operational_opt_in: result.rows[0].operational_opt_in,
    push_marketing_opt_in: result.rows[0].marketing_opt_in,
    push_opt_out: !result.rows[0].operational_opt_in && !result.rows[0].marketing_opt_in,
  };
}

export async function sendPushToSubscriptionRow({
  subscriptionRow,
  title,
  body,
  url,
  payload,
  source = "manual_test",
}) {
  const message = validateMessage({ title, body, url });
  const extraPayload = safePayload(payload);

  try {
    assertPushSingleDeviceMode({
      source,
      isAudience: false,
      isEngine: false,
      isMassSend: false,
      isCampaign: false,
    });
  } catch (error) {
    console.log("[push.single-device] send:blocked");
    throw error;
  }

  if (!subscriptionRow?.id || !subscriptionRow?.endpoint) {
    throw coded("push_test_subscription_not_found_or_inactive");
  }
  configureWebPush();

  const browserPayload = {
    ...extraPayload,
    title: message.title,
    body: message.body,
    url: message.url,
    event_key: "PUSH_SINGLE_DEVICE_TEST",
    category: "test",
    test_label: process.env.PUSH_TEST_PHONE_LABEL || DEFAULT_TEST_LABEL,
    created_at: new Date().toISOString(),
  };
  const pushSubscription = {
    endpoint: subscriptionRow.endpoint,
    keys: { p256dh: subscriptionRow.p256dh, auth: subscriptionRow.auth },
  };

  console.log("[push.single-device] send:start");
  try {
    assertAllowedTestSubscription({ subscriptionId: subscriptionRow.id });
    await webpush.sendNotification(pushSubscription, JSON.stringify(browserPayload));

    const dispatch = await query(
      `INSERT INTO public.notification_push_dispatches (
         user_id, subscription_id, event_key, category, title, body, url,
         payload, status, sent_at
       ) VALUES ($1, $2, 'PUSH_SINGLE_DEVICE_TEST', 'test', $3, $4, $5, $6::jsonb, 'sent', now())
       RETURNING id, status, sent_at`,
      [
        subscriptionRow.user_id || null,
        subscriptionRow.id,
        message.title,
        message.body,
        message.url,
        JSON.stringify(browserPayload),
      ]
    );
    await query(
      `UPDATE public.push_subscriptions
          SET last_success_at = now(), last_error_at = NULL, last_error = NULL, updated_at = now()
        WHERE id = $1`,
      [subscriptionRow.id]
    );
    console.log("[push.single-device] send:sent");
    return { ok: true, dispatch: dispatch.rows[0] };
  } catch (error) {
    if (error?.code === "push_subscription_not_allowed_in_test_mode") {
      console.log("[push.single-device] send:blocked");
      throw error;
    }
    const statusCode = Number(error?.statusCode || error?.status || 0);
    const errorMessage = safeErrorMessage(error);
    await query(
      `INSERT INTO public.notification_push_dispatches (
         user_id, subscription_id, event_key, category, title, body, url,
         payload, status, error_message
       ) VALUES ($1, $2, 'PUSH_SINGLE_DEVICE_TEST', 'test', $3, $4, $5, $6::jsonb, 'failed', $7)`,
      [
        subscriptionRow.user_id || null,
        subscriptionRow.id,
        message.title,
        message.body,
        message.url,
        JSON.stringify(browserPayload),
        errorMessage,
      ]
    ).catch(() => {});
    await query(
      `UPDATE public.push_subscriptions
          SET last_error_at = now(), last_error = $2,
              is_active = CASE WHEN $3::int IN (404, 410) THEN false ELSE is_active END,
              updated_at = now()
        WHERE id = $1`,
      [subscriptionRow.id, errorMessage, statusCode]
    ).catch(() => {});
    console.log("[push.single-device] send:failed");
    throw coded("push_send_failed");
  }
}

export async function sendTestPushToConfiguredSubscription({
  title,
  body,
  url = DEFAULT_URL,
  payload = {},
  source = "manual_test",
}) {
  assertPushSingleDeviceMode({
    source,
    isAudience: false,
    isEngine: false,
    isMassSend: false,
    isCampaign: false,
  });
  const subscriptionId = String(process.env.PUSH_TEST_SUBSCRIPTION_ID || "").trim();
  const result = await query(
    `SELECT * FROM public.push_subscriptions
      WHERE id = $1
        AND is_active = true`,
    [subscriptionId]
  );
  if (!result.rows[0]) throw coded("push_test_subscription_not_found_or_inactive");
  return sendPushToSubscriptionRow({
    subscriptionRow: result.rows[0],
    title,
    body,
    url,
    payload,
    source,
  });
}
