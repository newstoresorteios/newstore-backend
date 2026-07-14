import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

import { getManualNotificationCatalog } from "../src/services/notifications/manualNotificationCatalog.js";
import { buildManualNotificationPreview } from "../src/services/notifications/manualNotificationPreview.js";
import { sendManualEmailNotification } from "../src/services/notifications/manualEmailNotifications.js";
import { sendManualPushNotification } from "../src/services/notifications/manualPushNotifications.js";

function rowResult(rows) {
  return { rows, rowCount: rows.length };
}

function fakePg(handler) {
  return {
    calls: [],
    async query(sql, params = []) {
      this.calls.push({ sql, params });
      return handler(String(sql), params);
    },
  };
}

const templateColumns = [
  "id",
  "template_key",
  "provider_template_id",
  "name",
  "description",
  "default_message",
  "default_params",
  "message_params",
  "is_active",
  "provider_status",
  "language",
  "updated_at",
  "subject_template",
  "html_template",
  "text_template",
];

const pushRuleColumns = [
  "id",
  "event_key",
  "name",
  "description",
  "title_template",
  "body_template",
  "url_template",
  "category",
  "threshold_value",
  "cooldown_minutes",
  "is_active",
  "last_triggered_at",
];

function catalogPg({ emailRows = [] } = {}) {
  return fakePg((sql, params) => {
    if (sql.includes("information_schema.columns")) {
      const table = params[0];
      return rowResult((table === "notification_push_rules" ? pushRuleColumns : templateColumns).map((column_name) => ({ column_name })));
    }
    if (sql.includes("channel = 'whatsapp'")) {
      return rowResult([{ id: "w1", template_key: "DRAW_REMAINING_NUMBERS_50", provider_template_id: "25", name: "50 restantes" }]);
    }
    if (sql.includes("notification_push_rules")) {
      return rowResult([{ id: "p1", event_key: "DRAW_REMAINING_NUMBERS_50", title_template: "Restam {{remaining}}", body_template: "Poucos numeros" }]);
    }
    if (sql.includes("channel = 'email'")) return rowResult(emailRows);
    return rowResult([]);
  });
}

test("catalog returns whatsapp, push and database email templates without Brevo calls", async () => {
  const pgClient = catalogPg({
    emailRows: [{ id: "e1", template_key: "EMAIL_DB", name: "Email DB", subject_template: "Oi" }],
  });

  const catalog = await getManualNotificationCatalog({ pgClient });

  assert.equal(catalog.ok, true);
  assert.equal(catalog.channels.whatsapp.templates[0].provider_template_id, "25");
  assert.equal(catalog.channels.push.templates[0].event_key, "DRAW_REMAINING_NUMBERS_50");
  assert.equal(catalog.channels.email.templates[0].template_key, "EMAIL_DB");
  assert.equal(pgClient.calls.some((call) => /https?:\/\//i.test(call.sql)), false);
});

test("catalog returns builtin email templates when database has none", async () => {
  const catalog = await getManualNotificationCatalog({ pgClient: catalogPg({ emailRows: [] }) });
  assert.equal(catalog.channels.email.templates[0].source, "builtin");
  assert(catalog.channels.email.templates.some((item) => item.template_key === "GENERIC_ADMIN_EMAIL"));
});

function previewPg() {
  return fakePg((sql, params) => {
    if (sql.includes("FROM public.users")) {
      return rowResult([
        { id: 1, name: "A", email: "a@example.com", phone: "21999999999" },
        { id: 2, name: "B", email: "invalid", phone: "" },
      ].filter((row) => !Array.isArray(params[0]) || params[0].includes(row.id)));
    }
    if (sql.includes("FROM public.push_subscriptions") && sql.includes("COUNT")) {
      return rowResult([{ count: 1 }]);
    }
    if (sql.includes("FROM public.push_subscriptions")) {
      return rowResult([
        { id: "s1", user_id: 1, endpoint: "https://push/1", p256dh: "p", auth: "a" },
        { id: "s2", user_id: 1, endpoint: "https://push/2", p256dh: "p", auth: "a" },
      ]);
    }
    if (sql.includes("notification_push_rules")) {
      return rowResult([{ event_key: "DRAW_REMAINING_NUMBERS_50", title_template: "Restam {{remaining}}", body_template: "Corre" }]);
    }
    if (sql.includes("communication_consents")) {
      return rowResult([{ user_id: 1, channel: "whatsapp", category: "manual", status: "granted", source: "test", created_at: new Date() }]);
    }
    if (sql.includes("notification_templates") && sql.includes("channel = 'email'")) {
      return rowResult([]);
    }
    if (sql.includes("notification_templates") && sql.includes("channel = 'whatsapp'")) {
      return rowResult([{ template_key: "GENERIC_TEST", provider_template_id: "3", default_message: "Teste" }]);
    }
    return rowResult([]);
  });
}

test("preview push selected calculates users and devices without provider or dispatch", async () => {
  const pgClient = previewPg();
  const preview = await buildManualNotificationPreview({
    pgClient,
    payload: {
      channel: "push",
      template_key: "DRAW_REMAINING_NUMBERS_50",
      audience: "selected",
      user_ids: [1, 2],
      params: { remaining: 50 },
    },
  });

  assert.equal(preview.eligible_users, 1);
  assert.equal(preview.eligible_devices, 2);
  assert.equal(preview.inactive_subscriptions, 1);
  assert.equal(pgClient.calls.some((call) => call.sql.includes("INSERT INTO public.notification_dispatches")), false);
  assert.equal(pgClient.calls.some((call) => call.sql.includes("notification_campaigns")), false);
});

test("preview all_active_push calculates unique users and requires confirmation", async () => {
  const preview = await buildManualNotificationPreview({
    pgClient: previewPg(),
    payload: {
      channel: "push",
      template_key: "DRAW_REMAINING_NUMBERS_50",
      audience: "all_active_push",
      title: "Oi",
      message: "Mensagem",
    },
  });

  assert.equal(preview.eligible_users, 1);
  assert.equal(preview.eligible_devices, 2);
  assert.equal(preview.requires_bulk_confirmation, true);
});

test("preview whatsapp identifies phone and consent", async () => {
  const preview = await buildManualNotificationPreview({
    pgClient: previewPg(),
    payload: { channel: "whatsapp", template_key: "GENERIC_TEST", audience: "selected", user_ids: [1, 2] },
  });
  assert.equal(preview.valid_phones, 1);
  assert.equal(preview.missing_contact, 1);
});

test("preview email validates email addresses and removes invalid contacts", async () => {
  const preview = await buildManualNotificationPreview({
    pgClient: previewPg(),
    payload: { channel: "email", template_key: "GENERIC_ADMIN_EMAIL", audience: "selected", user_ids: [1, 2] },
  });
  assert.equal(preview.valid_emails, 1);
  assert.equal(preview.missing_contact, 1);
});

function dispatchPg() {
  let campaignId = 10;
  let dispatchId = 20;
  return fakePg((sql, params) => {
    if (sql.includes("FROM public.users")) {
      return rowResult([
        { id: 1, name: "A", email: "a@example.com", phone: "21999999999" },
        { id: 2, name: "B", email: "a@example.com", phone: "21988888888" },
      ].filter((row) => !Array.isArray(params[0]) || params[0].includes(row.id)));
    }
    if (sql.includes("FROM public.push_subscriptions") && sql.includes("COUNT")) return rowResult([{ count: 0 }]);
    if (sql.includes("FROM public.push_subscriptions")) {
      return rowResult([
        { id: "s1", user_id: 1, endpoint: "https://push/1", p256dh: "p", auth: "a" },
        { id: "s2", user_id: 1, endpoint: "https://push/2", p256dh: "p", auth: "a" },
      ]);
    }
    if (sql.includes("notification_push_rules")) return rowResult([{ event_key: "PUSH_TEMPLATE", title_template: "T", body_template: "B" }]);
    if (sql.includes("notification_templates") && sql.includes("channel = 'email'")) return rowResult([]);
    if (sql.includes("INSERT INTO public.notification_campaigns")) return rowResult([{ id: campaignId++, status: "created" }]);
    if (sql.includes("INSERT INTO public.notification_dispatches")) return rowResult([{ id: dispatchId++, status: "pending" }]);
    if (sql.includes("UPDATE public.notification_dispatches")) return rowResult([{ id: params[0], status: sql.includes("status = 'accepted'") ? "accepted" : "failed" }]);
    if (sql.includes("UPDATE public.notification_campaigns")) return rowResult([{ id: params[0], status: "created" }]);
    return rowResult([]);
  });
}

test("manual push sends one dispatch per eligible subscription and does not call automation ledger", async () => {
  const pgClient = dispatchPg();
  const sent = [];
  const result = await sendManualPushNotification({
    pgClient,
    adminUserId: 99,
    payload: {
      channel: "push",
      audience: "selected",
      user_ids: [1],
      template_key: "PUSH_TEMPLATE",
      title: "Titulo",
      message: "Mensagem",
      url: "/",
    },
    sendPush: async (args) => {
      sent.push(args.subscriptionRow.id);
      return { ok: true, dispatch: { id: `pd-${args.subscriptionRow.id}` } };
    },
  });

  assert.equal(result.sent, 2);
  assert.deepEqual(sent, ["s1", "s2"]);
  assert.equal(pgClient.calls.some((call) => call.sql.includes("notification_event_ledger")), false);
});

test("manual push all_active_push requires confirmation before sending", async () => {
  const result = await sendManualPushNotification({
    pgClient: dispatchPg(),
    payload: {
      channel: "push",
      audience: "all_active_push",
      template_key: "PUSH_TEMPLATE",
      title: "Titulo",
      message: "Mensagem",
      url: "/",
    },
    sendPush: async () => {
      throw new Error("should_not_send");
    },
  });
  assert.equal(result.error, "manual_bulk_confirmation_required");
});

test("manual push device failure does not stop remaining devices", async () => {
  let count = 0;
  const result = await sendManualPushNotification({
    pgClient: dispatchPg(),
    payload: {
      channel: "push",
      audience: "selected",
      user_ids: [1],
      template_key: "PUSH_TEMPLATE",
      title: "Titulo",
      message: "Mensagem",
      url: "/",
    },
    sendPush: async () => {
      count += 1;
      if (count === 1) {
        const error = new Error("gone");
        error.code = "push_subscription_gone_or_expired";
        error.provider_status = 410;
        throw error;
      }
      return { ok: true, dispatch: { id: "pd-ok" } };
    },
  });
  assert.equal(result.failed, 1);
  assert.equal(result.sent, 1);
});

test("manual email sends individually, dedupes duplicated email and uses no real smtp", async () => {
  const sent = [];
  const result = await sendManualEmailNotification({
    pgClient: dispatchPg(),
    adminUserId: 99,
    payload: {
      channel: "email",
      audience: "selected",
      user_ids: [1, 2],
      template_key: "GENERIC_ADMIN_EMAIL",
      subject: "Assunto",
      text: "Texto",
      html: "<p>Texto</p>",
    },
    transporter: {
      async sendMail(message) {
        sent.push(message.to);
        return { messageId: "smtp-1", accepted: [message.to] };
      },
    },
  });

  assert.equal(result.sent, 1);
  assert.deepEqual(sent, ["a@example.com"]);
});

test("manual services do not import automation event handlers", () => {
  const manualPush = fs.readFileSync("src/services/notifications/manualPushNotifications.js", "utf8");
  const manualEmail = fs.readFileSync("src/services/notifications/manualEmailNotifications.js", "utf8");
  const manualPreview = fs.readFileSync("src/services/notifications/manualNotificationPreview.js", "utf8");
  assert.equal(/handlePushAutomationEvent|notification_event_ledger|whatsappAutomationEvents/.test(manualPush), false);
  assert.equal(/handlePushAutomationEvent|notification_event_ledger|whatsappAutomationEvents/.test(manualEmail), false);
  assert.equal(/handlePushAutomationEvent|notification_event_ledger|whatsappAutomationEvents/.test(manualPreview), false);
});
