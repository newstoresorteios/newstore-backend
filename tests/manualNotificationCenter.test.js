import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

import { getManualNotificationCatalog } from "../src/services/notifications/manualNotificationCatalog.js";
import { buildManualNotificationPreview } from "../src/services/notifications/manualNotificationPreview.js";
import { sendManualEmailNotification } from "../src/services/notifications/manualEmailNotifications.js";
import { sendManualPushNotification } from "../src/services/notifications/manualPushNotifications.js";
import {
  manualSendSelected,
  resolveTemplateId,
} from "../src/services/notifications/notificationCenter.js";
import { normalizeProviderTemplateId } from "../src/services/notifications/manualWhatsAppTemplates.js";

const testEnv = {
  BREVO_WHATSAPP_GENERIC_TEST_TEMPLATE_ID: "3",
  CAPTIVE_PREAUTH_BREVO_TEMPLATE_ID: "24",
  BREVO_WHATSAPP_DRAW_REMAINING_50_TEMPLATE_ID: "25",
  BREVO_WHATSAPP_DRAW_REMAINING_10_TEMPLATE_ID: "26",
  BREVO_WHATSAPP_BALANCE_EXPIRING_15_TEMPLATE_ID: "27",
  NOTIFICATION_TEST_MODE: "true",
  NOTIFICATION_TEST_WHATSAPP_TO: "5521999999999",
};
const originalEnv = Object.fromEntries(
  Object.keys(testEnv).map((name) => [name, process.env[name]])
);
Object.assign(process.env, testEnv);
test.after(() => {
  for (const [name, value] of Object.entries(originalEnv)) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
});

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

function catalogPg({
  emailRows = [],
  whatsappRows = [{
    id: "w1",
    template_key: "DRAW_REMAINING_NUMBERS_50",
    provider_template_id: "25",
    name: "50 restantes",
    is_active: true,
  }],
  pushRows = [{
    id: "p1",
    event_key: "DRAW_REMAINING_NUMBERS_50",
    title_template: "Restam {{remaining}}",
    body_template: "Poucos numeros",
  }],
} = {}) {
  return fakePg((sql, params) => {
    if (sql.includes("information_schema.columns")) {
      const table = params[0];
      return rowResult((table === "notification_push_rules" ? pushRuleColumns : templateColumns).map((column_name) => ({ column_name })));
    }
    if (sql.includes("channel = 'whatsapp'")) {
      return rowResult(whatsappRows);
    }
    if (sql.includes("notification_push_rules")) {
      return rowResult(pushRows);
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
  const whatsappByKey = Object.fromEntries(
    catalog.channels.whatsapp.templates.map((item) => [item.template_key, item])
  );

  assert.equal(catalog.ok, true);
  assert.equal(whatsappByKey.GENERIC_TEST.provider_template_id, 3);
  assert.equal(whatsappByKey.CAPTIVE_PREAUTH_REQUEST.provider_template_id, 24);
  assert.equal(whatsappByKey.DRAW_REMAINING_NUMBERS_50.provider_template_id, 25);
  assert.equal(whatsappByKey.DRAW_REMAINING_NUMBERS_10.provider_template_id, 26);
  assert.equal(whatsappByKey.BALANCE_EXPIRING_15_DAYS.provider_template_id, 27);
  assert.equal(whatsappByKey.CAPTIVE_PREAUTH_REQUEST.manual_send_allowed, false);
  assert.equal(
    whatsappByKey.CAPTIVE_PREAUTH_REQUEST.manual_send_block_reason,
    "system_only_template"
  );
  assert.equal(catalog.channels.whatsapp.connected_templates_count, 5);
  assert.equal(catalog.channels.whatsapp.manual_templates_count, 4);
  assert.equal(catalog.channels.push.templates[0].event_key, "DRAW_REMAINING_NUMBERS_50");
  assert.equal(catalog.channels.email.templates[0].template_key, "EMAIL_DB");
  assert.equal(pgClient.calls.some((call) => /https?:\/\//i.test(call.sql)), false);
});

test("catalog excludes WhatsApp placeholders and keeps DRAW_REMAINING_NUMBERS_75 only in Push", async () => {
  const catalog = await getManualNotificationCatalog({
    pgClient: catalogPg({
      whatsappRows: [
        {
          id: "future",
          template_key: "DRAW_REMAINING_NUMBERS_75",
          provider_template_id: null,
          name: "Sorteio 75% vendido",
          is_active: true,
        },
        {
          id: "placeholder",
          template_key: "FUTURE_WHATSAPP",
          provider_template_id: "ID não informado",
          is_active: true,
        },
        {
          id: "inactive",
          template_key: "INACTIVE_WHATSAPP",
          provider_template_id: "888",
          is_active: false,
        },
      ],
      pushRows: [{
        id: "p75",
        event_key: "DRAW_REMAINING_NUMBERS_75",
        name: "Sorteio 75% vendido",
        title_template: "75% vendido",
        body_template: "Continue participando",
      }],
    }),
  });

  assert.equal(
    catalog.channels.whatsapp.templates.some(
      (item) => item.template_key === "DRAW_REMAINING_NUMBERS_75" || item.template_key === "FUTURE_WHATSAPP"
        || item.template_key === "INACTIVE_WHATSAPP"
    ),
    false
  );
  assert.equal(
    catalog.channels.push.templates.some((item) => item.event_key === "DRAW_REMAINING_NUMBERS_75"),
    true
  );
});

test("provider template id accepts only positive integers", () => {
  assert.equal(normalizeProviderTemplateId(3), 3);
  assert.equal(normalizeProviderTemplateId("24"), 24);
  for (const invalid of [null, undefined, "", 0, "0", NaN, "NaN", "ID não informado", 1.5]) {
    assert.equal(normalizeProviderTemplateId(invalid), null);
  }
});

test("database template wins over env and is deduplicated by key and provider id", async () => {
  const catalog = await getManualNotificationCatalog({
    pgClient: catalogPg({
      whatsappRows: [
        {
          id: "sync-999-a",
          template_key: "DRAW_REMAINING_NUMBERS_50",
          provider_template_id: "999",
          name: "Nome sincronizado",
          language: "pt_PT",
          provider_status: "approved",
          is_active: true,
        },
        {
          id: "sync-999-b",
          template_key: "DRAW_REMAINING_NUMBERS_50",
          provider_template_id: 999,
          name: "Duplicado",
          is_active: true,
        },
      ],
    }),
  });
  const matches = catalog.channels.whatsapp.templates.filter(
    (item) => item.template_key === "DRAW_REMAINING_NUMBERS_50"
  );

  assert.equal(matches.length, 1);
  assert.equal(matches[0].provider_template_id, 999);
  assert.equal(matches[0].name, "Nome sincronizado");
  assert.equal(matches[0].language, "pt_PT");
  assert.equal(matches[0].provider_status, "approved");
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
  const pgClient = previewPg();
  const preview = await buildManualNotificationPreview({
    pgClient,
    payload: { channel: "whatsapp", template_key: "GENERIC_TEST", audience: "selected", user_ids: [1, 2] },
  });
  assert.equal(preview.valid_phones, 1);
  assert.equal(preview.missing_contact, 1);
  assert.equal(preview.template.provider_template_id, 3);

  const sendResolverId = await resolveTemplateId({
    pgClient,
    templateKey: "GENERIC_TEST",
    channel: "whatsapp",
    provider: "brevo",
    explicitTemplateId: 9999,
  });
  assert.equal(sendResolverId, preview.template.provider_template_id);
});

test("preview rejects unknown and system-only WhatsApp templates", async () => {
  await assert.rejects(
    buildManualNotificationPreview({
      pgClient: previewPg(),
      payload: { channel: "whatsapp", template_key: "DRAW_REMAINING_NUMBERS_75", audience: "selected", user_ids: [1] },
    }),
    (error) => error?.code === "manual_template_not_found"
  );
  await assert.rejects(
    buildManualNotificationPreview({
      pgClient: previewPg(),
      payload: { channel: "whatsapp", template_key: "CAPTIVE_PREAUTH_REQUEST", audience: "selected", user_ids: [1] },
    }),
    (error) => error?.code === "manual_template_not_allowed"
  );
});

test("manual WhatsApp send rejects unknown template before dispatch or Brevo", async () => {
  const pgClient = previewPg();
  await assert.rejects(
    manualSendSelected({
      pgClient,
      channel: "whatsapp",
      provider: "brevo",
      templateKey: "DRAW_REMAINING_NUMBERS_75",
      templateId: 9999,
      recipients: [{ user_id: 1 }],
      dryRun: false,
    }),
    (error) => error?.code === "manual_template_not_found"
  );
  await assert.rejects(
    manualSendSelected({
      pgClient,
      channel: "whatsapp",
      provider: "brevo",
      templateKey: "CAPTIVE_PREAUTH_REQUEST",
      recipients: [{ user_id: 1 }],
      dryRun: false,
    }),
    (error) => error?.code === "manual_template_not_allowed"
  );
  assert.equal(
    pgClient.calls.some((call) => call.sql.includes("INSERT INTO public.notification_dispatches")),
    false
  );
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
