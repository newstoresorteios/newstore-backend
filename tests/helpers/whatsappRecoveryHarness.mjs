import fs from "node:fs";
import vm from "node:vm";

const rows = [];
let attempts = 0;
let fail = true;
const query = async (sql, p = []) => {
  if (sql.includes("FROM public.users")) return { rows: [{ id: 11, name: "Ana", phone: "5511999999999" }] };
  if (sql.includes("FROM public.notification_dispatches")) {
    const filtered = rows.filter((r) => r.userId === p[1] && r.payload.reference_key === p[2]);
    return { rows: sql.includes("status NOT IN") ? filtered.filter((r) => !["failed", "skipped"].includes(r.status)) : filtered };
  }
  if (sql.includes("notification_templates")) return { rows: [] };
  throw new Error(`Unexpected SQL ${sql}`);
};
const template = { template_key: "BALANCE_EXPIRING_15_DAYS", env_names: ["LOCAL_TEST_TEMPLATE_ID"], approved_default_template_id: 1 };
const deps = {
  "../../db.js": { query },
  "./brevoWhatsApp.js": { normalizePhoneBR: (phone) => phone, sendBrevoWhatsAppTemplate: async () => {
    attempts++; return fail ? { ok: false, error: "provider_unavailable" } : { ok: true, provider_status: "accepted" };
  } },
  "./communicationConsent.js": { assertWhatsAppConsent: async () => ({ ok: true }) },
  "./notificationLog.js": {
    createDispatch: async (args) => { const row = { ...args, id: rows.length + 1, status: "pending" }; rows.push(row); return row; },
    markDispatchAccepted: async ({ dispatchId }) => { rows.find((r) => r.id === dispatchId).status = "accepted"; },
    markDispatchFailed: async ({ dispatchId, status = "failed" }) => { rows.find((r) => r.id === dispatchId).status = status; },
  },
  "./manualWhatsAppTemplates.js": { BACKEND_BREVO_WHATSAPP_TEMPLATES: { DRAW_REMAINING_NUMBERS_50: template, DRAW_REMAINING_NUMBERS_10: template, BALANCE_EXPIRING_15_DAYS: template } },
  "../../config/publicSiteUrl.js": { resolvePublicSiteUrl: () => "https://example.test" },
};
const context = vm.createContext({ process, console, Set, Number, String, JSON });
const module = new vm.SourceTextModule(fs.readFileSync(new URL("../../src/services/notifications/whatsappAutomationEvents.js", import.meta.url), "utf8"), { context });
await module.link(async (name) => {
  const values = deps[name];
  return new vm.SyntheticModule(Object.keys(values), function () { for (const [k, v] of Object.entries(values)) this.setExport(k, v); }, { context });
});
await module.evaluate();
process.env.NOTIFICATION_WHATSAPP_AUTOMATION_ENABLED = "true";
const event = { eventKey: "BALANCE_EXPIRING_15_DAYS", referenceKey: "user:11:balance:15", recipientUserIds: [11], metadata: { balance_cents: 5000 }, dryRun: false };
const first = await module.namespace.handleWhatsAppAutomationEvent(event);
fail = false;
const second = await module.namespace.handleWhatsAppAutomationEvent(event);
const third = await module.namespace.handleWhatsAppAutomationEvent(event);
process.stdout.write(JSON.stringify({ first, second, third, attempts }));
