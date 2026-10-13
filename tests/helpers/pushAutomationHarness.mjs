import fs from "node:fs";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

export async function world() {
  const state = { ledger: new Map(), sent: new Set(), deliveries: [], fails: new Set(), seq: 0, clock: Date.now() };
  class ClockDate extends Date {
    constructor(...args) { super(...(args.length ? args : [state.clock])); }
    static now() { return state.clock; }
  }
  const query = async (sql, p = []) => {
    if (sql.includes("INSERT INTO public.notification_event_ledger")) {
      if (state.ledger.has(p[1])) return { rowCount: 0, rows: [] };
      const row = { id: `L${++state.seq}`, event_key: p[0], dedupe_key: p[1], status: "processing", mode: p[4], meta: JSON.parse(p[5]), created_at: new Date().toISOString() };
      state.ledger.set(p[1], row);
      return { rowCount: 1, rows: [{ ...row }] };
    }
    if (sql.includes("FROM public.notification_event_ledger")) {
      const row = state.ledger.get(p[0]);
      return { rows: row ? [{ ...row, meta: { ...row.meta } }] : [] };
    }
    if (sql.includes("UPDATE public.notification_event_ledger")) {
      const row = [...state.ledger.values()].find((r) => r.id === p[0]);
      if (!row) return { rowCount: 0, rows: [] };
      if (sql.includes("SET meta =")) {
        if (row.meta.claim_id !== p[1]) return { rowCount: 0, rows: [] };
        row.meta.processing_started_at = new Date(state.clock).toISOString();
        return { rowCount: 1, rows: [] };
      }
      if (sql.includes("SET status = 'processing'")) {
        if (sql.includes("status = $4") && (row.status !== p[3] || JSON.stringify(row.meta) !== p[4])) return { rowCount: 0, rows: [] };
        row.status = "processing"; row.mode = p[1]; row.meta = JSON.parse(p[2]);
      } else {
        if (sql.includes("claim_id") && row.meta.claim_id !== p[5]) return { rowCount: 0, rows: [] };
        row.status = p[1]; row.meta = { ...row.meta, ...JSON.parse(p[4]) };
      }
      return { rowCount: 1, rows: [{ ...row }] };
    }
    if (sql.includes("push_subscriptions")) {
      const rows = [{ id: "S1", user_id: 11 }, { id: "S2", user_id: 22 }];
      return { rows: sql.includes("NOT EXISTS") ? rows.filter((r) => !state.sent.has(r.id)) : rows };
    }
    if (sql.includes("COUNT(DISTINCT")) return { rows: [{ count: 0 }] };
    if (sql.includes("SELECT id") && sql.includes("notification_push_dispatches")) return { rows: state.sent.size ? [{ id: "already-sent" }] : [] };
    if (sql.includes("INSERT INTO public.notification_push_dispatches")) return { rowCount: 1, rows: [{ id: "D1" }] };
    throw new Error(`Unexpected SQL: ${sql}`);
  };
  const context = vm.createContext({ console: { log() {}, warn() {} }, process, Date: ClockDate, Set, Map, JSON,
    setInterval: (callback) => { state.heartbeat = callback; return { unref() {} }; },
    clearInterval: () => { state.heartbeat = null; },
  });
  const values = {
    "../../db.js": { query },
    "./pushRules.js": { getActivePushRuleByEventKey: async () => ({ title_template: "Resultado", body_template: "Disponível", url_template: "/", category: "operational" }), getAllowedPushRuleEvents: () => ["WINNER_DEFINED"] },
    "./pushNotifications.js": { sendPushToSubscriptionRow: async ({ subscriptionRow }) => {
      if (state.beforeSend) await state.beforeSend(subscriptionRow);
      state.deliveries.push(subscriptionRow.id);
      if (state.fails.has(subscriptionRow.id)) throw new Error("provider_unavailable");
      state.sent.add(subscriptionRow.id);
      return { ok: true };
    } },
    "./whatsappAutomationEvents.js": { handleWhatsAppAutomationEvent: async () => state.whatsappResult || ({ ok: true, status: "skipped", reason: "whatsapp_event_not_configured" }) },
    "node:crypto": { randomUUID: () => `claim-${++state.seq}` },
  };
  const source = fs.readFileSync(new URL("../../src/services/notifications/pushAutomationEvents.js", import.meta.url), "utf8");
  const module = new vm.SourceTextModule(source, { context });
  await module.link(async (name) => {
    const deps = values[name];
    if (!deps) throw new Error(`Unexpected import ${name}`);
    return new vm.SyntheticModule(Object.keys(deps), function () { for (const [k, v] of Object.entries(deps)) this.setExport(k, v); }, { context });
  });
  await module.evaluate();
  Object.assign(process.env, {
    PUSH_ENABLED: "true", PUSH_MODE: "production", PUSH_TEST_ONLY: "false", PUSH_ALLOW_PRODUCTION_SEND: "true",
    PUSH_ENGINE_ALLOW_REAL_SEND: "true", PUSH_ALLOW_ENGINE_EVENTS: "true", PUSH_ENGINE_REAL_SEND_EVENT_KEYS: "WINNER_DEFINED",
    PUSH_ENGINE_REQUIRE_OCCURRED_AT: "true", PUSH_ENGINE_REQUIRE_SCAN_ID: "true", PUSH_AUTOMATION_REQUIRE_REFERENCE_KEY: "true",
    PUSH_ALLOW_AUDIENCE: "false", PUSH_ALLOW_ADMIN_MASS_SEND: "false", PUSH_ALLOW_CAMPAIGNS: "false",
  });
  const event = { eventKey: "WINNER_DEFINED", referenceType: "draw", referenceKey: "draw:149:winner_defined", metadata: { draw_id: 149, winner_number: 0 }, occurredAt: new Date().toISOString(), scanId: "push-scan:test", dryRun: false };
  return { ...state, state, event, handle: (overrides = {}) => module.namespace.handlePushAutomationEvent({ ...event, ...overrides }), seed: (status, extra = {}) => {
    state.ledger.set("push:WINNER_DEFINED:draw:149:winner_defined", { id: "old", event_key: "WINNER_DEFINED", dedupe_key: "push:WINNER_DEFINED:draw:149:winner_defined", status, mode: "production", meta: {}, created_at: new Date().toISOString(), ...extra });
  } };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const w = await world();
  const scenario = process.argv[2];
  let result;
  if (scenario === "rejected") { const first = await w.handle({ occurredAt: null }); result = { first, ledgerSize: w.ledger.size, second: await w.handle(), deliveries: w.deliveries }; }
  if (scenario === "preview") { await w.handle({ dryRun: true }); result = { second: await w.handle(), deliveries: w.deliveries }; }
  if (scenario === "partial") { w.fails.add("S2"); const first = await w.handle(); w.fails.clear(); const second = await w.handle(); result = { first, second, deliveries: w.deliveries }; }
  if (scenario === "skipped") { w.seed("skipped"); result = { response: await w.handle(), deliveries: w.deliveries }; }
  if (scenario === "stale") { w.seed("processing", { created_at: new Date(Date.now() - 3600000).toISOString() }); result = { response: await w.handle(), deliveries: w.deliveries }; }
  if (scenario === "fresh") { w.seed("processing"); result = { response: await w.handle(), deliveries: w.deliveries }; }
  if (scenario === "twice") { await w.handle(); result = { response: await w.handle(), deliveries: w.deliveries }; }
  if (scenario === "old-partial") { w.seed("sent", { meta: { failed: 1 } }); w.sent.add("S1"); result = { response: await w.handle(), deliveries: w.deliveries }; }
  if (scenario === "all-already-sent") {
    w.seed("processing", { created_at: new Date(Date.now() - 3600000).toISOString() }); w.sent.add("S1"); w.sent.add("S2");
    const first = await w.handle(); result = { first, second: await w.handle(), deliveries: w.deliveries };
  }
  if (scenario === "whatsapp-failure") {
    w.state.whatsappResult = { ok: false, status: "failed", failed: 1 };
    const first = await w.handle(); w.state.whatsappResult = { ok: true, status: "accepted", failed: 0 };
    result = { first, second: await w.handle(), deliveries: w.deliveries };
  }
  if (scenario === "lost-lease") {
    w.state.beforeSend = async () => { for (const row of w.ledger.values()) row.meta.claim_id = "other-worker"; };
    try { result = { response: await w.handle(), deliveries: w.deliveries }; }
    catch (error) { result = { error: error.code, deliveries: w.deliveries }; }
  }
  if (scenario === "long-send") {
    let competing;
    w.state.beforeSend = async () => {
      w.state.beforeSend = null;
      w.state.clock += 21 * 60000;
      if (w.state.heartbeat) await w.state.heartbeat();
      competing = await w.handle({ occurredAt: new Date(w.state.clock).toISOString() });
    };
    result = { response: await w.handle(), competing, deliveries: w.deliveries, timerCleared: !w.state.heartbeat };
  }
  process.stdout.write(JSON.stringify(result));
}
