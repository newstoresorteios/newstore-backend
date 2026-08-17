import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

import { createPrincipalDrawConfigHandler } from "../src/routes/admin_dashboard.js";

const VALID_CONFIG = {
  ticket_price_cents: 5500,
  banner_title: "Texto promocional",
  max_numbers_per_selection: 5,
};

function responseRecorder() {
  return {
    statusCode: 200,
    body: null,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(body) {
      this.body = body;
      return this;
    },
  };
}

function createHarness(options = {}) {
  const calls = [];
  const draw = options.draw === undefined
    ? { id: 135, status: "open", draw_type: "principal" }
    : options.draw;
  let globalConfig = {
    ticket_price_cents: String(options.currentPrice ?? 5500),
    banner_title: "Configuração anterior",
    max_numbers_per_selection: "4",
  };
  let individualConfig = null;

  const client = {
    async query(sql, params = []) {
      const compact = String(sql).replace(/\s+/g, " ").trim();
      calls.push({ sql: compact, params });
      if (compact === "BEGIN" || compact === "COMMIT" || compact === "ROLLBACK") {
        return { rowCount: null, rows: [] };
      }
      if (compact.includes("FROM public.draws") && compact.includes("FOR UPDATE")) {
        return { rowCount: draw ? 1 : 0, rows: draw ? [draw] : [] };
      }
      if (compact.includes("WHERE key = 'ticket_price_cents'")) {
        return { rowCount: 1, rows: [{ value: String(options.currentPrice ?? 5500) }] };
      }
      if (compact.includes("AS has_payment")) {
        const active = Boolean(options.activity);
        return {
          rowCount: 1,
          rows: [{
            has_payment: active,
            has_active_reservation: false,
            has_sold_or_reserved_number: false,
            has_preauthorization: false,
            has_autopay_run: false,
          }],
        };
      }
      if (compact.startsWith("INSERT INTO public.app_config ")) {
        if (options.failOn === "global") throw new Error("global_write_failed");
        const [keys, values] = params;
        globalConfig = Object.fromEntries(keys.map((key, index) => [key, values[index]]));
        return { rowCount: 3, rows: [] };
      }
      if (compact.startsWith("INSERT INTO public.app_config_new")) {
        if (options.failOn === "individual") throw new Error("individual_write_failed");
        individualConfig = {
          banner_title: params[1],
          ticket_price_cents: params[2],
          max_numbers_per_selection: params[3],
        };
        return { rowCount: 1, rows: [] };
      }
      if (compact.includes("SELECT key, value") && compact.includes("FROM public.app_config")) {
        const rows = Object.entries(globalConfig).map(([key, value]) => ({ key, value }));
        return { rowCount: rows.length, rows };
      }
      if (compact.includes("FROM public.app_config_new") && compact.includes("LIMIT 1")) {
        const row = options.mismatch
          ? { ...individualConfig, banner_title: "Divergente" }
          : individualConfig;
        return { rowCount: row ? 1 : 0, rows: row ? [row] : [] };
      }
      throw new Error(`unexpected_query:${compact}`);
    },
    release() {},
  };
  const getPoolFn = async () => ({ connect: async () => client });
  return { calls, getPoolFn };
}

async function invoke(options = {}, body = VALID_CONFIG) {
  const harness = createHarness(options);
  const handler = createPrincipalDrawConfigHandler({ getPoolFn: harness.getPoolFn });
  const req = { params: { drawId: String(options.drawId ?? 135) }, body };
  const res = responseRecorder();
  await handler(req, res);
  return { ...harness, res };
}

function writeCalls(calls) {
  return calls.filter((call) => /^(?:INSERT|UPDATE|DELETE)\b/i.test(call.sql));
}

test("atualiza principal existente e sincroniza app_config com app_config_new", async () => {
  const { calls, res } = await invoke();
  assert.equal(res.statusCode, 200);
  assert.deepEqual(res.body, {
    ok: true,
    draw: { id: 135, status: "open", draw_type: "principal" },
    config: VALID_CONFIG,
    sync: { global: true, draw: true },
  });
  const globalWrite = calls.find((call) => call.sql.startsWith("INSERT INTO public.app_config "));
  const drawWrite = calls.find((call) => call.sql.startsWith("INSERT INTO public.app_config_new"));
  assert.deepEqual(globalWrite.params[1], ["5500", "Texto promocional", "5"]);
  assert.deepEqual(drawWrite.params, ["135", "Texto promocional", 5500, 5]);
  assert.equal(calls.at(-1).sql, "COMMIT");
});

test("principal legado com draw_type null é aceito", async () => {
  const { res } = await invoke({ draw: { id: 135, status: "closed", draw_type: null } });
  assert.equal(res.statusCode, 200);
  assert.equal(res.body.draw.draw_type, "principal");
  assert.equal(res.body.draw.status, "closed");
});

test("draw adicional é rejeitado e draw inexistente retorna 404", async () => {
  const additional = await invoke({ draw: { id: 135, status: "open", draw_type: "adicional" } });
  assert.equal(additional.res.statusCode, 400);
  assert.deepEqual(additional.res.body, { error: "principal_draw_required" });
  assert.equal(writeCalls(additional.calls).length, 0);
  assert.equal(additional.calls.at(-1).sql, "ROLLBACK");

  const missing = await invoke({ draw: null });
  assert.equal(missing.res.statusCode, 404);
  assert.deepEqual(missing.res.body, { error: "draw_not_found" });
  assert.equal(writeCalls(missing.calls).length, 0);
  assert.equal(missing.calls.at(-1).sql, "ROLLBACK");
});

test("valida os três campos antes de abrir transação ou escrever", async () => {
  const invalidPayloads = [
    { banner_title: "x", max_numbers_per_selection: 5 },
    { ...VALID_CONFIG, ticket_price_cents: 0 },
    { ...VALID_CONFIG, banner_title: undefined },
    { ...VALID_CONFIG, banner_title: "x".repeat(256) },
    { ...VALID_CONFIG, max_numbers_per_selection: 0 },
  ];
  for (const payload of invalidPayloads) {
    const { calls, res } = await invoke({}, payload);
    assert.equal(res.statusCode, 400);
    assert.equal(calls.length, 0);
  }
});

test("falha na escrita global ou individual realiza rollback", async () => {
  const globalFailure = await invoke({ failOn: "global" });
  assert.equal(globalFailure.res.statusCode, 500);
  assert.equal(globalFailure.calls.at(-1).sql, "ROLLBACK");
  assert.equal(globalFailure.calls.some((call) => call.sql.startsWith("INSERT INTO public.app_config_new")), false);

  const individualFailure = await invoke({ failOn: "individual" });
  assert.equal(individualFailure.res.statusCode, 500);
  assert.equal(individualFailure.calls.at(-1).sql, "ROLLBACK");
});

test("divergência após a escrita realiza rollback com erro específico", async () => {
  const { calls, res } = await invoke({ mismatch: true });
  assert.equal(res.statusCode, 500);
  assert.deepEqual(res.body, { error: "draw_config_sync_failed" });
  assert.equal(calls.at(-1).sql, "ROLLBACK");
  assert.equal(calls.some((call) => call.sql === "COMMIT"), false);
});

test("preço diferente é bloqueado quando existe atividade", async () => {
  const { calls, res } = await invoke({ currentPrice: 5000, activity: true });
  assert.equal(res.statusCode, 409);
  assert.equal(res.body.error, "draw_ticket_price_locked");
  assert.equal(writeCalls(calls).length, 0);
  assert.equal(calls.at(-1).sql, "ROLLBACK");
});

test("preço igual não é bloqueado e permite alterar frase ou limite", async () => {
  const phrase = await invoke(
    { currentPrice: 5500, activity: true },
    { ...VALID_CONFIG, banner_title: " Nova frase " }
  );
  assert.equal(phrase.res.statusCode, 200);
  assert.equal(phrase.res.body.config.banner_title, "Nova frase");
  assert.equal(phrase.calls.some((call) => call.sql.includes("AS has_payment")), false);

  const limit = await invoke(
    { currentPrice: 5500, activity: true },
    { ...VALID_CONFIG, max_numbers_per_selection: 9 }
  );
  assert.equal(limit.res.statusCode, 200);
  assert.equal(limit.res.body.config.max_numbers_per_selection, 9);
});

test("endpoint não altera draws, números ou reservas e não dispara integrações", async () => {
  const { calls, res } = await invoke();
  assert.equal(res.statusCode, 200);
  const sql = calls.map((call) => call.sql).join("\n");
  assert.doesNotMatch(sql, /\b(?:INSERT|UPDATE|DELETE)\s+(?:INTO\s+|FROM\s+)?public\.draws\b/i);
  assert.doesNotMatch(sql, /\b(?:INSERT|UPDATE|DELETE)\s+(?:INTO\s+|FROM\s+)?public\.numbers\b/i);
  assert.doesNotMatch(sql, /\b(?:INSERT|UPDATE|DELETE)\s+(?:INTO\s+|FROM\s+)?public\.reservations\b/i);
  assert.doesNotMatch(sql, /INSERT INTO public\.payments/i);

  const handlerSource = createPrincipalDrawConfigHandler.toString();
  assert.doesNotMatch(handlerSource, /runAutopay|createCaptive|Mercado|Vindi|notification|handlePush/i);

  const routeSource = await readFile(
    new URL("../src/routes/admin_dashboard.js", import.meta.url),
    "utf8"
  );
  assert.match(
    routeSource,
    /router\.patch\(\s*"\/draws\/:drawId\/config",\s*requireAuth,\s*requireAdmin,\s*createPrincipalDrawConfigHandler\(\)/
  );
});
