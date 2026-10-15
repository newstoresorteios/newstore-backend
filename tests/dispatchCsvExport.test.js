// tests/dispatchCsvExport.test.js
// Exportacao CSV do historico de disparos: autorizacao, filtros, lotes, escape e protecao de formulas.
import test, { before, after } from "node:test";
import assert from "node:assert/strict";

process.env.JWT_SECRET = process.env.JWT_SECRET || "test-secret-dispatch-csv";

const {
  CSV_COLUMNS,
  buildDispatchBatchQuery,
  createDispatchesCsvHandler,
  csvCell,
  formatBrasiliaDate,
  formatBrasiliaDateTime,
  parseDispatchExportFilters,
} = await import("../src/services/notifications/dispatchCsvExport.js");

const { default: express } = await import("express");
const { default: jwt } = await import("jsonwebtoken");

let servers = [];
async function serve(app) {
  const server = await new Promise((resolve) => {
    const s = app.listen(0, "127.0.0.1", () => resolve(s));
  });
  servers.push(server);
  return `http://127.0.0.1:${server.address().port}`;
}
after(async () => {
  await Promise.all(servers.map((s) => new Promise((resolve) => s.close(resolve))));
});

function tokenFor(payload) {
  return jwt.sign(payload, process.env.JWT_SECRET, { expiresIn: "5m" });
}

// ---- autorizacao (router real) ---------------------------------------------------------------------

let realBase;
before(async () => {
  const { default: adminNotifications } = await import("../src/routes/adminNotifications.js");
  const app = express();
  app.use(express.json());
  app.use("/api/admin/notifications", adminNotifications);
  realBase = await serve(app);
});

test("export.csv exige autenticacao (401 sem token)", async () => {
  const r = await fetch(`${realBase}/api/admin/notifications/dispatches/export.csv`);
  assert.equal(r.status, 401);
  assert.deepEqual(await r.json(), { error: "unauthorized" });
});

test("export.csv exige administrador (403 para usuario comum)", async () => {
  const r = await fetch(`${realBase}/api/admin/notifications/dispatches/export.csv?status=failed`, {
    headers: { Authorization: `Bearer ${tokenFor({ id: 7, role: "user" })}` },
  });
  assert.equal(r.status, 403);
  assert.deepEqual(await r.json(), { error: "forbidden" });
});

test("export.csv esta montado no caminho que o frontend chama (sem duplicar /api)", async () => {
  // Administrador passa pelos guardas: com filtro invalido a rota responde 400 antes de tocar no banco.
  const r = await fetch(`${realBase}/api/admin/notifications/dispatches/export.csv?from=nao-e-data`, {
    headers: { Authorization: `Bearer ${tokenFor({ id: 1, role: "admin" })}` },
  });
  assert.equal(r.status, 400);
  assert.deepEqual(await r.json(), { ok: false, error: "invalid_date", field: "from" });
  const dup = await fetch(`${realBase}/api/api/admin/notifications/dispatches/export.csv`);
  assert.equal(dup.status, 404);
});

// ---- filtros ---------------------------------------------------------------------------------------

test("filtros validos sao normalizados e vazios viram null", () => {
  assert.deepEqual(
    parseDispatchExportFilters({ status: " failed ", channel: "", provider: "brevo", from: "2026-10-01", to: "2026-10-15" }),
    { status: "failed", channel: null, provider: "brevo", from: "2026-10-01", to: "2026-10-15" }
  );
  assert.deepEqual(parseDispatchExportFilters({}), {
    status: null, channel: null, provider: null, from: null, to: null,
  });
  // Mesmo dia em from e to e valido (um dia inteiro).
  assert.equal(parseDispatchExportFilters({ from: "2026-10-15", to: "2026-10-15" }).to, "2026-10-15");
});

test("datas invalidas sao recusadas", () => {
  for (const bad of ["abc", "15/10/2026", "2026-10-15T10:00:00Z", "2026-02-30", "2026-13-01", "2026-00-10", "2026-1-5", "20261015"]) {
    assert.throws(() => parseDispatchExportFilters({ from: bad }), (e) => e.code === "invalid_date" && e.field === "from", bad);
    assert.throws(() => parseDispatchExportFilters({ to: bad }), (e) => e.code === "invalid_date" && e.field === "to", bad);
  }
  assert.equal(parseDispatchExportFilters({ from: "2028-02-29" }).from, "2028-02-29");
  assert.throws(() => parseDispatchExportFilters({ from: "2027-02-29" }), (e) => e.code === "invalid_date");
  assert.throws(() => parseDispatchExportFilters({ from: ["2026-10-01", "2026-10-02"] }), (e) => e.code === "invalid_date");
});

test("periodo invertido e filtros de texto invalidos sao recusados", () => {
  assert.throws(
    () => parseDispatchExportFilters({ from: "2026-10-16", to: "2026-10-15" }),
    (e) => e.code === "invalid_date_range"
  );
  assert.throws(() => parseDispatchExportFilters({ status: ["a", "b"] }), (e) => e.code === "invalid_filter" && e.field === "status");
  assert.throws(() => parseDispatchExportFilters({ channel: { $ne: "x" } }), (e) => e.code === "invalid_filter");
  assert.throws(() => parseDispatchExportFilters({ provider: "x".repeat(101) }), (e) => e.code === "invalid_filter");
});

// ---- consulta --------------------------------------------------------------------------------------

test("consulta e parametrizada: valores de filtro nunca entram no texto SQL", () => {
  const evil = "failed'; DROP TABLE notification_dispatches;--";
  const { text, params } = buildDispatchBatchQuery(
    { status: evil, channel: "email", provider: "brevo_smtp", from: "2026-10-01", to: "2026-10-15" },
    null,
    1000
  );
  assert.ok(!text.includes("DROP TABLE"));
  assert.ok(!text.includes("failed"));
  assert.deepEqual(params, [evil, "email", "brevo_smtp", "2026-10-01", "2026-10-15", 1000]);
  assert.match(text, /status = \$1/);
  assert.match(text, /channel = \$2/);
  assert.match(text, /provider = \$3/);
});

test("periodo usa America/Sao_Paulo e inclui o ultimo dia inteiro", () => {
  const { text } = buildDispatchBatchQuery({ from: "2026-10-01", to: "2026-10-15" }, null, 10);
  assert.match(text, /created_at >= \(\$1::date\)::timestamp AT TIME ZONE 'America\/Sao_Paulo'/);
  // limite superior exclusivo no inicio do dia seguinte
  assert.match(text, /created_at < \(\(\$2::date \+ 1\)::timestamp AT TIME ZONE 'America\/Sao_Paulo'\)/);
});

test("ordena por created_at DESC com desempate por id e nao aplica LIMIT fixo de 50", () => {
  const { text, params } = buildDispatchBatchQuery({}, null, 1000);
  assert.match(text, /ORDER BY created_at DESC, id DESC/);
  assert.deepEqual(params, [1000]);
  const withCursor = buildDispatchBatchQuery({}, { createdAt: "2026-10-15 10:00:00.123456+00", id: "99" }, 1000);
  assert.match(withCursor.text, /\(created_at, id\) < \(\$1::timestamptz, \$2\)/);
  assert.deepEqual(withCursor.params, ["2026-10-15 10:00:00.123456+00", "99", 1000]);
});

test("selecao usa colunas do esquema real e preserva status do provedor e de entrega", () => {
  const { text } = buildDispatchBatchQuery({}, null, 5);
  for (const col of [
    "id", "created_at", "channel", "provider", "event_key", "template_key", "user_id", "recipient",
    "status", "provider_status", "delivery_status", "error_message", "sent_at", "delivery_confirmed_at",
  ]) {
    assert.ok(new RegExp(`\\b${col}\\b`).test(text), col);
  }
  assert.equal(CSV_COLUMNS.length, 15);
});

// ---- escape e formulas -----------------------------------------------------------------------------

test("csvCell escapa aspas, ponto e virgula e quebras de linha", () => {
  assert.equal(csvCell('disse "oi"'), '"disse ""oi"""');
  assert.equal(csvCell("a;b"), '"a;b"');
  assert.equal(csvCell("linha1\nlinha2"), '"linha1\nlinha2"');
  assert.equal(csvCell("linha1\r\nlinha2"), '"linha1\r\nlinha2"');
  assert.equal(csvCell("simples"), "simples");
  assert.equal(csvCell("ação não é erro"), "ação não é erro");
  assert.equal(csvCell(null), "");
  assert.equal(csvCell(undefined), "");
  assert.equal(csvCell(42), "42");
  assert.equal(csvCell("a,b"), "a,b"); // virgula nao e separador neste arquivo
});

test("csvCell protege texto que comecaria como formula", () => {
  assert.equal(csvCell("=HYPERLINK(\"http://x\")"), "\"'=HYPERLINK(\"\"http://x\"\")\"");
  assert.equal(csvCell("+5521999999999"), "'+5521999999999");
  assert.equal(csvCell("-1+1"), "'-1+1");
  assert.equal(csvCell("@SUM(A1)"), "'@SUM(A1)");
  assert.equal(csvCell("\t=1+1"), "'\t=1+1");
  assert.equal(csvCell("  =1+1"), "'  =1+1");
  // numeros nao sao alterados (inclusive negativos)
  assert.equal(csvCell(-5), "-5");
  assert.equal(csvCell("ok = fim"), "ok = fim");
});

test("datas sao exibidas em America/Sao_Paulo", () => {
  assert.equal(formatBrasiliaDateTime(new Date("2026-10-15T02:30:00Z")), "2026-10-14 23:30:00");
  assert.equal(formatBrasiliaDateTime(new Date("2026-10-15T13:00:00Z")), "2026-10-15 10:00:00");
  assert.equal(formatBrasiliaDate(new Date("2026-10-16T02:30:00Z")), "2026-10-15");
  assert.equal(csvCell(new Date("2026-10-15T13:00:00Z")), "2026-10-15 10:00:00");
});

// ---- handler ---------------------------------------------------------------------------------------

function makeRow(i, overrides = {}) {
  // i maior = mais antigo, como na ordenacao DESC
  return {
    id: 1000 - i,
    created_at: new Date(Date.UTC(2026, 9, 15, 13, 0, 0) - i * 1000),
    channel: "email",
    provider: "brevo_smtp",
    event_key: "MANUAL_ADMIN_EMAIL",
    template_key: "NEW_DRAW_EMAIL",
    provider_template_id: null,
    user_id: i + 1,
    recipient: `user${i}@example.com`,
    status: "accepted",
    provider_status: "accepted",
    delivery_status: "unknown",
    error_message: null,
    sent_at: new Date(Date.UTC(2026, 9, 15, 13, 0, 1)),
    delivery_confirmed_at: null,
    _cursor_created_at: `2026-10-15 13:00:00.${String(i).padStart(6, "0")}+00`,
    _cursor_id: String(1000 - i),
    ...overrides,
  };
}

// fetch().text() descarta o BOM; le os bytes para provar que o arquivo realmente comeca com EF BB BF.
async function csvBody(response) {
  const buf = Buffer.from(await response.arrayBuffer());
  assert.deepEqual([...buf.subarray(0, 3)], [0xef, 0xbb, 0xbf], "arquivo deve comecar com BOM UTF-8");
  return buf.subarray(3).toString("utf8");
}

async function appFor(handlerOptions) {
  const app = express();
  app.get("/export", createDispatchesCsvHandler(handlerOptions));
  return serve(app);
}

function fakeDataset(rows) {
  const calls = [];
  return {
    calls,
    async queryFn(text, params) {
      calls.push({ text, params });
      const limit = params[params.length - 1];
      const hasCursor = text.includes("(created_at, id) <");
      let start = 0;
      if (hasCursor) {
        const cursorId = params[params.length - 2];
        start = rows.findIndex((r) => r._cursor_id === cursorId) + 1;
      }
      return { rows: rows.slice(start, start + limit) };
    },
  };
}

test("exporta mais de 50 registros em lotes, com BOM, ; e CRLF", async () => {
  const rows = Array.from({ length: 120 }, (_v, i) => makeRow(i));
  const db = fakeDataset(rows);
  const base = await appFor({ queryFn: db.queryFn, batchSize: 50, now: () => new Date("2026-10-16T02:30:00Z") });
  const r = await fetch(`${base}/export`);
  assert.equal(r.status, 200);
  assert.equal(r.headers.get("content-type"), "text/csv; charset=utf-8");
  assert.equal(
    r.headers.get("content-disposition"),
    'attachment; filename="notification-dispatches-2026-10-15.csv"'
  );
  const body = await csvBody(r);
  const lines = body.split("\r\n");
  assert.equal(lines.at(-1), "");
  const dataLines = lines.slice(0, -1);
  assert.equal(dataLines.length, 121); // cabecalho + 120
  assert.equal(
    dataLines[0],
    "ID;Data (Brasília);Canal;Provedor;Evento;Modelo;ID do modelo no provedor;Usuário;Destinatário;Status;Status do provedor;Status de entrega;Erro;Enviado em (Brasília);Entregue em (Brasília)"
  );
  assert.equal(dataLines[1].split(";")[0], "1000");
  assert.equal(dataLines[120].split(";")[0], "881");
  assert.equal(new Set(dataLines.slice(1).map((l) => l.split(";")[0])).size, 120); // sem repeticao
  // 3 consultas de dados: 50, 50, 20
  assert.equal(db.calls.length, 3);
  assert.equal(db.calls[1].params.at(-3), rows[49]._cursor_created_at);
  assert.equal(db.calls[1].params.at(-2), rows[49]._cursor_id);
  assert.equal(db.calls[2].params.at(-2), rows[99]._cursor_id);
});

test("lote exato multiplo do tamanho termina sem linha extra", async () => {
  const rows = Array.from({ length: 100 }, (_v, i) => makeRow(i));
  const db = fakeDataset(rows);
  const base = await appFor({ queryFn: db.queryFn, batchSize: 50 });
  const body = await csvBody(await fetch(`${base}/export`));
  assert.equal(body.split("\r\n").length - 2, 100);
  assert.equal(db.calls.length, 3); // 50, 50 e um lote vazio de confirmacao
});

test("sem registros retorna CSV valido contendo so o cabecalho", async () => {
  const base = await appFor({ queryFn: async () => ({ rows: [] }) });
  const r = await fetch(`${base}/export?status=failed&from=2026-10-01&to=2026-10-02`);
  assert.equal(r.status, 200);
  const body = await csvBody(r);
  const lines = body.split("\r\n");
  assert.equal(lines.length, 2);
  assert.match(lines[0], /^ID;Data \(Brasília\);Canal;/);
  assert.equal(lines[1], "");
});

test("filtros combinados chegam como parametros na ordem esperada", async () => {
  const db = fakeDataset([]);
  const base = await appFor({ queryFn: db.queryFn });
  const r = await fetch(`${base}/export?status=failed&channel=email&provider=brevo_smtp&from=2026-10-01&to=2026-10-15`);
  assert.equal(r.status, 200);
  await csvBody(r);
  assert.deepEqual(db.calls[0].params, ["failed", "email", "brevo_smtp", "2026-10-01", "2026-10-15", 1000]);
});

test("preserva status reais (skipped, unknown, failed) e erro; nao converte em sucesso", async () => {
  const rows = [
    makeRow(0, { status: "skipped", provider_status: null, delivery_status: null, error_message: "no_consent" }),
    makeRow(1, { status: "accepted", provider_status: "accepted", delivery_status: "unknown" }),
    makeRow(2, { status: "failed", provider_status: null, delivery_status: null, error_message: "manual_email_send_failed" }),
  ];
  const base = await appFor({ queryFn: fakeDataset(rows).queryFn });
  const lines = (await csvBody(await fetch(`${base}/export`))).split("\r\n");
  assert.match(lines[1], /;skipped;;;no_consent;/);
  assert.match(lines[2], /;accepted;accepted;unknown;;/);
  assert.match(lines[3], /;failed;;;manual_email_send_failed;/);
});

test("acentos, aspas, ponto e virgula, quebras de linha e formulas no arquivo", async () => {
  const rows = [
    makeRow(0, {
      recipient: "+5521999999999",
      error_message: 'Falha: "timeout"; ação\nnova linha',
      template_key: "=cmd|' /C calc'!A0",
      event_key: "@evento",
    }),
  ];
  const base = await appFor({ queryFn: fakeDataset(rows).queryFn });
  const body = await csvBody(await fetch(`${base}/export`));
  assert.ok(body.includes("'+5521999999999"));
  assert.ok(body.includes("'@evento"));
  assert.ok(body.includes(";'=cmd|' /C calc'!A0;")); // protegida e sem aspas: nao ha ; nem " na celula
  assert.ok(body.includes('"Falha: ""timeout""; ação\nnova linha"'));
  // o arquivo continua com 2 registros logicos (cabecalho + 1) apesar da quebra dentro da celula
  const parsed = parseCsv(body);
  assert.equal(parsed.length, 2);
  assert.equal(parsed[1][12], 'Falha: "timeout"; ação\nnova linha');
  assert.equal(parsed[1][8], "'+5521999999999");
});

test("datas do arquivo saem em America/Sao_Paulo", async () => {
  const rows = [makeRow(0, { created_at: new Date("2026-10-15T02:30:00Z"), sent_at: new Date("2026-10-15T13:00:00Z") })];
  const base = await appFor({ queryFn: fakeDataset(rows).queryFn });
  const parsed = parseCsv(await csvBody(await fetch(`${base}/export`)));
  assert.equal(parsed[1][1], "2026-10-14 23:30:00");
  assert.equal(parsed[1][13], "2026-10-15 10:00:00");
  assert.equal(parsed[1][14], "");
});

test("filtro invalido responde 400 sem consultar o banco", async () => {
  const db = fakeDataset([]);
  const base = await appFor({ queryFn: db.queryFn });
  for (const qs of ["from=2026-02-30", "to=ontem", "from=2026-10-16&to=2026-10-15", "status=a&status=b"]) {
    const r = await fetch(`${base}/export?${qs}`);
    assert.equal(r.status, 400, qs);
    const body = await r.json();
    assert.equal(body.ok, false);
  }
  assert.equal(db.calls.length, 0);
});

test("erro na primeira consulta responde 500 em JSON, sem expor detalhe", async () => {
  const base = await appFor({
    queryFn: async () => {
      throw new Error("password authentication failed for user x");
    },
  });
  const r = await fetch(`${base}/export`);
  assert.equal(r.status, 500);
  assert.deepEqual(await r.json(), { ok: false, error: "internal_error" });
});

test("erro depois de iniciar o envio derruba a conexao em vez de entregar arquivo truncado", async () => {
  let call = 0;
  const base = await appFor({
    batchSize: 2,
    queryFn: async () => {
      call += 1;
      if (call === 1) return { rows: [makeRow(0), makeRow(1)] };
      throw new Error("conexao perdida");
    },
  });
  await assert.rejects(async () => {
    const r = await fetch(`${base}/export`);
    await r.arrayBuffer();
  });
});

// Leitor de CSV minimo (separador ';', aspas duplas) para conferir a estrutura do arquivo.
function parseCsv(text) {
  const records = [];
  let field = "";
  let record = [];
  let quoted = false;
  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    if (quoted) {
      if (ch === '"' && text[i + 1] === '"') {
        field += '"';
        i += 1;
      } else if (ch === '"') quoted = false;
      else field += ch;
    } else if (ch === '"') quoted = true;
    else if (ch === ";") {
      record.push(field);
      field = "";
    } else if (ch === "\r" && text[i + 1] === "\n") {
      record.push(field);
      records.push(record);
      record = [];
      field = "";
      i += 1;
    } else field += ch;
  }
  if (field || record.length) {
    record.push(field);
    records.push(record);
  }
  return records;
}
