// tests/trayHealth.test.js
//
// PROVA: GET /api/tray/health e SOMENTE LEITURA.
//
// Incidente (2026-08-17): a primeira chamada a /api/tray/health apos um
// deploy disparou bootstrap OAuth de verdade (rede) e escreveu
// tray_access_token/tray_refresh_token/tray_access_exp_at no kv_store real
// de producao -- efeito colateral inaceitavel para um endpoint de health
// check publico e sem autenticacao (qualquer monitor/crawler dispararia
// autenticacao Tray so de bater na rota).
//
//   TEST_DATABASE_URL=postgres://... npm test
//
// Sem TEST_DATABASE_URL os testes sao pulados (nunca usam banco de producao).
import test, { before, beforeEach, after } from "node:test";
import assert from "node:assert/strict";

const TEST_DB = process.env.TEST_DATABASE_URL || "";
const SKIP = !TEST_DB;
const skipOpts = { skip: SKIP ? "defina TEST_DATABASE_URL para rodar os testes de integracao" : false };

let pool;
let originalFetch;
let fetchCalls;

before(async () => {
  if (SKIP) return;
  const pg = (await import("pg")).default;
  pool = new pg.Pool({ connectionString: TEST_DB, ssl: { rejectUnauthorized: false }, max: 5 });

  await pool.query(`
    create table if not exists kv_store (
      k text primary key,
      v text,
      updated_at timestamptz default now()
    )
  `);
});

after(async () => {
  if (SKIP || !pool) return;
  await pool.query("delete from kv_store where k like 'tray_%'").catch(() => {});
  await pool.end().catch(() => {});
});

beforeEach(async () => {
  if (SKIP) return;
  await pool.query("delete from kv_store where k like 'tray_%'");
  __resetTrayCacheForTests();

  // Nenhum teste aqui pode alcancar a rede de verdade. Se algum caminho
  // (bug) tentar chamar a Tray, o mock explode com um erro claro.
  fetchCalls = [];
  originalFetch = global.fetch;
  global.fetch = async (url, options) => {
    fetchCalls.push({ url: String(url), method: options?.method || "GET" });
    throw new Error(`REDE ALCANCADA INESPERADAMENTE: ${options?.method || "GET"} ${url}`);
  };

  process.env.TRAY_CONSUMER_KEY = "test-consumer-key";
  process.env.TRAY_CONSUMER_SECRET = "test-consumer-secret";
  process.env.TRAY_CODE = "test-code-unused-by-health";
  process.env.TRAY_API_BASE = "https://exemplo-loja.com.br/web_api";
});

function restoreFetch() {
  if (originalFetch) global.fetch = originalFetch;
}

async function seedKv(rows) {
  for (const [k, v] of Object.entries(rows)) {
    await pool.query(
      `insert into kv_store (k, v) values ($1,$2) on conflict (k) do update set v=excluded.v, updated_at=now()`,
      [k, v]
    );
  }
}

async function snapshotKv() {
  const { rows } = await pool.query("select k, v, updated_at from kv_store where k like 'tray_%' order by k");
  return rows;
}

// db.js le DATABASE_URL do processo (nao de config/env.js -- ja fixamos isso
// no incidente anterior). Redireciona para o Postgres local de teste antes
// de importar qualquer coisa que toque o banco.
process.env.DATABASE_URL = TEST_DB || process.env.DATABASE_URL;
const { trayTokenHealthReadOnly, trayToken, __resetTrayCacheForTests } = await import("../src/services/tray.js");

test("A: env configurada, sem token no kv_store -> nao chama rede, nao escreve, informa nao inicializado", skipOpts, async () => {
  try {
    const out = await trayTokenHealthReadOnly();

    assert.deepEqual(fetchCalls, [], "nao pode ter chamado a Tray");
    assert.equal(out.ok, false);
    assert.equal(out.configured, true);
    assert.equal(out.authMode, null);
    assert.equal(out.lastError, "tray_auth_not_initialized");

    const rows = await snapshotKv();
    assert.deepEqual(rows, [], "kv_store nao pode ter ganhado nenhuma linha tray_*");
  } finally {
    restoreFetch();
  }
});

test("B: access token valido no kv_store -> health ok, somente leitura", skipOpts, async () => {
  try {
    const futureExp = new Date(Date.now() + 60 * 60_000).toISOString().slice(0, 19).replace("T", " ");
    await seedKv({
      tray_access_token: "fake-access-token-abc",
      tray_access_exp_at: futureExp,
      tray_refresh_token: "fake-refresh-token-xyz",
    });
    const before = await snapshotKv();

    const out = await trayTokenHealthReadOnly();

    assert.deepEqual(fetchCalls, [], "nao pode ter chamado a Tray");
    assert.equal(out.ok, true);
    assert.equal(out.authMode, "cache");
    assert.equal(out.hasRefreshKV, true);
    assert.equal(out.lastError, null);
    assert.equal(out.expAccessAt, futureExp);

    const after = await snapshotKv();
    assert.deepEqual(after, before, "kv_store nao pode ter sido alterado por um health check");
  } finally {
    restoreFetch();
  }
});

test("C: access expirado + refresh presente -> health NAO renova, NAO altera kv_store", skipOpts, async () => {
  try {
    const pastExp = new Date(Date.now() - 60 * 60_000).toISOString().slice(0, 19).replace("T", " ");
    await seedKv({
      tray_access_token: "fake-expired-access-token",
      tray_access_exp_at: pastExp,
      tray_refresh_token: "fake-refresh-token-xyz",
    });
    const before = await snapshotKv();

    const out = await trayTokenHealthReadOnly();

    assert.deepEqual(fetchCalls, [], "health nunca pode chamar o endpoint de refresh da Tray");
    assert.equal(out.ok, false);
    assert.equal(out.hasRefreshKV, true);
    assert.equal(out.lastError, "tray_access_token_expired");

    const after = await snapshotKv();
    assert.deepEqual(after, before, "kv_store nao pode ter sido alterado -- refresh NAO deve ter sido acionado pelo health");
  } finally {
    restoreFetch();
  }
});

test("D: operacao real de catalogo com access expirado continua fazendo refresh e persistindo (fluxo antigo intacto)", skipOpts, async () => {
  try {
    const pastExp = new Date(Date.now() - 60 * 60_000).toISOString().slice(0, 19).replace("T", " ");
    await seedKv({
      tray_access_token: "fake-expired-access-token",
      tray_access_exp_at: pastExp,
      tray_refresh_token: "fake-refresh-token-xyz",
    });

    // Diferente do health: aqui simulamos a Tray respondendo ao refresh.
    global.fetch = async (url, options) => {
      fetchCalls.push({ url: String(url), method: options?.method || "GET" });
      return {
        ok: true,
        status: 200,
        headers: { get: (h) => (String(h).toLowerCase() === "content-type" ? "application/json" : null) },
        json: async () => ({
          access_token: "fresh-access-token-novo",
          refresh_token: "fresh-refresh-token-novo",
          date_expiration_access_token: new Date(Date.now() + 3 * 3600_000).toISOString().slice(0, 19).replace("T", " "),
          date_activated: new Date().toISOString().slice(0, 19).replace("T", " "),
        }),
      };
    };

    const token = await trayToken();

    assert.equal(token, "fresh-access-token-novo");
    assert.equal(fetchCalls.length, 1, "a operacao real precisa ter chamado a Tray para renovar");
    assert.match(fetchCalls[0].url, /\/auth\?refresh_token=/);

    const rows = await snapshotKv();
    const byKey = Object.fromEntries(rows.map((r) => [r.k, r.v]));
    assert.equal(byKey.tray_access_token, "fresh-access-token-novo", "o fluxo real ainda persiste o token novo");
    assert.equal(byKey.tray_refresh_token, "fresh-refresh-token-novo");
    assert.ok(Number.isFinite(Number(byKey.tray_access_exp_ms)), "expMs corrigido tambem deve ser persistido");
  } finally {
    restoreFetch();
  }
});

test("E: health nunca devolve segredo/token em nenhum cenario", skipOpts, async () => {
  try {
    const futureExp = new Date(Date.now() + 60 * 60_000).toISOString().slice(0, 19).replace("T", " ");
    await seedKv({ tray_access_token: "secret-abc", tray_access_exp_at: futureExp, tray_refresh_token: "secret-xyz" });

    const out = await trayTokenHealthReadOnly();
    const serialized = JSON.stringify(out);

    for (const forbidden of ["access_token", "refresh_token", "consumer_secret", "consumerSecret", "secret-abc", "secret-xyz", "test-consumer-secret"]) {
      assert.ok(!serialized.includes(forbidden), `resposta do health vazou "${forbidden}": ${serialized}`);
    }
  } finally {
    restoreFetch();
  }
});

test("F: sem TRAY_CONSUMER_KEY/SECRET -> nao configurado, nao chama rede", skipOpts, async () => {
  try {
    delete process.env.TRAY_CONSUMER_KEY;
    delete process.env.TRAY_CONSUMER_SECRET;

    const out = await trayTokenHealthReadOnly();

    assert.deepEqual(fetchCalls, []);
    assert.equal(out.ok, false);
    assert.equal(out.configured, false);
    assert.equal(out.lastError, "tray_env_missing_keys");
  } finally {
    process.env.TRAY_CONSUMER_KEY = "test-consumer-key";
    process.env.TRAY_CONSUMER_SECRET = "test-consumer-secret";
    restoreFetch();
  }
});

test("H: expAccessAt sem timezone pareceria expirado no parse ingenuo, mas expMs persistido (corrigido) ainda e valido", skipOpts, async () => {
  try {
    // "date_expiration_access_token" da Tray vem sem timezone, no relogio da
    // loja (BRT/UTC-3, ver computeExpMs). Um parser ingenuo que trata essa
    // string como UTC acha o token expirado ~3h antes da hora real. Por isso
    // persistimos tambem o expMs ja corrigido (calculado uma unica vez, no
    // momento do bootstrap/refresh real) para o health reusar sem refazer a
    // conta errada.
    const naiveLooksExpired = new Date(Date.now() - 60 * 60_000).toISOString().slice(0, 19).replace("T", " ");
    const correctedStillValidMs = Date.now() + 2 * 3600_000;
    await seedKv({
      tray_access_token: "fake-access-token-abc",
      tray_access_exp_at: naiveLooksExpired,
      tray_access_exp_ms: String(correctedStillValidMs),
      tray_refresh_token: "fake-refresh-token-xyz",
    });

    const out = await trayTokenHealthReadOnly();

    assert.deepEqual(fetchCalls, [], "nao pode ter chamado a Tray");
    assert.equal(out.ok, true, "o expMs corrigido diz que o token ainda e valido");
    assert.equal(out.authMode, "cache");
    assert.equal(out.lastError, null);
  } finally {
    restoreFetch();
  }
});

test("I: sem expMs persistido (token antigo, anterior a este fix) cai para o parse ingenuo do expAccessAt", skipOpts, async () => {
  try {
    const futureNaive = new Date(Date.now() + 60 * 60_000).toISOString().slice(0, 19).replace("T", " ");
    await seedKv({
      tray_access_token: "fake-access-token-abc",
      tray_access_exp_at: futureNaive,
      tray_refresh_token: "fake-refresh-token-xyz",
    });

    const out = await trayTokenHealthReadOnly();

    assert.deepEqual(fetchCalls, []);
    assert.equal(out.ok, true, "sem expMs persistido, usa o parse ingenuo (compatibilidade com tokens escritos antes do fix)");
  } finally {
    restoreFetch();
  }
});

test("G: prova de nao-escrita — duas chamadas consecutivas de health nao mudam nada no kv_store", skipOpts, async () => {
  try {
    const futureExp = new Date(Date.now() + 60 * 60_000).toISOString().slice(0, 19).replace("T", " ");
    await seedKv({
      tray_access_token: "fake-access-token-abc",
      tray_access_exp_at: futureExp,
      tray_refresh_token: "fake-refresh-token-xyz",
    });
    const snapshotBefore = await snapshotKv();

    await trayTokenHealthReadOnly();
    await trayTokenHealthReadOnly();

    const snapshotAfter = await snapshotKv();
    assert.deepEqual(snapshotAfter, snapshotBefore);
    assert.deepEqual(fetchCalls, []);
  } finally {
    restoreFetch();
  }
});
