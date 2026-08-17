// tests/migrationTarget.test.js
//
// PROVA de que o incidente de 2026-08-17 (migrations de teste caindo em
// producao porque .env.local sobrescrevia DATABASE_URL) nao se repete.
//
// Duas guardas cobertas:
//   1) resolveMigrationTarget (src/scripts/migrationTarget.js): modo teste
//      exige TEST_DATABASE_URL, sem fallback; modo normal aborta contra host
//      de producao sem ALLOW_PRODUCTION_MIGRATIONS=true.
//   2) config/env.js: .env.local nao sobrescreve env var ja definida no
//      processo (teste end-to-end com processo filho + .env.local real).
import test from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

import {
  resolveMigrationTarget,
  isProductionHost,
  safeHost,
  MigrationTargetError,
} from "../src/scripts/migrationTarget.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = resolve(__dirname, "..");

const FAKE_PROD_URL =
  "postgres://postgres.fake:senha-nao-real@aws-1-us-east-1.pooler.supabase.com:6543/postgres";
const FAKE_LOCAL_URL = "postgres://postgres:localtest123@127.0.0.1:5432/newstore_test";

test("isProductionHost reconhece hosts Supabase e rejeita local", () => {
  assert.equal(isProductionHost("aws-1-us-east-1.pooler.supabase.com"), true);
  assert.equal(isProductionHost("db.abcdefgh.supabase.co"), true);
  assert.equal(isProductionHost("127.0.0.1"), false);
  assert.equal(isProductionHost("localhost"), false);
  assert.equal(isProductionHost(""), false);
  assert.equal(isProductionHost(null), false);
});

test("safeHost nunca inclui usuario/senha", () => {
  const host = safeHost(FAKE_PROD_URL);
  assert.equal(host, "aws-1-us-east-1.pooler.supabase.com");
  assert.ok(!host.includes("senha-nao-real"));
  assert.ok(!host.includes("postgres.fake"));
});

// --- 1) modo teste: TEST_DATABASE_URL explicita tem prioridade -------------
test("modo teste usa TEST_DATABASE_URL mesmo com DATABASE_URL de producao no ambiente", () => {
  const target = resolveMigrationTarget({
    isTestMode: true,
    env: { TEST_DATABASE_URL: FAKE_LOCAL_URL, DATABASE_URL: FAKE_PROD_URL },
  });
  assert.equal(target.url, FAKE_LOCAL_URL);
  assert.equal(target.mode, "test");
  assert.equal(target.host, "127.0.0.1");
});

// --- 2) TEST_DATABASE_URL ausente: aborta, NUNCA cai para DATABASE_URL -----
test("modo teste SEM TEST_DATABASE_URL aborta e nao cai para DATABASE_URL", () => {
  assert.throws(
    () =>
      resolveMigrationTarget({
        isTestMode: true,
        env: { DATABASE_URL: FAKE_PROD_URL },
      }),
    (error) => {
      assert.ok(error instanceof MigrationTargetError);
      assert.match(error.message, /TEST_DATABASE_URL/);
      // A mensagem de erro tambem nao pode vazar a URL de producao.
      assert.ok(!error.message.includes(FAKE_PROD_URL));
      return true;
    }
  );
});

test("modo teste com TEST_DATABASE_URL vazio tambem aborta", () => {
  assert.throws(
    () =>
      resolveMigrationTarget({
        isTestMode: true,
        env: { TEST_DATABASE_URL: "   ", DATABASE_URL: FAKE_PROD_URL },
      }),
    MigrationTargetError
  );
});

// --- 3) migration normal detecta producao e aborta antes de aplicar --------
test("modo normal aborta contra host de producao sem ALLOW_PRODUCTION_MIGRATIONS", () => {
  assert.throws(
    () =>
      resolveMigrationTarget({
        isTestMode: false,
        env: { DATABASE_URL: FAKE_PROD_URL },
      }),
    (error) => {
      assert.ok(error instanceof MigrationTargetError);
      assert.match(error.message, /ABORTADO/);
      assert.match(error.message, /producao/i);
      return true;
    }
  );
});

test("modo normal permite producao com ALLOW_PRODUCTION_MIGRATIONS=true explicito", () => {
  const target = resolveMigrationTarget({
    isTestMode: false,
    env: { DATABASE_URL: FAKE_PROD_URL, ALLOW_PRODUCTION_MIGRATIONS: "true" },
  });
  assert.equal(target.url, FAKE_PROD_URL);
  assert.equal(target.mode, "normal");
});

test("modo normal contra host local nao exige ALLOW_PRODUCTION_MIGRATIONS", () => {
  const target = resolveMigrationTarget({
    isTestMode: false,
    env: { DATABASE_URL: FAKE_LOCAL_URL },
  });
  assert.equal(target.url, FAKE_LOCAL_URL);
  assert.equal(target.mode, "normal");
});

test("modo normal sem DATABASE_URL nenhum aborta com mensagem clara", () => {
  assert.throws(
    () => resolveMigrationTarget({ isTestMode: false, env: {} }),
    MigrationTargetError
  );
});

// --- 4) prova end-to-end: .env.local NAO sobrescreve env ja definida -------
// Reproduz o incidente real: processo filho com cwd isolado, .env.local
// simulando producao, e TEST_DATABASE_URL/DATABASE_URL ja exportados como o
// shell faria. Usa o config/env.js REAL do projeto (nao uma reimplementacao).
test("config/env.js real: .env.local nao sobrescreve DATABASE_URL ja exportado", () => {
  const scratch = mkdtempSync(join(tmpdir(), "newstore-env-guard-"));
  try {
    writeFileSync(
      join(scratch, ".env.local"),
      `DATABASE_URL=${FAKE_PROD_URL}\nJWT_SECRET=local-dev-secret-newstore\n`
    );

    const envJsPath = resolve(REPO_ROOT, "src/config/env.js").replace(/\\/g, "/");
    const script = [
      `import("file://${envJsPath}").then(() => {`,
      `  process.stdout.write(JSON.stringify({ DATABASE_URL: process.env.DATABASE_URL }));`,
      `});`,
    ].join("\n");

    const out = execFileSync(process.execPath, ["--input-type=module", "-e", script], {
      cwd: scratch,
      env: { ...process.env, DATABASE_URL: FAKE_LOCAL_URL, NODE_ENV: "test" },
      encoding: "utf-8",
    });

    const result = JSON.parse(out);
    assert.equal(
      result.DATABASE_URL,
      FAKE_LOCAL_URL,
      "DATABASE_URL exportado pelo processo deve vencer o .env.local"
    );
    assert.notEqual(result.DATABASE_URL, FAKE_PROD_URL);
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
});

test("config/env.js real: sem DATABASE_URL no processo, .env.local ainda preenche (uso normal)", () => {
  const scratch = mkdtempSync(join(tmpdir(), "newstore-env-guard-"));
  try {
    writeFileSync(join(scratch, ".env.local"), `DATABASE_URL=${FAKE_LOCAL_URL}\n`);

    const envJsPath = resolve(REPO_ROOT, "src/config/env.js").replace(/\\/g, "/");
    const script = [
      `import("file://${envJsPath}").then(() => {`,
      `  process.stdout.write(JSON.stringify({ DATABASE_URL: process.env.DATABASE_URL }));`,
      `});`,
    ].join("\n");

    const cleanEnv = { ...process.env };
    delete cleanEnv.DATABASE_URL;

    const out = execFileSync(process.execPath, ["--input-type=module", "-e", script], {
      cwd: scratch,
      env: { ...cleanEnv, NODE_ENV: "test" },
      encoding: "utf-8",
    });

    const result = JSON.parse(out);
    assert.equal(result.DATABASE_URL, FAKE_LOCAL_URL);
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
});
