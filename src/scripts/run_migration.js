// Script para executar uma migration localizada exclusivamente em src/migrations.
//
//   node src/scripts/run_migration.js 001_x.sql              (DATABASE_URL)
//   node src/scripts/run_migration.js --test 001_x.sql       (TEST_DATABASE_URL, obrigatorio)
//
// Ver src/scripts/migrationTarget.js para as guardas de producao/teste.
import "../config/env.js";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { basename, dirname, resolve } from "path";
import { resolveMigrationTarget, MigrationTargetError } from "./migrationTarget.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

async function runMigration() {
  const rawArgs = process.argv.slice(2);
  const isTestMode = rawArgs.includes("--test");
  const args = rawArgs.filter((a) => a !== "--test");
  const migrationName = args[0] || "001_add_vindi_columns.sql";

  if (
    migrationName !== basename(migrationName) ||
    migrationName.includes("..") ||
    !/^[a-zA-Z0-9][a-zA-Z0-9_.-]*\.sql$/.test(migrationName)
  ) {
    console.error("Nome de migration invalido.");
    process.exitCode = 1;
    return;
  }

  const migrationsDir = resolve(__dirname, "../migrations");
  const migrationPath = resolve(migrationsDir, migrationName);
  if (dirname(migrationPath) !== migrationsDir) {
    console.error("Migration fora de src/migrations.");
    process.exitCode = 1;
    return;
  }

  let target;
  try {
    target = resolveMigrationTarget({ isTestMode, env: process.env });
  } catch (error) {
    if (error instanceof MigrationTargetError) {
      console.error(error.message);
      process.exitCode = 1;
      return;
    }
    throw error;
  }

  // Define DATABASE_URL ANTES de importar db.js: o pool le a env no
  // carregamento do modulo, entao o import precisa ser dinamico e vir
  // depois da guarda acima -- nunca antes.
  process.env.DATABASE_URL = target.url;
  const { getPool } = await import("../db.js");

  const sql = readFileSync(migrationPath, "utf-8");
  const pool = await getPool();
  const client = await pool.connect();
  let transactionOpen = false;

  try {
    console.log(`Executando migration: ${migrationName} (modo ${target.mode}, host ${target.host || "?"})`);
    await client.query("BEGIN");
    transactionOpen = true;
    await client.query(sql);
    await client.query("COMMIT");
    transactionOpen = false;
    console.log(`Migration executada com sucesso: ${migrationName}`);
  } catch (error) {
    if (transactionOpen) await client.query("ROLLBACK").catch(() => {});
    console.error(`Erro ao executar migration ${migrationName}:`, error?.message || error);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

runMigration().catch((error) => {
  console.error("Falha ao preparar migration:", error?.message || error);
  process.exitCode = 1;
});
