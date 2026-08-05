// Script para executar uma migration localizada exclusivamente em src/migrations.
import "../config/env.js";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { basename, dirname, resolve } from "path";
import { getPool } from "../db.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

async function runMigration() {
  const migrationName = process.argv[2] || "001_add_vindi_columns.sql";
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

  const sql = readFileSync(migrationPath, "utf-8");
  const pool = await getPool();
  const client = await pool.connect();
  let transactionOpen = false;

  try {
    console.log(`Executando migration: ${migrationName}`);
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
