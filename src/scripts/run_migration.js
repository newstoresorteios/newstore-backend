// Script para executar migrations
import "dotenv/config";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import { getPool } from "../db.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

async function runMigration() {
  const pool = await getPool();
  const client = await pool.connect();

  try {
    console.log("Executando migration: 001_add_vindi_columns.sql");
    
    const migrationPath = join(__dirname, "../migrations/001_add_vindi_columns.sql");
    const sql = readFileSync(migrationPath, "utf-8");

    await client.query("BEGIN");
    await client.query(sql);
    await client.query("COMMIT");

    console.log("✓ Migration executada com sucesso!");
  } catch (e) {
    await client.query("ROLLBACK");
    console.error("✗ Erro ao executar migration:", e?.message || e);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
}

runMigration();

