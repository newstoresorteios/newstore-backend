// Script para executar migrations.
//
//   node src/scripts/run_migration.js 020_reward_products.sql
//   npm run migrate -- 020_reward_products.sql 021_nscredit_wallets.sql
//
// Cada arquivo roda na SUA PROPRIA transacao, na ordem informada: se o
// terceiro falhar, os dois primeiros permanecem aplicados e o erro aponta
// exatamente qual arquivo parou.
//
// Sem argumento o script NAO toca no banco: lista as migrations disponiveis
// e sai. Aplicar tudo as cegas nunca e o padrao seguro.
import "dotenv/config";
import { readFileSync, readdirSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join, basename } from "path";
import { getPool } from "../db.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const MIGRATIONS_DIR = join(__dirname, "../migrations");

function listMigrations() {
  try {
    return readdirSync(MIGRATIONS_DIR)
      .filter((f) => f.toLowerCase().endsWith(".sql"))
      .sort();
  } catch {
    return [];
  }
}

/** So aceita o nome do arquivo: nada de caminho arbitrario vindo da linha de comando. */
function resolveMigration(arg) {
  const name = basename(String(arg || "").trim());
  if (!name || !name.toLowerCase().endsWith(".sql")) {
    throw new Error(`nome de migration invalido: ${arg}`);
  }
  const available = listMigrations();
  if (!available.includes(name)) {
    throw new Error(`migration nao encontrada: ${name}`);
  }
  return { name, path: join(MIGRATIONS_DIR, name) };
}

async function runMigrations(names) {
  // Resolve TODOS os nomes antes de abrir conexao: um nome errado nao deve
  // sequer encostar no banco, muito menos aplicar metade do lote.
  const planned = names.map(resolveMigration);

  const pool = await getPool();

  for (const { name, path } of planned) {
    const sql = readFileSync(path, "utf-8");

    console.log(`Executando migration: ${name}`);
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(sql);
      await client.query("COMMIT");
      console.log(`  OK: ${name}`);
    } catch (e) {
      await client.query("ROLLBACK").catch(() => {});
      console.error(`  FALHOU: ${name} -> ${e?.message || e}`);
      client.release();
      await pool.end().catch(() => {});
      process.exit(1);
    }
    client.release();
  }

  await pool.end().catch(() => {});
  console.log("Migrations aplicadas com sucesso.");
}

const args = process.argv.slice(2).filter(Boolean);

if (args.length === 0) {
  console.log("Uso: node src/scripts/run_migration.js <arquivo.sql> [outro.sql ...]");
  console.log("");
  console.log("Migrations disponiveis:");
  for (const name of listMigrations()) console.log(`  ${name}`);
  process.exit(1);
}

runMigrations(args).catch((e) => {
  console.error(`Erro: ${e?.message || e}`);
  process.exit(1);
});
