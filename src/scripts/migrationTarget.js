// src/scripts/migrationTarget.js
//
// Resolve o banco-alvo de uma migration e decide se e seguro prosseguir.
// Modulo puro (sem I/O, sem import de pg/db.js) para ser testavel sem banco real.
//
// Duas guardas, cada uma fechando exatamente a falha do incidente de
// 2026-08-17 (migrations de teste caindo em producao via .env.local):
//
//   1) modo teste (--test): usa SOMENTE TEST_DATABASE_URL. Se ausente,
//      ABORTA. Nunca cai para DATABASE_URL -- essa queda e que causou o
//      incidente.
//   2) modo normal: usa DATABASE_URL. Se o host parecer producao (Supabase),
//      exige ALLOW_PRODUCTION_MIGRATIONS=true explicito no ambiente.

const PRODUCTION_HOST_PATTERNS = [/\.supabase\.co$/i, /\.supabase\.com$/i];

export function isProductionHost(hostname) {
  const host = String(hostname || "");
  if (!host) return false;
  return PRODUCTION_HOST_PATTERNS.some((re) => re.test(host));
}

/** So o hostname -- nunca logar a URL inteira (ela carrega usuario/senha). */
export function safeHost(url) {
  try {
    return new URL(String(url || "")).hostname || null;
  } catch {
    return null;
  }
}

export class MigrationTargetError extends Error {
  constructor(message) {
    super(message);
    this.name = "MigrationTargetError";
  }
}

/**
 * @param {{ isTestMode: boolean, env: Record<string,string|undefined> }} params
 * @returns {{ url: string, mode: "test"|"normal", host: string|null }}
 * @throws {MigrationTargetError} quando o alvo nao e seguro
 */
export function resolveMigrationTarget({ isTestMode, env }) {
  const e = env || {};

  if (isTestMode) {
    const url = String(e.TEST_DATABASE_URL || "").trim();
    if (!url) {
      throw new MigrationTargetError(
        "ABORTADO: --test exige TEST_DATABASE_URL definido explicitamente. " +
          "Nao ha fallback para DATABASE_URL em modo teste."
      );
    }
    return { url, mode: "test", host: safeHost(url) };
  }

  const url = String(e.DATABASE_URL || "").trim();
  if (!url) {
    throw new MigrationTargetError("DATABASE_URL nao definido.");
  }

  const host = safeHost(url);
  if (isProductionHost(host) && String(e.ALLOW_PRODUCTION_MIGRATIONS || "").trim() !== "true") {
    throw new MigrationTargetError(
      `ABORTADO: o host de destino (${host}) parece ser producao (Supabase). ` +
        "Defina ALLOW_PRODUCTION_MIGRATIONS=true explicitamente no ambiente para prosseguir."
    );
  }

  return { url, mode: "normal", host };
}
