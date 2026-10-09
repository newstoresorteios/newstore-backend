// URL publica do site NewStore usada em links de e-mails, WhatsApp e confirmacoes.
//
// Precedencia (primeira variavel valida vence):
//   PUBLIC_APP_URL > APP_PUBLIC_URL > FRONTEND_URL > SITE_URL > padrao oficial.
// PUBLIC_URL NAO entra aqui: ela e a URL publica do BACKEND (webhooks de pagamento).
//
// Um valor que aponte para um dominio da marca antiga (xnamai) e ignorado com aviso, para que uma
// variavel de ambiente herdada nunca volte a levar clientes da NewStore para outro site.
export const DEFAULT_PUBLIC_SITE_URL = "https://www.sorteionewstore.com.br";
export const PUBLIC_SITE_URL_ENV_ORDER = Object.freeze([
  "PUBLIC_APP_URL",
  "APP_PUBLIC_URL",
  "FRONTEND_URL",
  "SITE_URL",
]);

const LEGACY_HOST_PATTERN = /xnamai/i;
const warned = new Set();

function normalizeSiteUrl(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return null;
  let parsed;
  try {
    parsed = new URL(raw);
  } catch {
    return null;
  }
  if (parsed.protocol !== "https:" && parsed.protocol !== "http:") return null;
  return { url: `${parsed.origin}${parsed.pathname}`.replace(/\/+$/, ""), host: parsed.hostname };
}

export function isLegacyBrandHost(host) {
  return LEGACY_HOST_PATTERN.test(String(host ?? ""));
}

export function resolvePublicSiteUrl(env = process.env) {
  for (const name of PUBLIC_SITE_URL_ENV_ORDER) {
    const normalized = normalizeSiteUrl(env?.[name]);
    if (!normalized) continue;
    if (isLegacyBrandHost(normalized.host)) {
      if (!warned.has(name)) {
        warned.add(name);
        console.warn("[public-site-url] variavel ignorada: aponta para dominio da marca antiga", {
          variable: name,
          host: normalized.host,
        });
      }
      continue;
    }
    return normalized.url;
  }
  return DEFAULT_PUBLIC_SITE_URL;
}
