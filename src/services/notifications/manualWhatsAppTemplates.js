import { query } from "../../db.js";

export const BACKEND_BREVO_WHATSAPP_TEMPLATES = Object.freeze({
  GENERIC_TEST: Object.freeze({
    template_key: "GENERIC_TEST",
    env_names: Object.freeze([
      "BREVO_WHATSAPP_GENERIC_TEST_TEMPLATE_ID",
      "BREVO_WHATSAPP_TEMPLATE_ID",
    ]),
    name: "Template genérico/teste",
    description: "Template genérico conectado à Brevo para testes administrativos.",
    language: "pt_BR",
    default_params: Object.freeze({ message: "Digite a mensagem." }),
    manual_send_allowed: true,
  }),
  CAPTIVE_PREAUTH_REQUEST: Object.freeze({
    template_key: "CAPTIVE_PREAUTH_REQUEST",
    env_names: Object.freeze([
      "CAPTIVE_PREAUTH_BREVO_TEMPLATE_ID",
      "BREVO_WHATSAPP_CAPTIVE_AUTH_TEMPLATE_ID",
    ]),
    name: "Autorização de cativo",
    description: "Solicitação de autorização de pré-captura do cativo.",
    language: "pt_BR",
    default_params: Object.freeze({}),
    manual_send_allowed: false,
    manual_send_block_reason: "system_only_template",
  }),
  DRAW_REMAINING_NUMBERS_50: Object.freeze({
    template_key: "DRAW_REMAINING_NUMBERS_50",
    env_names: Object.freeze(["BREVO_WHATSAPP_DRAW_REMAINING_50_TEMPLATE_ID"]),
    approved_default_template_id: 25,
    name: "Restam 50 números",
    description: "Aviso automático ou manual de progresso do sorteio.",
    language: "pt_BR",
    default_params: Object.freeze({ nome: "Cliente", nome_sorteio: "Sorteio New Store", link_sorteio: "/" }),
    manual_send_allowed: true,
  }),
  DRAW_REMAINING_NUMBERS_10: Object.freeze({
    template_key: "DRAW_REMAINING_NUMBERS_10",
    env_names: Object.freeze(["BREVO_WHATSAPP_DRAW_REMAINING_10_TEMPLATE_ID"]),
    approved_default_template_id: 26,
    name: "Restam 10 números",
    description: "Aviso automático ou manual de progresso final do sorteio.",
    language: "pt_BR",
    default_params: Object.freeze({ nome: "Cliente", nome_sorteio: "Sorteio New Store", link_sorteio: "/" }),
    manual_send_allowed: true,
  }),
  BALANCE_EXPIRING_15_DAYS: Object.freeze({
    template_key: "BALANCE_EXPIRING_15_DAYS",
    env_names: Object.freeze(["BREVO_WHATSAPP_BALANCE_EXPIRING_15_TEMPLATE_ID"]),
    approved_default_template_id: 27,
    name: "Saldo vencendo em 15 dias",
    description: "Aviso de saldo com vencimento em 15 dias.",
    language: "pt_BR",
    default_params: Object.freeze({ nome: "Cliente", valor_saldo: "", link_conta: "/conta" }),
    manual_send_allowed: true,
  }),
});

function runQuery(pgClient, text, params) {
  if (pgClient) return pgClient.query(text, params);
  return query(text, params);
}

export function normalizeProviderTemplateId(value) {
  if (value == null || typeof value === "boolean") return null;
  const text = String(value).trim();
  if (!/^\d+$/.test(text)) return null;
  const id = Number(text);
  return Number.isSafeInteger(id) && id > 0 ? id : null;
}

export function isValidProviderTemplateId(value) {
  return normalizeProviderTemplateId(value) != null;
}

function configuredTemplateId(config) {
  for (const envName of config.env_names || []) {
    const id = normalizeProviderTemplateId(process.env[envName]);
    if (id != null) return { id, source: "env" };
  }
  const approvedDefault = normalizeProviderTemplateId(config.approved_default_template_id);
  return approvedDefault == null
    ? { id: null, source: "missing" }
    : { id: approvedDefault, source: "approved_default" };
}

function firstPresent(...values) {
  for (const value of values) {
    if (value != null && String(value).trim() !== "") return value;
  }
  return null;
}

function syncedBodyMetadata(row) {
  if (!row?.body_preview || typeof row.body_preview !== "string") return {};
  try {
    const parsed = JSON.parse(row.body_preview);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function normalizeConnectedTemplate(row, config, providerTemplateId) {
  const templateKey = String(row?.template_key || config?.template_key || "").trim();
  const manualAllowed = config?.manual_send_allowed !== false;
  const syncedMetadata = syncedBodyMetadata(row);
  return {
    ...(row || {}),
    template_key: templateKey,
    provider: "brevo",
    provider_template_id: providerTemplateId,
    name: firstPresent(row?.name, config?.name, templateKey),
    description: firstPresent(row?.description, config?.description),
    language: firstPresent(
      row?.language,
      row?.template_language,
      syncedMetadata.language,
      config?.language,
      "pt_BR"
    ),
    provider_status: firstPresent(row?.provider_status, syncedMetadata.status),
    is_active: true,
    available: true,
    sendable: true,
    configuration_status: "configured",
    source: "brevo",
    default_params: row?.default_params ?? config?.default_params ?? {},
    message_params: row?.message_params ?? null,
    manual_send_allowed: manualAllowed,
    ...(manualAllowed
      ? {}
      : { manual_send_block_reason: config?.manual_send_block_reason || "system_only_template" }),
  };
}

async function loadDatabaseTemplates(pgClient) {
  try {
    const result = await runQuery(
      pgClient,
      `SELECT *
         FROM public.notification_templates
        WHERE channel = 'whatsapp'
          AND provider = 'brevo'
        ORDER BY template_key`,
      []
    );
    return result.rows || [];
  } catch (error) {
    if (error?.code === "42P01" || error?.code === "42703") return [];
    throw error;
  }
}

export async function getConnectedBrevoWhatsAppTemplates({
  pgClient,
  databaseRows,
} = {}) {
  const rows = databaseRows || await loadDatabaseTemplates(pgClient);
  const usableRows = [];
  const unusablePairs = new Set();

  for (const row of rows) {
    const templateKey = String(row?.template_key || "").trim();
    const providerTemplateId = normalizeProviderTemplateId(row?.provider_template_id);
    if (!templateKey || providerTemplateId == null) continue;
    if (row?.is_active === false) {
      unusablePairs.add(`${templateKey}\u0000${providerTemplateId}`);
      continue;
    }
    usableRows.push({ row, templateKey, providerTemplateId });
  }

  const merged = [];
  const keysFromDatabase = new Set();
  for (const item of usableRows) {
    const config = BACKEND_BREVO_WHATSAPP_TEMPLATES[item.templateKey] || null;
    merged.push(normalizeConnectedTemplate(item.row, config, item.providerTemplateId));
    keysFromDatabase.add(item.templateKey);
  }

  for (const config of Object.values(BACKEND_BREVO_WHATSAPP_TEMPLATES)) {
    if (keysFromDatabase.has(config.template_key)) continue;
    const configured = configuredTemplateId(config);
    if (configured.id == null) continue;
    if (unusablePairs.has(`${config.template_key}\u0000${configured.id}`)) continue;
    merged.push(normalizeConnectedTemplate(null, config, configured.id));
  }

  const seen = new Set();
  return merged
    .filter((template) => {
      const pair = `${template.template_key}\u0000${template.provider_template_id}`;
      if (seen.has(pair)) return false;
      seen.add(pair);
      return true;
    })
    .sort((a, b) => a.template_key.localeCompare(b.template_key));
}

function coded(code, template = null) {
  const error = new Error(code);
  error.code = code;
  if (template) error.template = template;
  return error;
}

export async function resolveManualBrevoWhatsAppTemplate({
  pgClient,
  templateKey,
  requireManualAllowed = true,
} = {}) {
  const key = String(templateKey || "").trim();
  const templates = await getConnectedBrevoWhatsAppTemplates({ pgClient });
  const template = templates.find((item) => item.template_key === key) || null;
  if (!template) throw coded("manual_template_not_found");
  if (requireManualAllowed && template.manual_send_allowed === false) {
    throw coded("manual_template_not_allowed", template);
  }
  return template;
}
