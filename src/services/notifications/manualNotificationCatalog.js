import { query } from "../../db.js";
import { getConnectedBrevoWhatsAppTemplates } from "./manualWhatsAppTemplates.js";

function builtinEmailTemplate(template) {
  return {
    channel: "email",
    provider: "brevo_smtp",
    source: "builtin",
    editable: false,
    manual_send_allowed: true,
    ...template,
  };
}

function remainingNumbersEmailTemplate(remainingNumbers) {
  return builtinEmailTemplate({
    template_key: `EMAIL_DRAW_REMAINING_${remainingNumbers}`,
    name: `Restam ${remainingNumbers} números`,
    description: `Aviso por e-mail para divulgar que ainda existem ${remainingNumbers} números disponíveis.`,
    subject_template: `Restam ${remainingNumbers} números no sorteio {{draw_name}}`,
    html_template: `<p>Olá, {{name}}!</p><p>Restam ${remainingNumbers} números disponíveis no sorteio {{draw_name}}.</p><p><a href="{{draw_url}}">Acesse o site para escolher seus números</a></p>`,
    text_template: `Olá, {{name}}!\n\nRestam ${remainingNumbers} números disponíveis no sorteio {{draw_name}}.\n\nAcesse o site para escolher seus números:\n{{draw_url}}`,
    default_params: {
      remaining_numbers: remainingNumbers,
      draw_name: "New Store",
      draw_url: "/",
    },
  });
}

const EMAIL_BUILTIN_TEMPLATES = [
  builtinEmailTemplate({
    template_key: "GENERIC_ADMIN_EMAIL",
    name: "E-mail administrativo",
    description: "Mensagem administrativa manual.",
    subject_template: "Mensagem da New Store",
    html_template: "<p>{{message}}</p>",
    text_template: "{{message}}",
    default_params: { message: "Digite a mensagem." },
  }),
  builtinEmailTemplate({
    template_key: "NEW_DRAW_EMAIL",
    name: "Novo sorteio",
    description: "Aviso sobre novo sorteio.",
    subject_template: "Novo sorteio disponível — {{draw_name}}",
    html_template: "<p>Olá, {{name}}!</p><p>Um novo sorteio está disponível:</p><p><strong>{{draw_name}}</strong></p><p>Acesse para participar:</p><p><a href=\"{{draw_url}}\">{{draw_url}}</a></p><p>Boa sorte!</p><p>Equipe NewStore</p>",
    text_template: "Olá, {{name}}!\n\nUm novo sorteio está disponível:\n\n{{draw_name}}\n\nAcesse para participar:\n{{draw_url}}\n\nBoa sorte!\n\nEquipe NewStore",
    default_params: { name: "Cliente", draw_name: "New Store", draw_url: "/" },
  }),
  builtinEmailTemplate({
    template_key: "DRAW_CLOSED_EMAIL",
    name: "Sorteio encerrado — acompanhe o resultado",
    description: "Aviso automático aos participantes quando o sorteio é encerrado e aguarda o resultado da Lotomania.",
    subject_template: "Sorteio {{draw_name}} encerrado — acompanhe o resultado",
    html_template: "<p>Olá, {{name}}!</p><p>O sorteio <strong>{{draw_name}}</strong> foi encerrado.</p><p>O resultado será acompanhado pelo canal oficial da CAIXA no YouTube:</p><p><a href=\"https://www.youtube.com/@caixa\">https://www.youtube.com/@caixa</a></p><p>O vencedor será o participante que possuir o <strong>último número sorteado da Lotomania</strong>.</p><p>Boa sorte!</p><p>Equipe NewStore</p>",
    text_template: "Olá, {{name}}!\n\nO sorteio {{draw_name}} foi encerrado.\n\nAcompanhe o resultado pelo canal oficial da CAIXA:\n\nhttps://www.youtube.com/@caixa\n\nO vencedor será o participante que possuir o último número sorteado da Lotomania.\n\nBoa sorte!\n\nEquipe NewStore",
    default_params: { name: "Cliente", draw_name: "New Store" },
  }),
  builtinEmailTemplate({
    template_key: "RESULT_AVAILABLE_EMAIL",
    name: "Resultado disponivel",
    description: "Aviso manual de resultado disponivel.",
    subject_template: "Resultado disponivel",
    html_template: "<p>{{message}}</p>",
    text_template: "{{message}}",
    default_params: { message: "O resultado ja esta disponivel." },
  }),
  builtinEmailTemplate({
    template_key: "BALANCE_NOTICE_EMAIL",
    name: "Aviso de saldo",
    description: "Aviso manual relacionado a saldo.",
    subject_template: "Aviso sobre seu saldo",
    html_template: "<p>{{message}}</p>",
    text_template: "{{message}}",
    default_params: { message: "Voce tem um aviso sobre seu saldo." },
  }),
  remainingNumbersEmailTemplate(75),
  remainingNumbersEmailTemplate(50),
  remainingNumbersEmailTemplate(30),
  remainingNumbersEmailTemplate(15),
];
const REQUIRED_REMAINING_EMAIL_KEYS = new Set([
  "EMAIL_DRAW_REMAINING_75",
  "EMAIL_DRAW_REMAINING_50",
  "EMAIL_DRAW_REMAINING_30",
  "EMAIL_DRAW_REMAINING_15",
]);

function runQuery(pgClient, text, params) {
  if (pgClient) return pgClient.query(text, params);
  return query(text, params);
}

async function tableColumns(pgClient, tableName) {
  const result = await runQuery(
    pgClient,
    `SELECT column_name
       FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = $1`,
    [tableName]
  );
  return new Set((result.rows || []).map((row) => row.column_name));
}

function quoteIdent(name) {
  return `"${String(name).replace(/"/g, '""')}"`;
}

function selectExisting(columns, candidates) {
  return candidates.filter((column) => columns.has(column));
}

function project(row, columns, source) {
  const out = { source };
  for (const column of columns) out[column] = row[column] ?? null;
  return out;
}

export async function getManualNotificationCatalog({ pgClient } = {}) {
  const templateColumns = await tableColumns(pgClient, "notification_templates");
  const ruleColumns = await tableColumns(pgClient, "notification_push_rules");

  const whatsappColumns = selectExisting(templateColumns, [
    "id",
    "template_key",
    "provider_template_id",
    "name",
    "description",
    "body_preview",
    "default_message",
    "default_params",
    "message_params",
    "is_active",
    "provider_status",
    "language",
    "template_language",
    "updated_at",
  ]);
  const emailColumns = selectExisting(templateColumns, [
    "id",
    "template_key",
    "provider_template_id",
    "name",
    "description",
    "subject_template",
    "html_template",
    "text_template",
    "default_message",
    "default_params",
    "message_params",
    "is_active",
    "provider_status",
    "language",
    "template_language",
    "updated_at",
  ]);
  const pushColumns = selectExisting(ruleColumns, [
    "id",
    "event_key",
    "name",
    "description",
    "title_template",
    "body_template",
    "url_template",
    "category",
    "threshold_value",
    "cooldown_minutes",
    "is_active",
    "last_triggered_at",
  ]);

  let whatsappTemplates = [];
  if (whatsappColumns.length) {
    const result = await runQuery(
      pgClient,
      `SELECT ${whatsappColumns.map(quoteIdent).join(", ")}
         FROM public.notification_templates
        WHERE channel = 'whatsapp'
          AND provider = 'brevo'
        ORDER BY template_key`,
      []
    );
    const databaseRows = (result.rows || []).map((row) => project(row, whatsappColumns, "brevo"));
    whatsappTemplates = await getConnectedBrevoWhatsAppTemplates({
      pgClient,
      databaseRows,
    });
  } else {
    whatsappTemplates = await getConnectedBrevoWhatsAppTemplates({
      pgClient,
      databaseRows: [],
    });
  }

  let pushTemplates = [];
  if (pushColumns.length) {
    const result = await runQuery(
      pgClient,
      `SELECT ${pushColumns.map(quoteIdent).join(", ")}
         FROM public.notification_push_rules
        ORDER BY event_key`,
      []
    );
    pushTemplates = (result.rows || []).map((row) => project(row, pushColumns, "push_rule"));
  }

  let emailTemplates = [];
  if (emailColumns.length) {
    const result = await runQuery(
      pgClient,
      `SELECT ${emailColumns.map(quoteIdent).join(", ")}
         FROM public.notification_templates
        WHERE channel = 'email'
        ORDER BY template_key`,
      []
    );
    emailTemplates = (result.rows || []).map((row) => ({
      ...project(row, emailColumns, "database"),
      editable: true,
    }));
  }
  emailTemplates = emailTemplates.filter(
    (template) => !REQUIRED_REMAINING_EMAIL_KEYS.has(template.template_key)
  );
  const databaseEmailKeys = new Set(emailTemplates.map((template) => template.template_key));
  emailTemplates = [
    ...emailTemplates,
    ...EMAIL_BUILTIN_TEMPLATES.filter(
      (template) => !databaseEmailKeys.has(template.template_key)
    ),
  ];

  return {
    ok: true,
    channels: {
      whatsapp: {
        enabled: process.env.NOTIFICATION_WHATSAPP_AUTOMATION_ENABLED === "true",
        provider: "brevo",
        audiences: ["selected", "all_consented"],
        connected_templates_count: whatsappTemplates.length,
        manual_templates_count: whatsappTemplates.filter(
          (template) => template.manual_send_allowed !== false
        ).length,
        templates: whatsappTemplates,
      },
      push: {
        enabled: process.env.PUSH_ENABLED === "true",
        provider: "web_push",
        audiences: ["selected", "all_active_push", "all_consented"],
        templates: pushTemplates,
      },
      email: {
        enabled: Boolean(String(process.env.SMTP_HOST || "").trim()),
        provider: "brevo_smtp",
        audiences: ["selected", "all_with_email"],
        templates: emailTemplates,
      },
    },
  };
}

export function getBuiltinEmailTemplates() {
  return EMAIL_BUILTIN_TEMPLATES.map((template) => ({ ...template }));
}
