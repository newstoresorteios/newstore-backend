import { query } from "../../db.js";

const EMAIL_BUILTIN_TEMPLATES = [
  {
    template_key: "GENERIC_ADMIN_EMAIL",
    name: "E-mail administrativo",
    description: "Mensagem administrativa manual.",
    subject_template: "Mensagem da New Store",
    html_template: "<p>{{message}}</p>",
    text_template: "{{message}}",
    default_params: { message: "Digite a mensagem." },
    source: "builtin",
    editable: false,
  },
  {
    template_key: "NEW_DRAW_EMAIL",
    name: "Novo sorteio",
    description: "Aviso manual sobre novo sorteio.",
    subject_template: "Novo sorteio disponivel",
    html_template: "<p>{{message}}</p><p><a href=\"{{draw_url}}\">Acessar sorteio</a></p>",
    text_template: "{{message}}\n{{draw_url}}",
    default_params: { message: "Tem sorteio novo disponivel.", draw_url: "/" },
    source: "builtin",
    editable: false,
  },
  {
    template_key: "RESULT_AVAILABLE_EMAIL",
    name: "Resultado disponivel",
    description: "Aviso manual de resultado disponivel.",
    subject_template: "Resultado disponivel",
    html_template: "<p>{{message}}</p>",
    text_template: "{{message}}",
    default_params: { message: "O resultado ja esta disponivel." },
    source: "builtin",
    editable: false,
  },
  {
    template_key: "BALANCE_NOTICE_EMAIL",
    name: "Aviso de saldo",
    description: "Aviso manual relacionado a saldo.",
    subject_template: "Aviso sobre seu saldo",
    html_template: "<p>{{message}}</p>",
    text_template: "{{message}}",
    default_params: { message: "Voce tem um aviso sobre seu saldo." },
    source: "builtin",
    editable: false,
  },
];

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
    whatsappTemplates = (result.rows || []).map((row) => project(row, whatsappColumns, "brevo"));
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
  if (!emailTemplates.length) {
    emailTemplates = EMAIL_BUILTIN_TEMPLATES;
  }

  return {
    ok: true,
    channels: {
      whatsapp: {
        enabled: process.env.NOTIFICATION_WHATSAPP_AUTOMATION_ENABLED === "true",
        provider: "brevo",
        templates: whatsappTemplates,
      },
      push: {
        enabled: process.env.PUSH_ENABLED === "true",
        provider: "web_push",
        templates: pushTemplates,
      },
      email: {
        enabled: Boolean(String(process.env.SMTP_HOST || "").trim()),
        provider: "brevo_smtp",
        templates: emailTemplates,
      },
    },
  };
}

export function getBuiltinEmailTemplates() {
  return EMAIL_BUILTIN_TEMPLATES.map((template) => ({ ...template }));
}
