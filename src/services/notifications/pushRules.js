import { query } from "../../db.js";

export const PUSH_RULE_EVENTS = Object.freeze([
  "NEW_DRAW_PUBLISHED",
  "DRAW_REMAINING_NUMBERS_75",
  "DRAW_REMAINING_NUMBERS_50",
  "DRAW_REMAINING_NUMBERS_20",
  "DRAW_REMAINING_NUMBERS_10",
  "BALANCE_EXPIRING_30_DAYS",
  "BALANCE_EXPIRING_15_DAYS",
  "BALANCE_EXPIRING_10_DAYS",
  "BALANCE_EXPIRING_7_DAYS",
  "BALANCE_EXPIRED",
  "WINNER_DEFINED",
]);

const EVENT_SET = new Set(PUSH_RULE_EVENTS);

export const DEFAULT_PUSH_RULES = Object.freeze([
  {
    event_key: "NEW_DRAW_PUBLISHED",
    name: "Sorteio novo",
    description: "Aviso quando um novo sorteio é publicado.",
    title_template: "Novo sorteio disponível!",
    body_template: "Já está disponível um novo sorteio da New Store. Confira agora.",
    url_template: "/",
    category: "operational",
  },
  {
    event_key: "DRAW_REMAINING_NUMBERS_75",
    name: "Faltam 75 números",
    description: "Dispara quando o sorteio ativo atingir 75 números disponíveis restantes.",
    title_template: "Sorteio avançando!",
    body_template: "Restam 75 números disponíveis neste sorteio.",
    url_template: "/",
    category: "operational",
    threshold_value: 75,
  },
  {
    event_key: "DRAW_REMAINING_NUMBERS_50",
    name: "Faltam 50 numeros",
    description: "Aviso quando restam 50 numeros no sorteio.",
    title_template: "Sorteio avancando!",
    body_template: "Restam 50 numeros disponiveis neste sorteio.",
    url_template: "/",
    category: "operational",
    threshold_value: 50,
  },
  {
    event_key: "DRAW_REMAINING_NUMBERS_20",
    name: "Faltam 20 números",
    description: "Aviso quando restam 20 números no sorteio.",
    title_template: "Está acabando!",
    body_template: "Restam apenas 20 números disponíveis neste sorteio.",
    url_template: "/",
    category: "operational",
    threshold_value: 20,
  },
  {
    event_key: "DRAW_REMAINING_NUMBERS_10",
    name: "Faltam 10 números",
    description: "Aviso quando restam 10 números no sorteio.",
    title_template: "Últimos números!",
    body_template: "Restam apenas 10 números disponíveis neste sorteio.",
    url_template: "/",
    category: "operational",
    threshold_value: 10,
  },
  {
    event_key: "BALANCE_EXPIRING_30_DAYS",
    name: "Saldo vencendo em 30 dias",
    description: "Aviso de saldo com vencimento em até 30 dias.",
    title_template: "Saldo disponível na sua conta",
    body_template: "Você possui saldo que vence em até 30 dias.",
    url_template: "/conta",
    category: "operational",
    threshold_value: 30,
  },
  {
    event_key: "BALANCE_EXPIRING_15_DAYS",
    name: "Saldo vencendo em 15 dias",
    description: "Aviso de saldo com vencimento em ate 15 dias.",
    title_template: "Seu saldo esta perto de vencer",
    body_template: "Voce tem saldo disponivel que vence em ate 15 dias.",
    url_template: "/conta",
    category: "operational",
    threshold_value: 15,
  },
  {
    event_key: "BALANCE_EXPIRING_10_DAYS",
    name: "Saldo vencendo em 10 dias",
    description: "Aviso de saldo com vencimento em até 10 dias.",
    title_template: "Seu saldo está perto de vencer",
    body_template: "Você tem saldo disponível que vence em até 10 dias.",
    url_template: "/conta",
    category: "operational",
    threshold_value: 10,
  },
  {
    event_key: "BALANCE_EXPIRING_7_DAYS",
    name: "Saldo vencendo em 7 dias",
    description: "Aviso de saldo com vencimento em até 7 dias.",
    title_template: "Seu saldo vence em breve",
    body_template: "Você tem saldo disponível que vence em até 7 dias.",
    url_template: "/conta",
    category: "operational",
    threshold_value: 7,
  },
  {
    event_key: "BALANCE_EXPIRED",
    name: "Saldo vencido",
    description: "Aviso quando saldo da conta venceu.",
    title_template: "Saldo vencido",
    body_template: "Um saldo da sua conta venceu. Confira os detalhes na Área do cliente.",
    url_template: "/conta",
    category: "operational",
  },
  {
    event_key: "WINNER_DEFINED",
    name: "Ganhador definido",
    description: "Aviso quando o resultado do sorteio fica disponível.",
    title_template: "Resultado disponível",
    body_template: "O resultado do sorteio já está disponível.",
    url_template: "/",
    category: "operational",
  },
]);

function coded(code) {
  const err = new Error(code);
  err.code = code;
  return err;
}

function trimOrNull(value) {
  const text = String(value ?? "").trim();
  return text ? text : null;
}

function nullableInt(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  if (!Number.isInteger(n)) throw coded("push_rule_integer_invalid");
  return n;
}

function boolValue(value, fallback = false) {
  if (typeof value === "boolean") return value;
  if (value === undefined || value === null || value === "") return fallback;
  if (String(value).toLowerCase() === "true") return true;
  if (String(value).toLowerCase() === "false") return false;
  throw coded("push_rule_boolean_invalid");
}

export function validatePushRulePayload(payload = {}, { partial = false } = {}) {
  const out = {};

  if (!partial || Object.prototype.hasOwnProperty.call(payload, "event_key")) {
    const eventKey = String(payload.event_key || "").trim();
    if (!EVENT_SET.has(eventKey)) throw coded("push_rule_event_key_invalid");
    out.event_key = eventKey;
  }

  if (!partial || Object.prototype.hasOwnProperty.call(payload, "name")) {
    const name = String(payload.name || "").trim();
    if (!name) throw coded("push_rule_name_required");
    if (name.length > 120) throw coded("push_rule_name_too_long");
    out.name = name;
  }

  if (Object.prototype.hasOwnProperty.call(payload, "description")) {
    const description = trimOrNull(payload.description);
    if (description && description.length > 500) throw coded("push_rule_description_too_long");
    out.description = description;
  } else if (!partial) {
    out.description = null;
  }

  if (!partial || Object.prototype.hasOwnProperty.call(payload, "title_template")) {
    const title = String(payload.title_template || "").trim();
    if (!title) throw coded("push_rule_title_required");
    if (title.length > 100) throw coded("push_rule_title_too_long");
    out.title_template = title;
  }

  if (!partial || Object.prototype.hasOwnProperty.call(payload, "body_template")) {
    const body = String(payload.body_template || "").trim();
    if (!body) throw coded("push_rule_body_required");
    if (body.length > 260) throw coded("push_rule_body_too_long");
    out.body_template = body;
  }

  if (Object.prototype.hasOwnProperty.call(payload, "url_template")) {
    const url = trimOrNull(payload.url_template);
    if (url && !url.startsWith("/")) throw coded("push_rule_url_invalid");
    if (url && url.length > 200) throw coded("push_rule_url_too_long");
    out.url_template = url;
  } else if (!partial) {
    out.url_template = null;
  }

  if (!partial || Object.prototype.hasOwnProperty.call(payload, "category")) {
    const category = String(payload.category || "operational").trim();
    if (category !== "operational") throw coded("push_rule_category_invalid");
    out.category = category;
  }

  if (Object.prototype.hasOwnProperty.call(payload, "is_active")) {
    out.is_active = boolValue(payload.is_active, false);
  } else if (!partial) {
    out.is_active = false;
  }

  if (Object.prototype.hasOwnProperty.call(payload, "threshold_value")) {
    out.threshold_value = nullableInt(payload.threshold_value);
  } else if (!partial) {
    out.threshold_value = null;
  }

  if (Object.prototype.hasOwnProperty.call(payload, "cooldown_minutes")) {
    out.cooldown_minutes = nullableInt(payload.cooldown_minutes);
  } else if (!partial) {
    out.cooldown_minutes = 1440;
  }

  return out;
}

export function getAllowedPushRuleEvents() {
  return PUSH_RULE_EVENTS.slice();
}

export async function listPushRules() {
  const result = await query(
    `SELECT *
       FROM public.notification_push_rules
      ORDER BY event_key ASC`
  );
  return result.rows || [];
}

export async function createPushRule(payload, { adminUserId } = {}) {
  const data = validatePushRulePayload(payload);
  const result = await query(
    `INSERT INTO public.notification_push_rules (
       event_key, name, description, title_template, body_template,
       url_template, category, is_active, threshold_value, cooldown_minutes,
       created_by, updated_by
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$11)
     RETURNING *`,
    [
      data.event_key,
      data.name,
      data.description,
      data.title_template,
      data.body_template,
      data.url_template,
      data.category,
      data.is_active,
      data.threshold_value,
      data.cooldown_minutes,
      adminUserId || null,
    ]
  );
  return result.rows?.[0] || null;
}

export async function updatePushRule(id, payload, { adminUserId } = {}) {
  const data = validatePushRulePayload(payload, { partial: true });
  const allowed = [
    "name",
    "description",
    "title_template",
    "body_template",
    "url_template",
    "category",
    "is_active",
    "threshold_value",
    "cooldown_minutes",
  ];
  const keys = allowed.filter((key) => Object.prototype.hasOwnProperty.call(data, key));
  if (!keys.length) throw coded("push_rule_no_fields");

  const values = [];
  const setSql = keys.map((key) => {
    values.push(data[key]);
    return `${key} = $${values.length}`;
  });
  values.push(adminUserId || null);
  setSql.push(`updated_by = $${values.length}`);
  setSql.push("updated_at = now()");
  values.push(id);

  const result = await query(
    `UPDATE public.notification_push_rules
        SET ${setSql.join(", ")}
      WHERE id = $${values.length}
      RETURNING *`,
    values
  );
  if (!result.rows?.[0]) throw coded("push_rule_not_found");
  return result.rows[0];
}

export async function seedDefaultPushRules({ adminUserId } = {}) {
  const created = [];
  for (const rule of DEFAULT_PUSH_RULES) {
    const result = await query(
      `INSERT INTO public.notification_push_rules (
         event_key, name, description, title_template, body_template,
         url_template, category, is_active, threshold_value, cooldown_minutes,
         created_by, updated_by
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,false,$8,1440,$9,$9)
       ON CONFLICT (event_key) DO NOTHING
       RETURNING *`,
      [
        rule.event_key,
        rule.name,
        rule.description || null,
        rule.title_template,
        rule.body_template,
        rule.url_template || null,
        rule.category || "operational",
        rule.threshold_value ?? null,
        adminUserId || null,
      ]
    );
    if (result.rows?.[0]) created.push(result.rows[0]);
  }
  return created;
}

export async function getActivePushRuleByEventKey(eventKey) {
  const key = String(eventKey || "").trim();
  if (!EVENT_SET.has(key)) throw coded("push_rule_event_key_invalid");
  const result = await query(
    `SELECT id, event_key, name, title_template, body_template, url_template,
            category, cooldown_minutes, threshold_value, last_triggered_at
       FROM public.notification_push_rules
      WHERE event_key = $1
        AND is_active = true
      LIMIT 1`,
    [key]
  );
  return result.rows?.[0] || null;
}
