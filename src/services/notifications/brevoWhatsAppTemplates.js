// src/services/notifications/brevoWhatsAppTemplates.js
import { query } from "../../db.js";

async function runQuery(pgClient, text, params) {
  if (pgClient) return pgClient.query(text, params);
  return query(text, params);
}

function getBaseUrl() {
  return (process.env.BREVO_WHATSAPP_BASE_URL || "https://api.brevo.com/v3").replace(
    /\/+$/,
    ""
  );
}

function mapBrevoApiError(body, statusCode) {
  const message = String(body?.message || "");
  if (
    body?.code === "unauthorized" &&
    message.toLowerCase().includes("unrecognised ip address")
  ) {
    return "brevo_ip_not_authorized";
  }
  return body?.code || body?.message || `brevo_http_${statusCode}`;
}

function normalizeTemplateRow(template) {
  const brevoId = template?.id != null ? String(template.id) : null;
  return {
    brevo_id: brevoId,
    name: template?.name || template?.templateName || null,
    status: template?.status || null,
    type: template?.type || "whatsapp",
    language: template?.language || null,
    category: template?.category || null,
    raw: template,
  };
}

function buildTemplateKey(name, brevoId) {
  const prefix = "BREVO_WHATSAPP_";
  const idPart = String(brevoId || "").replace(/\D/g, "") || "UNKNOWN";
  if (!name || !String(name).trim()) {
    return `${prefix}${idPart}`;
  }
  const normalized = String(name)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .replace(/_+/g, "_");
  return `${prefix}${normalized || idPart}`;
}

function buildBodyPreview(t) {
  const parts = [t.status, t.language, t.category].filter(Boolean);
  return parts.length ? parts.join(" · ") : null;
}

function extractTemplateList(body) {
  if (Array.isArray(body)) return body;
  if (Array.isArray(body?.templates)) return body.templates;
  if (Array.isArray(body?.data)) return body.data;
  if (Array.isArray(body?.items)) return body.items;
  return [];
}

export async function fetchBrevoWhatsAppTemplates({
  limit = 50,
  offset = 0,
  sort = "desc",
} = {}) {
  const apiKey = String(process.env.BREVO_API_KEY || "").trim();
  if (!apiKey) {
    return {
      ok: false,
      error: "missing_brevo_api_key",
      count: 0,
      templates: [],
      raw: null,
    };
  }

  const baseUrl = getBaseUrl();
  const timeoutMs = Number(process.env.BREVO_WHATSAPP_TIMEOUT_MS) || 15000;
  const params = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
    sort: String(sort),
  });

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(
      `${baseUrl}/whatsappCampaigns/template-list?${params.toString()}`,
      {
        method: "GET",
        headers: {
          accept: "application/json",
          "api-key": apiKey,
        },
        signal: controller.signal,
      }
    );

    clearTimeout(timer);
    const text = await res.text();
    let body = {};
    try {
      body = text ? JSON.parse(text) : {};
    } catch {
      body = { raw: text };
    }

    if (!res.ok) {
      const error = mapBrevoApiError(body, res.status);
      return {
        ok: false,
        error,
        count: 0,
        templates: [],
        raw: body,
      };
    }

    const list = extractTemplateList(body);
    const templates = list.map(normalizeTemplateRow).filter((t) => t.brevo_id);
    const count =
      Number(body?.count) ||
      Number(body?.total) ||
      templates.length;

    return {
      ok: true,
      count,
      templates,
      raw: body,
    };
  } catch (error) {
    clearTimeout(timer);
    return {
      ok: false,
      error: error?.message || "brevo_fetch_failed",
      count: 0,
      templates: [],
      raw: null,
    };
  }
}

export async function syncBrevoWhatsAppTemplates({ pgClient } = {}) {
  const fetched = await fetchBrevoWhatsAppTemplates({
    limit: 50,
    offset: 0,
    sort: "desc",
  });

  if (!fetched.ok) {
    return {
      ok: false,
      error: fetched.error,
      fetched_count: 0,
      synced_count: 0,
      templates: [],
    };
  }

  const syncedRows = [];

  for (const t of fetched.templates) {
    const templateKey = buildTemplateKey(t.name, t.brevo_id);
    const isActive = String(t.status || "").toLowerCase() === "approved";
    const bodyPreview = buildBodyPreview(t);

    const r = await runQuery(
      pgClient,
      `INSERT INTO public.notification_templates (
          template_key,
          channel,
          provider,
          provider_template_id,
          name,
          description,
          body_preview,
          is_active,
          updated_at
        ) VALUES ($1, 'whatsapp', 'brevo', $2, $3, $4, $5, $6, NOW())
        ON CONFLICT (template_key, channel, provider)
        DO UPDATE SET
          provider_template_id = EXCLUDED.provider_template_id,
          name = EXCLUDED.name,
          description = EXCLUDED.description,
          body_preview = EXCLUDED.body_preview,
          is_active = EXCLUDED.is_active,
          updated_at = NOW()
        RETURNING *`,
      [
        templateKey,
        t.brevo_id,
        t.name,
        "Sincronizado automaticamente da Brevo",
        bodyPreview,
        isActive,
      ]
    );

    if (r.rows[0]) syncedRows.push(r.rows[0]);
  }

  return {
    ok: true,
    fetched_count: fetched.templates.length,
    synced_count: syncedRows.length,
    templates: syncedRows,
  };
}
