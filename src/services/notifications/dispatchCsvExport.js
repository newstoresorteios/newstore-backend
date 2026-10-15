// Exportacao CSV do historico de disparos (public.notification_dispatches).
//
// Contrato com o frontend (adminNotifications.js -> exportNotificationDispatchesCsv):
//   GET /api/admin/notifications/dispatches/export.csv?status=&channel=&provider=&from=&to=
// - status/channel/provider: igualdade exata, como na listagem GET /dispatches.
// - from/to: datas sem horario (YYYY-MM-DD) em America/Sao_Paulo; "to" inclui o dia inteiro.
//
// O arquivo e gerado em lotes por cursor (created_at DESC, id DESC): a memoria nao cresce com o historico.
import { query as defaultQuery } from "../../db.js";

export const EXPORT_TIME_ZONE = "America/Sao_Paulo";
export const EXPORT_BATCH_SIZE = 1000;
export const CSV_SEPARATOR = ";";
export const CSV_EOL = "\r\n";
const CSV_BOM = "﻿";
const MAX_FILTER_LENGTH = 100;

// Ordem = ordem das colunas do arquivo. `column` e o nome real em notification_dispatches.
export const CSV_COLUMNS = Object.freeze([
  { header: "ID", column: "id" },
  { header: "Data (Brasília)", column: "created_at" },
  { header: "Canal", column: "channel" },
  { header: "Provedor", column: "provider" },
  { header: "Evento", column: "event_key" },
  { header: "Modelo", column: "template_key" },
  { header: "ID do modelo no provedor", column: "provider_template_id" },
  { header: "Usuário", column: "user_id" },
  { header: "Destinatário", column: "recipient" },
  { header: "Status", column: "status" },
  { header: "Status do provedor", column: "provider_status" },
  { header: "Status de entrega", column: "delivery_status" },
  { header: "Erro", column: "error_message" },
  { header: "Enviado em (Brasília)", column: "sent_at" },
  { header: "Entregue em (Brasília)", column: "delivery_confirmed_at" },
]);

function coded(code, extra = {}) {
  const error = new Error(code);
  error.code = code;
  Object.assign(error, extra);
  return error;
}

// ---- filtros ---------------------------------------------------------------------------------------

function parseTextFilter(value, field) {
  if (value == null) return null;
  if (typeof value !== "string") throw coded("invalid_filter", { field });
  const text = value.trim();
  if (!text) return null;
  if (text.length > MAX_FILTER_LENGTH) throw coded("invalid_filter", { field });
  return text;
}

function parseDateOnly(value, field) {
  if (value == null) return null;
  if (typeof value !== "string") throw coded("invalid_date", { field });
  const text = value.trim();
  if (!text) return null;
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(text);
  if (!match) throw coded("invalid_date", { field });
  const [year, month, day] = [Number(match[1]), Number(match[2]), Number(match[3])];
  const probe = new Date(Date.UTC(year, month - 1, day));
  if (
    probe.getUTCFullYear() !== year ||
    probe.getUTCMonth() !== month - 1 ||
    probe.getUTCDate() !== day
  ) {
    throw coded("invalid_date", { field });
  }
  return text;
}

export function parseDispatchExportFilters(queryParams = {}) {
  const filters = {
    status: parseTextFilter(queryParams.status, "status"),
    channel: parseTextFilter(queryParams.channel, "channel"),
    provider: parseTextFilter(queryParams.provider, "provider"),
    from: parseDateOnly(queryParams.from, "from"),
    to: parseDateOnly(queryParams.to, "to"),
  };
  if (filters.from && filters.to && filters.from > filters.to) {
    throw coded("invalid_date_range", { field: "from" });
  }
  return filters;
}

// ---- CSV -------------------------------------------------------------------------------------------

const brasiliaFormatter = new Intl.DateTimeFormat("en-CA", {
  timeZone: EXPORT_TIME_ZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hourCycle: "h23",
});

function brasiliaParts(date) {
  const parts = {};
  for (const part of brasiliaFormatter.formatToParts(date)) parts[part.type] = part.value;
  return parts;
}

export function formatBrasiliaDateTime(value) {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  const p = brasiliaParts(date);
  return `${p.year}-${p.month}-${p.day} ${p.hour}:${p.minute}:${p.second}`;
}

export function formatBrasiliaDate(value) {
  const date = value instanceof Date ? value : new Date(value);
  const p = brasiliaParts(date);
  return `${p.year}-${p.month}-${p.day}`;
}

// Planilhas interpretam =, +, -, @ (e TAB/CR) no inicio da celula como formula.
const FORMULA_PREFIX = /^\s*[=+\-@]/;

export function csvCell(value) {
  if (value == null) return "";
  let text;
  if (value instanceof Date) {
    text = formatBrasiliaDateTime(value);
  } else if (typeof value === "object") {
    text = JSON.stringify(value);
  } else {
    text = String(value);
  }
  // Numeros e datas formatadas nunca comecam com formula; so protegemos texto.
  if (typeof value !== "number" && typeof value !== "bigint" && FORMULA_PREFIX.test(text)) {
    text = `'${text}`;
  }
  if (/[";\r\n]/.test(text)) {
    text = `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

export function csvLine(values) {
  return values.map(csvCell).join(CSV_SEPARATOR) + CSV_EOL;
}

export function csvHeaderLine() {
  return csvLine(CSV_COLUMNS.map((c) => c.header));
}

export function csvRowLine(row) {
  return csvLine(CSV_COLUMNS.map((c) => row?.[c.column]));
}

// ---- consulta --------------------------------------------------------------------------------------

const SELECT_COLUMNS = CSV_COLUMNS.map((c) => c.column).join(", ");

export function buildDispatchBatchQuery(filters, cursor, batchSize = EXPORT_BATCH_SIZE) {
  const conditions = [];
  const params = [];
  const add = (value) => {
    params.push(value);
    return `$${params.length}`;
  };

  if (filters.status) conditions.push(`status = ${add(filters.status)}`);
  if (filters.channel) conditions.push(`channel = ${add(filters.channel)}`);
  if (filters.provider) conditions.push(`provider = ${add(filters.provider)}`);
  if (filters.from) {
    conditions.push(
      `created_at >= (${add(filters.from)}::date)::timestamp AT TIME ZONE '${EXPORT_TIME_ZONE}'`
    );
  }
  if (filters.to) {
    // Limite exclusivo no inicio do dia seguinte: inclui o ultimo dia inteiro.
    conditions.push(
      `created_at < ((${add(filters.to)}::date + 1)::timestamp AT TIME ZONE '${EXPORT_TIME_ZONE}')`
    );
  }
  if (cursor) {
    conditions.push(`(created_at, id) < (${add(cursor.createdAt)}::timestamptz, ${add(cursor.id)})`);
  }

  const where = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
  const limit = add(batchSize);
  const text =
    `SELECT ${SELECT_COLUMNS}, created_at::text AS _cursor_created_at, id::text AS _cursor_id
       FROM public.notification_dispatches
      ${where}
      ORDER BY created_at DESC, id DESC
      LIMIT ${limit}`;
  return { text, params };
}

// Gera pedacos do arquivo. O primeiro pedaco (BOM + cabecalho + 1o lote) so sai depois do 1o lote ter sido
// lido, para a rota ainda poder responder 500 em JSON se a consulta falhar antes de enviar cabecalhos.
export async function* iterateDispatchCsv({
  queryFn = defaultQuery,
  filters,
  batchSize = EXPORT_BATCH_SIZE,
  onBatch = null,
} = {}) {
  let cursor = null;
  let first = true;
  for (;;) {
    const { text, params } = buildDispatchBatchQuery(filters, cursor, batchSize);
    const result = await queryFn(text, params);
    const rows = Array.isArray(result?.rows) ? result.rows : [];
    let chunk = first ? CSV_BOM + csvHeaderLine() : "";
    for (const row of rows) chunk += csvRowLine(row);
    if (onBatch) onBatch(rows.length);
    if (first || chunk) yield chunk;
    first = false;
    if (rows.length < batchSize) return;

    const last = rows[rows.length - 1];
    if (last?._cursor_created_at == null || last?._cursor_id == null) {
      // Sem cursor valido nao ha como continuar sem repetir ou pular linhas.
      throw coded("export_cursor_invalid");
    }
    cursor = { createdAt: last._cursor_created_at, id: last._cursor_id };
  }
}

// ---- handler Express -------------------------------------------------------------------------------

function waitForDrainOrClose(res) {
  return new Promise((resolve) => {
    const done = () => {
      res.off("drain", done);
      res.off("close", done);
      resolve();
    };
    res.once("drain", done);
    res.once("close", done);
  });
}

const FILTER_ERROR_CODES = new Set(["invalid_filter", "invalid_date", "invalid_date_range"]);

export function createDispatchesCsvHandler({
  queryFn = defaultQuery,
  now = () => new Date(),
  batchSize = EXPORT_BATCH_SIZE,
} = {}) {
  return async function dispatchesCsvHandler(req, res) {
    let filters;
    try {
      filters = parseDispatchExportFilters(req.query || {});
    } catch (error) {
      if (FILTER_ERROR_CODES.has(error?.code)) {
        return res.status(400).json({ ok: false, error: error.code, ...(error.field && { field: error.field }) });
      }
      throw error;
    }

    let total = 0;
    const iterator = iterateDispatchCsv({
      queryFn,
      filters,
      batchSize,
      onBatch: (count) => {
        total += count;
      },
    })[Symbol.asyncIterator]();

    let first;
    try {
      first = await iterator.next();
    } catch (error) {
      console.error("[admin/notifications] dispatches csv export error:", error?.message || error);
      return res.status(500).json({ ok: false, error: "internal_error" });
    }

    console.log("[admin/notifications] dispatches csv export:start", {
      status: filters.status,
      channel: filters.channel,
      provider: filters.provider,
      from: filters.from,
      to: filters.to,
    });

    res.status(200);
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="notification-dispatches-${formatBrasiliaDate(now())}.csv"`
    );
    res.setHeader("Cache-Control", "no-store");
    res.setHeader("X-Content-Type-Options", "nosniff");

    let closed = false;
    res.on("close", () => {
      closed = true;
    });

    try {
      let step = first;
      while (!step.done) {
        if (closed) {
          await iterator.return?.();
          return undefined;
        }
        if (!res.write(step.value)) await waitForDrainOrClose(res);
        step = await iterator.next();
      }
      res.end();
      console.log("[admin/notifications] dispatches csv export:done", { rows: total });
    } catch (error) {
      // Cabecalhos ja foram enviados: derrubar a conexao evita um arquivo truncado parecendo completo.
      console.error("[admin/notifications] dispatches csv export aborted:", error?.message || error);
      res.destroy(error);
    }
    return undefined;
  };
}
