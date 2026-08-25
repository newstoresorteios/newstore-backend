// src/services/rewardRedemptionAdmin.js
//
// READ MODEL administrativo do resgate da Loja de Premios NS.
//
// DIVISAO DE RESPONSABILIDADE (nao mudar sem decisao de produto):
//   NewStore administra o RESGATE (creditos, saga, evidencia, ledger).
//   Tray administra a LOGISTICA (separacao, envio, entrega, rastreio).
// Por isso este modulo NUNCA inventa estado de entrega local (pendente/
// enviado/entregue): esses estados nao existem no schema da NewStore e
// pertencem ao pedido Tray.
//
// ESTE ARQUIVO E SOMENTE LEITURA. Nenhuma funcao aqui escreve em
// reward_redemptions, coupon_balance_history ou na Tray. O admin observa,
// nunca muta o contrato financeiro.

import { query as defaultQuery } from "../db.js";
import { sanitizeTrayErrorBody } from "./rewardRedemption.js";
import { getTrayOrder } from "./trayOrderClient.js";
import { buildAdminTrayOrderView } from "./trayOrderLogistics.js";

const MAX_PAGE_SIZE = 100;

export class RedemptionAdminError extends Error {
  constructor(code, { status = 400, details = null } = {}) {
    super(code);
    this.name = "RedemptionAdminError";
    this.code = code;
    this.status = status;
    this.details = details;
  }
}

/* ─────────────────────────── Catalogo de status ─────────────────────────── */

/**
 * Os UNICOS status que existem — derivados do CHECK factual de
 * public.reward_redemptions (migrations 031 -> 032 -> 034) cruzado com o
 * que rewardRedemption.js realmente grava. Nenhum status inventado.
 *
 *   group             agrupamento usado pelas metricas do relatorio
 *   is_success        o cliente recebeu o beneficio (pedido Tray criado)
 *   credits_committed o debito de NSCreditos PERMANECE valendo
 *   is_compensated    o debito foi revertido por lancamento de compensacao
 *   severity          intencao visual do badge (o admin escolhe a cor)
 */
export const REDEMPTION_STATUS_CATALOG = Object.freeze([
  {
    status: "processing",
    label: "Processando",
    description: "Resgate criado. Nenhum NSCredito debitado ainda.",
    group: "in_progress",
    severity: "neutral",
    is_success: false,
    credits_committed: false,
    is_compensated: false,
  },
  {
    status: "credits_reserved",
    label: "Créditos debitados",
    description: "Débito aplicado no ledger. O pedido Tray ainda não foi solicitado.",
    group: "in_progress",
    severity: "info",
    is_success: false,
    credits_committed: true,
    is_compensated: false,
  },
  {
    status: "tray_order_pending",
    label: "Enviando à Tray",
    description: "Débito aplicado e criação do pedido Tray em andamento.",
    group: "in_progress",
    severity: "info",
    is_success: false,
    credits_committed: true,
    is_compensated: false,
  },
  {
    status: "tray_order_created",
    label: "Pedido criado (legado)",
    description:
      "Valor previsto na migration 031 e mantido no CHECK por compatibilidade. Nenhum caminho de código atual grava este status.",
    group: "legacy",
    severity: "neutral",
    is_success: false,
    credits_committed: true,
    is_compensated: false,
  },
  {
    status: "confirmed",
    label: "Confirmado",
    description: "Pedido Tray criado com sucesso. NSCréditos efetivamente consumidos.",
    group: "confirmed",
    severity: "success",
    is_success: true,
    credits_committed: true,
    is_compensated: false,
  },
  {
    status: "failed",
    label: "Falhou",
    description: "O débito não pôde ser aplicado. Nenhum NSCrédito saiu do saldo.",
    group: "failed",
    severity: "error",
    is_success: false,
    credits_committed: false,
    is_compensated: false,
  },
  {
    status: "compensated",
    label: "Compensado",
    description: "A Tray recusou o pedido de forma determinística. NSCréditos devolvidos ao cliente.",
    group: "compensated",
    severity: "neutral",
    is_success: false,
    credits_committed: false,
    is_compensated: true,
  },
  {
    status: "reconciliation_required",
    label: "Requer conciliação",
    description:
      "Resultado ambíguo na Tray (timeout/rede). Os NSCréditos seguem debitados e NUNCA são compensados às cegas — precisa de conferência manual do pedido na Tray.",
    group: "reconciliation_required",
    severity: "warning",
    is_success: false,
    credits_committed: true,
    is_compensated: false,
  },
  {
    status: "blocked_tray_contract_pending",
    label: "Bloqueado: contrato Tray",
    description: "Criação de pedido Tray indisponível no momento da tentativa. NSCréditos devolvidos.",
    group: "blocked",
    severity: "warning",
    is_success: false,
    credits_committed: false,
    is_compensated: true,
  },
  {
    status: "blocked_tray_customer_unmapped",
    label: "Bloqueado: cliente não mapeado (legado)",
    description:
      "Status histórico da migration 032. Substituído pela resolução real de Customer Tray; nenhum código atual o grava. NSCréditos devolvidos.",
    group: "blocked",
    severity: "warning",
    is_success: false,
    credits_committed: false,
    is_compensated: true,
  },
  {
    status: "blocked_tray_profile_incomplete",
    label: "Bloqueado: perfil incompleto",
    description:
      "Nenhum Customer Tray encontrado e o perfil NewStore ainda não tem os dados exigidos para criar um. NSCréditos devolvidos.",
    group: "blocked",
    severity: "warning",
    is_success: false,
    credits_committed: false,
    is_compensated: true,
  },
  {
    status: "blocked_tray_customer_ambiguous",
    label: "Bloqueado: cliente ambíguo",
    description:
      "Identidade Tray ambígua (mais de um Customer, ou e-mail e CPF apontando para cadastros diferentes). Nunca escolhida arbitrariamente. NSCréditos devolvidos.",
    group: "blocked",
    severity: "warning",
    is_success: false,
    credits_committed: false,
    is_compensated: true,
  },
]);

const STATUS_BY_CODE = new Map(REDEMPTION_STATUS_CATALOG.map((s) => [s.status, s]));

/** Status validos para filtro/consulta. Nada fora do CHECK do banco passa. */
export const REDEMPTION_STATUSES = Object.freeze(REDEMPTION_STATUS_CATALOG.map((s) => s.status));

function statusesInGroup(group) {
  return REDEMPTION_STATUS_CATALOG.filter((s) => s.group === group).map((s) => s.status);
}

const IN_PROGRESS_STATUSES = Object.freeze(statusesInGroup("in_progress"));
const BLOCKED_STATUSES = Object.freeze(statusesInGroup("blocked"));

/** Descricao administrativa de um status (nunca inventa rotulo). */
export function describeRedemptionStatus(status) {
  return STATUS_BY_CODE.get(status) || null;
}

/* ─────────────────────────── Helpers ─────────────────────────── */

function resolveDeps(deps = {}) {
  return {
    query: deps.query || defaultQuery,
    getTrayOrder: deps.getTrayOrder || getTrayOrder,
  };
}

function parsePaging({ page = 1, limit = 20 } = {}) {
  const safeLimit = Math.min(Math.max(Number(limit) || 20, 1), MAX_PAGE_SIZE);
  const safePage = Math.max(Number(page) || 1, 1);
  return { page: safePage, limit: safeLimit, offset: (safePage - 1) * safeLimit };
}

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Limite de periodo. Aceita "YYYY-MM-DD" (dia inteiro no fim do intervalo) ou
 * um timestamp ISO completo. Valor irreconhecivel e IGNORADO — nunca vira uma
 * data inventada que silenciosamente esconde linhas do relatorio.
 */
export function parseDateBoundary(raw, { endOfDay = false } = {}) {
  const s = String(raw ?? "").trim();
  if (!s) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {
    const base = new Date(`${s}T00:00:00.000Z`);
    if (Number.isNaN(base.getTime())) return null;
    if (endOfDay) base.setUTCDate(base.getUTCDate() + 1);
    return base.toISOString();
  }
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function toIso(value) {
  if (!value) return null;
  return value instanceof Date ? value.toISOString() : String(value);
}

function toInt(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : 0;
}

/** Centavos do ledger -> NSCreditos exibidos (mesma regra do cupom). */
function centsToCredits(cents) {
  const n = Number(cents);
  return Number.isFinite(n) ? Math.trunc(n) / 100 : 0;
}

/**
 * Monta o WHERE compartilhado por listagem e relatorio a partir dos mesmos
 * filtros — uma unica definicao de "quais resgates entram", nunca duas.
 */
function buildRedemptionFilters({ q = "", status = "", from = "", to = "" } = {}, params = []) {
  const where = [];

  const statusList = (Array.isArray(status) ? status : String(status ?? "").split(","))
    .map((s) => String(s ?? "").trim())
    .filter(Boolean);
  if (statusList.length) {
    const known = statusList.filter((s) => STATUS_BY_CODE.has(s));
    // Status desconhecido nunca "abre" o filtro: filtra por nada.
    params.push(known);
    where.push(`r.status = any($${params.length}::text[])`);
  }

  const fromIso = parseDateBoundary(from);
  if (fromIso) {
    params.push(fromIso);
    where.push(`r.created_at >= $${params.length}`);
  }
  const toIso_ = parseDateBoundary(to, { endOfDay: true });
  if (toIso_) {
    params.push(toIso_);
    where.push(`r.created_at < $${params.length}`);
  }

  // Busca administrativa: id do resgate, id do pedido Tray, id do usuario,
  // nome e e-mail. NUNCA CPF — PII que a operacao nao precisa e que este
  // painel jamais expoe (ver PII no relatorio da tarefa).
  const term = String(q ?? "").trim();
  if (term) {
    const clauses = [];
    params.push(`%${term}%`);
    const like = `$${params.length}`;
    clauses.push(`u.name ILIKE ${like}`);
    clauses.push(`u.email ILIKE ${like}`);

    if (UUID_RE.test(term)) {
      params.push(term);
      clauses.push(`r.id = $${params.length}::uuid`);
    }
    if (/^\d+$/.test(term)) {
      params.push(Number(term));
      clauses.push(`u.id = $${params.length}`);
      params.push(term);
      clauses.push(`r.tray_order_id = $${params.length}`);
    }
    where.push(`(${clauses.join(" OR ")})`);
  }

  return { where, params };
}

/* ─────────────────────────── Listagem ─────────────────────────── */

function mapListRow(row, itemCounts) {
  return {
    id: row.id,
    status: row.status,
    user: {
      id: toInt(row.user_id),
      name: row.user_name,
      email: row.user_email,
    },
    credits_amount: Number(row.credits_amount),
    item_count: toInt(itemCounts.get(row.id) ?? 0),
    tray_order_id: row.tray_order_id || null,
    created_at: toIso(row.created_at),
    updated_at: toIso(row.updated_at),
  };
}

/**
 * Listagem paginada de resgates reais.
 *
 * Sem N+1: 3 consultas fixas independentemente do tamanho da pagina —
 * (1) a pagina, com JOIN em users; (2) o total; (3) a contagem de itens de
 * TODA a pagina em uma unica agregacao por `= any($ids)`. Nunca uma query
 * por linha, e NUNCA uma chamada a Tray para renderizar a listagem.
 */
export async function listAdminRedemptions(filters = {}, deps = {}) {
  const d = resolveDeps(deps);
  const paging = parsePaging(filters);

  const params = [];
  const { where } = buildRedemptionFilters(filters, params);
  const whereSql = where.length ? `where ${where.join(" and ")}` : "";

  const pageParams = [...params];
  pageParams.push(paging.limit);
  const limitParam = `$${pageParams.length}`;
  pageParams.push(paging.offset);
  const offsetParam = `$${pageParams.length}`;

  const [page, count] = await Promise.all([
    d.query(
      `select r.id,
              r.status,
              r.credits_amount,
              r.tray_order_id,
              r.created_at,
              r.updated_at,
              u.id as user_id,
              coalesce(nullif(u.name,''), u.email, '-') as user_name,
              u.email as user_email
         from public.reward_redemptions r
         join public.users u on u.id = r.user_id
        ${whereSql}
        order by r.created_at desc
        limit ${limitParam} offset ${offsetParam}`,
      pageParams
    ),
    d.query(
      `select count(*)::int as total
         from public.reward_redemptions r
         join public.users u on u.id = r.user_id
        ${whereSql}`,
      params
    ),
  ]);

  const ids = page.rows.map((r) => r.id);
  const itemCounts = new Map();
  if (ids.length) {
    const counts = await d.query(
      `select redemption_id, count(*)::int as item_count
         from public.reward_redemption_items
        where redemption_id = any($1::uuid[])
        group by redemption_id`,
      [ids]
    );
    for (const row of counts.rows) itemCounts.set(row.redemption_id, row.item_count);
  }

  const total = toInt(count.rows[0]?.total);
  return {
    items: page.rows.map((row) => mapListRow(row, itemCounts)),
    paging: {
      page: paging.page,
      limit: paging.limit,
      total,
      total_pages: total === 0 ? 0 : Math.ceil(total / paging.limit),
    },
    statuses: REDEMPTION_STATUS_CATALOG,
  };
}

/* ─────────────────────────── Detalhe ─────────────────────────── */

/** Somente os campos do endereco que a operacao precisa ver. */
const ADDRESS_SNAPSHOT_FIELDS = Object.freeze([
  "recipient_name",
  "zipcode",
  "street",
  "number",
  "complement",
  "neighborhood",
  "city",
  "state",
  "country",
]);

export function mapAddressSnapshot(snapshot) {
  if (!snapshot || typeof snapshot !== "object") return null;
  const out = {};
  let hasAny = false;
  for (const key of ADDRESS_SNAPSHOT_FIELDS) {
    const value = snapshot[key];
    const s = value == null ? null : String(value).trim() || null;
    out[key] = s;
    if (s) hasAny = true;
  }
  return hasAny ? out : null;
}

/**
 * Chaves de reward_redemption_events.meta que podem chegar ao painel.
 * Tudo que nao esta aqui e DESCARTADO — nunca "imprime o meta cru".
 */
const EVENT_META_ALLOWED_KEYS = Object.freeze(["http_status", "tray_error_code", "tray_body"]);

const MAX_META_MESSAGES = 20;

function flattenMetaMessages(value, path = [], out = []) {
  if (out.length >= MAX_META_MESSAGES || value == null) return out;
  if (Array.isArray(value)) {
    for (const [i, v] of value.slice(0, 10).entries()) flattenMetaMessages(v, [...path, String(i)], out);
    return out;
  }
  if (typeof value === "object") {
    for (const [k, v] of Object.entries(value).slice(0, 20)) flattenMetaMessages(v, [...path, k], out);
    return out;
  }
  const s = String(value).trim();
  if (!s) return out;
  out.push(path.length ? `${path.join(".")}: ${s.slice(0, 200)}` : s.slice(0, 200));
  return out;
}

/**
 * Meta seguro para exibicao: whitelist de chaves + re-sanitizacao do corpo
 * da Tray (defesa em profundidade contra linhas gravadas por versoes
 * anteriores) + achatamento em mensagens curtas. NUNCA devolve token,
 * Authorization, senha, cookie, CPF, URL de banco ou objeto cru.
 */
export function buildAdminEventMeta(rawMeta) {
  let meta = rawMeta;
  if (typeof meta === "string") {
    try {
      meta = JSON.parse(meta);
    } catch {
      return null;
    }
  }
  if (!meta || typeof meta !== "object" || Array.isArray(meta)) return null;

  // Whitelist estrita: so estas chaves sao sequer lidas. Qualquer outra
  // (inclusive gravada por versoes futuras/antigas) e ignorada por completo.
  const out = {};
  for (const key of EVENT_META_ALLOWED_KEYS) {
    const value = meta[key];
    if (value == null) continue;

    if (key === "http_status") {
      if (Number.isFinite(Number(value))) out.http_status = Number(value);
      continue;
    }
    if (key === "tray_error_code") {
      const s = String(value).trim();
      if (s) out.tray_error_code = s.slice(0, 120);
      continue;
    }
    // tray_body: re-sanitizado (defesa em profundidade contra linhas
    // gravadas por versoes anteriores) e achatado em mensagens curtas.
    const messages = flattenMetaMessages(sanitizeTrayErrorBody(value));
    if (messages.length) out.tray_messages = messages;
  }
  return Object.keys(out).length ? out : null;
}

function mapItemRow(row) {
  return {
    id: row.id,
    reward_product_id: row.reward_product_id || null,
    tray_product_id: row.tray_product_id,
    tray_variant_id: row.tray_variant_id || null,
    product_name: row.product_name_snapshot,
    variant_name: row.variant_name_snapshot || null,
    image_url: row.image_url_snapshot || null,
    quantity: toInt(row.quantity),
    nscredits_unit_price: Number(row.nscredits_unit_price_snapshot),
    nscredits_total: Number(row.nscredits_total_snapshot),
  };
}

function mapEventRow(row) {
  return {
    id: Number(row.id),
    from_status: row.from_status || null,
    to_status: row.to_status,
    reason: row.reason || null,
    meta: buildAdminEventMeta(row.meta),
    created_at: toIso(row.created_at),
  };
}

function mapLedgerRow(row) {
  const deltaCents = toInt(row.delta_cents);
  return {
    id: Number(row.id),
    event_type: row.event_type,
    operation: deltaCents >= 0 ? "credit" : "debit",
    delta_cents: deltaCents,
    delta: centsToCredits(deltaCents),
    balance_before_cents: toInt(row.balance_before_cents),
    balance_before: centsToCredits(row.balance_before_cents),
    balance_after_cents: toInt(row.balance_after_cents),
    balance_after: centsToCredits(row.balance_after_cents),
    created_at: toIso(row.created_at),
  };
}

/**
 * Integridade financeira do resgate (item 20 da tarefa): SOMENTE LEITURA.
 * O saldo nunca e recalculado "na mao" — os numeros vem do proprio ledger
 * canonico (coupon_balance_history), que ja grava saldo antes/depois.
 *
 *   confirmado / requer conciliacao -> espera-se DEBITO e nenhuma compensacao
 *   compensado / bloqueado          -> espera-se DEBITO + COMPENSACAO igual
 *   falhou / processando            -> espera-se nenhum lancamento
 */
export function summarizeRedemptionLedger(entries, status) {
  const meta = STATUS_BY_CODE.get(status) || null;
  let debitCents = 0;
  let compensationCents = 0;
  for (const e of entries) {
    if (e.delta_cents < 0) debitCents += -e.delta_cents;
    else compensationCents += e.delta_cents;
  }

  let expectation = "no_movement";
  if (meta?.is_compensated) expectation = "debit_and_compensation";
  else if (meta?.credits_committed) expectation = "debit_only";

  let matches = true;
  if (expectation === "debit_only") matches = debitCents > 0 && compensationCents === 0;
  else if (expectation === "debit_and_compensation") matches = debitCents > 0 && compensationCents === debitCents;
  else matches = debitCents === 0 && compensationCents === 0;

  return {
    entries,
    debit_cents: debitCents,
    debit: centsToCredits(debitCents),
    compensation_cents: compensationCents,
    compensation: centsToCredits(compensationCents),
    net_cents: compensationCents - debitCents,
    net: centsToCredits(compensationCents - debitCents),
    balance_before_cents: entries.length ? entries[0].balance_before_cents : null,
    balance_after_cents: entries.length ? entries[entries.length - 1].balance_after_cents : null,
    expectation,
    matches_expectation: matches,
  };
}

/**
 * Visao administrativa completa de UM resgate. 4 consultas fixas.
 * NAO consulta a Tray — o status externo do pedido tem endpoint proprio
 * (getAdminRedemptionTrayOrder), acionado sob demanda.
 */
export async function getAdminRedemptionDetail(redemptionId, deps = {}) {
  const d = resolveDeps(deps);
  const id = String(redemptionId ?? "").trim();
  if (!UUID_RE.test(id)) throw new RedemptionAdminError("redemption_not_found", { status: 404 });

  const head = await d.query(
    `select r.*,
            u.id as user_id,
            coalesce(nullif(u.name,''), u.email, '-') as user_name,
            u.email as user_email
       from public.reward_redemptions r
       join public.users u on u.id = r.user_id
      where r.id = $1::uuid`,
    [id]
  );
  if (!head.rows.length) throw new RedemptionAdminError("redemption_not_found", { status: 404 });
  const row = head.rows[0];

  const [items, events, ledger] = await Promise.all([
    d.query(
      `select * from public.reward_redemption_items where redemption_id = $1::uuid order by created_at asc, id asc`,
      [id]
    ),
    d.query(
      `select * from public.reward_redemption_events where redemption_id = $1::uuid order by created_at asc, id asc`,
      [id]
    ),
    d.query(
      `select id, event_type, delta_cents, balance_before_cents, balance_after_cents, created_at
         from public.coupon_balance_history
        where redemption_id = $1::uuid
        order by created_at asc, id asc`,
      [id]
    ),
  ]);

  const ledgerEntries = ledger.rows.map(mapLedgerRow);

  return {
    redemption: {
      id: row.id,
      status: row.status,
      status_info: STATUS_BY_CODE.get(row.status) || null,
      credits_amount: Number(row.credits_amount),
      coupon_value_before_cents: toInt(row.coupon_value_before_cents),
      coupon_value_after_cents: row.coupon_value_after_cents == null ? null : toInt(row.coupon_value_after_cents),
      coupon_code_snapshot: row.coupon_code_snapshot || null,
      failure_reason: row.failure_reason || null,
      tray_order_id: row.tray_order_id || null,
      shipping_snapshot: row.shipping_snapshot || null,
      created_at: toIso(row.created_at),
      updated_at: toIso(row.updated_at),
    },
    // PII: nome e e-mail bastam para a operacao. CPF/telefone nunca saem daqui.
    user: { id: toInt(row.user_id), name: row.user_name, email: row.user_email },
    items: items.rows.map(mapItemRow),
    // Endereco ESCOLHIDO NO RESGATE (snapshot), nunca o endereco atual do
    // usuario: o historico do pedido nao muda quando o cliente se muda.
    address: mapAddressSnapshot(row.address_snapshot),
    events: events.rows.map(mapEventRow),
    ledger: summarizeRedemptionLedger(ledgerEntries, row.status),
  };
}

/* ─────────────────────────── Pedido Tray (READ-ONLY) ─────────────────────────── */

/**
 * Visao administrativa do pedido Tray.
 *
 * Delega ao normalizador COMPARTILHADO (trayOrderLogistics.js), construido a
 * partir dos campos comprovados numa auditoria read-only de pedidos reais --
 * os mesmos campos que alimentam o acompanhamento do cliente. Admin e cliente
 * compartilham a normalizacao, nunca a autorizacao.
 */
export function mapTrayOrderForAdmin(raw) {
  return buildAdminTrayOrderView(raw);
}

/**
 * Estado atual do pedido na Tray — SOMENTE GET.
 *
 * Nunca e chamado pela listagem (uma chamada externa por linha seria N+1 de
 * rede); so ao abrir o detalhe / clicar em atualizar. O Admin NewStore JAMAIS
 * altera pedido, status, frete, estoque ou preco na Tray.
 */
export async function getAdminRedemptionTrayOrder(redemptionId, deps = {}) {
  const d = resolveDeps(deps);
  const id = String(redemptionId ?? "").trim();
  if (!UUID_RE.test(id)) throw new RedemptionAdminError("redemption_not_found", { status: 404 });

  const { rows } = await d.query(`select tray_order_id from public.reward_redemptions where id = $1::uuid`, [id]);
  if (!rows.length) throw new RedemptionAdminError("redemption_not_found", { status: 404 });

  const trayOrderId = rows[0].tray_order_id || null;
  if (!trayOrderId) throw new RedemptionAdminError("tray_order_not_created", { status: 409 });

  // getTrayOrder (GET /orders/:id): `/orders/:id/full` responde 404 nesta
  // loja -- ver o achado documentado em trayOrderClient.js.
  const full = await d.getTrayOrder(trayOrderId);
  return {
    tray_order_id: String(trayOrderId),
    order: mapTrayOrderForAdmin(full?.raw),
    fetched_at: new Date().toISOString(),
  };
}

/* ─────────────────────────── Relatorios ─────────────────────────── */

/**
 * Metricas reais do resgate.
 *
 * REGRA CRITICA: "NSCreditos resgatados" conta SOMENTE `confirmed`. Uma
 * tentativa compensada teve DEBITO + COMPENSACAO no ledger (efeito liquido
 * zero) e NUNCA pode aparecer como credito efetivamente consumido — ela
 * aparece apenas no proprio card de compensados.
 */
export async function getRedemptionReportMetrics(filters = {}, deps = {}) {
  const d = resolveDeps(deps);
  const params = [];
  const { where } = buildRedemptionFilters(filters, params);
  const whereSql = where.length ? `where ${where.join(" and ")}` : "";

  const inProgressParam = `$${params.length + 1}`;
  const blockedParam = `$${params.length + 2}`;
  const aggParams = [...params, IN_PROGRESS_STATUSES, BLOCKED_STATUSES];

  const [agg, byStatus] = await Promise.all([
    d.query(
      `select
         count(*)::int as total_attempts,
         count(*) filter (where r.status = 'confirmed')::int as confirmed_redemptions,
         count(distinct r.user_id) filter (where r.status = 'confirmed')::int as unique_customers,
         coalesce(sum(r.credits_amount) filter (where r.status = 'confirmed'), 0)::bigint as credits_redeemed,
         count(*) filter (where r.tray_order_id is not null)::int as tray_orders_created,
         count(*) filter (where r.status = any(${inProgressParam}::text[]))::int as in_progress,
         count(*) filter (where r.status = 'reconciliation_required')::int as reconciliation_required,
         count(*) filter (where r.status = 'compensated')::int as compensated,
         count(*) filter (where r.status = 'failed')::int as failed,
         count(*) filter (where r.status = any(${blockedParam}::text[]))::int as blocked,
         coalesce(sum(r.credits_amount) filter (where r.status = 'compensated'), 0)::bigint as credits_compensated
       from public.reward_redemptions r
       join public.users u on u.id = r.user_id
       ${whereSql}`,
      aggParams
    ),
    d.query(
      `select r.status, count(*)::int as total
         from public.reward_redemptions r
         join public.users u on u.id = r.user_id
        ${whereSql}
        group by r.status`,
      params
    ),
  ]);

  const row = agg.rows[0] || {};
  const counts = Object.fromEntries(REDEMPTION_STATUSES.map((s) => [s, 0]));
  for (const r of byStatus.rows) counts[r.status] = toInt(r.total);

  return {
    total_attempts: toInt(row.total_attempts),
    confirmed_redemptions: toInt(row.confirmed_redemptions),
    unique_customers: toInt(row.unique_customers),
    credits_redeemed: Number(row.credits_redeemed || 0),
    tray_orders_created: toInt(row.tray_orders_created),
    in_progress: toInt(row.in_progress),
    reconciliation_required: toInt(row.reconciliation_required),
    compensated: toInt(row.compensated),
    failed: toInt(row.failed),
    blocked: toInt(row.blocked),
    credits_compensated: Number(row.credits_compensated || 0),
    by_status: counts,
  };
}

/**
 * Relatorio da aba Relatorios: metricas + os ultimos resgates.
 *
 * Os "ultimos resgates" REUSAM listAdminRedemptions — mesma query, mesmo
 * mapeamento, mesma definicao de filtro. Nunca uma segunda implementacao
 * que possa divergir da listagem.
 */
export async function getRedemptionReport(filters = {}, deps = {}) {
  const recentLimit = Math.min(Math.max(Number(filters.recent_limit) || 5, 1), 20);
  const [metrics, recent] = await Promise.all([
    getRedemptionReportMetrics(filters, deps),
    listAdminRedemptions({ ...filters, page: 1, limit: recentLimit }, deps),
  ]);

  return {
    redemptions: metrics,
    recent: recent.items,
    statuses: REDEMPTION_STATUS_CATALOG,
  };
}

/* ─────────────────────────── Status operacional ─────────────────────────── */

/**
 * Evidencia factual de que o webhook de pedido da Tray realmente ENTREGOU
 * alguma notificacao. A rota existir NAO prova ativacao externa: a unica
 * prova persistida hoje e um lancamento DIRECT_TRAY_SPEND no ledger, gravado
 * exclusivamente por handleTrayOrderWebhook. Sem lancamento, reportamos
 * "nao comprovada" — nunca "webhook ativo" so porque o endpoint existe.
 */
export async function getTrayOrderWebhookEvidence(deps = {}) {
  const d = resolveDeps(deps);
  const { rows } = await d.query(
    `select count(*)::int as total, max(created_at) as last_event_at
       from public.coupon_balance_history
      where event_type = 'DIRECT_TRAY_SPEND'`
  );
  const total = toInt(rows[0]?.total);
  return {
    route: "ready",
    delivery: total > 0 ? "verified" : "unverified",
    events_total: total,
    last_event_at: toIso(rows[0]?.last_event_at),
  };
}

/** Numeros factuais dos pedidos Tray criados pelo resgate (PostgreSQL, sem Tray). */
export async function getTrayOrdersModuleStatus(deps = {}) {
  const d = resolveDeps(deps);
  const { rows } = await d.query(
    `select count(*)::int as orders_created,
            max(updated_at) filter (where tray_order_id is not null) as last_order_at
       from public.reward_redemptions
      where tray_order_id is not null`
  );
  return {
    orders_created: toInt(rows[0]?.orders_created),
    last_order_at: toIso(rows[0]?.last_order_at),
  };
}
