// src/services/trayCatalog.js
//
// Normalizacao do catalogo Tray + servico de leitura de catalogo.
//
// REGRA: a NewStore e OBSERVADORA do catalogo Tray.
// Nenhuma funcao aqui altera dado da Tray. Os campos factuais
// (available, available_in_store, stock, availability, has_variation,
// when_stock_runs_out) sao preservados exatamente como vieram.
// Os campos de APRESENTACAO ficam em `presentation` e sao derivados
// explicitamente desses factuais — ver derivePresentation().
//
// Esta camada nao conhece SQL e nao conhece reward_products.

import {
  fetchTrayProducts,
  fetchTrayProduct,
  fetchTrayVariants,
  fetchTrayBrands,
} from "./trayCatalogClient.js";

/** Valores de ProductSettings.when_stock_runs_out documentados pela Tray. */
export const WHEN_STOCK_RUNS_OUT = {
  DEACTIVATE: "deactivate_product",
  CONTINUE_IMMEDIATE: "continue_selling_immediate",
  EXTENDED_LEAD_TIME: "sell_extended_lead_time",
};

function toNullableString(value) {
  if (value === undefined || value === null) return null;
  const s = String(value).trim();
  return s === "" ? null : s;
}

function toNullableInt(value) {
  if (value === undefined || value === null || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

function toNullableFloat(value) {
  if (value === undefined || value === null || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

/** Tray usa 0/1. Preservamos o inteiro factual; boolean fica so na apresentacao. */
function toTrayFlagInt(value) {
  if (value === undefined || value === null || value === "") return null;
  if (value === true) return 1;
  if (value === false) return 0;
  const n = Number(value);
  return Number.isFinite(n) ? (n ? 1 : 0) : null;
}

function toBool(value) {
  return toTrayFlagInt(value) === 1;
}

/**
 * Tray devolve "YYYY-MM-DD HH:mm:ss" sem timezone.
 * Interpretamos como UTC — mesma convencao ja usada em services/tray.js.
 */
export function parseTrayDate(value) {
  const s = toNullableString(value);
  if (!s) return null;
  if (/^0{4}-0{2}-0{2}/.test(s)) return null;
  const iso = s.includes("T") ? s : `${s.replace(" ", "T")}Z`;
  const ms = new Date(iso).getTime();
  return Number.isFinite(ms) ? new Date(ms).toISOString() : null;
}

/** Desembrulha `{ Foo: {...} }` quando a Tray aninha o objeto. */
function unwrap(entry, key) {
  if (entry && typeof entry === "object" && entry[key] && typeof entry[key] === "object") {
    return entry[key];
  }
  return entry;
}

function imageUrlOf(entry) {
  const img = unwrap(entry, "ProductImage");
  if (!img || typeof img !== "object") return null;
  // Determinismo documentado: preferimos https; se so houver http, usamos http.
  return toNullableString(img.https) || toNullableString(img.http) || null;
}

/** Todas as imagens retornadas pela Tray, na ordem de retorno. Nunca inventa imagem. */
export function collectImages(raw) {
  const list = raw?.ProductImage;
  if (!Array.isArray(list)) return [];
  return list.map(imageUrlOf).filter(Boolean);
}

/**
 * Imagem principal: PRIMEIRA imagem retornada pela Tray, preferindo a URL https.
 * Sem imagem na Tray -> null (o frontend mostra placeholder local).
 */
export function pickPrimaryImage(raw) {
  return collectImages(raw)[0] ?? null;
}

function productUrlOf(raw) {
  const url = raw?.url;
  if (typeof url === "string") return toNullableString(url);
  if (url && typeof url === "object") {
    return toNullableString(url.https) || toNullableString(url.http) || null;
  }
  return null;
}

/**
 * Apresentacao derivada — NUNCA sobrescreve os factuais.
 *
 * Ordem de decisao, toda ela ancorada em campo factual da Tray:
 *   1. available === 0            -> a propria Tray desativou o produto
 *   2. available_in_store === 0   -> produto oculto na vitrine da loja
 *   3. estoque > 0                -> disponivel
 *   4. tem variacao com estoque   -> disponivel
 *   5. estoque <= 0               -> depende de ProductSettings.when_stock_runs_out
 *   6. estoque <= 0 + prazo       -> availability_days > 0 significa venda sob encomenda
 */
export function derivePresentation({
  tray_available,
  tray_available_in_store,
  stock,
  when_stock_runs_out,
  availability_days,
  availability_text: availabilityText,
  has_variation,
  variants,
} = {}) {
  if (toTrayFlagInt(tray_available) === 0) {
    return { is_available: false, reason: "unavailable_in_tray" };
  }
  if (toTrayFlagInt(tray_available_in_store) === 0) {
    return { is_available: false, reason: "hidden_in_store" };
  }

  const stockValue = toNullableInt(stock);
  if (stockValue !== null && stockValue > 0) {
    return { is_available: true, reason: "available" };
  }

  if (has_variation && Array.isArray(variants)) {
    const anyVariantInStock = variants.some((v) => {
      const vStock = toNullableInt(v?.stock);
      const vAvailable = toTrayFlagInt(v?.available ?? v?.tray_available);
      return vStock !== null && vStock > 0 && vAvailable !== 0;
    });
    if (anyVariantInStock) {
      return { is_available: true, reason: "available_in_variant" };
    }
  }

  const rule = toNullableString(when_stock_runs_out);
  if (rule === WHEN_STOCK_RUNS_OUT.CONTINUE_IMMEDIATE) {
    return { is_available: true, reason: "available_on_demand" };
  }
  if (rule === WHEN_STOCK_RUNS_OUT.EXTENDED_LEAD_TIME) {
    return { is_available: true, reason: "available_extended_lead_time" };
  }

  // Estoque zerado, mas a Tray continua marcando available=1 E informando um
  // prazo de entrega. E o caso de venda sob encomenda: marcar como indisponivel
  // aqui seria sobrepor o que a propria Tray afirma.
  // Observado na loja real: produto 15526, stock 0, available 1,
  // "Disponível em 45 dias úteis".
  //
  // DIVERGENCIA FACTUAL entre endpoints da Tray: `GET /products` (listagem)
  // NAO devolve `availability_days`, mas `GET /products/:id` devolve.
  // O texto `availability` vem nos dois — por isso ele tambem conta como sinal,
  // senao a listagem e o detalhe discordariam sobre o mesmo produto.
  const days = toNullableInt(availability_days);
  const hasLeadTimeStatement = (days !== null && days > 0) || toNullableString(availabilityText) !== null;
  if (hasLeadTimeStatement) {
    return { is_available: true, reason: "available_extended_lead_time" };
  }

  return { is_available: false, reason: "out_of_stock" };
}

export function normalizeTrayVariants(rawVariants) {
  if (!Array.isArray(rawVariants)) return [];

  return rawVariants.map((entry) => {
    const v = unwrap(entry, "Variant") || {};
    const values = Array.isArray(v.VariantValue)
      ? v.VariantValue.map((item) => {
          const vv = unwrap(item, "VariantValue") || {};
          return { type: toNullableString(vv.type), value: toNullableString(vv.value) };
        }).filter((x) => x.type || x.value)
      : [];

    return {
      variant_id: toNullableString(v.id),
      tray_product_id: toNullableString(v.product_id),
      reference: toNullableString(v.reference),
      ean: toNullableString(v.ean),
      price: toNullableFloat(v.price),
      stock: toNullableInt(v.stock),
      minimum_stock: toNullableInt(v.minimum_stock),
      tray_available: toTrayFlagInt(v.available),
      values,
    };
  });
}

export function normalizeTrayProduct(raw, { variants = null } = {}) {
  const settings = raw?.ProductSettings && typeof raw.ProductSettings === "object" ? raw.ProductSettings : {};

  const normalizedVariants = variants
    ? normalizeTrayVariants(variants)
    : normalizeTrayVariants(raw?.Variant);

  const factual = {
    tray_product_id: toNullableString(raw?.id),
    name: toNullableString(raw?.name),
    description_small: toNullableString(raw?.description_small),
    reference: toNullableString(raw?.reference),
    brand: toNullableString(raw?.brand),
    image_url: pickPrimaryImage(raw),
    images: collectImages(raw),
    tray_product_url: productUrlOf(raw),

    // Campos factuais de disponibilidade — preservados como a Tray devolveu.
    stock: toNullableInt(raw?.stock),
    tray_available: toTrayFlagInt(raw?.available),
    tray_available_in_store: toTrayFlagInt(raw?.available_in_store),
    availability_text: toNullableString(raw?.availability),
    availability_days: toNullableInt(raw?.availability_days),
    has_variation: toBool(raw?.has_variation),
    when_stock_runs_out: toNullableString(settings.when_stock_runs_out ?? raw?.when_stock_runs_out),
    order_days_availability: toNullableInt(settings.order_days_availability ?? raw?.order_days_availability),

    // Referencia administrativa apenas.
    tray_price: toNullableFloat(raw?.price),
    tray_modified_at: parseTrayDate(raw?.modified),

    variants: normalizedVariants,
  };

  return { ...factual, presentation: derivePresentation(factual) };
}

/* ─────────────────────────── Servico de catalogo ─────────────────────────── */

/**
 * Listagem paginada do catalogo Tray.
 * Usa o endpoint de listagem com paginacao — NUNCA uma consulta por linha da tabela.
 */
export async function listTrayCatalog(params = {}, options = {}) {
  const { rawProducts, paging } = await fetchTrayProducts(params, options);
  return { items: rawProducts.map((raw) => normalizeTrayProduct(raw)), paging };
}

/**
 * Consulta individual — usada ao abrir detalhe, ao publicar e ao sincronizar.
 * `withVariants` so dispara a chamada extra quando o produto realmente tem variacao.
 */
export async function getTrayCatalogProduct(trayProductId, options = {}) {
  const raw = await fetchTrayProduct(trayProductId, options);
  const normalized = normalizeTrayProduct(raw);

  if (options.withVariants && normalized.has_variation && normalized.variants.length === 0) {
    const rawVariants = await fetchTrayVariants(trayProductId, options);
    return normalizeTrayProduct(raw, { variants: rawVariants });
  }

  return normalized;
}

export async function listTrayBrands(options = {}) {
  return await fetchTrayBrands(options);
}
