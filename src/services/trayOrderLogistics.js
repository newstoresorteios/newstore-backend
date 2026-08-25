// src/services/trayOrderLogistics.js
//
// Projeção READ-ONLY da logística do pedido Tray.
//
// DIVISÃO DE RESPONSABILIDADE (não mudar sem decisão de produto):
//   NewStore -> resgate, NSCréditos, pedido, histórico, conciliação.
//   Tray     -> separação, frete, transportadora, envio, rastreio, entrega.
//
// A NewStore NÃO persiste logística: não existe coluna de carrier, tracking
// ou shipping_status no PostgreSQL, e este arquivo não cria nenhuma. Ele só
// traduz o que a Tray devolveu AGORA, em uma consulta ao vivo.
//
// ─────────────── CAMPOS REAIS (auditoria read-only, GET /orders) ───────────────
//
// Levantados contra a loja real: 150 pedidos pela listagem + detalhe de 6
// pedidos ENVIADO/FINALIZADO. Nenhum campo aqui foi suposto por "API de
// ecommerce normalmente tem".
//
//   CAMPO                    EXEMPLO REAL              SIGNIFICADO                     CLIENTE  ADMIN
//   status                   "ENVIADO", "FINALIZADO",  Status COMERCIAL do pedido       não*    sim
//                            "AGUARDANDO PAGAMENTO"    (não é status logístico)
//   OrderStatus.type         "open"|"closed"|"canceled" Família do status               não*    sim
//   has_shipment             0 | 1                     Existe envio registrado          sim     sim
//   is_traceable             0 | 1                     Envio rastreável                 sim     sim
//   shipment                 "Sedex", "PENDENTE TRAY"  Forma/serviço de envio           sim     sim
//   shipment_integrator      "Correios"                Integrador/transportadora        sim     sim
//   shipment_date            "2026-06-26"              Data de postagem                 sim     sim
//   sending_date             "2026-06-26"              Idem (espelha shipment_date)     sim     sim
//   sending_code             "LW067310786US"           Código de rastreamento           sim     sim
//   tracking_url             "https://.../rastreio?…"  URL de rastreamento              sim     sim
//   estimated_delivery_date  "2026-07-10"              Previsão de entrega              sim**   sim**
//   modified                 "2026-08-06 12:19:36"     Última atualização do pedido     sim     sim
//   shipment_value           "250.58"                  Valor do frete                   não     sim
//   store_note               "Resgate Loja NS / …"      Identificador INTERNO            NUNCA   sim
//
//   * o status cru vai só para o admin (item 71): o cliente vê a fase
//     normalizada, nunca "AGUARDANDO PAGAMENTO" solto.
//   ** só quando existe envio de verdade — ver ESTIMATIVA abaixo.
//
// ─────────────── CAMPOS QUE A TRAY NÃO PREENCHE (não usar) ───────────────
//
//   delivered, delivered_status, delivery_date: existem no schema do detalhe
//     mas vieram VAZIOS em 6/6 pedidos ENVIADO/FINALIZADO reais. Não há
//     evidência de entrega, então NENHUMA fase "entregue" é emitida aqui.
//   delivery_time: veio "14", "31", "32" -- é PRAZO em dias, não horário de
//     entrega. Nunca tratar como comprovação de entrega.
//   FINALIZADO é fechamento comercial do pedido, NÃO prova de entrega.

/** Datas "vazias" que a Tray devolve em vez de null. */
function isBlank(value) {
  if (value == null) return true;
  const s = String(value).trim();
  return s === "" || s === "0" || s === "0000-00-00" || s === "0000-00-00 00:00:00";
}

function text(value, max = 120) {
  if (value == null) return null;
  const s = String(value).trim();
  return s ? s.slice(0, max) : null;
}

/** "2026-06-26" / "2026-08-06 12:19:36" -> string factual, ou null. */
function dateText(value) {
  if (isBlank(value)) return null;
  const s = String(value).trim();
  return /^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$/.test(s) ? s : null;
}

function flag(value) {
  return String(value ?? "").trim() === "1";
}

/**
 * Só aceita URL de rastreamento http(s) que a PRÓPRIA Tray devolveu.
 * Nunca montamos uma URL a partir do código de rastreio (item 27): sem
 * contrato factual, montar link é inventar.
 */
export function safeTrackingUrl(raw) {
  const s = text(raw, 500);
  if (!s) return null;
  try {
    const url = new URL(s);
    return url.protocol === "http:" || url.protocol === "https:" ? url.toString() : null;
  } catch {
    return null;
  }
}

/** Famílias de status do pedido, pela própria Tray (OrderStatus.type). */
function isCanceled(order) {
  const type = text(order?.OrderStatus?.type)?.toLowerCase();
  if (type === "canceled") return true;
  return /^cancelad/i.test(text(order?.status) || "");
}

/**
 * Evidência factual de que existe um envio. Qualquer um destes campos
 * preenchido é prova; nenhum deles preenchido significa "ainda não há
 * logística", NUNCA "em separação" (item 17: status comercial não vira
 * status logístico).
 */
function shippingEvidence(order) {
  const shipmentDate = dateText(order?.shipment_date) || dateText(order?.sending_date);
  const trackingCode = text(order?.sending_code, 60);
  const trackingUrl = safeTrackingUrl(order?.tracking_url);
  const hasShipment = flag(order?.has_shipment);
  return {
    shipmentDate,
    trackingCode,
    trackingUrl,
    hasShipment,
    any: Boolean(hasShipment || shipmentDate || trackingCode || trackingUrl),
  };
}

export const TRAY_LOGISTICS_PHASES = Object.freeze({
  RECEIVED: "received",
  SHIPPED: "shipped",
  CANCELED: "canceled",
});

/**
 * Normaliza a logística factual de um pedido Tray.
 *
 * Fases possíveis — e SÓ estas, porque só estas têm evidência no dado real:
 *   received  o pedido existe na Tray e ainda não tem envio registrado
 *   shipped   existe evidência de postagem (data, código ou URL de rastreio)
 *   canceled  a própria Tray classificou o pedido como cancelado
 *
 * NÃO existe fase "delivered": ver o cabeçalho deste arquivo — os campos de
 * entrega vêm vazios até em pedidos FINALIZADOS.
 *
 * Campos ausentes são OMITIDOS (item 26): nunca devolvemos `carrier: null`
 * só para completar a estrutura.
 */
export function normalizeTrayLogistics(order) {
  if (!order || typeof order !== "object") return null;

  const evidence = shippingEvidence(order);
  const canceled = isCanceled(order);

  const logistics = {};

  // Forma de envio: "PENDENTE TRAY" é o rótulo que a própria NewStore grava
  // na criação do pedido para satisfazer um campo obrigatório -- não é uma
  // forma de envio real e nunca deve ser mostrada como se fosse.
  const shipmentMethod = text(order?.shipment, 100);
  if (shipmentMethod && !/^pendente tray$/i.test(shipmentMethod)) {
    logistics.shipment_method = shipmentMethod;
  }

  const carrier = text(order?.shipment_integrator, 100);
  if (carrier) logistics.carrier = carrier;

  if (evidence.trackingCode) logistics.tracking_code = evidence.trackingCode;
  if (evidence.trackingUrl) logistics.tracking_url = evidence.trackingUrl;
  if (evidence.shipmentDate) logistics.shipped_at = evidence.shipmentDate;
  if (flag(order?.is_traceable)) logistics.is_traceable = true;

  // ESTIMATIVA: estimated_delivery_date vem preenchido em 100% dos pedidos,
  // inclusive nos que ainda não foram enviados -- no pedido real da Loja NS
  // ele veio IGUAL à data do pedido, ou seja, é um default, não uma previsão.
  // Por isso só expomos a previsão quando existe envio de verdade E a data é
  // posterior à data do pedido. Prazo inventado é pior que prazo ausente.
  const estimated = dateText(order?.estimated_delivery_date);
  const orderDate = dateText(order?.date);
  if (evidence.any && estimated && (!orderDate || estimated > orderDate)) {
    logistics.estimated_delivery_at = estimated;
  }

  const updatedAt = dateText(order?.modified);
  if (updatedAt) logistics.updated_at = updatedAt;

  if (canceled) {
    logistics.phase = TRAY_LOGISTICS_PHASES.CANCELED;
    logistics.label = "Pedido cancelado";
    return logistics;
  }

  if (evidence.any) {
    logistics.phase = TRAY_LOGISTICS_PHASES.SHIPPED;
    logistics.label = "Pedido enviado";
    return logistics;
  }

  logistics.phase = TRAY_LOGISTICS_PHASES.RECEIVED;
  logistics.label = "Pedido recebido pela Tray";
  logistics.hint = "Aguardando atualização da separação/envio.";
  return logistics;
}

/**
 * DTO do CLIENTE. Whitelist explícita: a resposta crua da Tray NUNCA chega ao
 * navegador. Sem status comercial cru (item 71), sem store_note (item 16),
 * sem Customer/CustomerAddress, sem valores financeiros do pedido, sem PII.
 */
export function buildCustomerTrayStatus(order) {
  const logistics = normalizeTrayLogistics(order);
  if (!logistics) return null;
  const id = text(order?.id, 40);
  return {
    available: true,
    order: id ? { id } : {},
    logistics,
  };
}

/**
 * Visão do ADMIN: a mesma normalização + os campos operacionais que só fazem
 * sentido para quem administra (status comercial cru da Tray, frete, origem).
 * Continua sendo whitelist -- Customer, CustomerAddress, ProductsSold,
 * pagamento e qualquer PII ficam de fora.
 */
export function buildAdminTrayOrderView(order) {
  if (!order || typeof order !== "object") return null;
  const view = {
    tray_order_id: text(order?.id, 40),
    status: text(order?.status, 60),
    status_type: text(order?.OrderStatus?.type, 30),
    payment_method: text(order?.payment_method, 60),
    point_sale: text(order?.point_sale, 60),
    shipment: text(order?.shipment, 100),
    shipment_value: text(order?.shipment_value, 30),
    total: text(order?.total, 30),
    created_at: dateText(order?.date),
    updated_at: dateText(order?.modified),
    logistics: normalizeTrayLogistics(order),
  };
  for (const key of Object.keys(view)) {
    if (view[key] == null) delete view[key];
  }
  return view;
}
