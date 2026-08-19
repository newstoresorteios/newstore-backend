// src/services/trayOrderClient.js
//
// Criação de pedido Tray REAL — POST /orders. Única mutação de pedido
// autorizada nesta camada (ver ALLOWED_MUTATIONS em trayCatalogClient.js).
//
// Contrato (schema oficial Tray, tray-tecnologia/tray-api-ai-plugin,
// skills/pedidos/schemas/pedido.create.json — cita
// https://developers.tray.com.br/#api-de-pedidos):
//   required: ["customer_id", "products"]
//   demais campos (shipping_method, shipping_cost, payment_method) são
//   OPCIONAIS — por isso NUNCA enviamos payment_method: não existe valor
//   documentado para "resgate sem cobrança real" (pix/boleto/cartão seriam
//   invenção), e o schema confirma que o campo pode simplesmente ser
//   omitido. Frete está fora do escopo desta fase (decisão do responsável)
//   — nunca enviamos shipping_method/shipping_cost.
//
// Identificação do resgate: campo oficial `notes` ("Observações livres do
// pedido") — é onde a Loja NS se identifica para quem olhar o pedido na
// Tray, sem usar nenhum campo fora do schema.
//
// PENDÊNCIA CONHECIDA (não inventada, documentada no relatório): o campo
// exato de variação dentro de `products` não foi confirmado explicitamente
// na documentação consultada. Usamos o id da própria variação como
// product_id — é o padrão mais consistente com o modelo de catálogo Tray já
// usado neste repositório (variantes são entidades endereçáveis própria
// para estoque/preço), mas isso deve ser confirmado no primeiro teste
// controlado antes de confiar nisso em volume.

import { trayMutationRequest } from "./trayMutationClient.js";
import { TrayCatalogError } from "./trayCatalogClient.js";

/**
 * @param {object} params
 * @param {string|number} params.customerId ID Tray do cliente (nunca users.id)
 * @param {Array<{trayProductId:string, trayVariantId?:string|null, quantity:number}>} params.items
 * @param {string} params.notes texto livre identificando o resgate (redemption_id, coupon_code)
 */
export async function createTrayOrder({ customerId, items, notes }, options = {}) {
  const cid = Number(customerId);
  if (!Number.isFinite(cid) || cid <= 0) throw new TrayCatalogError("customer_id_invalid", { status: 400 });

  const list = Array.isArray(items) ? items : [];
  if (!list.length) throw new TrayCatalogError("order_items_empty", { status: 400 });

  const products = list.map((item) => {
    const productId = Number(item.trayVariantId || item.trayProductId);
    const quantity = Number(item.quantity);
    if (!Number.isFinite(productId) || productId <= 0) throw new TrayCatalogError("order_item_product_id_invalid", { status: 400 });
    if (!Number.isFinite(quantity) || quantity <= 0) throw new TrayCatalogError("order_item_quantity_invalid", { status: 400 });
    return { product_id: productId, quantity };
  });

  const body = {
    Order: {
      customer_id: cid,
      products,
      notes: String(notes || "").slice(0, 1000),
      // Deliberadamente ausentes (não inventados): payment_method, shipping_method, shipping_cost.
    },
  };

  const result = await trayMutationRequest("TRAY_ORDER_CREATE", "POST", "/orders", body, options);

  const orderId = result?.id ?? result?.Order?.id ?? result?.order_id ?? result?.order?.id ?? null;
  if (!orderId) {
    throw new TrayCatalogError("tray_order_id_missing", { status: 502, publicDetails: { tray_body: result } });
  }

  return { orderId: String(orderId), raw: result };
}
