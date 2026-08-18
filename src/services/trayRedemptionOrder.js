// src/services/trayRedemptionOrder.js
//
// UNICO ponto de integracao que falta para o resgate real virar um pedido
// Tray de verdade. Deliberadamente NAO IMPLEMENTADO — ver relatorio da
// Fase E ("hard gate", item 11/61 do pedido original).
//
// Por que parado aqui, e nao adivinhado:
//   1. O contrato de "Cadastrar Pedido" da Tray nao documenta, em nenhuma
//      fonte oficial auditada, um payment_type/payment_method que
//      represente "resgate por creditos" ou "pedido ja liquidado
//      externamente" — o enum encontrado e limitado a gateways reais
//      (credit_card, boleto, pix, transfer, deposit). Inventar um valor
//      aqui seria a "gambiarra financeira" que o pedido original proibiu
//      explicitamente (item 11).
//   2. A reconciliacao de gasto DIRETO do cupom na Tray (fora da Loja NS)
//      depende do webhook de `order`, cuja ativacao para ESTA loja exige
//      abrir chamado no suporte Tray — uma acao de conta, fora do alcance
//      de qualquer mudanca de codigo.
//
// O que ESTE arquivo entrega mesmo assim: um ponto de integracao unico,
// com um contrato de erro estavel, para que a saga em rewardRedemption.js
// (criacao de redemption, debito atomico, compensacao, timeout ambiguo)
// possa ser construida e testada de verdade HOJE — sem inventar nada do
// lado Tray. Quando a decisao de payment_type/webhook estiver resolvida,
// so esta funcao precisa mudar.

export class TrayOrderNotImplementedError extends Error {
  constructor(reason = "tray_order_contract_pending") {
    super(reason);
    this.name = "TrayOrderNotImplementedError";
    this.code = reason;
    // Deterministico: sabemos com certeza que NENHUMA chamada saiu para a
    // Tray, entao e seguro compensar (creditar de volta) imediatamente.
    this.ambiguous = false;
  }
}

export class TrayOrderAmbiguousError extends Error {
  constructor(reason = "tray_order_result_ambiguous") {
    super(reason);
    this.name = "TrayOrderAmbiguousError";
    this.code = reason;
    // Timeout ou resposta inconclusiva: NAO sabemos se o pedido foi criado.
    // Nunca compensar automaticamente aqui (item 34) — precisa reconciliar.
    this.ambiguous = true;
  }
}

/**
 * @param {object} params
 * @param {string} params.redemptionId
 * @param {string} params.idempotencyKey correlation id estavel (item 35) —
 *   quando implementado, deve ser reenviado identico em qualquer retry.
 * @param {Array} params.items snapshot dos itens do resgate (produto/variacao/qtd)
 * @param {object} params.address endereco de entrega
 * @param {object} params.shippingOption opcao de frete escolhida (cotacao real Tray)
 * @param {object} params.couponSnapshot { coupon_code, tray_coupon_id }
 * @throws {TrayOrderNotImplementedError} sempre, ate a Fase E ser desbloqueada
 */
export async function createTrayRedemptionOrder(_params) {
  throw new TrayOrderNotImplementedError();
}
