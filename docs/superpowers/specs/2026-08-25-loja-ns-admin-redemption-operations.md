# Loja NS — administração operacional do resgate (read model)

Data: 2026-08-25
Escopo: **ADMIN / READ MODEL / OBSERVABILIDADE**. Nenhuma mudança de contrato
público, financeiro ou de integração.

## Divisão de responsabilidade (a decisão que rege este módulo)

```
NewStore  -> administra o RESGATE  (créditos, saga, evidência, ledger)
Tray      -> administra a LOGÍSTICA (separação, envio, entrega, rastreio)
```

Consequência prática: o painel **não tem** estados de entrega
(pendente/enviado/entregue). Eles não existem no schema da NewStore e
inventá-los criaria uma segunda logística divergente da real. O único estado
de entrega exibido é o que a própria Tray devolve, no detalhe do resgate.

## O que foi construído

Um read model somente-leitura sobre tabelas que **já existiam**:
`reward_redemptions`, `reward_redemption_items`, `reward_redemption_events`,
`coupon_balance_history`, `user_addresses`, `users`, `reward_products`.

- `src/services/rewardRedemptionAdmin.js` — listagem paginada, detalhe,
  relatório e status operacional. Nenhuma função escreve em lugar nenhum.
- `src/routes/admin_store.js` — novas rotas sob o mesmo
  `requireAuth + requireAdmin` já usado pelo módulo.

**Zero migrations.** Nenhum dado necessário faltava no schema.

## Status do resgate — a lista real

Derivada do `CHECK` factual de `public.reward_redemptions`
(migrations 031 → 032 → 034) cruzada com o que `rewardRedemption.js`
realmente grava. `tests/rewardRedemptionAdmin.test.js` compara o catálogo com
o `.sql` da migration: se um status novo entrar no banco sem entrar no
catálogo, o teste falha.

| status | significado | sucesso? | crédito comprometido? | compensado? | rótulo admin |
|---|---|---|---|---|---|
| `processing` | resgate criado, nada debitado | não | não | não | Processando |
| `credits_reserved` | débito aplicado, pedido ainda não solicitado | não | **sim** | não | Créditos debitados |
| `tray_order_pending` | criação do pedido Tray em andamento | não | **sim** | não | Enviando à Tray |
| `tray_order_created` | valor da migration 031; nenhum código atual grava | não | sim | não | Pedido criado (legado) |
| `confirmed` | pedido Tray criado, resgate concluído | **sim** | **sim** | não | Confirmado |
| `failed` | débito não pôde ser aplicado | não | não | não | Falhou |
| `compensated` | recusa determinística da Tray, créditos devolvidos | não | não | **sim** | Compensado |
| `reconciliation_required` | resultado ambíguo; **nunca compensa às cegas** | não | **sim** | não | Requer conciliação |
| `blocked_tray_contract_pending` | criação de pedido indisponível; devolvido | não | não | **sim** | Bloqueado: contrato Tray |
| `blocked_tray_customer_unmapped` | status histórico (migration 032); devolvido | não | não | **sim** | Bloqueado: cliente não mapeado (legado) |
| `blocked_tray_profile_incomplete` | perfil sem os dados que a Tray exige; devolvido | não | não | **sim** | Bloqueado: perfil incompleto |
| `blocked_tray_customer_ambiguous` | identidade Tray ambígua; devolvido | não | não | **sim** | Bloqueado: cliente ambíguo |

## Regra crítica do relatório

**NSCréditos resgatados conta SOMENTE `confirmed`.**

Uma tentativa compensada teve `REDEMPTION_DEBIT` + `REDEMPTION_COMPENSATION`
no ledger — efeito líquido zero. Somá-la em "resgatados" inflaria o número com
tentativas que nunca consumiram crédito nenhum. Ela aparece apenas no card de
compensados. O mesmo vale para `unique_customers` (cliente que *resgatou*, não
que *tentou*) e para `reconciliation_required`, cujo crédito está preso à espera
de conferência manual e por isso também não é crédito resgatado.

`tray_orders_created` usa a coluna factual `tray_order_id IS NOT NULL` — nunca
inferência por evento.

## Tray é read-only aqui

O único ponto de contato é `getTrayOrderFull()` (`GET /orders/:id/full`), já
existente, acionado **sob demanda** ao abrir/atualizar o detalhe — nunca por
linha da listagem (seria N+1 de rede). Nenhum `POST`/`PUT`/`DELETE`, nenhuma
alteração de status, frete, estoque ou preço. `tests/trayNoMutation.test.js`
inclui este serviço na prova de não-mutação.

## PII

- Listagem: nome e e-mail. Sem endereço, sem telefone, sem CPF.
- Detalhe: acrescenta o **snapshot** de endereço do resgate (nunca o endereço
  atual do usuário — o histórico do pedido não muda quando o cliente se muda).
- `reward_redemption_events.meta` passa por whitelist (`http_status`,
  `tray_error_code`, `tray_body` re-sanitizado e achatado em mensagens curtas).
  Token, senha, cookie, CPF e URL de banco nunca chegam ao painel.
- A busca administrativa cobre id do resgate, id do pedido Tray, id do usuário,
  nome e e-mail. **Nunca CPF.**

## Webhook de pedido: rota pronta ≠ entrega comprovada

A rota `POST /api/webhooks/tray/order` existir não prova que a Tray liberou o
escopo `order`. A única evidência persistida hoje de uma entrega real é um
lançamento `DIRECT_TRAY_SPEND` em `coupon_balance_history`, gravado
exclusivamente por `handleTrayOrderWebhook`. Sem lançamento, o painel reporta
`Endpoint: Disponível` + `Entrega Tray: Não comprovada` — nunca "webhook ativo".
Nenhuma migration foi criada só para registrar recebimento.
