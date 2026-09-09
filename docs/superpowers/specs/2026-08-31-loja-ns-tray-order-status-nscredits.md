# Loja NS — status operacional do pedido Tray após resgate em NSCréditos

> **SUPERSEDED (parcialmente) em 2026-09-09.**
> A decisão registrada abaixo em *"Risco aceito, deliberado"* — **"o fluxo não
> cria Payment na Tray"** e **"`has_payment` pode continuar `0`"** — **não vale
> mais** para resgates NOVOS. A regra de negócio atual exige um Payment REAL no
> pedido Tray e `has_payment === "1"` antes de o resgate virar `confirmed`.
> Ver: `docs/superpowers/specs/2026-09-09-loja-ns-tray-payment-nscredits.md`.
>
> O resto deste documento (resolução dinâmica do status "A ENVIAR", fail-closed,
> tratamento de timeout do `PUT`) continua **válido e em vigor**. Este texto é
> mantido como registro histórico e **não** foi reescrito.

Data: 2026-08-31
Escopo: **backend, somente os PRÓXIMOS resgates**. Zero migration, zero
alteração retroativa, zero pagamento Tray, zero mudança de frontend.

## Problema

O resgate já funcionava ponta a ponta (débito de NSCréditos → `POST /orders`
→ resgate confirmado), mas o pedido nascia **AGUARDANDO PAGAMENTO** na Tray.
A equipe da loja não tinha como saber, olhando o pedido, que ele já estava
liquidado — e o pedido não entrava no fluxo de separação.

## Decisão de negócio (final)

NSCréditos são a forma de liquidação do resgate, **dentro da NewStore**. A
Tray recebe o pedido para operação/logística, não para receber dinheiro.

```
débito NSCréditos (ledger NewStore)
        ↓
POST /orders
        ↓
persiste tray_order_id
        ↓
PUT /orders/:id  (somente Order.status_id)
        ↓
GET /orders/:id  (confirmação)
        ↓
resgate confirmed
```

### Risco aceito, deliberado

> **Esta subseção foi SUPERADA em 2026-09-09.** Ela descreve o que era verdade
> entre 31/08 e 09/09. Não use como referência para o comportamento atual.

- Este fluxo **não cria Payment na Tray**: nenhum `POST/PUT/DELETE /payments`,
  nenhum pagamento de R$ 0,00, nenhum PIX/cartão/boleto fictício.
- Portanto **`has_payment` pode continuar `0`** no pedido Tray. Isso é
  esperado, não é bug, e **não deve ser "corrigido" adicionando Payment**.
- A fonte financeira do resgate continua sendo o ledger de NSCréditos da
  NewStore (`coupon_balance_history`, `REDEMPTION_DEBIT`).
- O `status_id` é usado apenas para liberar o fluxo operacional/separação.

## Descoberta do status — nunca hardcoded

O ID **não** é fixo no código (em particular, `16` dos exemplos da
documentação **não** é assumido). Em runtime:

1. `GET` read-only na listagem de status da conta. Dois caminhos documentados
   são tentados, nessa ordem, e o primeiro que devolver status de verdade
   vence: `/order_status` e `/orders/statuses`. Só `404` faz cair para o
   próximo — auth/rate-limit/5xx/timeout sobem como falha.
2. O rótulo é lido de `status` **ou** `name` (a Tray usa `status` no pedido e
   `name` na listagem de status).
3. Escolha por nome normalizado (sem acento, sem espaço duplo, maiúsculas):
   **somente `A ENVIAR` exato**. Nenhum equivalente por aproximação textual.
   Empate (mais de um) também é recusado.
4. Cache simples em memória do par `{id, status}` (sem tabela, sem migration,
   sem scheduler); `{ cache: false }` desliga nos testes.

**Fail-closed:** sem status identificado com segurança, nenhuma mutation sai —
nada de status arbitrário num pedido real.

## Mutation autorizada

A allow-list explícita de `trayCatalogClient.js` ganhou **uma** entrada:

```
TRAY_ORDER_STATUS_UPDATE → PUT   (destino: /orders/:id)
```

`PUT` continua bloqueado para qualquer outra operação, `assertReadOnlyMethod`
continua valendo para o catálogo, e nenhuma operação de pagamento existe.

O corpo é mínimo — **só** o que muda:

```json
{ "Order": { "status_id": "<ID_REAL>" } }
```

Nada de `Customer`, `ProductsSold`, `price`, `shipment`, `discount`,
`tracking` ou `sending_code`: o contrato do `POST /orders`, validado em
produção, não foi reaberto.

## Confirmação pós-PUT

`GET /orders/:id` (nunca `/full`, que responde **404** nesta loja — ver o
spec de logística de 2026-08-25). Critério de sucesso: `Order.id` correto
**e** `OrderStatus.id`/`OrderStatus.status` igual ao alvo. `has_payment`
**não** é critério de sucesso.

## Falhas depois que o pedido existe

Assim que o `POST /orders` responde, o `tray_order_id` é gravado **antes** de
qualquer outra chamada. A partir daí, qualquer falha do passo de status
(lookup fail-closed, PUT recusado, timeout não confirmado) leva a:

- `reconciliation_required`, com `tray_order_id` e `failure_reason`
  preservados e o erro sanitizado no `meta` do evento;
- **nenhum** segundo `POST /orders`;
- **nenhuma** compensação automática de NSCréditos;
- **nenhum** ledger/resgate apagado.

### Timeout do PUT (ambíguo)

`tray_timeout`/`tray_unreachable` no PUT **não** viram retry cego. A única
pergunta legítima é "a Tray aplicou?", e quem responde é o `GET /orders/:id`:

- status já no alvo → sucesso;
- status ainda antigo, ou GET indisponível → `tray_order_status_unconfirmed`
  → `reconciliation_required`. Nunca um segundo PUT.

## `store_note` — determinístico, sem PII

Construído **a partir da redemption** (nunca `nota existente + nota nova`), o
mesmo resgate sempre gera exatamente o mesmo bloco:

```
RESGATE LOJA NS

Forma de liquidação: NSCréditos
Pagamento monetário: NÃO APLICÁVEL

NSCréditos utilizados: 750

Itens:
- product_id=14518 | quantidade=1 | NSCréditos unitários=300 | total=300 NSCréditos
- product_id=14722 | variant_id=552 | quantidade=2 | NSCréditos unitários=225 | total=450 NSCréditos

Total do resgate: 750 NSCréditos
redemption_id=<uuid>

Pedido liquidado integralmente através de NSCréditos.
Não houve cobrança via PIX, cartão, boleto ou dinheiro.
```

- `product_id` é o **Tray** `tray_product_id` do item do resgate (nunca índice
  de array, nunca SKU, nunca o id local de `reward_products`).
- `variant_id` só aparece quando existe — nunca `variant_id=null`.
- Valores em NSCréditos **humanos** (`381,50`, não `38150`).
- O `coupon_code` **saiu** da nota. Não há CPF, e-mail, telefone, endereço,
  token ou cupom.

## O que NÃO mudou

`prepare`/`confirm`/carrinho, cálculo do resgate, `coupon_value_cents`,
`coupon_balance_history`, `REDEMPTION_DEBIT`/`REDEMPTION_COMPENSATION`,
`DIRECT_TRAY_SPEND`, `ensureTrayCouponForUser`, idempotência financeira,
contrato do `POST /orders`, `getTrayOrderFull()`, webhook de pedido,
`trayOrderLogistics.js`, admin, frontend, feature flags e env vars.

Pedidos antigos (inclusive os que aparecem CANCELADOS na Tray) **não** foram
tocados: nenhum batch, nenhuma correção retroativa.

## Pendência factual

A listagem de status **não pôde ser consultada contra a loja real** nesta
máquina (o token Tray vem do banco de produção, e nenhum teste de escrita ou
leitura foi apontado para produção). Portanto o nome/ID reais desta conta são
resolvidos **em runtime**, e o comportamento fail-closed acima é o que
protege o pedido caso `A ENVIAR` não exista nesta conta. Um smoke controlado
de **um único** resgate real depois do deploy confirma nome, ID e o
`has_payment` factual.
