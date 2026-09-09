# Loja NS — Payment real na Tray para resgates em NSCréditos

Data: 2026-09-09
Escopo: **backend, somente os PRÓXIMOS resgates**. Zero migration, zero
alteração retroativa, zero mudança de frontend, zero toque em pedidos
existentes.

## O que mudou

Este documento **SUPERA** a decisão registrada em
`2026-08-31-loja-ns-tray-order-status-nscredits.md`, seção *"Risco aceito,
deliberado"*:

| Decisão de 31/08 (revogada) | Regra a partir de 09/09 |
|---|---|
| "Este fluxo **não cria Payment na Tray**" | Todo resgate novo **cria** um Payment real (`POST /payments`) |
| "`has_payment` pode continuar `0`. Isso é esperado, não é bug" | `has_payment` **precisa** ser `"1"`; `"0"` bloqueia a confirmação |
| Liquidação = débito de NSCréditos no ledger NewStore | Liquidação = débito no ledger **E** Payment real na Tray |

O documento de 31/08 **não foi reescrito**. Ele recebeu apenas uma nota de
*superseded* no topo e na subseção afetada. O restante dele (resolução
dinâmica do status "A ENVIAR", fail-closed, tratamento de timeout do `PUT`)
continua em vigor e é reutilizado, não substituído.

## Definição de `confirmed`

Um `reward_redemption` só pode virar `confirmed` quando **todas** as
condições abaixo forem verdadeiras e **verificadas contra a Tray**:

1. existe pedido Tray (`tray_order_id` persistido);
2. existe um Payment correspondente **àquele resgate** no pedido;
3. `GET /orders/:id` devolve `has_payment === "1"`;
4. o pedido está em estado operacional compatível com expedição
   (preferencialmente `"A ENVIAR"`);
5. só então o redemption local vira `confirmed`.

**Não confundir:** `Order.payment_form = "NSCréditos"` é um rótulo do pedido.
Ele **não** representa pagamento confirmado e nunca representou. Quem
representa é o Payment.

**Nunca** definir `has_payment = 1` manualmente no payload do Order — quem
marca isso é a própria Tray, ao receber o Payment. Há teste estrutural
(`tests/trayPaymentContract.test.js`) que falha se alguém tentar.

## O valor do Payment — regra crítica

```
NSCréditos  ──✗──>  reais
```

NSCréditos pertencem ao **ledger interno** da NewStore e não têm taxa de
conversão para moeda. O `payment.value` vem **exclusivamente** do valor
monetário factual do pedido Tray:

```
GET /orders/:id  ->  Order.total  ->  payment.value
```

Exemplo real do domínio:

| | |
|---|---|
| resgate custa | `5.000 NSCréditos` |
| `Order.total` na Tray | `"489.99"` |
| `payment.value` | `"489.99"` |

Nunca `5000`, nunca `credits_amount`, nunca um valor calculado no frontend.
Se `Order.total` não for um valor monetário reconhecível, falhamos **antes**
de criar qualquer Payment (`tray_order_total_invalid`) — um pagamento com
valor inventado corromperia um pedido real.

## Payload oficial

```json
{
  "Payment": {
    "order_id": "<tray_order_id>",
    "method": "NSCréditos",
    "value": "<Order.total factual da Tray>",
    "date": "<YYYY-MM-DD>",
    "note": "LOJA_NS_REDEMPTION:<redemption_id>"
  }
}
```

- `method` representa **NSCréditos**. Nenhum gateway fictício, nenhum
  PIX/boleto/cartão. É a mesma string de `Order.payment_form`
  (`LOJA_NS_ORDER_DEFAULTS.payment_form`) — uma única fonte de verdade.
- `date` é `YYYY-MM-DD` no fuso `America/Sao_Paulo`. Usar UTC gravaria o dia
  seguinte para qualquer resgate feito à noite no Brasil.
- `note` carrega o marker determinístico — ver *Idempotência*.
- **O envelope é `Payment` com P maiúsculo.** Provado no smoke controlado
  (pedido 25894): com `{ "payment": {...} }` a Tray responde **400** e as
  `causes` vêm como `Payment.value` / `Payment.method` / `Payment.order_id` =
  *"Este campo não pode ser deixado em branco"* — ela simplesmente não enxerga
  o objeto minúsculo. Mesmo padrão já conhecido em `Order` e `ProductsSold`.

## Fluxo

A saga existente foi preservada e modificada cirurgicamente. O único ponto
alterado é o passo pós-`POST /orders`.

```
confirm redemption
    ↓
validar carrinho / endereço / usuário
    ↓
debitar NSCréditos no ledger            (inalterado)
    ↓
sincronizar cupom Tray                  (inalterado)
    ↓
POST /orders                            (inalterado)
    ↓
PERSISTIR tray_order_id IMEDIATAMENTE   (inalterado — antes de qualquer Payment)
    ↓
┌─ settleTrayRedemptionOrder ─────────────────────────────┐
│  GET /orders/:id        -> Order.total factual          │
│  ensureTrayRedemptionPayment:                           │
│      GET /payments?order_id=<id>                        │
│      existe Payment deste resgate?                      │
│        SIM -> reutiliza (zero POST)                     │
│        NÃO -> POST /payments (uma única vez)            │
│               -> confirmar existência por GET           │
│  GET /orders/:id        -> EXIGE has_payment === "1"    │
│  status operacional:                                    │
│      já em "A ENVIAR"? -> nenhum PUT redundante         │
│      senão -> resolução dinâmica do ID + PUT + GET      │
│  confirmação final: has_payment === "1" + status        │
└─────────────────────────────────────────────────────────┘
    ↓
redemption = confirmed
```

O `tray_order_id` continua sendo persistido **antes** de qualquer chamada de
pagamento: se o processo morrer no meio, o resgate ainda aponta para o pedido
real, nunca para um segundo `POST /orders`.

## Idempotência do Payment

Não assumimos que `POST /payments` tenha idempotency-key nativa — a Tray não
documenta uma. A identidade do pagamento é um **marker determinístico**
derivado do `redemption_id`, gravado em `payment.note`:

```
LOJA_NS_REDEMPTION:<redemption_id>
```

Antes de **qualquer** `POST`, lemos `GET /payments?order_id=<id>` e
procuramos um Payment que corresponda ao resgate em **três eixos**:

- `order_id` igual ao pedido;
- marker presente em `note`;
- `value` igual ao esperado.

Se existir → **não** criamos outro Payment.

Marker correto com **valor divergente** não é aceito em silêncio nem
"corrigido" com um segundo pagamento: é `tray_payment_value_mismatch`
(fail-closed, auditoria humana).

Listagem em formato desconhecido **não** vira "não existe Payment" —
responder isso sem certeza levaria a um POST duplicado. É
`tray_payment_list_invalid`.

## Timeout / resultado ambíguo

Cenário: `POST /payments` cai por timeout ou rede e não sabemos se a Tray
persistiu.

**Proibido repetir o POST.** A única pergunta legítima é *"a Tray
persistiu?"*, e quem responde é um `GET`:

```
POST /payments  ──timeout──>  GET /payments?order_id=<id>
                                    ├─ encontrou o Payment esperado
                                    │     -> criação confirmada, segue
                                    └─ não encontrou
                                          -> tray_payment_unconfirmed
```

Em `tray_payment_unconfirmed` (e em qualquer outra falha após o pedido
existir):

- o redemption vai para **`reconciliation_required`**;
- `tray_order_id` é **preservado**;
- as informações do débito são **preservadas**;
- **nenhum** segundo Order;
- **nenhum** segundo Payment;
- **nenhuma** compensação automática de créditos após um pedido externo
  ambíguo.

Isso preserva a filosofia fail-closed já existente na saga.

## Allowlist

Auditada em `src/services/trayCatalogClient.js` (`ALLOWED_MUTATIONS`) e
`src/services/trayMutationClient.js`.

Adicionada **exclusivamente**:

```
TRAY_REDEMPTION_PAYMENT_CREATE  ->  POST  /payments
```

Endurecimento aplicado junto: cada entrada da allowlist agora amarra
**operação + método + recurso (path)**, não mais só operação + método.
Autorizar `POST` para uma operação jamais autoriza aquela operação a postar
em outro endpoint da Tray — em particular, `/payments` **não** foi liberado
genericamente. `trayMutationClient` sempre informa o path, então nenhuma
requisição real escapa da amarração.

Continuam bloqueados, antes da rede:

- `PUT /payments*` (qualquer operação)
- `DELETE /payments*`
- `PATCH /payments*`
- toda mutação não relacionada

## Status do pedido

A resolução dinâmica do status "A ENVIAR" de 31/08 foi **preservada e
reutilizada**. Nenhum ID é hardcoded — nem `1`, nem `27`, nem qualquer outro.

Após criar/confirmar o Payment:

- se a própria Tray já moveu o pedido para "A ENVIAR", **nenhum `PUT`
  redundante** é emitido — e nem o lookup da listagem de status é necessário,
  porque o próprio pedido carrega o `OrderStatus` factual;
- caso contrário, resolvemos o ID factual de "A ENVIAR" pela listagem,
  executamos o mecanismo existente (`PUT` + `GET` de confirmação) e
  validamos.

`advanceTrayOrderToOperationalStatus` ganhou um parâmetro opcional `order`
(uma leitura já feita do pedido). Quando ele não é informado, o comportamento
é **idêntico ao anterior** — por isso toda a cobertura de status de 31/08
continua passando sem alteração.

## Preservado (não tocado)

Regras de NSCréditos, cálculo de saldo, ledger imutável, `FOR UPDATE`,
`idempotency_key` do redemption, sincronização de cupom, catálogo Tray, preço
em NSCréditos, estoque Tray, fluxo de carrinho, autenticação, frontend,
painel admin, sorteios, Vindi, Mercado Pago, demais integrações e produtos da
Tray.

## Pedidos legados

Os pedidos Tray **#25666** e **#25668** (cancelados, criados em 24/08/2026)
**não** foram tocados: nenhum Payment criado neles, nenhum status alterado,
nenhum pedido recriado, nenhum crédito devolvido, nenhum batch retroativo.
A implementação atua **somente sobre resgates novos**. Há teste estrutural
garantindo que nenhum desses números aparece no código.

## Códigos de erro novos

| Código | Significado | Efeito no redemption |
|---|---|---|
| `tray_order_total_invalid` | `Order.total` ausente/irreconhecível | `reconciliation_required` (nenhum Payment criado) |
| `tray_payment_list_invalid` | `GET /payments` em formato desconhecido | `reconciliation_required` |
| `tray_payment_value_mismatch` | Payment do resgate existe com valor divergente | `reconciliation_required` |
| `tray_payment_unconfirmed` | resultado ambíguo e Payment não encontrado no GET | `reconciliation_required` |
| `tray_payment_not_reflected` | Payment criado mas `has_payment` ≠ `"1"` | `reconciliation_required` |

Todos ocorrem **depois** do pedido existir, portanto todos preservam
`tray_order_id` e o débito, e nenhum dispara compensação automática.

## Arquivos

| Arquivo | Mudança |
|---|---|
| `src/services/trayPaymentClient.js` | **novo** — marker, listagem, matching, criação e `ensureTrayRedemptionPayment` |
| `src/services/trayCatalogClient.js` | allowlist: nova operação + amarração de path |
| `src/services/trayMutationClient.js` | passa o path para a guarda |
| `src/services/trayOrderClient.js` | `readTrayOrderTotal`, `trayOrderHasPayment`, `order` opcional em `advance…` |
| `src/services/trayRedemptionOrder.js` | **novo** `settleTrayRedemptionOrder` |
| `src/services/rewardRedemption.js` | saga passa a liquidar via `settleTrayRedemptionOrder` |

## Contrato factual confirmado (auditoria read-only, 2026-09-09)

Validado **contra a loja real**, somente `GET`. Nenhuma mutação emitida.

### `GET /payments?order_id=<id>`

Raiz: `paging`, `sort`, `availableFilters`, `appliedFilters`, `Payments`.

```json
{
  "paging": { "total": 1, "page": 1, "offset": 0, "limit": 30, "maxLimit": 50 },
  "availableFilters": ["id", "order_id", "note"],
  "appliedFilters": { "Payment.order_id": "25884" },
  "Payments": [ { "Payment": {
      "created": "...", "modified": "...", "id": "16704",
      "order_id": "25884", "payment_method_id": "10547",
      "payment_place": "Pix - Vindi", "value": "4630.95",
      "date": "2026-09-09", "note": "..." } } ]
}
```

- **O `Payment` lido NÃO tem campo `method`.** O rótulo vem em
  `payment_place` e o identificador em `payment_method_id`. `method` só existe
  no corpo que **enviamos** no `POST`. `normalizeTrayPayment` deriva `method`
  de `payment_place` por isso.
- Pedido **sem** pagamento devolve `Payments: []` com `paging.total: 0` —
  nunca 404. O caso vazio é um array vazio, não um erro.
- `limit` default é **30** (`maxLimit` 50). `listTrayPaymentsByOrder` envia
  `limit=50` explicitamente: paginar por engano seria ler "não existe
  Payment" e duplicar o `POST`.
- `note` é um filtro suportado (`availableFilters`), reforçando a escolha do
  marker determinístico. O matching atual filtra por `order_id` e compara o
  marker em memória — suficiente e menos dependente de semântica de busca.

### `GET /orders/:id`

`total`, `has_payment`, `status`, `OrderStatus{id,status}` presentes e
factuais. Observação: `payment_form` aparece na **listagem** `/orders`, mas
não no detalhe `/orders/:id` — motivo a mais para `has_payment` ser o único
sinal aceito.

### Status operacional

`GET /order_status` responde **404** nesta conta; o fallback documentado
`GET /orders/statuses` responde e é usado. `resolveTrayOperationalStatus`
executado contra a conta real devolveu:

```
{ "id": "1", "status": "A ENVIAR" }
```

Exatamente uma correspondência — o fail-closed não dispara. **O ID real é
`1`**, não `27`: a resolução dinâmica é o que torna isso correto, e nenhum ID
está hardcoded.

### Divergência observada (não bloqueia)

No pedido pago de exemplo, `Payment.value` (`4630.95`) **difere** de
`Order.total` (`4651.88`). Ou seja, na Tray o valor do pagamento não é
necessariamente igual ao total do pedido. Para o resgate a regra de negócio
exige que sejam **iguais** — é uma decisão nossa, não uma invariante da Tray.
Por isso `tray_payment_value_mismatch` é fail-closed e não auto-corrige.

## Smoke controlado — executado em 2026-09-09

Um único resgate real na conta interna (jpjp), 1× Kit De Reparo Relojoeiro,
600 NSCréditos. Pedido Tray **25894**.

### Primeira tentativa — falhou como devia

`POST /payments` com envelope minúsculo → **400**. Resultado:

```
redemption = reconciliation_required
failure_reason = tray_request_invalid
tray_order_id = 25894          (preservado)
débito de 600 NSCréditos       (preservado, 1 única linha no ledger)
compensação automática         (nenhuma)
segundo pedido / segundo Payment (nenhum)
```

O `meta` do evento guardou o corpo sanitizado da Tray, que apontou o campo
exato — foi o que permitiu corrigir sem tentativa e erro. **O fail-closed
funcionou em produção, com dinheiro real.**

Uma re-execução com a mesma `idempotency_key` devolveu `replayed` e emitiu
**zero** chamadas HTTP e **zero** delta de saldo — idempotência provada
contra o banco real.

### Segunda tentativa — envelope corrigido

Liquidação do **mesmo** pedido 25894 (caminho de reconciliação, sem criar
pedido novo nem debitar de novo):

```
GET  /orders/25894              -> total "299.99"
GET  /payments?order_id=25894   -> vazio
POST /payments                  -> 201, Payment id 16712
GET  /payments?order_id=25894   -> confirmado
GET  /orders/25894              -> has_payment "1", status "A ENVIAR" (id 1)
```

5 chamadas: 4 `GET` + 1 `POST`. **Nenhum `PUT`** — a própria Tray moveu o
pedido para "A ENVIAR" ao receber o pagamento, e o caminho "não emitir PUT
redundante" foi exercitado em produção (`statusUpdated: false`).

Estado final factual do pedido 25894:

| campo | valor |
|---|---|
| `Order.total` | `"299.99"` |
| `Payment.value` | `"299.99"` — **igual ao total, não aos 600 NSCréditos** |
| `Payment.note` | `LOJA_NS_REDEMPTION:85a4aa03-…` |
| `has_payment` | **`"1"`** |
| `OrderStatus` | `A ENVIAR` (id `1`) |

### Achado: `payment_place` volta vazio

A Tray **aceitou** `method: "NSCréditos"` no `POST` (as `causes` do 400 a
listam como campo obrigatório), mas na leitura o Payment volta com
`payment_place: ""` e `payment_method_id: ""`. Ou seja: o pagamento é
registrado e `has_payment` vira `"1"`, mas **o rótulo do meio de pagamento
não fica gravado**.

Consequência prática: quem olhar o pagamento no painel da Tray não vê
"NSCréditos" escrito ali. A identificação do resgate continua garantida pelo
`note` (marker) e por `Order.payment_form`/`store_note`. Se o rótulo visível
passar a ser requisito, o caminho é descobrir o `payment_method_id` numérico
da conta e enviá-lo — **não** inventar um gateway. Isso não bloqueia o fluxo
e não afeta a idempotência, que nunca dependeu de `method`.

## Pendência restante

Nenhuma no contrato: leitura e escrita estão validadas contra a loja real.

Resta apenas a decisão de negócio sobre os **5 resgates históricos** com
`has_payment = "0"` (3.400 NSCréditos), que esta implementação deliberadamente
**não** tocou.
