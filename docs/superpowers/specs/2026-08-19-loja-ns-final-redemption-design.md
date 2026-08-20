# Loja NS — fechamento do resgate por NSCréditos (design)

Data: 2026-08-19
Branch: `jpedro` (backend e frontend), não mesclado em `main`, não em produção.

## Objetivo

Completar, de ponta a ponta, o fluxo de resgate da Loja de Prêmios NS iniciado na
Fase 5 Foundation: carrinho → revisão → confirmação → pedido Tray real → cupom
Tray sincronizado → rastreio em "Meus Pedidos" — sem inventar nenhum contrato
Tray, sem reintroduzir frete, e sem tocar em nenhuma área do sistema fora deste
domínio (sorteios, Mercado Pago, Vindi, cativos, pré-autorização, autopay, cron,
e-mail, WhatsApp, AGIS).

## Arquitetura (inalterada nesta fase)

```
Browser → Backend NewStore → Tray
```

O navegador nunca fala com a Tray. `users.coupon_value_cents` continua sendo a
única fonte de saldo (nenhuma reintrodução de `nscredit_wallets`). O carrinho
local (`reward_carts`/`reward_cart_items`) continua autoritativo para a UI —
nenhuma escrita na Tray acontece por causa de add/remove de item.

## O que foi decidido nesta fase

### 1. Representação do resgate no pedido Tray (bloqueio histórico resolvido)

Fonte: `tray-tecnologia/tray-api-ai-plugin` (repositório oficial da Tray,
hospedado no domínio da própria Tray), `skills/pedidos/schemas/pedido.create.json`.

- `POST {TRAY_API_BASE}/orders?access_token={token}`, corpo `{"Order": {...}}`.
- `required: ["customer_id", "products"]` — **somente isso**.
- `payment_method`, `shipping_method`, `shipping_cost` são **opcionais** no
  schema oficial. Decisão: **omitir todos os três** — nunca inventar um
  `payment_method` fictício (pix/boleto/cartão) para representar "pago com
  créditos". `shipping_*` fica de fora porque frete está fora de escopo.
- Identificação do resgate: campo oficial `notes` ("Observações livres do
  pedido"), preenchido com `Resgate Loja NS / redemption_id=<id> /
  coupon_code=<code>`. É o único mecanismo documentado e não inventado
  disponível para isso.

### 2. Cliente Tray (`customer_id`)

A Tray exige `customer_id` — um ID interno dela, nunca `users.id`. Resolvido
via `GET /customers?email=<email>` (`trayCustomerClient.js`), reaproveitando o
e-mail já cadastrado no NewStore. **Nunca cria um cliente novo**: o
`POST /customers` da Tray exige `birth_date`, campo que o cadastro da NewStore
não coleta. Quando não há cliente Tray correspondente, o resgate termina em
`blocked_tray_customer_unmapped` — um bloqueio isolado e determinístico
(nenhuma mutação ocorreu, é seguro compensar), não um bloqueio geral do
recurso.

### 3. Carrinho Tray — decidido que não é necessário

A documentação oficial (`skills/carrinho-compras`) afirma explicitamente que o
recurso de carrinho não é para pedidos finalizados ("use tray-pedidos"), e o
schema de criação de pedido embute os produtos diretamente, sem referência a
carrinho/sessão. Conclusão: nenhuma camada de "Tray cart client" foi
construída — reduz uma camada inteira do plano original sem perder cobertura
de contrato.

### 4. Saga (débito → pedido Tray → sincronização de cupom)

Já seguia o padrão de transação curta desde a Fase Foundation (auditado, não
reescrito):

```
TX curta A (applyCouponLedgerEntry: debit)
  SELECT users ... FOR UPDATE
  valida expiração/saldo
  UPDATE coupon_value_cents
  INSERT coupon_balance_history (REDEMPTION_DEBIT)
  COMMIT
                    │
                    ▼  (fora de qualquer transação)
        sincroniza cupom Tray (best-effort)
                    │
                    ▼
        createTrayRedemptionOrder (rede real)
     ┌──────────────┼──────────────────────┐
     ▼ sucesso       ▼ falha determinística  ▼ timeout/rede instável
  confirmed      TX curta B (credit)     reconciliation_required
  + tray_order_id  + resincroniza cupom    (créditos NUNCA compensados
                    Tray                    automaticamente aqui)
```

Erros determinísticos que levam à compensação: `TrayCustomerNotFoundError`
(→ `blocked_tray_customer_unmapped`) e qualquer `TrayCatalogError` que não seja
timeout/rede (400/401/404/5xx). Timeout/indisponibilidade de rede na criação
do pedido vira `TrayOrderAmbiguousError` → `reconciliation_required`, nunca
compensado às cegas.

**Gap conhecido, documentado e não inventado**: uma busca ativa do pedido na
Tray (por `customer_id` + `notes`, via `GET /orders`) antes de decidir
compensar um timeout ambíguo não foi implementada, porque a documentação
oficial não confirma se o campo `notes` é devolvido no `GET` após a criação —
não há ambiente de homologação Tray disponível para verificar isso sem criar
um pedido real. Até essa confirmação, todo caso ambíguo fica em
`reconciliation_required` para resolução manual.

### 5. Sincronização do cupom Tray

Reaproveita `ensureTrayCouponForUser` (já existente, usada no login) — lê
`users.coupon_value_cents` fresco do banco a cada chamada. Chamada em dois
pontos da saga: logo após o débito (antes do pedido Tray) e, se compensado,
de novo após a compensação. Sempre best-effort (nunca bloqueia o resgate).

Direção do erro deliberada: se sincronizar logo após o débito e depois
precisar compensar, o cupom Tray fica temporariamente **abaixo** do saldo real
— nunca acima. Nunca abre uma janela de double-spend.

### 6. Reconciliação de gasto direto na Tray (P0)

Auditoria (`skills/cupons`, `skills/webhooks`): a Tray não expõe contador de
usos restantes via `GET /discount_coupons/:id`. O único mecanismo documentado
para detectar uso é o webhook de escopo `order` (`insert`/`update`), consultando
`GET /orders/:id/full` para os campos `coupon_code`/`discount` — nunca confiar
só no payload do webhook (que só traz `seller_id`/`scope_id`/`scope_name`/`act`).

Implementado: `POST /api/webhooks/tray/order`. Ao confirmar `coupon_code`
correspondente a um `users.coupon_code` e `discount > 0`, zera o saldo local
(o cupom é single-use — `usage_counter_limit=1` já configurado por
`trayCouponEnsure.js` — então uma vez aplicado em qualquer pedido, nunca pode
ser usado de novo, independente do valor exato do desconto). Idempotente por
`tray_order_id`, reaproveitando a mesma `UNIQUE(idempotency_key)` do ledger da
saga de resgate — nenhuma tabela nova de deduplicação.

Auditados todos os pontos que chamam `ensureTrayCouponForUser` (login,
`/api/coupons/ensure`, reprocessamento admin de pendentes): todos leem o saldo
fresco do banco a cada chamada — nenhum deles usa um snapshot antigo. Conclusão:
a proteção contra "login ressuscita saldo já gasto" não exige nenhuma mudança
no fluxo de login — exige que `coupon_value_cents` esteja sempre correto, o que
o webhook agora garante **assim que estiver ativo**.

**BLOQUEADO POR CONFIGURAÇÃO EXTERNA**: o escopo `order` só é liberado pela
Tray mediante chamado de suporte informando a URL deste endpoint. Até essa
ativação acontecer, a vulnerabilidade original (saldo local não reflete gasto
direto na Tray) **permanece aberta em produção**. Isso não foi escondido nem
contornado — é reportado explicitamente como bloqueio pendente.

## Frontend

`/loja/resgate` (rota protegida, nova): endereço (reaproveita
`/api/store/checkout/addresses`, já existente da Fase Foundation) → revisão
(itens, total, saldo antes/depois, cupom, endereço — **sem nenhum campo de
frete**) → confirmação com `idempotency_key` estável (gerada uma vez por
carregamento da página) e anti-duplo-clique real → tela de resultado honesta
por status (`confirmed` / `reconciliation_required` / demais estados
bloqueados-e-compensados, cada um com texto e prova de saldo específicos).

Kill-switch do lado do cliente (`REACT_APP_REWARD_REDEMPTION_ENABLED`, default
`false`) espelha o do backend (`REWARD_REDEMPTION_ENABLED`, também `false` em
produção hoje) — o botão CONTINUAR só habilita quando os dois concordam.

## Não-escopo confirmado

Frete/cotação/transportadora/prazo de entrega: nada disso foi tocado, criado
ou expandido. O código de cotação de frete da Fase Foundation
(`checkoutShipping.js`/`trayShipping.js`) permanece isolado e intocado —
`shipping_option` segue aceito no corpo de `/confirm` apenas como metadado
(`shipping_snapshot`), nunca influenciando o pedido Tray.

## Addendum (rodada 2 — "Reta Final Definitiva")

### Duas formas documentadas de `POST /orders` — decisão confirmada

Auditoria mais profunda (página real `developers.tray.com.br`, seção
"Cadastrar Pedido#post", não só o schema resumido do `tray-api-ai-plugin`)
revelou que a documentação real mostra um exemplo com `Order.Customer`
inline (`CustomerAddress`/`ProductsSold` aninhados, payload
`application/x-www-form-urlencoded`, sem `customer_id`), diferente do
contrato `customer_id`+`products` (JSON, `additionalProperties:false`)
citado pelo `pedido.create.json` do plugin.

**Decisão confirmada com o solicitante**: manter `customer_id`+`products`
— é o contrato curado e explicitamente validável (`required`,
`additionalProperties:false`) que a própria Tray disponibiliza para
consumo por agentes de IA, menor risco de campo rejeitado. A variante
inline permanece documentada aqui como alternativa conhecida, não
descartada por engano — se o teste controlado (M7) revelar que
`customer_id`+`products` não é aceito na conta real, essa é a rota B
já mapeada (ver exemplo completo abaixo).

Exemplo real da variante inline (não usada, registrada por referência):

```
Order.point_sale, Order.session_id, Order.shipment, Order.shipment_value,
Order.payment_form,
Order.Customer.{type,name,cpf,email,rg,gender,phone},
Order.Customer.CustomerAddress[0].{address,zip_code,number,complement,
  neighborhood,city,state,country,type},
Order.Customer.ProductsSold[0].{product_id,variant_id,price,original_price,quantity},
Order.MarketplaceOrder.{...} (só para pedidos importados de marketplace —
  Mercado Livre no exemplo oficial; não se aplica ao nosso caso)
```

### `notes` confirmado tanto em create quanto em update

`pedido.update.json` (schema curado, `PUT /orders/:id`) usa o MESMO campo
`notes` (não `store_note`/`customer_note`, que pertencem à nomenclatura
mais antiga vista na página HTML). Como `notes` já é aceito no
`POST /orders` (create), nenhuma chamada `PUT` extra foi adicionada só
para identificação do resgate.

### `variant_id` (P0 da rodada 1, corrigido)

A mesma auditoria confirmou `product_id`/`variant_id` como campos
SEPARADOS dentro de cada item de `ProductsSold`/`products` — nunca a
variação substituindo o produto. `trayOrderClient.js` corrigido para
enviar ambos quando o item tem variação.

### `POST /customers` — campos reais vs. schema resumido

A página real documenta bem mais campos opcionais que o
`cliente.create.json` do plugin sugeria (`rg`, `cellphone`, `nickname`,
`observation`, `type`, `company_name`, `cnpj`, `state_inscription`,
`reseller`, `discount`, `blocked`, `credit_limit`, `indicator_id`,
`profile_customer_id`, endereço embutido direto). O `required` continua
confiável apenas via o schema curado (`name`, `email`, `birth_date`) —
as tabelas da página real não marcam obrigatoriedade em nenhum campo.
Deliberadamente enviamos SOMENTE `name`/`email`/`birth_date` (+`phone` se
já presente) — YAGNI, item 8 do pedido: nunca coletar PII que a Tray não
exige.
