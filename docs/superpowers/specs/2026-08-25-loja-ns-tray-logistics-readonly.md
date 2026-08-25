# Loja NS — acompanhamento logístico Tray (read-only)

Data: 2026-08-25
Escopo: **projeção READ-ONLY**. Zero migrations, zero mutação Tray, zero
efeito financeiro.

## Divisão de responsabilidade

```
NewStore -> resgate, NSCréditos, pedido, histórico do cliente, conciliação
Tray     -> separação, frete, transportadora, envio, rastreamento, entrega
```

A NewStore **não passa a controlar logística**. Ela consulta e exibe. Não
existe (e não foi criada) nenhuma coluna de `carrier`, `tracking`,
`shipping_status`, `shipped` ou `delivered` no PostgreSQL.

Os dois domínios nunca se misturam na UI: o card mostra **RESGATE**
(status da NewStore) e, separadamente, **ACOMPANHAMENTO** (o que a Tray
sabe agora). Se a consulta à Tray falhar, "Resgate confirmado" continua
verdadeiro — só o bloco de acompanhamento fica indisponível.

## Achado factual: `GET /orders/:id/full` responde 404

Auditoria read-only contra a loja real: `/orders/:id/full` devolve **404 em
todos os pedidos** desta loja, enquanto **`GET /orders/:id` funciona** e traz
o `Order` completo, com todos os campos de logística.

- Foi criado `getTrayOrder()` em `trayOrderClient.js` (mesmo `trayCatalogGet`,
  mesma autenticação, mesmo timeout, mesma trava de somente-leitura — nenhum
  client novo) usando o caminho que a loja aceita.
- `getTrayOrderFull()` ficou **intocado**: ele pertence ao reconciliador do
  webhook (dinheiro) e alterá-lo estava fora do escopo desta tarefa.
- **Pendência para outra tarefa:** o webhook `DIRECT_TRAY_SPEND` usa `/full` e
  portanto bateria em 404 se o escopo `order` for ativado pela Tray. Hoje isso
  é inofensivo (o escopo nunca foi ativado — ver o relatório de Configurações
  do admin), mas precisa ser corrigido antes da ativação.

## Campos reais da Tray (150 pedidos na listagem + 6 detalhes ENVIADO/FINALIZADO)

| Campo | Exemplo real | Significado | Cliente | Admin |
|---|---|---|---|---|
| `status` | `ENVIADO`, `FINALIZADO`, `AGUARDANDO PAGAMENTO` | status **comercial**, não logístico | ✗ | ✓ |
| `OrderStatus.type` | `open` / `closed` / `canceled` | família do status | ✗ | ✓ |
| `has_shipment` | `0` / `1` | existe envio registrado | ✓ | ✓ |
| `is_traceable` | `0` / `1` | envio rastreável | ✓ | ✓ |
| `shipment` | `Sedex`, `PENDENTE TRAY` | forma de envio | ✓ | ✓ |
| `shipment_integrator` | `Correios` | transportadora/integrador | ✓ | ✓ |
| `shipment_date` / `sending_date` | `2026-06-26` | data de postagem | ✓ | ✓ |
| `sending_code` | `LW067310786US` | código de rastreamento | ✓ | ✓ |
| `tracking_url` | `https://…/rastreio?cod_acesso=…` | URL de rastreamento | ✓ | ✓ |
| `estimated_delivery_date` | `2026-07-10` | previsão de entrega | ✓* | ✓* |
| `modified` | `2026-08-06 12:19:36` | última atualização | ✓ | ✓ |
| `shipment_value` | `250.58` | valor do frete | ✗ | ✓ |
| `store_note` | `Resgate Loja NS / redemption_id=…` | identificador **interno** | **nunca** | ✓ |

\* `estimated_delivery_date` vem preenchido em **100%** dos pedidos, inclusive
nos que nunca foram enviados — no pedido real da Loja NS ele veio **igual à
data do pedido**, ou seja, é um default, não uma previsão. Só é exposto quando
existe envio de verdade **e** a data é posterior à data do pedido. Prazo
inventado é pior que prazo ausente.

### Campos que a Tray não preenche — e por isso não são usados

`delivered`, `delivered_status`, `delivery_date` existem no schema do detalhe
e vieram **vazios em 6/6** pedidos reais `ENVIADO`/`FINALIZADO`.
`delivery_time` veio `"14"`, `"31"`, `"32"` — é **prazo em dias**, não horário
de entrega.

Conclusão: **não existe fase "entregue"**. `FINALIZADO` é fechamento
comercial do pedido, nunca prova de entrega.

## Fases emitidas

| Fase | Evidência exigida | Rótulo |
|---|---|---|
| `received` | o pedido existe na Tray e nada mais | "Pedido recebido pela Tray" + "Aguardando atualização da separação/envio." |
| `shipped` | `has_shipment=1` **ou** data de postagem **ou** código **ou** URL de rastreio | "Pedido enviado" |
| `canceled` | `OrderStatus.type = canceled` | "Pedido cancelado" |

`AGUARDANDO PAGAMENTO` **nunca** vira "aguardando envio" nem "em separação":
status comercial não é status logístico.

## Segurança

- Rota do cliente: `GET /api/store/redemptions/:id/tray-status`, sob
  `requireAuth`. O navegador manda apenas o **ID do resgate**; o
  `tray_order_id` sai do banco depois da checagem de posse, que está na
  própria cláusula `WHERE id = $1 AND user_id = $2` — resgate de terceiro é
  indistinguível de inexistente (**404**, sem enumeração).
- Sem `tray_order_id` **nenhuma** chamada externa acontece.
- Admin e cliente compartilham só a **normalização**
  (`trayOrderLogistics.js`), nunca a autorização: admin segue com
  `requireAuth + requireAdmin`, cliente com `requireAuth` + posse.
- DTO do cliente é whitelist: sem resposta crua, sem status comercial cru,
  sem `store_note`, sem `Customer`/`CustomerAddress`/`ProductsSold`/`Payment`,
  sem `access_code`, sem PII.
- `tracking_url` só é aceita se for `http(s)` **devolvida pela Tray**; nunca
  montamos URL a partir do código de rastreio. O link abre com
  `rel="noopener noreferrer"`.

## Consulta sob demanda

A listagem de "Meus Pedidos" usa **somente PostgreSQL** — nenhuma chamada à
Tray. O acompanhamento é carregado quando o cliente clica em ACOMPANHAR
PEDIDO, uma consulta por pedido pedido. Sem polling, sem `setInterval`, sem
cron, sem cache persistente novo.
