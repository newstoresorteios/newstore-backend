# Loja NS — fechamento do resgate por NSCréditos (plano executado)

Data: 2026-08-19 · Branch: `jpedro` (backend + frontend) · Não mesclado em `main`.

Ver o desenho completo em
`docs/superpowers/specs/2026-08-19-loja-ns-final-redemption-design.md`.

## Ordem de execução (todas as fases concluídas nesta rodada)

| Fase | Escopo | Status |
|---|---|---|
| A | Auditoria factual do que já existia (Foundation) | Concluída |
| B | Pesquisa do contrato oficial Tray (cart/order/pagamento) | Concluída |
| C | Tray cart client | Concluída — decidido que **não é necessário** (ver design doc) |
| D | Tray order client real + resolução de `customer_id` | Concluída |
| E | Saga externa (TX curta fora de chamada Tray) | Concluída — já seguia o padrão; documentado |
| F | Sincronização imediata do cupom Tray pós-débito | Concluída |
| G | Reconciliação de gasto direto na Tray (P0) + auditoria de login | Concluída — webhook pronto, bloqueado por ativação externa |
| H | Frontend: carrinho, revisão, confirmação, sucesso, estados honestos | Concluída |
| I | Meus Pedidos: status real + número do pedido Tray | Concluída (dobrada com H) |
| J | Testes completos (Postgres descartável) + build | Concluída |
| K/L | Documentação, commits, gate de produção, relatório final | Em andamento (este documento) |

## Commits desta rodada

Backend (`jpedro`, à frente de `origin/jpedro`):
1. `2f9da8e` — pedido Tray real via customer lookup + `POST /orders`
2. `19d55b6` — documenta o padrão de TX curta já usado pela saga
3. `f69fde9` — sincroniza o cupom Tray imediatamente pós-resgate
4. `fbe46c9` — reconcilia gasto direto do cupom na Tray (webhook, P0)
5. `bc845f7` — atualiza comentário desatualizado em `store_redemptions.js`
   (correção de documentação, feita por um subagente durante a auditoria do
   frontend — fora do escopo explícito dado a ele; revisada, é apenas um
   comentário, factualmente correta, sem mudança de comportamento; registrada
   aqui por transparência)

Frontend (`jpedro`, à frente de `origin/jpedro`):
1. `c06223e` — fecha o fluxo de resgate no frontend

## Testes

- Backend: 564/564 (unit + integration) contra Postgres descartável real
  (`initdb`, sem serviço Windows, sem credenciais de produção).
- Frontend: 310/310 (`--runInBand`; sob `--maxWorkers=50%` algumas suites
  não relacionadas a este trabalho falham por contenção de recursos do
  ambiente — confirmado como não-regressão rodando os mesmos arquivos
  isolados e em modo serial).
- Build de produção do frontend: limpo (`Compiled successfully`).
- `git diff --check`: limpo nos dois repositórios.

## Gate de produção

`REWARD_REDEMPTION_ENABLED` (backend) e `REACT_APP_REWARD_REDEMPTION_ENABLED`
(frontend) continuam com default `false`. Nenhuma mutação real foi feita
contra a Tray de produção nesta rodada — todo teste de criação de pedido/
cliente Tray usa mocks fiéis ao schema oficial (nunca payload fictício).

**Não mesclado em `main`, não implantado, não habilitado em produção.**
Push de `jpedro` autorizado pelo solicitante quando os gates locais
estiverem verdes — main e produção exigem autorização explícita adicional
após este relatório.
