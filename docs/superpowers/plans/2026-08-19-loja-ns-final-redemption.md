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

## Gate de produção (rodada 1)

`REWARD_REDEMPTION_ENABLED` (backend) e `REACT_APP_REWARD_REDEMPTION_ENABLED`
(frontend) continuam com default `false`. Nenhuma mutação real foi feita
contra a Tray de produção nesta rodada — todo teste de criação de pedido/
cliente Tray usa mocks fiéis ao schema oficial (nunca payload fictício).

**Não mesclado em `main`, não implantado, não habilitado em produção.**
Push de `jpedro` autorizado pelo solicitante quando os gates locais
estiverem verdes — main e produção exigem autorização explícita adicional
após este relatório.

---

## Rodada 2 — "Reta Final Definitiva" (perfil completo + Customer Tray real)

Fecha as duas dependências que ainda impediam o resgate real: (1) a Tray
exige `birth_date` pra criar um `Customer`, campo que a NewStore não
coletava; (2) o P0 de `variant_id` da rodada 1 usava a variação como
`product_id` — corrigido depois que uma auditoria mais profunda da
documentação REAL (não só o schema resumido do plugin de IA) confirmou
`product_id`/`variant_id` como campos separados.

### M1-M2 — Auditoria + decisão de estratégia

Fonte: `developers.tray.com.br` (página real, seção "Cadastrar Pedido#post"),
não só o `pedido.create.json` resumido do `tray-api-ai-plugin` usado na
rodada 1. A página real documenta uma variante com `Customer`/`CustomerAddress`/
`ProductsSold` inline (form-urlencoded), diferente do schema `customer_id`+
`products` (JSON, `additionalProperties:false`) que o plugin cita como
contrato validável.

**Decisão (aprovada explicitamente)**: manter `customer_id`+`products` —
é o contrato curado e validável pela própria Tray para uso por agentes,
menor risco de payload rejeitado. `notes` confirmado como campo real de
create (`pedido.create.json`) e de update (`pedido.update.json`, mesmo
nome — os `store_note`/`customer_note` vistos na página real pertencem à
seção de update com nomenclatura antiga, não ao schema curado atual).
Nenhuma chamada `PUT` extra foi adicionada.

### M3 — Perfil (migration 033)

`users.birth_date` (date) + `users.tray_customer_id` (cache do mapping,
unique parcial). `cpf`/`rg`/`gender` deliberadamente NÃO adicionados —
opcionais no schema oficial de `POST /customers`
(`required: ["name","email","birth_date"]`). `GET /api/me` (reusado, não
duplicado) devolve `profile_complete_for_reward` + `missing_reward_fields`;
novo `PATCH /api/me/birth-date` espelha `/api/me/phone`.

### M4 — Customer Tray real + variant_id (P0)

`trayCustomerResolver.js` (novo): cache → busca por e-mail → cria
(`POST /customers`) se o perfil estiver completo. Protegido contra criação
duplicada com `pg_advisory_lock` por `userId` — prova real (Postgres, não
mock) de que 5 tentativas concorrentes resultam em exatamente 1 criação.
`trayCustomerClient.js` corrigido: match ambíguo (mais de um Customer com
o mesmo e-mail exato) agora vira erro `tray_customer_ambiguous`, nunca
escolhe o primeiro cegamente. `trayOrderClient.js`: `products[]` agora
envia `product_id` e `variant_id` como campos separados (nunca mais a
variação substituindo o produto). Migration 034 adiciona
`blocked_tray_profile_incomplete` e `blocked_tray_customer_ambiguous` ao
`CHECK` de `reward_redemptions.status`.

### M5 — Frontend: perfil completo

`/conta`: nova seção "Data de nascimento", mesmo padrão do telefone.
`/loja/resgate`: antes da revisão, checa `profile_complete_for_reward`
(carregado em paralelo com o checkout bootstrap); se faltar, mostra
"Complete seus dados para continuar" com só o campo de nascimento —
nunca um erro técnico genérico. `prepareRedemption` só dispara quando o
perfil já está completo.

### M6 — Verificação + revisão de diff

Backend: 592/592 (unit + integração, Postgres descartável real).
Frontend: 312/312, build de produção limpo. `git diff --check` limpo nos
dois repositórios. Revisão manual de `git diff origin/main...jpedro` nos
dois repositórios (27 arquivos backend, 12 arquivos frontend) — confirmado:
nenhuma feature fora da Loja NS, nenhum secret, nenhuma alteração em
sorteios/cron/autopay/Vindi/Mercado Pago/captive, nenhum arquivo
inesperado.

### M7 — Teste controlado real (bloqueado — aguardando autorização)

Não executado nesta rodada. Requer autorização explícita do solicitante
sobre qual usuário/produto QA usar antes de qualquer mutação real
(`POST /customers` e/ou `POST /orders`) — ver relatório final.

### Commits da rodada 2 (backend, `jpedro`)

1. `26f3a56` — birth_date + tray_customer_id no perfil (M3)
2. `60702c6` — Customer Tray real (resolve+cria) + `variant_id` (M4, P0)

### Commits da rodada 2 (frontend, `jpedro`)

1. `3d00af8` — completar perfil (birth_date) na conta e no resgate

### Gate de produção (rodada 2)

Mesmo estado: `REWARD_REDEMPTION_ENABLED=false`,
`REACT_APP_REWARD_REDEMPTION_ENABLED=false`. Nenhuma mutação real contra a
Tray. Não mesclado em `main`. Bloqueadores restantes: M7 (teste
controlado, aguardando autorização) e ativação do escopo `order` do
webhook pela Tray (P0, já reportado na rodada 1, inalterado).
