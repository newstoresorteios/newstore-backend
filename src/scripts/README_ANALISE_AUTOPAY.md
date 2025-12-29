# Análise de Recorrência do Autopay

Este script analisa a recorrência e o comportamento do sistema de autopay em produção.

## Como executar

```bash
npm run analisar-autopay
```

Ou diretamente:

```bash
node src/scripts/analisar_recorrencia_autopay.js
```

## O que o script analisa

O script gera um relatório completo com as seguintes seções:

### 1. Estatísticas Gerais do Autopay
- Total de execuções
- Número de draws processados
- Usuários e perfis únicos
- Taxa de sucesso
- Receita total gerada
- Primeira e última execução

### 2. Execuções por Dia
- Histórico dos últimos 30 dias
- Execuções, draws e usuários por dia
- Receita diária

### 3. Análise de Recorrência por Sorteio
- Status de cada sorteio
- Quando o autopay foi executado
- Quantidade de execuções por sorteio
- Receita por sorteio

### 4. Sorteios Abertos sem Autopay Executado
- Identifica sorteios que estão abertos mas não tiveram autopay executado
- Mostra há quanto tempo estão abertos

### 5. Análise de Frequência de Execução
- Média de horas entre execuções
- Mínimo, máximo e mediana de intervalos
- Padrões de recorrência

### 6. Padrão de Execução por Sorteio
- Tempo entre abertura do sorteio e execução do autopay
- Quantidade de execuções por sorteio

### 7. Erros Mais Comuns
- Tipos de erro mais frequentes
- Quantidade de ocorrências
- Draws e usuários afetados

### 8. Resumo Executivo
- Draws abertos vs. sem autopay
- Perfis ativos e prontos
- Execuções recentes (7 e 30 dias)

## Requisitos

- Conexão com o banco de dados PostgreSQL configurada via variáveis de ambiente
- Acesso de leitura às tabelas:
  - `autopay_runs`
  - `autopay_profiles`
  - `draws`
  - `payments`

## Notas

- O script detecta automaticamente se a tabela `autopay_runs` possui a coluna `created_at`
- Se não possuir, usa timestamps dos `payments` relacionados para análise temporal
- Todas as consultas são apenas de leitura (SELECT), não modificam dados





