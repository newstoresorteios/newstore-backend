// Script para analisar a recorrência do autopay em produção
import "dotenv/config";
import { getPool } from "../db.js";

const pool = await getPool();

console.log("=".repeat(80));
console.log("ANÁLISE DE RECORRÊNCIA DO AUTOPAY");
console.log("=".repeat(80));
console.log();

try {
  // Verificar se a coluna created_at existe
  const checkColumn = await pool.query(`
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_schema = 'public' 
      AND table_name = 'autopay_runs' 
      AND column_name = 'created_at'
  `);
  const hasCreatedAt = checkColumn.rows.length > 0;
  
  if (!hasCreatedAt) {
    console.log("ℹ️  Nota: A tabela autopay_runs não possui coluna created_at.");
    console.log("   Usando timestamps dos payments relacionados para análise temporal.");
    console.log();
  }

  // 1. Análise geral de execuções do autopay
  console.log("1. ESTATÍSTICAS GERAIS DO AUTOPAY");
  console.log("-".repeat(80));
  
  const statsQuery = hasCreatedAt 
    ? `
      SELECT 
        COUNT(*) as total_execucoes,
        COUNT(DISTINCT draw_id) as total_draws_processados,
        COUNT(DISTINCT user_id) as total_usuarios_unicos,
        COUNT(DISTINCT autopay_id) as total_perfis_unicos,
        SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) as execucoes_ok,
        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as execucoes_erro,
        SUM(CASE WHEN status = 'ok' THEN COALESCE(amount_cents, 0) ELSE 0 END) as total_receita_cents,
        MIN(created_at) as primeira_execucao,
        MAX(created_at) as ultima_execucao
      FROM autopay_runs
    `
    : `
      SELECT 
        COUNT(*) as total_execucoes,
        COUNT(DISTINCT draw_id) as total_draws_processados,
        COUNT(DISTINCT user_id) as total_usuarios_unicos,
        COUNT(DISTINCT autopay_id) as total_perfis_unicos,
        SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) as execucoes_ok,
        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as execucoes_erro,
        SUM(CASE WHEN status = 'ok' THEN COALESCE(amount_cents, 0) ELSE 0 END) as total_receita_cents,
        MIN(COALESCE((SELECT MIN(created_at) FROM payments WHERE id = payment_id), NOW())) as primeira_execucao,
        MAX(COALESCE((SELECT MAX(created_at) FROM payments WHERE id = payment_id), NOW())) as ultima_execucao
      FROM autopay_runs ar
    `;
  
  const stats = await pool.query(statsQuery);
  
  const s = stats.rows[0];
  console.log(`Total de execuções: ${s.total_execucoes}`);
  console.log(`Draws processados: ${s.total_draws_processados}`);
  console.log(`Usuários únicos: ${s.total_usuarios_unicos}`);
  console.log(`Perfis únicos: ${s.total_perfis_unicos}`);
  console.log(`Execuções OK: ${s.execucoes_ok}`);
  console.log(`Execuções com erro: ${s.execucoes_erro}`);
  console.log(`Taxa de sucesso: ${s.total_execucoes > 0 ? ((s.execucoes_ok / s.total_execucoes) * 100).toFixed(2) : 0}%`);
  console.log(`Receita total: R$ ${(s.total_receita_cents / 100).toFixed(2)}`);
  console.log(`Primeira execução: ${s.primeira_execucao || 'N/A'}`);
  console.log(`Última execução: ${s.ultima_execucao || 'N/A'}`);
  console.log();

  // 2. Análise por período (diária)
  console.log("2. EXECUÇÕES POR DIA");
  console.log("-".repeat(80));
  
  const dailyQuery = hasCreatedAt
    ? `
      SELECT 
        DATE(created_at) as dia,
        COUNT(*) as execucoes,
        COUNT(DISTINCT draw_id) as draws,
        COUNT(DISTINCT user_id) as usuarios,
        SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) as ok,
        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as erro,
        SUM(CASE WHEN status = 'ok' THEN COALESCE(amount_cents, 0) ELSE 0 END) as receita_cents
      FROM autopay_runs
      GROUP BY DATE(created_at)
      ORDER BY dia DESC
      LIMIT 30
    `
    : `
      SELECT 
        DATE(COALESCE((SELECT MIN(created_at) FROM payments WHERE id = ar.payment_id), NOW())) as dia,
        COUNT(*) as execucoes,
        COUNT(DISTINCT draw_id) as draws,
        COUNT(DISTINCT user_id) as usuarios,
        SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) as ok,
        SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) as erro,
        SUM(CASE WHEN status = 'ok' THEN COALESCE(amount_cents, 0) ELSE 0 END) as receita_cents
      FROM autopay_runs ar
      GROUP BY DATE(COALESCE((SELECT MIN(created_at) FROM payments WHERE id = ar.payment_id), NOW()))
      ORDER BY dia DESC
      LIMIT 30
    `;
  
  const daily = await pool.query(dailyQuery);
  
  if (daily.rows.length === 0) {
    console.log("Nenhuma execução encontrada.");
  } else {
    console.log("Data       | Execuções | Draws | Usuários | OK | Erro | Receita");
    console.log("-".repeat(80));
    for (const row of daily.rows) {
      const dia = new Date(row.dia).toLocaleDateString('pt-BR');
      const receita = (row.receita_cents / 100).toFixed(2);
      console.log(
        `${dia.padEnd(10)} | ${String(row.execucoes).padStart(9)} | ${String(row.draws).padStart(5)} | ${String(row.usuarios).padStart(8)} | ${String(row.ok).padStart(2)} | ${String(row.erro).padStart(4)} | R$ ${receita.padStart(10)}`
      );
    }
  }
  console.log();

  // 3. Análise de recorrência por draw
  console.log("3. ANÁLISE DE RECORRÊNCIA POR SORTEIO");
  console.log("-".repeat(80));
  
  const draws = await pool.query(`
    SELECT 
      d.id as draw_id,
      d.status,
      d.opened_at,
      d.autopay_ran_at,
      d.closed_at,
      COUNT(DISTINCT ar.id) as total_execucoes_autopay,
      COUNT(DISTINCT ar.user_id) as usuarios_afetados,
      SUM(CASE WHEN ar.status = 'ok' THEN COALESCE(ar.amount_cents, 0) ELSE 0 END) as receita_cents,
      MIN(ar.created_at) as primeira_execucao_autopay,
      MAX(ar.created_at) as ultima_execucao_autopay
    FROM draws d
    LEFT JOIN autopay_runs ar ON ar.draw_id = d.id
    GROUP BY d.id, d.status, d.opened_at, d.autopay_ran_at, d.closed_at
    ORDER BY d.id DESC
    LIMIT 20
  `);
  
  if (draws.rows.length === 0) {
    console.log("Nenhum sorteio encontrado.");
  } else {
    console.log("Draw ID | Status | Aberto em | Autopay executado | Execuções | Usuários | Receita");
    console.log("-".repeat(80));
    for (const row of draws.rows) {
      const opened = row.opened_at ? new Date(row.opened_at).toLocaleString('pt-BR') : 'N/A';
      const autopayRan = row.autopay_ran_at ? new Date(row.autopay_ran_at).toLocaleString('pt-BR') : 'NÃO EXECUTADO';
      const receita = (row.receita_cents / 100).toFixed(2);
      console.log(
        `${String(row.draw_id).padStart(7)} | ${String(row.status).padStart(6)} | ${opened.padEnd(19)} | ${autopayRan.padEnd(18)} | ${String(row.total_execucoes_autopay).padStart(9)} | ${String(row.usuarios_afetados).padStart(8)} | R$ ${receita.padStart(10)}`
      );
    }
  }
  console.log();

  // 4. Sorteios abertos sem autopay executado
  console.log("4. SORTEIOS ABERTOS SEM AUTOPAY EXECUTADO");
  console.log("-".repeat(80));
  
  const pending = await pool.query(`
    SELECT 
      id,
      status,
      opened_at,
      autopay_ran_at,
      EXTRACT(EPOCH FROM (NOW() - opened_at)) / 3600 as horas_aberto
    FROM draws
    WHERE status IN ('open', 'aberto')
      AND autopay_ran_at IS NULL
    ORDER BY opened_at DESC
  `);
  
  if (pending.rows.length === 0) {
    console.log("✓ Todos os sorteios abertos já tiveram autopay executado.");
  } else {
    console.log(`⚠️  ATENÇÃO: ${pending.rows.length} sorteio(s) aberto(s) sem autopay executado:`);
    for (const row of pending.rows) {
      const opened = row.opened_at ? new Date(row.opened_at).toLocaleString('pt-BR') : 'N/A';
      const horas = row.horas_aberto ? row.horas_aberto.toFixed(1) : 'N/A';
      console.log(`  - Draw #${row.id} (aberto há ${horas} horas em ${opened})`);
    }
  }
  console.log();

  // 5. Análise de frequência de execução
  console.log("5. ANÁLISE DE FREQUÊNCIA DE EXECUÇÃO");
  console.log("-".repeat(80));
  
  const frequencyQuery = hasCreatedAt
    ? `
      WITH execucoes AS (
        SELECT 
          draw_id,
          created_at,
          LAG(created_at) OVER (ORDER BY created_at) as execucao_anterior
        FROM autopay_runs
        WHERE draw_id IS NOT NULL
      ),
      intervalos AS (
        SELECT 
          draw_id,
          created_at,
          execucao_anterior,
          EXTRACT(EPOCH FROM (created_at - execucao_anterior)) / 3600 as horas_entre_execucoes
        FROM execucoes
        WHERE execucao_anterior IS NOT NULL
      )
      SELECT 
        COUNT(*) as total_intervalos,
        AVG(horas_entre_execucoes) as media_horas,
        MIN(horas_entre_execucoes) as min_horas,
        MAX(horas_entre_execucoes) as max_horas,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY horas_entre_execucoes) as mediana_horas
      FROM intervalos
    `
    : `
      WITH execucoes AS (
        SELECT 
          ar.draw_id,
          COALESCE((SELECT MIN(created_at) FROM payments WHERE id = ar.payment_id), NOW()) as execucao_time,
          LAG(COALESCE((SELECT MIN(created_at) FROM payments WHERE id = ar.payment_id), NOW())) OVER (ORDER BY COALESCE((SELECT MIN(created_at) FROM payments WHERE id = ar.payment_id), NOW())) as execucao_anterior
        FROM autopay_runs ar
        WHERE ar.draw_id IS NOT NULL
      ),
      intervalos AS (
        SELECT 
          draw_id,
          execucao_time,
          execucao_anterior,
          EXTRACT(EPOCH FROM (execucao_time - execucao_anterior)) / 3600 as horas_entre_execucoes
        FROM execucoes
        WHERE execucao_anterior IS NOT NULL
      )
      SELECT 
        COUNT(*) as total_intervalos,
        AVG(horas_entre_execucoes) as media_horas,
        MIN(horas_entre_execucoes) as min_horas,
        MAX(horas_entre_execucoes) as max_horas,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY horas_entre_execucoes) as mediana_horas
      FROM intervalos
    `;
  
  const frequency = await pool.query(frequencyQuery);
  
  const freq = frequency.rows[0];
  if (freq.total_intervalos > 0) {
    console.log(`Total de intervalos analisados: ${freq.total_intervalos}`);
    console.log(`Média de horas entre execuções: ${freq.media_horas ? freq.media_horas.toFixed(2) : 'N/A'} horas`);
    console.log(`Mínimo: ${freq.min_horas ? freq.min_horas.toFixed(2) : 'N/A'} horas`);
    console.log(`Máximo: ${freq.max_horas ? freq.max_horas.toFixed(2) : 'N/A'} horas`);
    console.log(`Mediana: ${freq.mediana_horas ? freq.mediana_horas.toFixed(2) : 'N/A'} horas`);
  } else {
    console.log("Dados insuficientes para análise de frequência.");
  }
  console.log();

  // 6. Análise de padrão de execução por draw
  console.log("6. PADRÃO DE EXECUÇÃO POR SORTEIO");
  console.log("-".repeat(80));
  
  const pattern = await pool.query(`
    SELECT 
      d.id as draw_id,
      d.opened_at,
      d.autopay_ran_at,
      EXTRACT(EPOCH FROM (d.autopay_ran_at - d.opened_at)) / 60 as minutos_apos_abertura,
      COUNT(DISTINCT ar.id) as execucoes_autopay
    FROM draws d
    LEFT JOIN autopay_runs ar ON ar.draw_id = d.id
    WHERE d.autopay_ran_at IS NOT NULL
    GROUP BY d.id, d.opened_at, d.autopay_ran_at
    ORDER BY d.id DESC
    LIMIT 10
  `);
  
  if (pattern.rows.length === 0) {
    console.log("Nenhum padrão encontrado.");
  } else {
    console.log("Draw ID | Minutos após abertura | Execuções autopay");
    console.log("-".repeat(80));
    for (const row of pattern.rows) {
      const minutos = row.minutos_apos_abertura ? row.minutos_apos_abertura.toFixed(1) : 'N/A';
      console.log(
        `${String(row.draw_id).padStart(7)} | ${minutos.padStart(20)} | ${String(row.execucoes_autopay).padStart(18)}`
      );
    }
  }
  console.log();

  // 7. Erros mais comuns
  console.log("7. ERROS MAIS COMUNS");
  console.log("-".repeat(80));
  
  const errors = await pool.query(`
    SELECT 
      error,
      COUNT(*) as ocorrencias,
      COUNT(DISTINCT draw_id) as draws_afetados,
      COUNT(DISTINCT user_id) as usuarios_afetados
    FROM autopay_runs
    WHERE status = 'error'
      AND error IS NOT NULL
    GROUP BY error
    ORDER BY ocorrencias DESC
    LIMIT 10
  `);
  
  if (errors.rows.length === 0) {
    console.log("Nenhum erro registrado.");
  } else {
    console.log("Erro | Ocorrências | Draws afetados | Usuários afetados");
    console.log("-".repeat(80));
    for (const row of errors.rows) {
      const errorMsg = (row.error || 'N/A').substring(0, 40);
      console.log(
        `${errorMsg.padEnd(40)} | ${String(row.ocorrencias).padStart(12)} | ${String(row.draws_afetados).padStart(14)} | ${String(row.usuarios_afetados).padStart(16)}`
      );
    }
  }
  console.log();

  // 8. Resumo executivo
  console.log("8. RESUMO EXECUTIVO");
  console.log("-".repeat(80));
  
  const summary = await pool.query(`
    SELECT 
      (SELECT COUNT(*) FROM draws WHERE status IN ('open', 'aberto')) as draws_abertos,
      (SELECT COUNT(*) FROM draws WHERE status IN ('open', 'aberto') AND autopay_ran_at IS NULL) as draws_sem_autopay,
      (SELECT COUNT(*) FROM autopay_profiles WHERE active = true) as perfis_ativos,
      (SELECT COUNT(*) FROM autopay_profiles WHERE active = true AND mp_customer_id IS NOT NULL AND mp_card_id IS NOT NULL) as perfis_prontos,
      (SELECT COUNT(*) FROM autopay_runs ${hasCreatedAt ? "WHERE created_at >= NOW() - INTERVAL '7 days'" : "WHERE payment_id IN (SELECT id FROM payments WHERE created_at >= NOW() - INTERVAL '7 days')"}) as execucoes_ultimos_7_dias,
      (SELECT COUNT(*) FROM autopay_runs ${hasCreatedAt ? "WHERE created_at >= NOW() - INTERVAL '30 days'" : "WHERE payment_id IN (SELECT id FROM payments WHERE created_at >= NOW() - INTERVAL '30 days')"}) as execucoes_ultimos_30_dias
  `);
  
  const sum = summary.rows[0];
  console.log(`Draws abertos: ${sum.draws_abertos}`);
  console.log(`Draws sem autopay executado: ${sum.draws_sem_autopay}`);
  console.log(`Perfis autopay ativos: ${sum.perfis_ativos}`);
  console.log(`Perfis prontos (com cartão): ${sum.perfis_prontos}`);
  console.log(`Execuções últimos 7 dias: ${sum.execucoes_ultimos_7_dias}`);
  console.log(`Execuções últimos 30 dias: ${sum.execucoes_ultimos_30_dias}`);
  console.log();

  console.log("=".repeat(80));
  console.log("ANÁLISE CONCLUÍDA");
  console.log("=".repeat(80));

} catch (error) {
  console.error("Erro ao executar análise:", error);
  process.exit(1);
} finally {
  await pool.end();
}

