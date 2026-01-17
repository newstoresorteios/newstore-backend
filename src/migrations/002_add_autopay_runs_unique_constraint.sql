-- Migration: Adiciona constraint UNIQUE(user_id, draw_id) na tabela autopay_runs
-- para evitar cobrança duplicada para o mesmo usuário no mesmo sorteio
-- 
-- Esta migration é opcional, mas recomendada para garantir idempotência
-- O código já tem proteção lógica, mas a constraint adiciona proteção no nível do banco

DO $$
BEGIN
  -- Verifica se a constraint já existe
  IF NOT EXISTS (
    SELECT 1 
    FROM pg_constraint 
    WHERE conname = 'autopay_runs_user_draw_unique'
  ) THEN
    -- Adiciona constraint UNIQUE(user_id, draw_id)
    ALTER TABLE public.autopay_runs 
    ADD CONSTRAINT autopay_runs_user_draw_unique 
    UNIQUE (user_id, draw_id);
    
    RAISE NOTICE 'Constraint autopay_runs_user_draw_unique criada com sucesso';
  ELSE
    RAISE NOTICE 'Constraint autopay_runs_user_draw_unique já existe';
  END IF;
END $$;

-- Cria índice para melhorar performance das consultas
CREATE INDEX IF NOT EXISTS idx_autopay_runs_user_draw 
ON public.autopay_runs(user_id, draw_id);

