-- Migration: Garantir unicidade GLOBAL de números cativos (autopay_numbers.n)
-- Objetivo: impedir que dois usuários tenham o mesmo número cativo
-- Estratégia:
-- 1) Detecta duplicados e registra via NOTICE
-- 2) Remove duplicados mantendo 1 registro por número (menor id)
-- 3) Cria índice UNIQUE em public.autopay_numbers(n)

DO $$
DECLARE
  dup_count int := 0;
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'autopay_numbers'
  ) THEN
    RAISE NOTICE 'Tabela public.autopay_numbers não existe. Nada a fazer.';
    RETURN;
  END IF;

  -- Detecta duplicados
  SELECT COUNT(*) INTO dup_count
  FROM (
    SELECT n
    FROM public.autopay_numbers
    GROUP BY n
    HAVING COUNT(*) > 1
  ) d;

  IF dup_count > 0 THEN
    RAISE NOTICE 'Foram encontrados % números duplicados em public.autopay_numbers. Limpando...', dup_count;

    -- Remove duplicados mantendo o menor id (estratégia simples e determinística)
    WITH ranked AS (
      SELECT
        id,
        n,
        ROW_NUMBER() OVER (PARTITION BY n ORDER BY id ASC) AS rn
      FROM public.autopay_numbers
    )
    DELETE FROM public.autopay_numbers an
    USING ranked r
    WHERE an.id = r.id
      AND r.rn > 1;

    RAISE NOTICE 'Duplicados removidos com sucesso.';
  ELSE
    RAISE NOTICE 'Nenhum duplicado encontrado em public.autopay_numbers.';
  END IF;
END $$;

-- Índice UNIQUE global por número cativo
CREATE UNIQUE INDEX IF NOT EXISTS autopay_numbers_unique_n
ON public.autopay_numbers (n);


