-- Remove a unicidade (user_id, draw_id) de autopay_runs para permitir
-- múltiplas tentativas administrativas por usuário/sorteio.
-- Não apaga dados e não altera a primary key.

BEGIN;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'autopay_runs_user_draw_unique'
      AND conrelid = 'public.autopay_runs'::regclass
  ) THEN
    ALTER TABLE public.autopay_runs
      DROP CONSTRAINT autopay_runs_user_draw_unique;
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS autopay_runs_user_draw_created_idx
  ON public.autopay_runs (
    user_id,
    draw_id,
    created_at DESC
  );

COMMIT;
