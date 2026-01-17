-- Migration: Índice para idempotência do Autopay Runner por (autopay_id, draw_id)
-- Ajuda nas queries do runner: select 1 from autopay_runs where autopay_id=$1 and draw_id=$2 and status='ok'

DO $$
BEGIN
  IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema='public' AND table_name='autopay_runs') THEN
    CREATE INDEX IF NOT EXISTS idx_autopay_runs_autopay_draw
      ON public.autopay_runs(autopay_id, draw_id);
  END IF;
END $$;


