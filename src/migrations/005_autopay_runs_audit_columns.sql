-- Migration: alinhar schema de public.autopay_runs para auditoria do AutopayRunner (Vindi)
-- - Evita erros 42703 (updated_at inexistente) e viabiliza rastreabilidade (traceIds, provider req/res).

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables
     WHERE table_schema='public' AND table_name='autopay_runs'
  ) THEN
    RAISE NOTICE 'Tabela public.autopay_runs não existe. Nada a fazer.';
    RETURN;
  END IF;

  ALTER TABLE public.autopay_runs
    ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS run_trace_id uuid,
    ADD COLUMN IF NOT EXISTS attempt_trace_id uuid,
    ADD COLUMN IF NOT EXISTS reservation_id uuid,
    ADD COLUMN IF NOT EXISTS provider text,
    ADD COLUMN IF NOT EXISTS status text,
    ADD COLUMN IF NOT EXISTS amount_cents int4,
    ADD COLUMN IF NOT EXISTS provider_status int4,
    ADD COLUMN IF NOT EXISTS provider_bill_id text,
    ADD COLUMN IF NOT EXISTS provider_charge_id text,
    ADD COLUMN IF NOT EXISTS provider_request jsonb,
    ADD COLUMN IF NOT EXISTS provider_response jsonb,
    ADD COLUMN IF NOT EXISTS error_message text;

  -- Índices úteis
  CREATE INDEX IF NOT EXISTS idx_autopay_runs_attempt_trace_id ON public.autopay_runs(attempt_trace_id);
  CREATE INDEX IF NOT EXISTS idx_autopay_runs_run_trace_id ON public.autopay_runs(run_trace_id);
END $$;

-- Trigger updated_at
CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END; $$;

DROP TRIGGER IF EXISTS trg_autopay_runs_updated_at ON public.autopay_runs;
CREATE TRIGGER trg_autopay_runs_updated_at
BEFORE UPDATE ON public.autopay_runs
FOR EACH ROW EXECUTE PROCEDURE public.set_updated_at();


