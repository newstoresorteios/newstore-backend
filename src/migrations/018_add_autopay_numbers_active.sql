-- Migration: adicionar controle individual de participacao para numeros cativos
-- Nao altera perfis, cartoes, pagamentos, reservas ou numeros vendidos.

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
      FROM information_schema.tables
     WHERE table_schema = 'public'
       AND table_name = 'autopay_numbers'
  ) THEN
    RAISE NOTICE 'Tabela public.autopay_numbers nao existe. Nada a fazer.';
    RETURN;
  END IF;

  ALTER TABLE public.autopay_numbers
    ADD COLUMN IF NOT EXISTS active boolean NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

  CREATE INDEX IF NOT EXISTS idx_autopay_numbers_active
    ON public.autopay_numbers(active);
END $$;
