-- 007_payments_vindi_bill_unique.sql
-- Evita duplicidade de pagamento Vindi por bill_id (idempotência hardening).
-- Índice parcial para não afetar pagamentos não-Vindi / sem bill.

DO $$
BEGIN
  IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'payments') THEN
    -- Se já existir índice equivalente, não recria
    EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS payments_vindi_bill_id_uniq
             ON public.payments(vindi_bill_id)
             WHERE vindi_bill_id IS NOT NULL';
  END IF;
END $$;


