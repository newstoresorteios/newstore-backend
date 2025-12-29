-- Migration: Adicionar suporte Vindi ao autopay
-- Data: 2024
-- Descrição: Adiciona colunas Vindi nas tabelas autopay_profiles e payments

-- ============================================================================
-- 1. Tabela autopay_profiles: adicionar colunas Vindi
-- ============================================================================

-- Verifica se a tabela existe antes de alterar
DO $$
BEGIN
  IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'autopay_profiles') THEN
    
    -- Adiciona colunas Vindi (se não existirem)
    IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'autopay_profiles' AND column_name = 'vindi_customer_id') THEN
      ALTER TABLE public.autopay_profiles ADD COLUMN vindi_customer_id text;
    END IF;

    IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'autopay_profiles' AND column_name = 'vindi_payment_profile_id') THEN
      ALTER TABLE public.autopay_profiles ADD COLUMN vindi_payment_profile_id text;
    END IF;

    IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'autopay_profiles' AND column_name = 'vindi_last_four') THEN
      ALTER TABLE public.autopay_profiles ADD COLUMN vindi_last_four text;
    END IF;

    IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'autopay_profiles' AND column_name = 'vindi_status') THEN
      ALTER TABLE public.autopay_profiles ADD COLUMN vindi_status text;
    END IF;

    IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'autopay_profiles' AND column_name = 'vindi_failed_reason') THEN
      ALTER TABLE public.autopay_profiles ADD COLUMN vindi_failed_reason text;
    END IF;

    -- Garante que updated_at existe (pode já existir)
    IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'autopay_profiles' AND column_name = 'updated_at') THEN
      ALTER TABLE public.autopay_profiles ADD COLUMN updated_at timestamptz DEFAULT now();
    END IF;

    -- Cria índices úteis
    CREATE INDEX IF NOT EXISTS idx_autopay_profiles_vindi_customer_id ON public.autopay_profiles(vindi_customer_id) WHERE vindi_customer_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_autopay_profiles_vindi_payment_profile_id ON public.autopay_profiles(vindi_payment_profile_id) WHERE vindi_payment_profile_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_autopay_profiles_user_id ON public.autopay_profiles(user_id);

  END IF;
END $$;

-- ============================================================================
-- 2. Tabela payments: adicionar colunas provider e Vindi
-- ============================================================================

DO $$
BEGIN
  IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'payments') THEN
    
    -- Adiciona coluna provider (enum implícito: 'mercadopago'|'vindi')
    IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'payments' AND column_name = 'provider') THEN
      ALTER TABLE public.payments ADD COLUMN provider text DEFAULT 'mercadopago';
    END IF;

    -- Colunas Vindi
    IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'payments' AND column_name = 'vindi_bill_id') THEN
      ALTER TABLE public.payments ADD COLUMN vindi_bill_id text;
    END IF;

    IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'payments' AND column_name = 'vindi_charge_id') THEN
      ALTER TABLE public.payments ADD COLUMN vindi_charge_id text;
    END IF;

    IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'payments' AND column_name = 'vindi_status') THEN
      ALTER TABLE public.payments ADD COLUMN vindi_status text;
    END IF;

    -- vindi_payload_json (opcional, para debug - cuidado com dados sensíveis)
    IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'payments' AND column_name = 'vindi_payload_json') THEN
      ALTER TABLE public.payments ADD COLUMN vindi_payload_json jsonb;
    END IF;

    -- Cria índices úteis
    CREATE INDEX IF NOT EXISTS idx_payments_provider ON public.payments(provider);
    CREATE INDEX IF NOT EXISTS idx_payments_vindi_bill_id ON public.payments(vindi_bill_id) WHERE vindi_bill_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_payments_vindi_charge_id ON public.payments(vindi_charge_id) WHERE vindi_charge_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_payments_draw_id ON public.payments(draw_id) WHERE draw_id IS NOT NULL;

  END IF;
END $$;

-- ============================================================================
-- 3. Tabela autopay_runs: adicionar coluna provider (opcional, para auditoria)
-- ============================================================================

DO $$
BEGIN
  IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'autopay_runs') THEN
    
    IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'autopay_runs' AND column_name = 'provider') THEN
      ALTER TABLE public.autopay_runs ADD COLUMN provider text DEFAULT 'mercadopago';
    END IF;

    CREATE INDEX IF NOT EXISTS idx_autopay_runs_provider ON public.autopay_runs(provider) WHERE provider IS NOT NULL;

  END IF;
END $$;

-- ============================================================================
-- 4. Comentários para documentação
-- ============================================================================

COMMENT ON COLUMN public.autopay_profiles.vindi_customer_id IS 'ID do customer na Vindi';
COMMENT ON COLUMN public.autopay_profiles.vindi_payment_profile_id IS 'ID do payment_profile (cartão) na Vindi';
COMMENT ON COLUMN public.autopay_profiles.vindi_last_four IS 'Últimos 4 dígitos do cartão (Vindi)';
COMMENT ON COLUMN public.autopay_profiles.vindi_status IS 'Status do perfil na Vindi';
COMMENT ON COLUMN public.payments.provider IS 'Provider do pagamento: mercadopago ou vindi';
COMMENT ON COLUMN public.payments.vindi_bill_id IS 'ID da bill na Vindi';
COMMENT ON COLUMN public.payments.vindi_charge_id IS 'ID do charge na Vindi';

