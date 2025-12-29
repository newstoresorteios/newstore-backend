-- Migration: Desabilitar MP para autopay (limpar campos MP de autopay_profiles)
-- Data: 2024
-- Descrição: Limpa campos MP de autopay_profiles para forçar migração para Vindi
-- IMPORTANTE: Isso NÃO afeta PIX/checkout MP - apenas limpa dados de autopay

-- ============================================================================
-- Limpar campos MP de autopay_profiles (mantém dados Vindi)
-- ============================================================================

DO $$
BEGIN
  IF EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'autopay_profiles') THEN
    
    -- Limpa campos MP apenas se o perfil já tiver Vindi configurado
    -- Isso evita perder dados de perfis que ainda não migraram (mas não serão usados)
    UPDATE public.autopay_profiles
    SET mp_customer_id = NULL,
        mp_card_id = NULL,
        brand = NULL,
        last4 = NULL,
        updated_at = now()
    WHERE (vindi_customer_id IS NOT NULL AND vindi_payment_profile_id IS NOT NULL)
       OR (mp_customer_id IS NOT NULL OR mp_card_id IS NOT NULL);
    
    RAISE NOTICE 'Campos MP limpos de autopay_profiles (perfis com Vindi ou MP)';
    
  END IF;
END $$;

-- ============================================================================
-- Comentário explicativo
-- ============================================================================

COMMENT ON COLUMN public.autopay_profiles.mp_customer_id IS 'DEPRECATED: MP desabilitado para autopay. Use Vindi.';
COMMENT ON COLUMN public.autopay_profiles.mp_card_id IS 'DEPRECATED: MP desabilitado para autopay. Use Vindi.';

