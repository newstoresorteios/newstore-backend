-- Migration 009: Repair/normalize coupon ledger + credit flags
-- Objetivo: garantir schema correto e idempotente para crédito de saldo
-- - coupon_balance_history.payment_id MUST be TEXT (compatível com payments.id text)
-- - UNIQUE(payment_id, event_type) (ou equivalente) para anti-duplicação
-- - payments.coupon_credited / coupon_credited_at garantidos
-- - users.coupon_value_cents garantido (DEFAULT 0, NOT NULL)
--
-- IMPORTANTE: idempotente e seguro (não destrutivo para dados).

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1) Garantir tabela base (se não existir)
CREATE TABLE IF NOT EXISTS public.coupon_balance_history (
  id uuid primary key default gen_random_uuid(),
  user_id int4 not null references public.users(id) on delete cascade,
  payment_id text null,
  delta_cents int4 not null,
  balance_before_cents int4 not null,
  balance_after_cents int4 not null,
  event_type text not null,
  channel text null,
  status text null,
  draw_id int4 null,
  reservation_id text null,
  run_trace_id text null,
  meta jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

-- 2) Garantir colunas essenciais (ADD IF NOT EXISTS)
ALTER TABLE public.coupon_balance_history
  ADD COLUMN IF NOT EXISTS payment_id text null;

ALTER TABLE public.coupon_balance_history
  ADD COLUMN IF NOT EXISTS reservation_id text null;

ALTER TABLE public.coupon_balance_history
  ADD COLUMN IF NOT EXISTS meta jsonb not null default '{}'::jsonb;

ALTER TABLE public.coupon_balance_history
  ADD COLUMN IF NOT EXISTS created_at timestamptz not null default now();

-- 3) Repair: payment_id -> TEXT (se existir com tipo diferente)
DO $$
DECLARE
  payment_type text;
BEGIN
  SELECT data_type
    INTO payment_type
    FROM information_schema.columns
   WHERE table_schema='public'
     AND table_name='coupon_balance_history'
     AND column_name='payment_id';

  IF payment_type IS NOT NULL AND payment_type <> 'text' THEN
    -- Drop FK/constraints comuns (se existirem) para permitir alteração de tipo
    BEGIN
      EXECUTE 'ALTER TABLE public.coupon_balance_history DROP CONSTRAINT IF EXISTS coupon_balance_history_payment_id_fkey';
    EXCEPTION WHEN OTHERS THEN
      -- ignore
    END;

    -- Drop índices conhecidos que dependem da coluna (se existirem)
    BEGIN
      EXECUTE 'DROP INDEX IF EXISTS public.idx_coupon_hist_payment';
    EXCEPTION WHEN OTHERS THEN
      -- ignore
    END;
    BEGIN
      EXECUTE 'DROP INDEX IF EXISTS public.uq_coupon_hist_payment_credit_purchase';
    EXCEPTION WHEN OTHERS THEN
      -- ignore
    END;
    BEGIN
      EXECUTE 'DROP INDEX IF EXISTS public.uq_coupon_hist_payment_event_type';
    EXCEPTION WHEN OTHERS THEN
      -- ignore
    END;

    -- Altera tipo com cast seguro
    EXECUTE 'ALTER TABLE public.coupon_balance_history ALTER COLUMN payment_id TYPE text USING payment_id::text';
  END IF;
END $$;

-- 4) Repair: reservation_id -> TEXT (se existir com tipo diferente)
DO $$
DECLARE
  res_type text;
BEGIN
  SELECT data_type
    INTO res_type
    FROM information_schema.columns
   WHERE table_schema='public'
     AND table_name='coupon_balance_history'
     AND column_name='reservation_id';

  IF res_type IS NOT NULL AND res_type <> 'text' THEN
    EXECUTE 'ALTER TABLE public.coupon_balance_history ALTER COLUMN reservation_id TYPE text USING reservation_id::text';
  END IF;
END $$;

-- 5) Índices úteis
CREATE INDEX IF NOT EXISTS idx_coupon_hist_user_created
  ON public.coupon_balance_history (user_id, created_at desc);

CREATE INDEX IF NOT EXISTS idx_coupon_hist_payment
  ON public.coupon_balance_history (payment_id);

-- Anti-duplicação: 1 evento por payment/event_type (ex.: CREDIT_PURCHASE)
CREATE UNIQUE INDEX IF NOT EXISTS uq_coupon_hist_payment_event_type
  ON public.coupon_balance_history (payment_id, event_type)
  WHERE payment_id IS NOT NULL;

-- 6) Flags de controle em payments (repair)
ALTER TABLE public.payments
  ADD COLUMN IF NOT EXISTS coupon_credited boolean;
ALTER TABLE public.payments
  ADD COLUMN IF NOT EXISTS coupon_credited_at timestamptz null;

UPDATE public.payments
   SET coupon_credited = false
 WHERE coupon_credited IS NULL;

ALTER TABLE public.payments
  ALTER COLUMN coupon_credited SET DEFAULT false;

DO $$
BEGIN
  BEGIN
    EXECUTE 'ALTER TABLE public.payments ALTER COLUMN coupon_credited SET NOT NULL';
  EXCEPTION WHEN OTHERS THEN
    -- se não der (dados sujos/lock), mantém best-effort
  END;
END $$;

-- 7) users.coupon_value_cents (repair)
ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS coupon_value_cents int4;

UPDATE public.users
   SET coupon_value_cents = 0
 WHERE coupon_value_cents IS NULL;

ALTER TABLE public.users
  ALTER COLUMN coupon_value_cents SET DEFAULT 0;

DO $$
BEGIN
  BEGIN
    EXECUTE 'ALTER TABLE public.users ALTER COLUMN coupon_value_cents SET NOT NULL';
  EXCEPTION WHEN OTHERS THEN
    -- best-effort
  END;
END $$;

