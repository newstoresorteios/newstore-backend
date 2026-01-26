-- Migration: Ledger de saldo (coupon) + flags de crédito
-- Objetivo: crédito idempotente/concurrent-safe via payments.status='approved'
-- IMPORTANTE: Apenas aditiva/compatível (não destrutiva)

-- gen_random_uuid() (Supabase/Postgres) — seguro e aditivo
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================================
-- 1) Ledger / histórico do saldo
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.coupon_balance_history (
  id uuid primary key default gen_random_uuid(),
  user_id int4 not null references public.users(id) on delete cascade,

  -- payments.id neste projeto é text (ex.: id do MP como string, ou autopay:vindi:bill:...)
  payment_id text null references public.payments(id) on delete set null,

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

CREATE INDEX IF NOT EXISTS idx_coupon_hist_user_created
  ON public.coupon_balance_history (user_id, created_at desc);

CREATE INDEX IF NOT EXISTS idx_coupon_hist_payment
  ON public.coupon_balance_history (payment_id);

-- Anti-duplicação (ledger): 1 crédito por payment/event_type
CREATE UNIQUE INDEX IF NOT EXISTS uq_coupon_hist_payment_credit_purchase
  ON public.coupon_balance_history (payment_id, event_type)
  WHERE payment_id IS NOT NULL AND event_type = 'CREDIT_PURCHASE';

-- ============================================================================
-- 2) Flags de controle em payments (somente ADD)
-- ============================================================================

ALTER TABLE public.payments
  ADD COLUMN IF NOT EXISTS coupon_credited boolean not null default false;

ALTER TABLE public.payments
  ADD COLUMN IF NOT EXISTS coupon_credited_at timestamptz null;

-- ============================================================================
-- 3) Garantir colunas em users (somente ADD)
-- ============================================================================

ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS coupon_value_cents int4 not null default 0;

ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS coupon_updated_at timestamptz null;

