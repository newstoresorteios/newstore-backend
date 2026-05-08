-- Migration 010: event_occurred_at for financial/event time semantics
-- created_at = technical insertion timestamp
-- event_occurred_at = business/financial event timestamp

ALTER TABLE public.coupon_balance_history
  ADD COLUMN IF NOT EXISTS event_occurred_at timestamptz null;

-- CREDIT_PURCHASE linked to payment: prefer paid_at, then payment.created_at
UPDATE public.coupon_balance_history h
   SET event_occurred_at = COALESCE(p.paid_at, p.created_at, h.created_at)
  FROM public.payments p
 WHERE h.payment_id IS NOT NULL
   AND h.payment_id = p.id
   AND h.event_type = 'CREDIT_PURCHASE'
   AND h.event_occurred_at IS NULL;

-- ADMIN_BALANCE_ADJUSTMENT: occurred when ledger was created
UPDATE public.coupon_balance_history h
   SET event_occurred_at = h.created_at
 WHERE h.event_type = 'ADMIN_BALANCE_ADJUSTMENT'
   AND h.event_occurred_at IS NULL;

-- Generic repair events: fallback to created_at
UPDATE public.coupon_balance_history h
   SET event_occurred_at = COALESCE(h.event_occurred_at, h.created_at)
 WHERE h.event_type IN (
   'REPAIR_BALANCE_RECONCILIATION',
   'CREDIT_PURCHASE_RECONCILE_PAYMENT_AMOUNT',
   'CREDIT_PURCHASE_ADJUSTMENT_PAYMENT_AMOUNT',
   'CREDIT_PURCHASE_RECONCILIATION',
   'USER_BALANCE_RECONCILE_FULL_LEDGER'
 )
   AND h.event_occurred_at IS NULL;

-- Legacy baseline: fallback to created_at
UPDATE public.coupon_balance_history h
   SET event_occurred_at = COALESCE(h.event_occurred_at, h.created_at)
 WHERE h.event_type = 'LEGACY_BALANCE_BASELINE'
   AND h.event_occurred_at IS NULL;

-- Catch-all fallback to keep timeline queryable
UPDATE public.coupon_balance_history h
   SET event_occurred_at = COALESCE(h.event_occurred_at, h.created_at)
 WHERE h.event_occurred_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_coupon_hist_user_event_occurred
  ON public.coupon_balance_history (user_id, event_occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_coupon_hist_event_occurred
  ON public.coupon_balance_history (event_occurred_at DESC);

