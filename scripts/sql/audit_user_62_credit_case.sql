-- Audit focused on user_id = 62

-- 1) Payments
SELECT
  p.id,
  p.draw_id,
  p.numbers,
  p.amount_cents,
  p.status,
  p.provider,
  p.created_at,
  p.paid_at
FROM public.payments p
WHERE p.user_id = 62
ORDER BY p.created_at DESC, p.id DESC;

-- 2) Ledgers
SELECT
  h.payment_id,
  h.delta_cents,
  h.balance_before_cents,
  h.balance_after_cents,
  h.event_type,
  h.channel,
  h.run_trace_id,
  h.meta,
  h.created_at,
  h.event_occurred_at
FROM public.coupon_balance_history h
WHERE h.user_id = 62
ORDER BY h.created_at ASC, h.id ASC;

-- 3) Payment x ledger comparison
WITH payment_base AS (
  SELECT
    p.id AS payment_id,
    p.amount_cents
  FROM public.payments p
  WHERE p.user_id = 62
    AND lower(p.status) IN ('approved','paid','pago')
),
credit_by_payment AS (
  SELECT
    h.payment_id,
    COALESCE(SUM(h.delta_cents), 0)::int AS credited_cents
  FROM public.coupon_balance_history h
  WHERE h.user_id = 62
    AND h.payment_id IS NOT NULL
    AND h.event_type IN (
      'CREDIT_PURCHASE',
      'CREDIT_PURCHASE_RECONCILE_PAYMENT_AMOUNT',
      'CREDIT_PURCHASE_ADJUSTMENT_PAYMENT_AMOUNT',
      'CREDIT_PURCHASE_RECONCILIATION'
    )
  GROUP BY h.payment_id
)
SELECT
  pb.payment_id,
  pb.amount_cents,
  COALESCE(cp.credited_cents, 0) AS credited_cents,
  (pb.amount_cents - COALESCE(cp.credited_cents, 0))::int AS diff_cents
FROM payment_base pb
LEFT JOIN credit_by_payment cp ON cp.payment_id = pb.payment_id
ORDER BY ABS(pb.amount_cents - COALESCE(cp.credited_cents, 0)) DESC, pb.payment_id DESC;

-- 4) Totals
WITH payment_totals AS (
  SELECT
    COALESCE(SUM(p.amount_cents), 0)::int AS total_payment_amount_cents
  FROM public.payments p
  WHERE p.user_id = 62
    AND lower(p.status) IN ('approved','paid','pago')
),
credit_totals AS (
  SELECT
    COALESCE(SUM(h.delta_cents), 0)::int AS total_credited_cents
  FROM public.coupon_balance_history h
  WHERE h.user_id = 62
    AND h.payment_id IS NOT NULL
    AND h.event_type IN (
      'CREDIT_PURCHASE',
      'CREDIT_PURCHASE_RECONCILE_PAYMENT_AMOUNT',
      'CREDIT_PURCHASE_ADJUSTMENT_PAYMENT_AMOUNT',
      'CREDIT_PURCHASE_RECONCILIATION'
    )
),
admin_adj AS (
  SELECT
    COUNT(*)::int AS admin_adjustment_count,
    COALESCE(SUM(delta_cents), 0)::int AS admin_adjustment_cents
  FROM public.coupon_balance_history
  WHERE user_id = 62
    AND event_type = 'ADMIN_BALANCE_ADJUSTMENT'
),
first_ledger AS (
  SELECT balance_before_cents
  FROM public.coupon_balance_history
  WHERE user_id = 62
  ORDER BY created_at ASC, id ASC
  LIMIT 1
),
last_ledger AS (
  SELECT balance_after_cents
  FROM public.coupon_balance_history
  WHERE user_id = 62
  ORDER BY created_at DESC, id DESC
  LIMIT 1
)
SELECT
  pt.total_payment_amount_cents,
  ct.total_credited_cents,
  (pt.total_payment_amount_cents - ct.total_credited_cents)::int AS net_diff_cents,
  aa.admin_adjustment_count,
  aa.admin_adjustment_cents,
  COALESCE(fl.balance_before_cents, 0)::int AS first_ledger_balance_before_cents,
  (COALESCE(fl.balance_before_cents, 0) + pt.total_payment_amount_cents + aa.admin_adjustment_cents)::int AS expected_balance_preserving_initial,
  COALESCE(ll.balance_after_cents, 0)::int AS current_balance_from_last_ledger,
  (COALESCE(ll.balance_after_cents, 0) - (COALESCE(fl.balance_before_cents, 0) + pt.total_payment_amount_cents + aa.admin_adjustment_cents))::int AS excess_cents
FROM payment_totals pt
CROSS JOIN credit_totals ct
CROSS JOIN admin_adj aa
LEFT JOIN first_ledger fl ON true
LEFT JOIN last_ledger ll ON true;

