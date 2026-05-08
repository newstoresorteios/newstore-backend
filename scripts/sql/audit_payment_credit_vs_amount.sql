-- Audit: compare payment.amount_cents vs credited cents by payment_id
-- Includes only payment-linked credit events. Excludes admin-only adjustments/baselines.

WITH pay AS (
  SELECT
    p.id AS payment_id,
    p.user_id,
    p.draw_id,
    p.provider,
    lower(p.status) AS payment_status,
    p.amount_cents,
    p.coupon_credited,
    p.numbers,
    p.created_at AS payment_created_at,
    p.paid_at AS payment_paid_at,
    to_jsonb(p) AS pjson
  FROM public.payments p
  WHERE lower(p.status) IN ('approved','paid','pago')
    AND COALESCE(p.amount_cents, 0) > 0
    AND COALESCE(lower(p.provider), '') <> 'admin_assign_no_coupon'
    AND COALESCE((to_jsonb(p)->'meta'->>'no_coupon_credit')::boolean, false) = false
    AND COALESCE((to_jsonb(p)->'payload'->>'no_coupon_credit')::boolean, false) = false
    AND COALESCE((to_jsonb(p)->'vindi_payload_json'->>'no_coupon_credit')::boolean, false) = false
),
ledger AS (
  SELECT
    h.payment_id,
    COALESCE(SUM(h.delta_cents), 0)::int AS credited_cents,
    MIN(h.created_at) AS ledger_created_at_min,
    MAX(h.created_at) AS ledger_created_at_max
  FROM public.coupon_balance_history h
  WHERE h.payment_id IS NOT NULL
    AND h.event_type IN (
      'CREDIT_PURCHASE',
      'CREDIT_PURCHASE_RECONCILE_PAYMENT_AMOUNT',
      'CREDIT_PURCHASE_ADJUSTMENT_PAYMENT_AMOUNT',
      'CREDIT_PURCHASE_RECONCILIATION'
    )
  GROUP BY h.payment_id
)
SELECT
  p.payment_id,
  p.user_id,
  u.name,
  u.email,
  p.provider,
  p.payment_status,
  p.draw_id,
  p.amount_cents,
  COALESCE(l.credited_cents, 0) AS credited_cents,
  (p.amount_cents - COALESCE(l.credited_cents, 0))::int AS diff_cents,
  p.coupon_credited,
  p.numbers,
  p.payment_created_at,
  p.payment_paid_at,
  l.ledger_created_at_min,
  l.ledger_created_at_max
FROM pay p
LEFT JOIN ledger l ON l.payment_id = p.payment_id
LEFT JOIN public.users u ON u.id = p.user_id
-- AND p.user_id = 62
-- AND p.payment_id = '...'
ORDER BY ABS(p.amount_cents - COALESCE(l.credited_cents, 0)) DESC, p.payment_id DESC;

