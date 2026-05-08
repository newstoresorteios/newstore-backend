-- Audit: overcredit (diff_cents < 0) for manual review.
-- No data changes.

WITH pay AS (
  SELECT
    p.id AS payment_id,
    p.user_id,
    p.draw_id,
    p.provider,
    lower(p.status) AS payment_status,
    p.amount_cents
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
    COALESCE(SUM(h.delta_cents), 0)::int AS credited_cents
  FROM public.coupon_balance_history h
  WHERE h.payment_id IS NOT NULL
    AND h.event_type IN (
      'CREDIT_PURCHASE',
      'CREDIT_PURCHASE_RECONCILE_PAYMENT_AMOUNT',
      'CREDIT_PURCHASE_ADJUSTMENT_PAYMENT_AMOUNT',
      'CREDIT_PURCHASE_RECONCILIATION'
    )
  GROUP BY h.payment_id
),
admin_adj_total AS (
  SELECT user_id, COUNT(*)::int AS c, COALESCE(SUM(delta_cents), 0)::int AS s
  FROM public.coupon_balance_history
  WHERE event_type = 'ADMIN_BALANCE_ADJUSTMENT'
  GROUP BY user_id
),
admin_adj_after AS (
  SELECT
    p.payment_id,
    COUNT(h.*)::int AS c_after,
    COALESCE(SUM(h.delta_cents), 0)::int AS s_after
  FROM pay p
  LEFT JOIN public.coupon_balance_history h
    ON h.user_id = p.user_id
   AND h.event_type = 'ADMIN_BALANCE_ADJUSTMENT'
   AND h.created_at >= COALESCE(
     (SELECT pp.paid_at FROM public.payments pp WHERE pp.id = p.payment_id),
     (SELECT pp.created_at FROM public.payments pp WHERE pp.id = p.payment_id)
   )
  GROUP BY p.payment_id
)
SELECT
  p.payment_id,
  p.user_id,
  u.name,
  u.email,
  p.provider,
  p.draw_id,
  p.amount_cents,
  COALESCE(l.credited_cents, 0) AS credited_cents,
  ABS(p.amount_cents - COALESCE(l.credited_cents, 0))::int AS overcredited_cents,
  COALESCE(u.coupon_value_cents, 0)::int AS current_balance_cents,
  COALESCE(a.c, 0) AS admin_adjustment_count_total,
  COALESCE(a.s, 0) AS admin_adjustment_cents_total,
  COALESCE(aa.c_after, 0) AS admin_adjustment_count_after_payment,
  COALESCE(aa.s_after, 0) AS admin_adjustment_cents_after_payment,
  CASE
    WHEN COALESCE(a.c, 0) = 0
     AND (COALESCE(u.coupon_value_cents, 0) + (p.amount_cents - COALESCE(l.credited_cents, 0))) >= 0
    THEN 'ELIGIBLE_FOR_SELECTED_DEBIT_NO_ADMIN_ADJUSTMENT'
    WHEN COALESCE(a.c, 0) > 0
    THEN 'MANUAL_REVIEW_REQUIRED_HAS_ADMIN_ADJUSTMENT'
    WHEN (COALESCE(u.coupon_value_cents, 0) + (p.amount_cents - COALESCE(l.credited_cents, 0))) < 0
    THEN 'MANUAL_REVIEW_REQUIRED_BALANCE_WOULD_GO_NEGATIVE'
    ELSE 'MANUAL_REVIEW_REQUIRED'
  END AS debit_safety
FROM pay p
LEFT JOIN ledger l ON l.payment_id = p.payment_id
LEFT JOIN public.users u ON u.id = p.user_id
LEFT JOIN admin_adj_total a ON a.user_id = p.user_id
LEFT JOIN admin_adj_after aa ON aa.payment_id = p.payment_id
WHERE (p.amount_cents - COALESCE(l.credited_cents, 0)) < 0
-- AND p.user_id = 62
ORDER BY ABS(p.amount_cents - COALESCE(l.credited_cents, 0)) DESC, p.payment_id DESC;

