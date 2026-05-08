-- Audit: users.coupon_value_cents vs full ledger balance (all delta events)

WITH ledger_by_user AS (
  SELECT
    h.user_id,
    COALESCE(SUM(h.delta_cents), 0)::int AS ledger_balance_cents,
    COALESCE(SUM(h.delta_cents) FILTER (WHERE h.event_type = 'CREDIT_PURCHASE'), 0)::int AS credit_purchase_cents,
    COALESCE(SUM(h.delta_cents) FILTER (WHERE h.event_type IN (
      'CREDIT_PURCHASE_RECONCILE_PAYMENT_AMOUNT',
      'CREDIT_PURCHASE_ADJUSTMENT_PAYMENT_AMOUNT',
      'CREDIT_PURCHASE_RECONCILIATION'
    )), 0)::int AS payment_repair_cents,
    COALESCE(SUM(h.delta_cents) FILTER (WHERE h.event_type = 'ADMIN_BALANCE_ADJUSTMENT'), 0)::int AS admin_adjustment_cents,
    COUNT(*) FILTER (WHERE h.event_type = 'ADMIN_BALANCE_ADJUSTMENT')::int AS admin_adjustment_count,
    COALESCE(SUM(h.delta_cents) FILTER (WHERE h.event_type = 'LEGACY_BALANCE_BASELINE'), 0)::int AS legacy_baseline_cents,
    MAX(h.created_at) AS last_ledger_created_at
  FROM public.coupon_balance_history h
  GROUP BY h.user_id
)
SELECT
  u.id AS user_id,
  u.name,
  u.email,
  COALESCE(u.coupon_value_cents, 0)::int AS user_balance_cents,
  COALESCE(l.ledger_balance_cents, 0)::int AS ledger_balance_cents,
  (COALESCE(u.coupon_value_cents, 0)::int - COALESCE(l.ledger_balance_cents, 0)::int) AS diff_cents,
  COALESCE(l.credit_purchase_cents, 0)::int AS credit_purchase_cents,
  COALESCE(l.payment_repair_cents, 0)::int AS payment_repair_cents,
  COALESCE(l.admin_adjustment_cents, 0)::int AS admin_adjustment_cents,
  COALESCE(l.admin_adjustment_count, 0)::int AS admin_adjustment_count,
  COALESCE(l.legacy_baseline_cents, 0)::int AS legacy_baseline_cents,
  l.last_ledger_created_at
FROM public.users u
LEFT JOIN ledger_by_user l ON l.user_id = u.id
WHERE COALESCE(u.coupon_value_cents, 0)::int <> COALESCE(l.ledger_balance_cents, 0)::int
ORDER BY ABS(COALESCE(u.coupon_value_cents, 0)::int - COALESCE(l.ledger_balance_cents, 0)::int) DESC, u.id DESC;

