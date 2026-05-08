-- Create LEGACY_BALANCE_BASELINE for users with positive legacy diff (user balance > ledger)
-- Safety: does not apply to users with pending overcredit by payment diff.
-- Does not update users.coupon_value_cents (ledger-only alignment).

BEGIN;

WITH payment_diff AS (
  SELECT
    p.user_id,
    p.id AS payment_id,
    (COALESCE(p.amount_cents, 0) - COALESCE((
      SELECT SUM(h.delta_cents)::int
      FROM public.coupon_balance_history h
      WHERE h.payment_id = p.id
        AND h.event_type IN (
          'CREDIT_PURCHASE',
          'CREDIT_PURCHASE_RECONCILE_PAYMENT_AMOUNT',
          'CREDIT_PURCHASE_ADJUSTMENT_PAYMENT_AMOUNT',
          'CREDIT_PURCHASE_RECONCILIATION'
        )
    ), 0))::int AS diff_cents
  FROM public.payments p
  WHERE lower(p.status) IN ('approved','paid','pago')
    AND COALESCE(p.amount_cents, 0) > 0
),
users_with_pending_overcredit AS (
  SELECT DISTINCT user_id
  FROM payment_diff
  WHERE diff_cents < 0
),
ledger_sum AS (
  SELECT user_id, COALESCE(SUM(delta_cents), 0)::int AS ledger_balance_cents
  FROM public.coupon_balance_history
  GROUP BY user_id
),
candidates AS (
  SELECT
    u.id AS user_id,
    COALESCE(u.coupon_value_cents, 0)::int AS current_user_balance_cents,
    COALESCE(l.ledger_balance_cents, 0)::int AS current_ledger_balance_cents,
    (COALESCE(u.coupon_value_cents, 0)::int - COALESCE(l.ledger_balance_cents, 0)::int) AS diff_cents
  FROM public.users u
  LEFT JOIN ledger_sum l ON l.user_id = u.id
  WHERE (COALESCE(u.coupon_value_cents, 0)::int - COALESCE(l.ledger_balance_cents, 0)::int) > 0
    AND NOT EXISTS (
      SELECT 1 FROM users_with_pending_overcredit x WHERE x.user_id = u.id
    )
),
ins AS (
  INSERT INTO public.coupon_balance_history
    (user_id, payment_id, delta_cents, balance_before_cents, balance_after_cents,
     event_type, channel, status, draw_id, reservation_id, run_trace_id, meta, event_occurred_at)
  SELECT
    c.user_id,
    NULL,
    c.diff_cents,
    c.current_ledger_balance_cents,
    c.current_user_balance_cents,
    'LEGACY_BALANCE_BASELINE',
    'MIGRATION',
    'approved',
    NULL,
    NULL,
    'legacy_balance_baseline_20260508',
    jsonb_build_object(
      'source', 'manual_sql.create_legacy_balance_baseline',
      'reason', 'coupon_balance_history_introduced_after_manual_admin_operations',
      'current_user_balance_cents', c.current_user_balance_cents,
      'current_ledger_balance_cents', c.current_ledger_balance_cents,
      'diff_cents', c.diff_cents
    ),
    now()
  FROM candidates c
  WHERE NOT EXISTS (
    SELECT 1
    FROM public.coupon_balance_history h
    WHERE h.user_id = c.user_id
      AND h.event_type = 'LEGACY_BALANCE_BASELINE'
      AND h.run_trace_id = 'legacy_balance_baseline_20260508'
  )
  RETURNING user_id
)
SELECT
  (SELECT COUNT(*)::int FROM candidates) AS positive_legacy_diff_candidates,
  (SELECT COUNT(*)::int FROM ins) AS baselines_inserted;

COMMIT;

