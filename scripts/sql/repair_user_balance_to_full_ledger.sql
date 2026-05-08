-- Repair user balance cache to full ledger balance
-- Use only after payment repairs + overcredit review + optional legacy baseline.
-- Auto applies only positive delta (ledger > user cache). Negative delta is manual review only.

BEGIN;

WITH ledger_sum AS (
  SELECT user_id, COALESCE(SUM(delta_cents), 0)::int AS ledger_balance_cents
  FROM public.coupon_balance_history
  GROUP BY user_id
),
diff AS (
  SELECT
    u.id AS user_id,
    COALESCE(u.coupon_value_cents, 0)::int AS user_balance_cents,
    COALESCE(l.ledger_balance_cents, 0)::int AS ledger_balance_cents,
    (COALESCE(l.ledger_balance_cents, 0)::int - COALESCE(u.coupon_value_cents, 0)::int) AS delta_cents
  FROM public.users u
  LEFT JOIN ledger_sum l ON l.user_id = u.id
),
positive AS (
  SELECT * FROM diff WHERE delta_cents > 0
),
negative AS (
  SELECT * FROM diff WHERE delta_cents < 0
),
ins AS (
  INSERT INTO public.coupon_balance_history
    (user_id, payment_id, delta_cents, balance_before_cents, balance_after_cents,
     event_type, channel, status, draw_id, reservation_id, run_trace_id, meta, event_occurred_at)
  SELECT
    p.user_id,
    NULL,
    p.delta_cents,
    p.user_balance_cents,
    p.ledger_balance_cents,
    'USER_BALANCE_RECONCILE_FULL_LEDGER',
    'REPAIR',
    'approved',
    NULL,
    NULL,
    'repair_user_balance_to_full_ledger_20260508',
    jsonb_build_object(
      'source', 'manual_sql.repair_user_balance_to_full_ledger',
      'previous_user_balance_cents', p.user_balance_cents,
      'ledger_balance_cents', p.ledger_balance_cents,
      'diff_cents', p.delta_cents
    ),
    now()
  FROM positive p
  ON CONFLICT DO NOTHING
  RETURNING user_id, delta_cents
),
upd AS (
  UPDATE public.users u
     SET coupon_value_cents = u.coupon_value_cents + ins.delta_cents,
         coupon_updated_at = now()
    FROM ins
   WHERE u.id = ins.user_id
  RETURNING u.id
)
SELECT
  (SELECT COUNT(*)::int FROM positive) AS positive_candidates,
  (SELECT COUNT(*)::int FROM negative) AS manual_review_required_negative_candidates,
  (SELECT COUNT(*)::int FROM ins) AS ledger_rows_inserted,
  (SELECT COUNT(*)::int FROM upd) AS users_updated;

COMMIT;

-- Manual review list for negative diffs (not auto-applied):
WITH ledger_sum AS (
  SELECT user_id, COALESCE(SUM(delta_cents), 0)::int AS ledger_balance_cents
  FROM public.coupon_balance_history
  GROUP BY user_id
)
SELECT
  u.id AS user_id,
  u.name,
  u.email,
  COALESCE(u.coupon_value_cents, 0)::int AS user_balance_cents,
  COALESCE(l.ledger_balance_cents, 0)::int AS ledger_balance_cents,
  (COALESCE(l.ledger_balance_cents, 0)::int - COALESCE(u.coupon_value_cents, 0)::int) AS delta_cents,
  'manual_review_required_negative_delta' AS action
FROM public.users u
LEFT JOIN ledger_sum l ON l.user_id = u.id
WHERE (COALESCE(l.ledger_balance_cents, 0)::int - COALESCE(u.coupon_value_cents, 0)::int) < 0
ORDER BY ABS(COALESCE(l.ledger_balance_cents, 0)::int - COALESCE(u.coupon_value_cents, 0)::int) DESC;

