-- Repair (manual/selected): negative diff adjustments by explicit payment_id list only.
-- Safety gates:
-- - selected payments only
-- - only diff < 0 (overcredit)
-- - only users WITHOUT ADMIN_BALANCE_ADJUSTMENT
-- - only if resulting balance would NOT go negative
-- - idempotent (ON CONFLICT DO NOTHING + unique payment_id/event_type)

BEGIN;

WITH selected_payments(payment_id) AS (
  VALUES
    -- ('payment_id_aqui')
    ('')
),
pay AS (
  SELECT
    p.id AS payment_id,
    p.user_id,
    p.draw_id,
    p.amount_cents
  FROM public.payments p
  JOIN selected_payments sp ON sp.payment_id = p.id
  WHERE lower(p.status) IN ('approved','paid','pago')
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
admin_adj AS (
  SELECT user_id, COUNT(*)::int AS admin_adjustment_count_total
  FROM public.coupon_balance_history
  WHERE event_type = 'ADMIN_BALANCE_ADJUSTMENT'
  GROUP BY user_id
),
diff AS (
  SELECT
    p.payment_id,
    p.user_id,
    p.draw_id,
    p.amount_cents,
    COALESCE(l.credited_cents, 0) AS already_credited_cents,
    (p.amount_cents - COALESCE(l.credited_cents, 0))::int AS diff_cents,
    COALESCE(a.admin_adjustment_count_total, 0)::int AS admin_adjustment_count_total
  FROM pay p
  LEFT JOIN ledger l ON l.payment_id = p.payment_id
  LEFT JOIN admin_adj a ON a.user_id = p.user_id
  WHERE (p.amount_cents - COALESCE(l.credited_cents, 0)) < 0
),
eligible AS (
  SELECT
    d.*,
    COALESCE(u.coupon_value_cents, 0)::int AS current_balance_cents
  FROM diff d
  JOIN public.users u ON u.id = d.user_id
  WHERE d.admin_adjustment_count_total = 0
    AND (COALESCE(u.coupon_value_cents, 0)::int + d.diff_cents) >= 0
),
ins AS (
  INSERT INTO public.coupon_balance_history
    (user_id, payment_id, delta_cents, balance_before_cents, balance_after_cents,
     event_type, channel, status, draw_id, reservation_id, run_trace_id, meta, event_occurred_at)
  SELECT
    e.user_id,
    e.payment_id,
    e.diff_cents,
    e.current_balance_cents,
    (e.current_balance_cents + e.diff_cents),
    'CREDIT_PURCHASE_RECONCILE_PAYMENT_AMOUNT',
    'REPAIR',
    'approved',
    e.draw_id,
    NULL,
    'repair_payment_amount_negative_selected',
    jsonb_build_object(
      'source', 'manual_sql.repair_payment_overcredit_selected',
      'payment_amount_cents', e.amount_cents,
      'already_credited_cents', e.already_credited_cents,
      'diff_cents', e.diff_cents,
      'reason', 'payment_credit_above_amount_cents_selected_no_admin_adjustment'
    ),
    now()
  FROM eligible e
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
  (SELECT COUNT(*)::int FROM diff) AS selected_overcredit_rows,
  (SELECT COUNT(*)::int FROM eligible) AS eligible_rows,
  (SELECT COUNT(*)::int FROM ins) AS ledger_rows_inserted,
  (SELECT COUNT(*)::int FROM upd) AS users_updated;

COMMIT;

