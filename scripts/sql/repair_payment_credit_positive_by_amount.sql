-- Repair (manual): apply only positive diffs (under-credited payments)
-- Idempotent by unique index (payment_id,event_type) + ON CONFLICT DO NOTHING.
-- Does NOT debit anyone and does not touch supercredits (diff < 0).

BEGIN;

WITH pay AS (
  SELECT
    p.id AS payment_id,
    p.user_id,
    p.draw_id,
    p.amount_cents,
    lower(p.status) AS payment_status
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
diff AS (
  SELECT
    p.payment_id,
    p.user_id,
    p.draw_id,
    p.amount_cents,
    COALESCE(l.credited_cents, 0) AS already_credited_cents,
    (p.amount_cents - COALESCE(l.credited_cents, 0))::int AS diff_cents
  FROM pay p
  LEFT JOIN ledger l ON l.payment_id = p.payment_id
  WHERE (p.amount_cents - COALESCE(l.credited_cents, 0)) > 0
),
ul AS (
  SELECT
    u.id AS user_id,
    COALESCE(u.coupon_value_cents, 0)::int AS balance_before_cents
  FROM public.users u
  JOIN diff d ON d.user_id = u.id
  FOR UPDATE
),
ins AS (
  INSERT INTO public.coupon_balance_history
    (user_id, payment_id, delta_cents, balance_before_cents, balance_after_cents,
     event_type, channel, status, draw_id, reservation_id, run_trace_id, meta, event_occurred_at)
  SELECT
    d.user_id,
    d.payment_id,
    d.diff_cents,
    ul.balance_before_cents,
    (ul.balance_before_cents + d.diff_cents),
    'CREDIT_PURCHASE_RECONCILE_PAYMENT_AMOUNT',
    'REPAIR',
    'approved',
    d.draw_id,
    NULL,
    'repair_payment_amount_positive',
    jsonb_build_object(
      'source', 'manual_sql.repair_payment_credit_positive_by_amount',
      'payment_amount_cents', d.amount_cents,
      'already_credited_cents', d.already_credited_cents,
      'diff_cents', d.diff_cents,
      'reason', 'payment_credit_below_amount_cents'
    ),
    now()
  FROM diff d
  JOIN ul ON ul.user_id = d.user_id
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
  (SELECT COUNT(*)::int FROM diff) AS positive_diff_candidates,
  (SELECT COUNT(*)::int FROM ins) AS ledger_rows_inserted,
  (SELECT COUNT(*)::int FROM upd) AS users_updated;

COMMIT;

