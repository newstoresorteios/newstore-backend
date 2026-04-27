-- Repair (manual): reconciliar users.coupon_value_cents com o ledger
-- IMPORTANTE:
-- - Rode manualmente, com cuidado, em janela de manutenção.
-- - Este script cria um evento de ajuste no ledger e atualiza users.coupon_value_cents na mesma transação.
-- - Não é executado automaticamente pelo backend.

BEGIN;

WITH ledger_sum AS (
  SELECT
    user_id,
    COALESCE(SUM(delta_cents), 0)::int AS ledger_balance_cents
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
  WHERE COALESCE(u.coupon_value_cents, 0)::int <> COALESCE(l.ledger_balance_cents, 0)::int
),
ins AS (
  INSERT INTO public.coupon_balance_history
    (user_id, payment_id, delta_cents, balance_before_cents, balance_after_cents,
     event_type, channel, status, draw_id, reservation_id, run_trace_id, meta)
  SELECT
    d.user_id,
    NULL,
    d.delta_cents,
    d.user_balance_cents,
    (d.user_balance_cents + d.delta_cents),
    'REPAIR_BALANCE_RECONCILIATION',
    'REPAIR',
    NULL,
    NULL,
    NULL,
    NULL,
    jsonb_build_object(
      'source', 'manual_sql.repair_coupon_balance_missing_difference',
      'previous_balance_cents', d.user_balance_cents,
      'ledger_balance_cents', d.ledger_balance_cents,
      'delta_cents', d.delta_cents
    )
  FROM diff d
  WHERE d.delta_cents <> 0
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
  (SELECT COUNT(*)::int FROM diff) AS users_with_diff,
  (SELECT COUNT(*)::int FROM ins)  AS ledger_rows_inserted,
  (SELECT COUNT(*)::int FROM upd)  AS users_updated;

COMMIT;

