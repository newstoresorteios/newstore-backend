-- Audit: users.coupon_value_cents vs soma do ledger (coupon_balance_history)
-- Objetivo: detectar usuários com saldo divergente do ledger.
-- Observação: event_types de débito/uso devem estar no ledger com delta negativo.

WITH ledger_sum AS (
  SELECT
    user_id,
    COALESCE(SUM(delta_cents), 0)::int AS ledger_balance_cents,
    COUNT(*)::int AS ledger_rows
  FROM public.coupon_balance_history
  GROUP BY user_id
),
u AS (
  SELECT
    id AS user_id,
    COALESCE(coupon_value_cents, 0)::int AS user_balance_cents,
    coupon_updated_at
  FROM public.users
)
SELECT
  u.user_id,
  u.user_balance_cents,
  COALESCE(l.ledger_balance_cents, 0) AS ledger_balance_cents,
  (u.user_balance_cents - COALESCE(l.ledger_balance_cents, 0)) AS diff_cents,
  COALESCE(l.ledger_rows, 0) AS ledger_rows,
  u.coupon_updated_at
FROM u
LEFT JOIN ledger_sum l ON l.user_id = u.user_id
WHERE u.user_balance_cents <> COALESCE(l.ledger_balance_cents, 0)
ORDER BY ABS(u.user_balance_cents - COALESCE(l.ledger_balance_cents, 0)) DESC, u.user_id DESC;

