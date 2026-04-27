-- Audit: pagamentos finalizados sem ledger de crédito (CREDIT_PURCHASE)
-- Uso: rode manualmente em produção/staging para encontrar pagamentos que deveriam ter crédito,
-- mas não possuem registro correspondente em public.coupon_balance_history.
--
-- Critério: payments.status IN ('approved','paid','pago') e NÃO existe ledger CREDIT_PURCHASE.

WITH cfg AS (
  SELECT value::int AS ticket_price_cents
  FROM public.app_config
  WHERE key = 'ticket_price_cents'
  LIMIT 1
),
base AS (
  SELECT
    p.id AS payment_id,
    p.user_id,
    p.provider,
    lower(p.status) AS status_l,
    p.numbers,
    COALESCE(array_length(p.numbers, 1), 0)::int AS qty,
    p.amount_cents,
    cfg.ticket_price_cents,
    (COALESCE(array_length(p.numbers, 1), 0)::int * cfg.ticket_price_cents)::int AS expected_credit_cents
  FROM public.payments p
  CROSS JOIN cfg
  WHERE lower(p.status) IN ('approved','paid','pago')
),
ledger AS (
  SELECT
    h.payment_id,
    SUM(h.delta_cents)::int AS ledger_credit_cents,
    COUNT(*)::int AS ledger_rows
  FROM public.coupon_balance_history h
  WHERE h.event_type = 'CREDIT_PURCHASE'
    AND h.payment_id IS NOT NULL
  GROUP BY h.payment_id
)
SELECT
  b.payment_id,
  b.user_id,
  b.provider,
  b.status_l,
  b.qty,
  b.amount_cents,
  b.ticket_price_cents,
  b.expected_credit_cents,
  COALESCE(l.ledger_credit_cents, 0) AS ledger_credit_cents,
  COALESCE(l.ledger_rows, 0) AS ledger_rows
FROM base b
LEFT JOIN ledger l ON l.payment_id = b.payment_id
WHERE COALESCE(l.ledger_rows, 0) = 0
  AND b.qty > 0
ORDER BY b.payment_id DESC;

