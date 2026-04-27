-- Audit: compara delta_cents do ledger vs preço atual do app_config
-- Atenção: se houve mudança de preço no tempo, este audit evidencia créditos feitos com preço antigo.
-- Isso é útil para auditoria/correção pós-mudança.

WITH cfg AS (
  SELECT value::int AS ticket_price_cents
  FROM public.app_config
  WHERE key = 'ticket_price_cents'
  LIMIT 1
),
base AS (
  SELECT
    h.id AS ledger_id,
    h.created_at AS ledger_created_at,
    h.user_id,
    h.payment_id,
    h.delta_cents AS ledger_delta_cents,
    p.provider,
    lower(p.status) AS payment_status_l,
    p.numbers,
    COALESCE(array_length(p.numbers, 1), 0)::int AS qty,
    cfg.ticket_price_cents,
    (COALESCE(array_length(p.numbers, 1), 0)::int * cfg.ticket_price_cents)::int AS expected_delta_cents
  FROM public.coupon_balance_history h
  JOIN public.payments p ON p.id = h.payment_id
  CROSS JOIN cfg
  WHERE h.event_type = 'CREDIT_PURCHASE'
    AND h.payment_id IS NOT NULL
)
SELECT
  ledger_id,
  ledger_created_at,
  user_id,
  payment_id,
  provider,
  payment_status_l,
  qty,
  ticket_price_cents,
  ledger_delta_cents,
  expected_delta_cents,
  (expected_delta_cents - ledger_delta_cents) AS diff_cents
FROM base
WHERE qty > 0
  AND ledger_delta_cents <> expected_delta_cents
ORDER BY ABS(expected_delta_cents - ledger_delta_cents) DESC, ledger_created_at DESC;

