-- Migration 027: fonte canônica da validade do saldo de cupom.
-- A regra comercial existente usa a última compra aprovada como início e
-- seis meses como validade. Usuários sem uma compra de origem continuam
-- auditáveis na view, mas não recebem uma data de vencimento inventada.

CREATE OR REPLACE VIEW public.user_coupon_balance_expiry AS
WITH approved_purchase AS (
  SELECT
    p.user_id,
    MAX(
      GREATEST(
        COALESCE(p.paid_at, '-infinity'::timestamptz),
        COALESCE(p.created_at, '-infinity'::timestamptz)
      )
    ) AS balance_reference_at
  FROM public.payments p
  WHERE p.user_id IS NOT NULL
    AND lower(trim(COALESCE(p.status, ''))) IN ('approved', 'paid', 'pago', 'completed')
  GROUP BY p.user_id
), balance_source AS (
  SELECT
    u.id AS user_id,
    u.name,
    u.email,
    COALESCE(u.coupon_value_cents, 0)::integer AS balance_cents,
    NULLIF(ap.balance_reference_at, '-infinity'::timestamptz) AS balance_reference_at
  FROM public.users u
  LEFT JOIN approved_purchase ap ON ap.user_id = u.id
  WHERE COALESCE(u.coupon_value_cents, 0) > 0
), expiry_date AS (
  SELECT
    source.*,
    CASE
      WHEN source.balance_reference_at IS NULL THEN NULL
      ELSE (
        (source.balance_reference_at AT TIME ZONE 'America/Sao_Paulo')::date
        + INTERVAL '6 months'
      )::date
    END AS expires_on
  FROM balance_source source
)
SELECT
  expiry.user_id,
  expiry.name,
  expiry.email,
  expiry.balance_cents,
  expiry.balance_reference_at,
  CASE
    WHEN expiry.expires_on IS NULL THEN NULL
    ELSE expiry.expires_on::timestamp AT TIME ZONE 'America/Sao_Paulo'
  END AS expires_at,
  expiry.expires_on,
  CASE
    WHEN expiry.expires_on IS NULL THEN NULL
    ELSE expiry.expires_on - (CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date
  END AS days_to_expire,
  CASE
    WHEN expiry.balance_reference_at IS NULL THEN NULL
    ELSE 'last_approved_purchase'
  END::text AS expiry_source
FROM expiry_date expiry;

COMMENT ON VIEW public.user_coupon_balance_expiry IS
  'Saldo positivo e validade canônica: última compra aprovada + 6 meses, em datas de America/Sao_Paulo.';
