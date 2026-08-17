-- Migration 027: fonte canonica de saldo e vencimento de cupom.
--
-- Prioridade de vencimento:
--   1) users.coupon_expires_at, quando definido manualmente;
--   2) ultimo credito positivo real em coupon_balance_history
--      (CREDIT_PURCHASE aprovado ou ADMIN_BALANCE_ADJUSTMENT) + 6 meses;
--   3) sem origem valida -> expires_at NULL.
-- coupon_tray_sync nao participa deste calculo em nenhuma hipotese.

ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS coupon_expires_at timestamptz;

COMMENT ON COLUMN public.users.coupon_expires_at IS
  'Vencimento explícito opcional do saldo. Quando nulo, o vencimento é calculado pelo último crédito real registrado no histórico.';

CREATE INDEX IF NOT EXISTS idx_users_coupon_expires_at
  ON public.users (coupon_expires_at)
  WHERE coupon_expires_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_coupon_balance_history_user_created
  ON public.coupon_balance_history (user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_coupon_balance_history_positive_credit
  ON public.coupon_balance_history (user_id, created_at DESC)
  WHERE (
    delta_cents > 0
    AND (
      (event_type = 'CREDIT_PURCHASE' AND lower(btrim(COALESCE(status, ''))) = 'approved')
      OR event_type = 'ADMIN_BALANCE_ADJUSTMENT'
    )
  );

DROP VIEW IF EXISTS public.user_coupon_balance_expiry;

CREATE VIEW public.user_coupon_balance_expiry AS
WITH last_positive_balance_credit AS (
  SELECT DISTINCT ON (h.user_id)
    h.user_id,
    h.id AS balance_history_id,
    h.created_at AS balance_credit_at,
    h.event_type AS balance_credit_event_type,
    h.channel AS balance_credit_channel,
    h.payment_id AS balance_credit_payment_id,
    h.draw_id AS balance_credit_draw_id,
    h.delta_cents AS balance_credit_delta_cents,
    h.balance_before_cents AS history_balance_before_cents,
    h.balance_after_cents AS history_balance_after_cents,
    h.status AS balance_credit_status
  FROM public.coupon_balance_history h
  WHERE h.user_id IS NOT NULL
    AND h.delta_cents > 0
    AND (
      (h.event_type = 'CREDIT_PURCHASE' AND lower(btrim(COALESCE(h.status, ''))) = 'approved')
      OR h.event_type = 'ADMIN_BALANCE_ADJUSTMENT'
    )
  ORDER BY h.user_id, h.created_at DESC, h.id DESC
), balance_base AS (
  SELECT
    u.id AS user_id,
    COALESCE(NULLIF(btrim(u.name), ''), NULLIF(btrim(u.email), ''), 'Cliente') AS name,
    NULLIF(lower(btrim(u.email)), '') AS email,
    NULLIF(btrim(u.coupon_code), '') AS coupon_code,
    COALESCE(u.coupon_value_cents, 0)::bigint AS balance_cents,
    u.coupon_expires_at,
    credit.balance_history_id,
    credit.balance_credit_at,
    credit.balance_credit_event_type,
    credit.balance_credit_channel,
    credit.balance_credit_payment_id,
    credit.balance_credit_draw_id,
    credit.balance_credit_delta_cents,
    credit.history_balance_before_cents,
    credit.history_balance_after_cents,
    credit.balance_credit_status,
    CASE
      WHEN u.coupon_expires_at IS NOT NULL THEN u.coupon_expires_at
      WHEN credit.balance_credit_at IS NOT NULL THEN credit.balance_credit_at + INTERVAL '6 months'
      ELSE NULL
    END AS expires_at,
    CASE
      WHEN u.coupon_expires_at IS NOT NULL THEN u.coupon_expires_at - INTERVAL '6 months'
      WHEN credit.balance_credit_at IS NOT NULL THEN credit.balance_credit_at
      ELSE NULL
    END AS balance_reference_at,
    CASE
      WHEN u.coupon_expires_at IS NOT NULL THEN 'users.coupon_expires_at'
      WHEN credit.balance_credit_at IS NOT NULL AND credit.balance_credit_event_type = 'CREDIT_PURCHASE' THEN 'coupon_balance_history.credit_purchase_plus_6_months'
      WHEN credit.balance_credit_at IS NOT NULL AND credit.balance_credit_event_type = 'ADMIN_BALANCE_ADJUSTMENT' THEN 'coupon_balance_history.admin_credit_plus_6_months'
      ELSE 'missing'
    END AS expiry_source
  FROM public.users u
  LEFT JOIN last_positive_balance_credit credit ON credit.user_id = u.id
  WHERE COALESCE(u.coupon_value_cents, 0) > 0
), localized_balance AS (
  SELECT
    base.user_id,
    base.name,
    base.email,
    base.coupon_code,
    base.balance_cents,
    base.coupon_expires_at,
    base.balance_history_id,
    base.balance_credit_at,
    base.balance_credit_event_type,
    base.balance_credit_channel,
    base.balance_credit_payment_id,
    base.balance_credit_draw_id,
    base.balance_credit_delta_cents,
    base.history_balance_before_cents,
    base.history_balance_after_cents,
    base.balance_credit_status,
    base.expires_at,
    base.balance_reference_at,
    base.expiry_source,
    CASE
      WHEN base.expires_at IS NOT NULL THEN (base.expires_at AT TIME ZONE 'America/Sao_Paulo')::date
      ELSE NULL
    END AS expires_on,
    (CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date AS current_date_sao_paulo
  FROM balance_base base
), calculated_balance AS (
  SELECT
    localized.user_id,
    localized.name,
    localized.email,
    localized.coupon_code,
    localized.balance_cents,
    localized.coupon_expires_at,
    localized.balance_history_id,
    localized.balance_credit_at,
    localized.balance_credit_event_type,
    localized.balance_credit_channel,
    localized.balance_credit_payment_id,
    localized.balance_credit_draw_id,
    localized.balance_credit_delta_cents,
    localized.history_balance_before_cents,
    localized.history_balance_after_cents,
    localized.balance_credit_status,
    localized.expires_at,
    localized.balance_reference_at,
    localized.expiry_source,
    localized.expires_on,
    localized.current_date_sao_paulo,
    CASE
      WHEN localized.expires_on IS NOT NULL THEN localized.expires_on - localized.current_date_sao_paulo
      ELSE NULL
    END AS days_to_expire
  FROM localized_balance localized
)
SELECT
  user_id,
  name,
  email,
  coupon_code,
  balance_cents,
  balance_reference_at,
  expires_at,
  expires_on,
  days_to_expire,
  expiry_source,
  balance_history_id,
  balance_credit_at,
  balance_credit_event_type,
  balance_credit_channel,
  balance_credit_payment_id,
  balance_credit_draw_id,
  balance_credit_delta_cents,
  history_balance_before_cents,
  history_balance_after_cents,
  balance_credit_status,
  CASE
    WHEN email IS NULL THEN false
    WHEN email NOT LIKE '%@%' THEN false
    ELSE true
  END AS email_is_usable,
  CASE
    WHEN days_to_expire IS NULL THEN false
    WHEN days_to_expire < 0 THEN true
    ELSE false
  END AS is_expired,
  CASE
    WHEN days_to_expire IS NULL THEN 'missing_expiry'
    WHEN days_to_expire < 0 THEN 'expired'
    WHEN days_to_expire = 0 THEN 'expires_today'
    WHEN days_to_expire = 3 THEN 'expires_in_3_days'
    WHEN days_to_expire = 7 THEN 'expires_in_7_days'
    WHEN days_to_expire = 10 THEN 'expires_in_10_days'
    WHEN days_to_expire = 20 THEN 'expires_in_20_days'
    WHEN days_to_expire = 30 THEN 'expires_in_30_days'
    ELSE 'active'
  END AS expiry_stage
FROM calculated_balance calculated;

COMMENT ON VIEW public.user_coupon_balance_expiry IS
  'Fonte canônica somente leitura para saldo e vencimento. Usa coupon_expires_at quando informado; caso contrário, utiliza o último crédito positivo registrado em coupon_balance_history mais 6 meses.';

GRANT SELECT ON public.user_coupon_balance_expiry TO service_role;
