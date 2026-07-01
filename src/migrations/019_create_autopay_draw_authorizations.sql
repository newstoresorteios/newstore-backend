-- Migration: base de pre-autorizacao de participacao para numeros cativos
-- Nao envia WhatsApp, nao cobra cartao, nao cria payment/reservation e nao altera numeros.

CREATE TABLE IF NOT EXISTS public.autopay_draw_authorizations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  draw_id bigint NOT NULL,
  user_id bigint NOT NULL,

  autopay_profile_id uuid NULL,
  autopay_number_id uuid NULL,

  captive_number integer NOT NULL,
  amount_cents integer NOT NULL,

  status text NOT NULL DEFAULT 'pending',

  token_hash text NOT NULL UNIQUE,

  expires_at timestamptz NOT NULL,

  created_by bigint NULL,

  notification_dispatch_id uuid NULL,
  notification_status text NULL,
  notification_error text NULL,

  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),

  authorized_at timestamptz NULL,
  declined_at timestamptz NULL,
  expired_at timestamptz NULL,
  charged_at timestamptz NULL,

  CONSTRAINT autopay_draw_authorizations_status_check
    CHECK (status IN ('pending', 'authorized', 'declined', 'expired', 'charged', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_autopay_draw_authorizations_draw_id
ON public.autopay_draw_authorizations(draw_id);

CREATE INDEX IF NOT EXISTS idx_autopay_draw_authorizations_user_id
ON public.autopay_draw_authorizations(user_id);

CREATE INDEX IF NOT EXISTS idx_autopay_draw_authorizations_status_expires
ON public.autopay_draw_authorizations(status, expires_at);

CREATE UNIQUE INDEX IF NOT EXISTS idx_autopay_draw_authorizations_unique_draw_user_number
ON public.autopay_draw_authorizations(draw_id, user_id, captive_number);

CREATE OR REPLACE FUNCTION public.set_autopay_draw_authorizations_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END; $$;

DROP TRIGGER IF EXISTS trg_autopay_draw_authorizations_updated_at
ON public.autopay_draw_authorizations;

CREATE TRIGGER trg_autopay_draw_authorizations_updated_at
BEFORE UPDATE ON public.autopay_draw_authorizations
FOR EACH ROW EXECUTE PROCEDURE public.set_autopay_draw_authorizations_updated_at();
