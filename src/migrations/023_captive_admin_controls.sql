-- Controles administrativos de cativos por sorteio e historico de notificacoes.
-- Aplicar manualmente antes do deploy do backend.

CREATE TABLE IF NOT EXISTS public.autopay_draw_captive_overrides (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  draw_id integer NOT NULL REFERENCES public.draws(id) ON DELETE CASCADE,
  autopay_number_id uuid NOT NULL REFERENCES public.autopay_numbers(id) ON DELETE CASCADE,
  user_id integer NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  enabled boolean NOT NULL,
  reason text NOT NULL,
  updated_by integer NOT NULL REFERENCES public.users(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT autopay_draw_captive_overrides_reason_check
    CHECK (length(trim(reason)) > 0),
  CONSTRAINT autopay_draw_captive_overrides_draw_number_key
    UNIQUE (draw_id, autopay_number_id)
);

CREATE INDEX IF NOT EXISTS idx_autopay_draw_captive_overrides_draw_enabled
ON public.autopay_draw_captive_overrides(draw_id, enabled);

CREATE INDEX IF NOT EXISTS idx_autopay_draw_captive_overrides_user_draw
ON public.autopay_draw_captive_overrides(user_id, draw_id);

CREATE OR REPLACE FUNCTION public.set_autopay_draw_captive_overrides_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END; $$;

DROP TRIGGER IF EXISTS trg_autopay_draw_captive_overrides_updated_at
ON public.autopay_draw_captive_overrides;

CREATE TRIGGER trg_autopay_draw_captive_overrides_updated_at
BEFORE UPDATE ON public.autopay_draw_captive_overrides
FOR EACH ROW EXECUTE PROCEDURE public.set_autopay_draw_captive_overrides_updated_at();

CREATE TABLE IF NOT EXISTS public.captive_preauth_notification_attempts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  authorization_id uuid NOT NULL
    REFERENCES public.autopay_draw_authorizations(id) ON DELETE CASCADE,
  draw_id integer NOT NULL REFERENCES public.draws(id) ON DELETE CASCADE,
  user_id integer NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  captive_number smallint NOT NULL,
  amount_cents integer NOT NULL,
  template_id text NULL,
  attempt_type text NOT NULL,
  status text NOT NULL,
  error_code text NULL,
  provider_dispatch_id uuid NULL
    REFERENCES public.notification_dispatches(id) ON DELETE SET NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT captive_preauth_notification_attempts_amount_check
    CHECK (amount_cents > 0),
  CONSTRAINT captive_preauth_notification_attempts_type_check
    CHECK (attempt_type IN ('initial', 'reissue', 'manual_activation')),
  CONSTRAINT captive_preauth_notification_attempts_status_check
    CHECK (status IN ('accepted', 'sent', 'delivered', 'skipped', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_captive_preauth_attempts_draw_created
ON public.captive_preauth_notification_attempts(draw_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_captive_preauth_attempts_user_created
ON public.captive_preauth_notification_attempts(user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_captive_preauth_attempts_authorization_created
ON public.captive_preauth_notification_attempts(authorization_id, created_at DESC);
