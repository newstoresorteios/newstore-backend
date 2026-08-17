-- Auditoria append-only das decisoes de pre-autorizacao de cativos.
-- Aplicar manualmente antes do deploy do backend.

CREATE TABLE IF NOT EXISTS public.captive_preauth_authorization_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  authorization_id uuid NOT NULL
    REFERENCES public.autopay_draw_authorizations(id) ON DELETE RESTRICT,
  draw_id integer NOT NULL REFERENCES public.draws(id) ON DELETE RESTRICT,
  user_id integer NOT NULL REFERENCES public.users(id) ON DELETE RESTRICT,
  autopay_number_id uuid NOT NULL
    REFERENCES public.autopay_numbers(id) ON DELETE RESTRICT,
  captive_number integer NOT NULL,
  amount_cents integer NOT NULL,
  previous_status text NOT NULL,
  new_status text NOT NULL,
  authorization_source text NOT NULL,
  admin_user_id integer NULL REFERENCES public.users(id) ON DELETE RESTRICT,
  origin text NOT NULL DEFAULT 'admin_panel',
  authorization_ids uuid[] NOT NULL DEFAULT '{}'::uuid[],
  captive_numbers smallint[] NOT NULL DEFAULT '{}'::smallint[],
  quantity integer NOT NULL DEFAULT 1,
  unit_amount_cents integer NULL,
  total_amount_cents integer NULL,
  result text NULL,
  provider_bill_id text NULL,
  provider_charge_id text NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT captive_preauth_authorization_events_amount_check
    CHECK (amount_cents > 0),
  CONSTRAINT captive_preauth_authorization_events_group_quantity_check
    CHECK (quantity > 0),
  CONSTRAINT captive_preauth_authorization_events_group_amounts_check
    CHECK (
      (unit_amount_cents IS NULL OR unit_amount_cents > 0)
      AND (total_amount_cents IS NULL OR total_amount_cents > 0)
    ),
  CONSTRAINT captive_preauth_authorization_events_source_check
    CHECK (authorization_source IN ('admin', 'account', 'public', 'token', 'confirmation_code', 'system')),
  CONSTRAINT captive_preauth_authorization_events_admin_check
    CHECK (authorization_source <> 'admin' OR admin_user_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_captive_preauth_authorization_events_source
ON public.captive_preauth_authorization_events(authorization_source, new_status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_captive_preauth_authorization_events_draw_created
ON public.captive_preauth_authorization_events(draw_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_captive_preauth_authorization_events_user_created
ON public.captive_preauth_authorization_events(user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_captive_preauth_authorization_events_authorization_created
ON public.captive_preauth_authorization_events(authorization_id, created_at DESC);
