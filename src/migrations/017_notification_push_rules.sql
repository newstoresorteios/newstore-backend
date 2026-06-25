-- Migration: regras/modelos de Push automático
-- Idempotente / aditiva
-- Não aplicar automaticamente.

BEGIN;

CREATE TABLE IF NOT EXISTS public.notification_push_rules (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  event_key text NOT NULL UNIQUE,
  name text NOT NULL,
  description text NULL,
  title_template text NOT NULL,
  body_template text NOT NULL,
  url_template text NULL,
  category text NOT NULL DEFAULT 'operational',
  is_active boolean NOT NULL DEFAULT false,
  threshold_value integer NULL,
  cooldown_minutes integer NULL DEFAULT 1440,
  last_triggered_at timestamptz NULL,
  created_by bigint NULL REFERENCES public.users(id) ON DELETE SET NULL,
  updated_by bigint NULL REFERENCES public.users(id) ON DELETE SET NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_notification_push_rules_event_key
ON public.notification_push_rules(event_key);

CREATE INDEX IF NOT EXISTS idx_notification_push_rules_active
ON public.notification_push_rules(is_active);

COMMIT;
