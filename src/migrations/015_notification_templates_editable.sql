-- Migration: templates editáveis localmente (defaults + sync Brevo)
-- Idempotente / aditiva

BEGIN;

ALTER TABLE public.notification_templates
ADD COLUMN IF NOT EXISTS default_message text NULL,
ADD COLUMN IF NOT EXISTS default_params jsonb NOT NULL DEFAULT '{}'::jsonb,
ADD COLUMN IF NOT EXISTS template_language text NULL,
ADD COLUMN IF NOT EXISTS template_category text NULL,
ADD COLUMN IF NOT EXISTS is_synced_from_brevo boolean NOT NULL DEFAULT false,
ADD COLUMN IF NOT EXISTS last_synced_at timestamptz NULL;

ALTER TABLE public.notification_templates
ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_notification_templates_key_active
ON public.notification_templates (template_key, is_active);

COMMIT;
