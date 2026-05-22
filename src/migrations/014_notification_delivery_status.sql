-- Migration: status de entrega Brevo WhatsApp (provider vs delivery)
-- Idempotente / aditiva

BEGIN;

ALTER TABLE public.notification_dispatches
ADD COLUMN IF NOT EXISTS provider_status text NULL,
ADD COLUMN IF NOT EXISTS delivery_status text NULL,
ADD COLUMN IF NOT EXISTS delivery_event jsonb NULL,
ADD COLUMN IF NOT EXISTS delivery_events_raw jsonb NULL,
ADD COLUMN IF NOT EXISTS delivery_checked_at timestamptz NULL,
ADD COLUMN IF NOT EXISTS delivery_confirmed_at timestamptz NULL,
ADD COLUMN IF NOT EXISTS last_provider_event_at timestamptz NULL;

CREATE INDEX IF NOT EXISTS idx_notification_dispatches_delivery_status
ON public.notification_dispatches (delivery_status);

CREATE INDEX IF NOT EXISTS idx_notification_dispatches_provider_status
ON public.notification_dispatches (provider_status);

COMMIT;
