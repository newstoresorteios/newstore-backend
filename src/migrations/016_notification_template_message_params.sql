-- Migration: parâmetros de mensagem dinâmica em templates WhatsApp
-- Idempotente / aditiva

BEGIN;

ALTER TABLE public.notification_templates
ADD COLUMN IF NOT EXISTS supports_free_message boolean NOT NULL DEFAULT false,
ADD COLUMN IF NOT EXISTS message_param_name text NULL,
ADD COLUMN IF NOT EXISTS required_params jsonb NOT NULL DEFAULT '[]'::jsonb,
ADD COLUMN IF NOT EXISTS param_mapping jsonb NOT NULL DEFAULT '{}'::jsonb;

COMMIT;
