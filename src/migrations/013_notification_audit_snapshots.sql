-- Migration: auditoria de campanhas e dispatches (snapshots)
-- Idempotente / aditiva

BEGIN;

ALTER TABLE public.notification_campaigns
ADD COLUMN IF NOT EXISTS campaign_type text NULL,
ADD COLUMN IF NOT EXISTS message_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
ADD COLUMN IF NOT EXISTS audience_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
ADD COLUMN IF NOT EXISTS audience_count_expected integer NULL,
ADD COLUMN IF NOT EXISTS audience_count_created integer NULL,
ADD COLUMN IF NOT EXISTS audience_count_sent integer NULL,
ADD COLUMN IF NOT EXISTS audience_count_failed integer NULL,
ADD COLUMN IF NOT EXISTS audience_count_skipped integer NULL;

ALTER TABLE public.notification_dispatches
ADD COLUMN IF NOT EXISTS campaign_id uuid NULL,
ADD COLUMN IF NOT EXISTS message_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb,
ADD COLUMN IF NOT EXISTS recipient_snapshot jsonb NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS idx_notification_dispatches_campaign
ON public.notification_dispatches (campaign_id);

CREATE INDEX IF NOT EXISTS idx_notification_campaigns_type_status
ON public.notification_campaigns (campaign_type, status);

COMMIT;
