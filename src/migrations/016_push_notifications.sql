-- Migration: Web Push subscriptions, dispatches, event ledger, consents
-- Idempotente / aditiva

BEGIN;

ALTER TABLE public.users
ADD COLUMN IF NOT EXISTS push_operational_opt_in boolean NOT NULL DEFAULT false,
ADD COLUMN IF NOT EXISTS push_marketing_opt_in boolean NOT NULL DEFAULT false,
ADD COLUMN IF NOT EXISTS push_opt_in_at timestamptz NULL,
ADD COLUMN IF NOT EXISTS push_opt_in_source text NULL,
ADD COLUMN IF NOT EXISTS push_opt_out boolean NOT NULL DEFAULT false,
ADD COLUMN IF NOT EXISTS push_opt_out_at timestamptz NULL;

CREATE TABLE IF NOT EXISTS public.push_subscriptions (
id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
user_id bigint NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
endpoint text NOT NULL UNIQUE,
p256dh text NOT NULL,
auth text NOT NULL,
user_agent text NULL,
device_label text NULL,
browser_name text NULL,
device_type text NULL,
is_active boolean NOT NULL DEFAULT true,
last_success_at timestamptz NULL,
last_error_at timestamptz NULL,
last_error text NULL,
created_at timestamptz NOT NULL DEFAULT now(),
updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_push_subscriptions_user_id
ON public.push_subscriptions(user_id);

CREATE INDEX IF NOT EXISTS idx_push_subscriptions_active
ON public.push_subscriptions(is_active);

CREATE TABLE IF NOT EXISTS public.notification_push_dispatches (
id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
user_id bigint NULL REFERENCES public.users(id) ON DELETE SET NULL,
subscription_id uuid NULL REFERENCES public.push_subscriptions(id) ON DELETE SET NULL,
event_key text NULL,
category text NULL,
title text NOT NULL,
body text NOT NULL,
url text NULL,
payload jsonb NOT NULL DEFAULT '{}'::jsonb,
mode text NOT NULL DEFAULT 'test',
source text NULL,
status text NOT NULL DEFAULT 'pending',
error_message text NULL,
sent_at timestamptz NULL,
created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_notification_push_dispatches_user_id
ON public.notification_push_dispatches(user_id);

CREATE INDEX IF NOT EXISTS idx_notification_push_dispatches_status
ON public.notification_push_dispatches(status);

CREATE INDEX IF NOT EXISTS idx_notification_push_dispatches_event
ON public.notification_push_dispatches(event_key, created_at);

CREATE TABLE IF NOT EXISTS public.notification_event_ledger (
id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
event_key text NOT NULL,
dedupe_key text NOT NULL UNIQUE,
channel text NOT NULL DEFAULT 'push',
category text NULL,
entity_type text NULL,
entity_id text NULL,
user_id bigint NULL REFERENCES public.users(id) ON DELETE SET NULL,
status text NOT NULL DEFAULT 'created',
mode text NOT NULL DEFAULT 'test',
meta jsonb NOT NULL DEFAULT '{}'::jsonb,
created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_notification_event_ledger_event_key
ON public.notification_event_ledger(event_key);

CREATE INDEX IF NOT EXISTS idx_notification_event_ledger_user_id
ON public.notification_event_ledger(user_id);

CREATE TABLE IF NOT EXISTS public.communication_consents (
id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
user_id bigint REFERENCES public.users(id) ON DELETE CASCADE,
channel text NOT NULL,
category text NOT NULL,
status text NOT NULL,
source text NOT NULL,
ip text NULL,
user_agent text NULL,
meta jsonb NOT NULL DEFAULT '{}'::jsonb,
created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_communication_consents_user_id
ON public.communication_consents(user_id);

CREATE INDEX IF NOT EXISTS idx_communication_consents_channel_category
ON public.communication_consents(channel, category, status);

COMMIT;
