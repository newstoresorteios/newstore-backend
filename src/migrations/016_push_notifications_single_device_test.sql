CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS public.push_subscriptions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id bigint NULL REFERENCES public.users(id) ON DELETE SET NULL,
  endpoint text NOT NULL UNIQUE,
  p256dh text NOT NULL,
  auth text NOT NULL,
  user_agent text NULL,
  device_label text NULL,
  is_active boolean NOT NULL DEFAULT true,
  test_label text NULL,
  operational_opt_in boolean NOT NULL DEFAULT true,
  marketing_opt_in boolean NOT NULL DEFAULT false,
  last_success_at timestamptz NULL,
  last_error_at timestamptz NULL,
  last_error text NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_push_subscriptions_user_id
  ON public.push_subscriptions(user_id);

CREATE INDEX IF NOT EXISTS idx_push_subscriptions_is_active
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
  status text NOT NULL DEFAULT 'pending',
  error_message text NULL,
  sent_at timestamptz NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_notification_push_dispatches_user_id
  ON public.notification_push_dispatches(user_id);

CREATE INDEX IF NOT EXISTS idx_notification_push_dispatches_subscription_id
  ON public.notification_push_dispatches(subscription_id);

CREATE INDEX IF NOT EXISTS idx_notification_push_dispatches_status
  ON public.notification_push_dispatches(status);
