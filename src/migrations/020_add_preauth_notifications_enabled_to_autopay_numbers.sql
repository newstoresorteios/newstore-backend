ALTER TABLE public.autopay_numbers
ADD COLUMN IF NOT EXISTS preauth_notifications_enabled boolean NOT NULL DEFAULT true;

CREATE INDEX IF NOT EXISTS idx_autopay_numbers_preauth_notifications_enabled
ON public.autopay_numbers(preauth_notifications_enabled);
