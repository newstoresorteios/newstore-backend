ALTER TABLE public.autopay_draw_authorizations
ADD COLUMN IF NOT EXISTS confirmation_code_hash text NULL,
ADD COLUMN IF NOT EXISTS confirmation_code_created_at timestamptz NULL;

CREATE INDEX IF NOT EXISTS idx_autopay_draw_authorizations_confirmation_code_hash
ON public.autopay_draw_authorizations(confirmation_code_hash)
WHERE confirmation_code_hash IS NOT NULL;
