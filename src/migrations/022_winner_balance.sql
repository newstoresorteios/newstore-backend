ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS winner_balance_cents int4 NULL,
  ADD COLUMN IF NOT EXISTS winner_balance_updated_at timestamptz NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
      FROM pg_constraint
     WHERE conname = 'users_winner_balance_cents_positive_or_null'
       AND conrelid = 'public.users'::regclass
  ) THEN
    ALTER TABLE public.users
      ADD CONSTRAINT users_winner_balance_cents_positive_or_null
      CHECK (winner_balance_cents IS NULL OR winner_balance_cents > 0);
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.winner_balance_history (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id int4 NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,
  admin_user_id int4 NULL REFERENCES public.users(id) ON DELETE SET NULL,
  previous_balance_cents int4 NULL,
  new_balance_cents int4 NULL,
  action text NOT NULL,
  reason text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT winner_balance_history_action_check
    CHECK (action IN ('ASSIGNED', 'UPDATED', 'HIDDEN'))
);

CREATE INDEX IF NOT EXISTS idx_winner_balance_history_user_created_at
  ON public.winner_balance_history (user_id, created_at DESC);
