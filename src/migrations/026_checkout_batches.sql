-- Checkout agrupado multi-sorteio (somente aditivo e idempotente).
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS public.checkout_batches (
  id uuid PRIMARY KEY,
  user_id integer NOT NULL REFERENCES public.users(id) ON DELETE RESTRICT,
  idempotency_key text NOT NULL,
  selection_hash text NOT NULL,
  status text NOT NULL,
  provider text NOT NULL DEFAULT 'mercadopago',
  provider_payment_id text NULL,
  amount_cents integer NOT NULL,
  qr_code text NULL,
  qr_code_base64 text NULL,
  expires_at timestamptz NOT NULL,
  payment_create_started_at timestamptz NULL,
  paid_at timestamptz NULL,
  settled_at timestamptz NULL,
  error_code text NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT checkout_batches_user_idempotency_key_unique UNIQUE (user_id, idempotency_key),
  CONSTRAINT checkout_batches_amount_positive CHECK (amount_cents > 0),
  CONSTRAINT checkout_batches_status_check CHECK (
    status IN ('reserved', 'creating_payment', 'pending', 'approved', 'settled', 'expired', 'failed', 'manual_review')
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS checkout_batches_provider_payment_id_unique
  ON public.checkout_batches (provider_payment_id) WHERE provider_payment_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS checkout_batches_user_status_idx
  ON public.checkout_batches (user_id, status);
CREATE INDEX IF NOT EXISTS checkout_batches_provider_payment_id_idx
  ON public.checkout_batches (provider_payment_id);
CREATE INDEX IF NOT EXISTS checkout_batches_expires_at_idx
  ON public.checkout_batches (expires_at);

CREATE TABLE IF NOT EXISTS public.checkout_batch_items (
  id uuid PRIMARY KEY,
  batch_id uuid NOT NULL REFERENCES public.checkout_batches(id) ON DELETE CASCADE,
  reservation_id uuid NOT NULL REFERENCES public.reservations(id) ON DELETE RESTRICT,
  draw_id integer NOT NULL REFERENCES public.draws(id) ON DELETE RESTRICT,
  draw_type text NOT NULL,
  numbers integer[] NOT NULL,
  unit_price_cents integer NOT NULL,
  amount_cents integer NOT NULL,
  child_payment_id text NULL REFERENCES public.payments(id) ON DELETE SET NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT checkout_batch_items_reservation_unique UNIQUE (reservation_id),
  CONSTRAINT checkout_batch_items_batch_draw_unique UNIQUE (batch_id, draw_id),
  CONSTRAINT checkout_batch_items_numbers_nonempty CHECK (cardinality(numbers) > 0),
  CONSTRAINT checkout_batch_items_unit_price_positive CHECK (unit_price_cents > 0),
  CONSTRAINT checkout_batch_items_amount_positive CHECK (amount_cents > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS checkout_batch_items_child_payment_unique
  ON public.checkout_batch_items (child_payment_id) WHERE child_payment_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS checkout_batch_items_batch_id_idx
  ON public.checkout_batch_items (batch_id);
CREATE INDEX IF NOT EXISTS checkout_batch_items_draw_id_idx
  ON public.checkout_batch_items (draw_id);
CREATE INDEX IF NOT EXISTS checkout_batch_items_reservation_id_idx
  ON public.checkout_batch_items (reservation_id);
