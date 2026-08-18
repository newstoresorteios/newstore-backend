-- Migration 031: resgate real da Loja de Premios (Fase 5).
--
-- Decisao de negocio (Fase 5): o saldo de NSCreditos DEIXA de ser
-- nscredit_wallets/nscredit_transactions e PASSA a ser o cupom individual
-- do usuario:
--
--   users.coupon_value_cents        -> saldo atual
--   coupon_balance_history          -> ledger canonico (mesmo ledger do
--                                       cupom legado, nao um segundo ledger)
--
-- nscredit_wallets/nscredit_transactions NAO sao apagadas (compatibilidade
-- para tras), mas deixam de ser fonte de saldo da Loja a partir desta
-- migration. Ver src/services/nscreditWallet.js.
--
-- Extensao aditiva do ledger canonico: coupon_balance_history ja tinha
-- balance_before_cents/balance_after_cents/event_type/meta, mas faltavam
-- idempotencia e um vinculo tipado com o resgate que originou o lancamento.

ALTER TABLE public.coupon_balance_history
  ADD COLUMN IF NOT EXISTS idempotency_key text NULL;

ALTER TABLE public.coupon_balance_history
  ADD COLUMN IF NOT EXISTS redemption_id uuid NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_coupon_hist_idempotency_key
  ON public.coupon_balance_history (idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_coupon_hist_redemption
  ON public.coupon_balance_history (redemption_id)
  WHERE redemption_id IS NOT NULL;

-- Defesa em profundidade agora que o ledger debita saldo real (resgate):
-- o banco garante a aritmetica, igual ja acontece em nscredit_transactions.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conname = 'coupon_balance_history_math_check'
       AND conrelid = 'public.coupon_balance_history'::regclass
  ) THEN
    ALTER TABLE public.coupon_balance_history
      ADD CONSTRAINT coupon_balance_history_math_check
      CHECK (balance_after_cents = balance_before_cents + delta_cents);
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conname = 'users_coupon_value_cents_non_negative'
       AND conrelid = 'public.users'::regclass
  ) THEN
    ALTER TABLE public.users
      ADD CONSTRAINT users_coupon_value_cents_non_negative
      CHECK (coupon_value_cents >= 0);
  END IF;
END $$;

-- ============================================================================
-- 1) Endereco de entrega (necessario para cotacao de frete e pedido Tray)
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.user_addresses (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id integer NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,

  recipient_name text NOT NULL,
  zipcode text NOT NULL,
  street text NOT NULL,
  number text NOT NULL,
  complement text NULL,
  neighborhood text NOT NULL,
  city text NOT NULL,
  state text NOT NULL,
  country text NOT NULL DEFAULT 'BR',

  is_default boolean NOT NULL DEFAULT false,

  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT user_addresses_zipcode_digits CHECK (zipcode ~ '^[0-9]{8}$'),
  CONSTRAINT user_addresses_state_len CHECK (char_length(state) = 2)
);

CREATE INDEX IF NOT EXISTS idx_user_addresses_user
  ON public.user_addresses (user_id, is_default DESC, created_at DESC);

-- ============================================================================
-- 2) Resgate (saga) — ver src/services/rewardRedemption.js
-- ============================================================================

CREATE TABLE IF NOT EXISTS public.reward_redemptions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  user_id integer NOT NULL REFERENCES public.users(id) ON DELETE RESTRICT,
  cart_id uuid NULL REFERENCES public.reward_carts(id) ON DELETE SET NULL,
  address_id uuid NULL REFERENCES public.user_addresses(id) ON DELETE SET NULL,

  -- Maquina de estados explicita (nao success/error). Ver item 27 do pedido.
  status text NOT NULL DEFAULT 'processing',

  credits_amount bigint NOT NULL,

  -- Snapshot do saldo do cupom no momento do debito (auditoria/compensacao).
  coupon_value_before_cents integer NOT NULL,
  coupon_value_after_cents integer NULL,

  -- Identidade do beneficio no momento do resgate (o cupom e o MESMO usado
  -- na Tray; snapshot aqui e so para nao depender de users mudar depois).
  coupon_code_snapshot text NULL,
  tray_coupon_id_snapshot text NULL,

  -- Preenchido somente quando a Fase E (pedido Tray real) estiver
  -- desbloqueada. Ver relatorio: contrato de pagamento pendente de decisao.
  tray_session_id text NULL,
  tray_order_id text NULL,

  shipping_snapshot jsonb NULL,
  address_snapshot jsonb NULL,

  -- Chave estavel gerada uma unica vez por tentativa de resgate. Nunca
  -- regenerada em retry.
  idempotency_key text NOT NULL,

  failure_reason text NULL,

  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT reward_redemptions_status_check CHECK (status IN (
    'processing',
    'credits_reserved',
    'tray_order_pending',
    'tray_order_created',
    'confirmed',
    'failed',
    'compensated',
    'reconciliation_required',
    'blocked_tray_contract_pending'
  )),
  CONSTRAINT reward_redemptions_credits_positive CHECK (credits_amount > 0),
  CONSTRAINT reward_redemptions_before_non_negative CHECK (coupon_value_before_cents >= 0),
  CONSTRAINT reward_redemptions_after_non_negative CHECK (coupon_value_after_cents IS NULL OR coupon_value_after_cents >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_reward_redemptions_idempotency_key
  ON public.reward_redemptions (idempotency_key);

CREATE INDEX IF NOT EXISTS idx_reward_redemptions_user_created
  ON public.reward_redemptions (user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_reward_redemptions_status
  ON public.reward_redemptions (status, created_at DESC);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conname = 'fk_coupon_hist_redemption'
       AND conrelid = 'public.coupon_balance_history'::regclass
  ) THEN
    ALTER TABLE public.coupon_balance_history
      ADD CONSTRAINT fk_coupon_hist_redemption
      FOREIGN KEY (redemption_id) REFERENCES public.reward_redemptions(id) ON DELETE SET NULL;
  END IF;
END $$;

-- Snapshots imutaveis por item (produto/variacao/preco no momento do resgate).
CREATE TABLE IF NOT EXISTS public.reward_redemption_items (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  redemption_id uuid NOT NULL REFERENCES public.reward_redemptions(id) ON DELETE CASCADE,

  reward_product_id uuid NULL REFERENCES public.reward_products(id) ON DELETE SET NULL,
  tray_product_id text NOT NULL,
  tray_variant_id text NULL,

  product_name_snapshot text NOT NULL,
  variant_name_snapshot text NULL,
  image_url_snapshot text NULL,

  quantity integer NOT NULL,
  nscredits_unit_price_snapshot bigint NOT NULL,
  nscredits_total_snapshot bigint NOT NULL,

  -- Preco real do produto na Tray (BRL), necessario para o pedido factual.
  -- NUNCA e o mesmo numero que nscredits_unit_price_snapshot (ver item 36).
  tray_price_cents_snapshot integer NULL,

  created_at timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT reward_redemption_items_quantity_positive CHECK (quantity >= 1),
  CONSTRAINT reward_redemption_items_price_positive CHECK (nscredits_unit_price_snapshot > 0)
);

CREATE INDEX IF NOT EXISTS idx_reward_redemption_items_redemption
  ON public.reward_redemption_items (redemption_id);

-- Trilha de eventos da saga — auditoria de cada transicao de estado.
CREATE TABLE IF NOT EXISTS public.reward_redemption_events (
  id bigserial PRIMARY KEY,
  redemption_id uuid NOT NULL REFERENCES public.reward_redemptions(id) ON DELETE CASCADE,

  from_status text NULL,
  to_status text NOT NULL,
  reason text NULL,
  meta jsonb NOT NULL DEFAULT '{}'::jsonb,

  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_reward_redemption_events_redemption
  ON public.reward_redemption_events (redemption_id, created_at);

CREATE OR REPLACE FUNCTION public.set_reward_redemptions_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END; $$;

DROP TRIGGER IF EXISTS trg_reward_redemptions_updated_at ON public.reward_redemptions;
CREATE TRIGGER trg_reward_redemptions_updated_at
BEFORE UPDATE ON public.reward_redemptions
FOR EACH ROW EXECUTE PROCEDURE public.set_reward_redemptions_updated_at();

DROP TRIGGER IF EXISTS trg_user_addresses_updated_at ON public.user_addresses;
CREATE TRIGGER trg_user_addresses_updated_at
BEFORE UPDATE ON public.user_addresses
FOR EACH ROW EXECUTE PROCEDURE public.set_reward_redemptions_updated_at();
