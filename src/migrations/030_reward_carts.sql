-- Migration: carrinho da Loja de Premios (intencao de resgate).
--
-- O carrinho pertence a NEWSTORE. Nao existe carrinho na Tray nesta fase.
-- Adicionar ao carrinho NAO reserva estoque e NAO garante disponibilidade
-- no fechamento — a Tray continua livre para vender o mesmo produto.
--
-- Carrinho != pedido. As tabelas de pedido (reward_orders) entram na Fase 5.
-- Nao existe tabela de reserva de estoque nesta fase.

CREATE TABLE IF NOT EXISTS public.reward_carts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  user_id integer NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,

  -- Nesta fase so 'active' e usado. Os demais existem para a Fase 5 nao
  -- precisar de migration nova, mas nenhum fluxo os produz ainda.
  status text NOT NULL DEFAULT 'active',

  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT reward_carts_status_check
    CHECK (status IN ('active', 'processing', 'completed', 'cancelled'))
);

-- No maximo UM carrinho ativo por usuario.
CREATE UNIQUE INDEX IF NOT EXISTS idx_reward_carts_one_active_per_user
ON public.reward_carts(user_id)
WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_reward_carts_user_status
ON public.reward_carts(user_id, status);

CREATE TABLE IF NOT EXISTS public.reward_cart_items (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  cart_id uuid NOT NULL REFERENCES public.reward_carts(id) ON DELETE CASCADE,
  reward_product_id uuid NOT NULL REFERENCES public.reward_products(id) ON DELETE CASCADE,

  -- Identidade factual na Tray, desnormalizada para auditoria da intencao.
  tray_product_id text NOT NULL,
  tray_variant_id text NULL,

  quantity integer NOT NULL,

  -- Snapshot do preco NO MOMENTO DA ADICAO. Serve para DETECTAR mudanca
  -- (price_changed), nao para congelar o preco: a Fase 5 usara o valor vigente.
  nscredits_unit_price_snapshot bigint NOT NULL,

  product_name_snapshot text NULL,
  variant_name_snapshot text NULL,
  image_url_snapshot text NULL,

  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT reward_cart_items_quantity_positive
    CHECK (quantity >= 1),

  CONSTRAINT reward_cart_items_price_positive
    CHECK (nscredits_unit_price_snapshot > 0)
);

-- Unicidade do item no carrinho.
-- Dois indices parciais porque NULL nao participa de UNIQUE comum:
-- produtos COM variacao sao unicos por (carrinho, produto, variacao);
-- produtos SIMPLES sao unicos por (carrinho, produto).
CREATE UNIQUE INDEX IF NOT EXISTS idx_reward_cart_items_unique_with_variant
ON public.reward_cart_items(cart_id, reward_product_id, tray_variant_id)
WHERE tray_variant_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_reward_cart_items_unique_simple
ON public.reward_cart_items(cart_id, reward_product_id)
WHERE tray_variant_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_reward_cart_items_cart
ON public.reward_cart_items(cart_id, created_at);

CREATE OR REPLACE FUNCTION public.set_reward_carts_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END; $$;

DROP TRIGGER IF EXISTS trg_reward_carts_updated_at ON public.reward_carts;
CREATE TRIGGER trg_reward_carts_updated_at
BEFORE UPDATE ON public.reward_carts
FOR EACH ROW EXECUTE PROCEDURE public.set_reward_carts_updated_at();

DROP TRIGGER IF EXISTS trg_reward_cart_items_updated_at ON public.reward_cart_items;
CREATE TRIGGER trg_reward_cart_items_updated_at
BEFORE UPDATE ON public.reward_cart_items
FOR EACH ROW EXECUTE PROCEDURE public.set_reward_carts_updated_at();
