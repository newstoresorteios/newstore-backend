-- Migration: catalogo curado da Loja de Premios (NSCreditos).
-- Somente leitura da Tray: esta tabela guarda o SNAPSHOT local do produto Tray
-- mais os campos que sao propriedade exclusiva da NewStore
-- (nscredits_price, is_published, display_order, published_at, published_by).
-- Publicar/despublicar aqui NAO altera nada na Tray.

CREATE TABLE IF NOT EXISTS public.reward_products (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Identidade factual na Tray
  tray_product_id text NOT NULL UNIQUE,

  -- Propriedade exclusiva da NewStore
  nscredits_price integer NOT NULL,
  is_published boolean NOT NULL DEFAULT true,
  display_order integer NOT NULL DEFAULT 0,

  -- Snapshot factual (somente leitura da Tray)
  name text NULL,
  description_small text NULL,
  reference text NULL,
  brand text NULL,
  image_url text NULL,
  images_snapshot jsonb NOT NULL DEFAULT '[]'::jsonb,
  tray_product_url text NULL,

  stock integer NULL,
  tray_available smallint NULL,
  tray_available_in_store smallint NULL,
  availability_text text NULL,
  availability_days integer NULL,
  has_variation boolean NOT NULL DEFAULT false,
  when_stock_runs_out text NULL,
  order_days_availability integer NULL,

  variants_snapshot jsonb NOT NULL DEFAULT '[]'::jsonb,

  -- Referencia administrativa apenas (nao exposto no endpoint publico)
  tray_price_snapshot numeric(12,2) NULL,
  tray_modified_at timestamptz NULL,
  last_synced_at timestamptz NULL,

  published_at timestamptz NULL,
  published_by integer NULL,

  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT reward_products_nscredits_price_positive
    CHECK (nscredits_price > 0),

  CONSTRAINT reward_products_display_order_non_negative
    CHECK (display_order >= 0),

  CONSTRAINT reward_products_variants_snapshot_is_array
    CHECK (jsonb_typeof(variants_snapshot) = 'array'),

  CONSTRAINT reward_products_images_snapshot_is_array
    CHECK (jsonb_typeof(images_snapshot) = 'array')
);

CREATE INDEX IF NOT EXISTS idx_reward_products_published_order
ON public.reward_products(is_published, display_order, id);

CREATE INDEX IF NOT EXISTS idx_reward_products_last_synced_at
ON public.reward_products(last_synced_at);

CREATE OR REPLACE FUNCTION public.set_reward_products_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END; $$;

DROP TRIGGER IF EXISTS trg_reward_products_updated_at
ON public.reward_products;

CREATE TRIGGER trg_reward_products_updated_at
BEFORE UPDATE ON public.reward_products
FOR EACH ROW EXECUTE PROCEDURE public.set_reward_products_updated_at();
