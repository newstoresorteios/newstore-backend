-- 033_users_reward_profile_fields.sql
--
-- Reta final do resgate (Loja NS): a Tray exige, para criar um Customer
-- (POST /customers), name + email + birth_date (required no schema oficial
-- curado pela propria Tray, tray-tecnologia/tray-api-ai-plugin,
-- skills/clientes/schemas/cliente.create.json). name/email a NewStore ja
-- coleta; birth_date nao. cpf/rg/gender sao OPCIONAIS nesse schema -- por
-- isso, deliberadamente, NAO sao adicionados aqui (YAGNI + minimizacao de
-- PII: nao coletar dado que a Tray nao exige).
--
-- tray_customer_id: cache do mapping estavel NewStore user <-> Tray
-- Customer, para nao precisar buscar por e-mail a cada pedido (uma vez
-- resolvido/criado, persiste). Unique parcial (so quando preenchido) --
-- dois usuarios NewStore nunca podem apontar pro mesmo Customer Tray.

BEGIN;

ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS birth_date date NULL,
  ADD COLUMN IF NOT EXISTS tray_customer_id text NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_users_tray_customer_id
  ON public.users (tray_customer_id)
  WHERE tray_customer_id IS NOT NULL;

COMMIT;
