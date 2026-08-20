-- 035_users_cpf.sql
--
-- M7.1: o teste controlado real (POST /customers contra a conta Tray real
-- desta loja) devolveu 400 -- "Este campo nao pode ser deixado em
-- branco": {"Customer": {"cpf": [...]}}. Isso prevalece sobre qualquer
-- schema curado que classificava cpf como opcional -- para ESTA loja
-- Tray, cpf e obrigatorio na criacao de Customer.
--
-- users.cpf: somente os 11 digitos, sem pontuacao (normalizado no
-- backend antes de gravar -- ver rewardProfile.js). CHECK estrutural
-- (formato) aqui; a validacao matematica dos digitos verificadores fica
-- no codigo (nao reimplementavel em SQL puro de forma legivel).
--
-- UNIQUE parcial: um CPF nunca pode ficar associado silenciosamente a
-- duas contas NewStore. Coluna nova -> todos os valores existentes sao
-- NULL, entao nao ha conflito possivel na criacao do indice.

BEGIN;

ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS cpf text NULL;

ALTER TABLE public.users
  ADD CONSTRAINT users_cpf_format_check
  CHECK (cpf IS NULL OR cpf ~ '^[0-9]{11}$');

CREATE UNIQUE INDEX IF NOT EXISTS uq_users_cpf
  ON public.users (cpf)
  WHERE cpf IS NOT NULL;

COMMIT;
