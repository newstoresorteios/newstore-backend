-- Migration: carteira de NSCreditos da Loja de Premios.
--
-- NSCreditos sao uma moeda propria da Loja. NAO tem relacao com
-- coupon_value_cents, saldo em reais, Mercado Pago, Vindi ou preco Tray.
-- Esta migration NAO altera a tabela `users` nem qualquer estrutura existente.
--
-- nscredit_wallets      -> saldo materializado (performance)
-- nscredit_transactions -> ledger imutavel (fonte auditavel)
--
-- O saldo nunca e sobrescrito diretamente: toda mudanca e uma operacao
-- (credit | debit) gravada no ledger dentro da mesma transacao.

CREATE TABLE IF NOT EXISTS public.nscredit_wallets (
  user_id integer PRIMARY KEY REFERENCES public.users(id) ON DELETE CASCADE,

  balance bigint NOT NULL DEFAULT 0,

  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT nscredit_wallets_balance_non_negative CHECK (balance >= 0)
);

CREATE TABLE IF NOT EXISTS public.nscredit_transactions (
  id bigserial PRIMARY KEY,

  user_id integer NOT NULL REFERENCES public.users(id) ON DELETE CASCADE,

  -- credit | debit. Nao existe "set": saldo nao e sobrescrito.
  operation text NOT NULL,
  amount bigint NOT NULL,

  balance_before bigint NOT NULL,
  balance_after bigint NOT NULL,

  -- Nesta fase todas as movimentacoes sao 'admin'. A coluna e aberta para
  -- origens futuras (ex.: 'redemption', 'promotion') sem migration nova.
  source_type text NOT NULL DEFAULT 'admin',
  source_id text NULL,

  reason text NULL,

  -- Administrador responsavel. Vem SEMPRE da sessao autenticada.
  created_by integer NULL REFERENCES public.users(id) ON DELETE SET NULL,

  -- Evita aplicar a mesma operacao duas vezes (duplo clique / reenvio).
  idempotency_key text NULL,

  created_at timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT nscredit_transactions_operation_check
    CHECK (operation IN ('credit', 'debit')),

  CONSTRAINT nscredit_transactions_amount_positive
    CHECK (amount > 0),

  CONSTRAINT nscredit_transactions_balance_before_non_negative
    CHECK (balance_before >= 0),

  CONSTRAINT nscredit_transactions_balance_after_non_negative
    CHECK (balance_after >= 0),

  -- O proprio banco garante a aritmetica do ledger.
  CONSTRAINT nscredit_transactions_balance_math CHECK (
    (operation = 'credit' AND balance_after = balance_before + amount)
    OR
    (operation = 'debit'  AND balance_after = balance_before - amount)
  ),

  -- Movimentacao administrativa exige motivo e responsavel.
  CONSTRAINT nscredit_transactions_admin_requires_reason CHECK (
    source_type <> 'admin'
    OR (reason IS NOT NULL AND length(btrim(reason)) > 0)
  ),
  CONSTRAINT nscredit_transactions_admin_requires_author CHECK (
    source_type <> 'admin' OR created_by IS NOT NULL
  )
);

-- Idempotencia: a mesma chave nunca pode gerar duas movimentacoes.
CREATE UNIQUE INDEX IF NOT EXISTS idx_nscredit_transactions_idempotency_key
ON public.nscredit_transactions(idempotency_key)
WHERE idempotency_key IS NOT NULL;

-- Historico do cliente, do mais recente para o mais antigo.
CREATE INDEX IF NOT EXISTS idx_nscredit_transactions_user_created
ON public.nscredit_transactions(user_id, created_at DESC, id DESC);

-- Auditoria por origem (ex.: localizar as movimentacoes de um resgate futuro).
CREATE INDEX IF NOT EXISTS idx_nscredit_transactions_source
ON public.nscredit_transactions(source_type, source_id);

-- Auditoria por administrador responsavel.
CREATE INDEX IF NOT EXISTS idx_nscredit_transactions_created_by
ON public.nscredit_transactions(created_by, created_at DESC);

CREATE OR REPLACE FUNCTION public.set_nscredit_wallets_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END; $$;

DROP TRIGGER IF EXISTS trg_nscredit_wallets_updated_at
ON public.nscredit_wallets;

CREATE TRIGGER trg_nscredit_wallets_updated_at
BEFORE UPDATE ON public.nscredit_wallets
FOR EACH ROW EXECUTE PROCEDURE public.set_nscredit_wallets_updated_at();
