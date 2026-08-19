-- 032_reward_redemptions_customer_unmapped.sql
--
-- Fase 5 (fechamento do resgate): adiciona o status
-- 'blocked_tray_customer_unmapped' ao CHECK de public.reward_redemptions.
--
-- Motivo: a criacao de pedido Tray exige customer_id (ID interno Tray do
-- cliente), que so existe quando ha um cliente Tray com o mesmo e-mail do
-- usuario. Criar um cliente novo na Tray exige birth_date (POST
-- /customers), campo que a NewStore nao coleta em nenhum lugar do
-- cadastro. Esse caso e deterministico (nenhuma mutacao Tray ocorre) e
-- sempre resulta em compensacao imediata dos NSCreditos — precisa de um
-- status proprio, distinto de 'compensated' (falha generica) e de
-- 'blocked_tray_contract_pending' (que significava "contrato ainda nao
-- implementado", o que deixou de ser verdade nesta fase).

BEGIN;

ALTER TABLE public.reward_redemptions
  DROP CONSTRAINT IF EXISTS reward_redemptions_status_check;

ALTER TABLE public.reward_redemptions
  ADD CONSTRAINT reward_redemptions_status_check CHECK (status IN (
    'processing',
    'credits_reserved',
    'tray_order_pending',
    'tray_order_created',
    'confirmed',
    'failed',
    'compensated',
    'reconciliation_required',
    'blocked_tray_contract_pending',
    'blocked_tray_customer_unmapped'
  ));

COMMIT;
