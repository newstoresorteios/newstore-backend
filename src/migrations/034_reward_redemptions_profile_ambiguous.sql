-- 034_reward_redemptions_profile_ambiguous.sql
--
-- Reta final Loja NS: substitui a semantica de "customer Tray nao
-- encontrado = bloqueio permanente" (blocked_tray_customer_unmapped,
-- migration 032) por resolucao real com criacao (trayCustomerResolver.js).
-- Os bloqueios isolados possiveis agora sao outros:
--
--   blocked_tray_profile_incomplete: nenhum Customer Tray encontrado por
--     e-mail E o perfil NewStore ainda nao tem birth_date (exigido pela
--     Tray pra criar um Customer novo). Resolve sozinho assim que o
--     usuario completa o perfil (rewardProfile.js) e tenta de novo.
--
--   blocked_tray_customer_ambiguous: mais de um Customer Tray com o
--     mesmo e-mail exato — nunca escolhido arbitrariamente (item 11).
--
-- blocked_tray_customer_unmapped permanece no CHECK (nao remove valor que
-- pode existir em linhas historicas), mas nenhum codigo novo produz esse
-- status.

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
    'blocked_tray_customer_unmapped',
    'blocked_tray_profile_incomplete',
    'blocked_tray_customer_ambiguous'
  ));

COMMIT;
