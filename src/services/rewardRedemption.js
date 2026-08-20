// src/services/rewardRedemption.js
//
// Saga do resgate real (Fase 5). Postgres + Tray NAO sao ACID (item 31 do
// pedido original), entao cada passo e uma operacao local atomica propria,
// com compensacao explicita quando o passo seguinte falha — NUNCA uma
// transacao Postgres aberta durante uma chamada HTTP a Tray (item 23):
//
//   TX curta A -> applyCouponLedgerEntry(debit): abre sua PROPRIA
//     transacao (SELECT ... FOR UPDATE + update + insert no ledger),
//     comita e devolve. Nenhuma chamada de rede acontece dentro dela.
//   Fora de transacao -> createTrayRedemptionOrder (rede real, sem lock
//     nenhum seguro).
//   TX curta B (se necessario) -> applyCouponLedgerEntry(credit) de
//     compensacao, de novo em sua PROPRIA transacao isolada.
//
// Timeout/rede instavel na criacao do pedido vira TrayOrderAmbiguousError
// (trayRedemptionOrder.js) -> status 'reconciliation_required', creditos
// permanecem debitados, NUNCA compensa as cegas (item 34). GAP CONHECIDO,
// documentado no relatorio: uma busca ATIVA do pedido na Tray (por
// customer_id + notes, via GET /orders) antes de decidir confirmar ou
// compensar nao foi implementada — a documentacao oficial auditada nao
// confirma se `notes` e devolvido no GET apos a criacao, e nao ha ambiente
// de homologacao Tray disponivel para verificar isso sem criar um pedido
// real. Ate essa confirmacao, todo caso ambiguo fica em
// 'reconciliation_required' para resolucao manual — nunca uma decisao
// automatica sem evidencia.
//
// prepare NAO debita (item 39). Só confirm debita, e só uma vez por
// idempotency_key (item 28).

import { resolveDeps as resolveCartDeps, findActiveCart, loadItems, buildCart } from "./rewardCart.js";
import { validateCart } from "./rewardCartValidator.js";
import { getCouponBalance, applyCouponLedgerEntry, CouponLedgerError } from "./couponLedger.js";
import { getUserAddress } from "./userAddress.js";
import {
  createTrayRedemptionOrder,
  TrayOrderNotImplementedError,
  TrayCustomerProfileIncompleteError,
  TrayCustomerAmbiguousError,
  TrayCustomerIdentityConflictError,
  TrayOrderAmbiguousError,
} from "./trayRedemptionOrder.js";
import { ensureTrayCouponForUser } from "./trayCouponEnsure.js";

export class RedemptionError extends Error {
  constructor(code, { status = 400, details = null } = {}) {
    super(code);
    this.name = "RedemptionError";
    this.code = code;
    this.status = status;
    this.details = details;
  }
}

/**
 * Kill-switch de produção (item 24/25 do pedido): o botão CONTINUAR
 * desabilitado no frontend NAO e protecao suficiente — qualquer um pode
 * chamar a API diretamente. Default SEGURO e "false": so uma variavel de
 * ambiente explicita "true" liga o confirm. Qualquer outro valor (ausente,
 * "false", vazio, typo) mantem desligado.
 */
export function isRewardRedemptionEnabled(env = process.env) {
  return String(env.REWARD_REDEMPTION_ENABLED || "").trim().toLowerCase() === "true";
}

function resolveDeps(deps = {}) {
  return {
    ...resolveCartDeps(deps),
    createTrayRedemptionOrder: deps.createTrayRedemptionOrder || createTrayRedemptionOrder,
    ensureTrayCouponForUser: deps.ensureTrayCouponForUser || ensureTrayCouponForUser,
  };
}

/**
 * Fase F (item 33): mantem o cupom Tray sincronizado com
 * users.coupon_value_cents IMEDIATAMENTE apos qualquer mudanca de saldo do
 * resgate — nunca esperando o proximo login. Reusa ensureTrayCouponForUser
 * (mesma funcao do gatilho de login), que ja le o saldo FRESCO do banco e e
 * best-effort por design (nunca lanca, so loga e devolve status FAILED).
 * Chamado depois que o saldo local ja mudou (debito ou compensacao) —
 * nunca antes, e nunca dentro da transacao que move o saldo.
 */
async function syncTrayCouponBestEffort(userId, d) {
  try {
    await d.ensureTrayCouponForUser(userId);
  } catch (e) {
    console.warn("[reward.redemption] sync do cupom Tray pos-saldo falhou (nao bloqueia o resgate)", { userId, error: e?.message || String(e) });
  }
}

async function loadValidCart(userId, deps) {
  const out = await validateCart(userId, deps);
  if (out.cart.total_items === 0) throw new RedemptionError("cart_empty", { status: 409 });
  return out;
}

/* ─────────────────────────── Prepare (somente leitura) ─────────────────────────── */

/**
 * Resumo de confirmacao (item 41): produto/variacao/qtd, creditos gastos,
 * saldo antes/depois PREVISTO, endereco, frete, cupom. NAO cria nada,
 * NAO debita nada.
 */
export async function prepareRedemption(userId, { addressId } = {}, deps = {}) {
  const d = resolveDeps(deps);

  const validated = await loadValidCart(userId, d);
  if (!validated.valid) throw new RedemptionError("cart_invalid", { status: 409, details: { issues: validated.issues } });

  const address = await getUserAddress(userId, addressId, deps);
  if (!address) throw new RedemptionError("address_not_found", { status: 404 });

  const balance = await getCouponBalance(userId, deps);

  return {
    items: validated.items,
    credits_amount: validated.cart.total_nscredits,
    coupon_balance_before: balance.balance_cents / 100,
    coupon_balance_after_preview: balance.balance_cents / 100 - validated.cart.total_nscredits,
    coupon_code: balance.coupon_code,
    coupon_expires_at: balance.expires_at,
    address,
  };
}

/* ─────────────────────────── Persistencia da saga ─────────────────────────── */

async function findRedemptionByIdempotencyKey(query, idempotencyKey) {
  const { rows } = await query(`select * from public.reward_redemptions where idempotency_key = $1`, [idempotencyKey]);
  return rows[0] || null;
}

async function createRedemptionRow(query, { userId, cartId, addressId, creditsAmount, balanceBeforeCents, couponCode, trayCouponId, addressSnapshot, idempotencyKey }) {
  const { rows } = await query(
    `insert into public.reward_redemptions
       (user_id, cart_id, address_id, status, credits_amount, coupon_value_before_cents,
        coupon_code_snapshot, tray_coupon_id_snapshot, address_snapshot, idempotency_key)
     values ($1,$2,$3,'processing',$4,$5,$6,$7,$8::jsonb,$9)
     returning *`,
    [userId, cartId, addressId, creditsAmount, balanceBeforeCents, couponCode, trayCouponId, JSON.stringify(addressSnapshot), idempotencyKey]
  );
  return rows[0];
}

async function insertRedemptionItems(query, redemptionId, items) {
  for (const item of items) {
    // eslint-disable-next-line no-await-in-loop
    await query(
      `insert into public.reward_redemption_items
         (redemption_id, reward_product_id, tray_product_id, tray_variant_id,
          product_name_snapshot, variant_name_snapshot, image_url_snapshot,
          quantity, nscredits_unit_price_snapshot, nscredits_total_snapshot)
       values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [
        redemptionId,
        item.reward_product_id,
        item.tray_product_id,
        item.tray_variant_id,
        item.name,
        item.variant_name,
        item.image_url,
        item.quantity,
        item.current_nscredits_price,
        item.nscredits_subtotal,
      ]
    );
  }
}

async function recordEvent(query, redemptionId, { from, to, reason = null, meta = null }) {
  await query(
    `insert into public.reward_redemption_events (redemption_id, from_status, to_status, reason, meta)
     values ($1,$2,$3,$4,$5::jsonb)`,
    [redemptionId, from, to, reason, JSON.stringify(meta || {})]
  );
}

async function setStatus(query, redemptionId, status, extra = {}) {
  const sets = ["status = $2", "updated_at = now()"];
  const params = [redemptionId, status];
  if ("coupon_value_after_cents" in extra) {
    params.push(extra.coupon_value_after_cents);
    sets.push(`coupon_value_after_cents = $${params.length}`);
  }
  if ("failure_reason" in extra) {
    params.push(extra.failure_reason);
    sets.push(`failure_reason = $${params.length}`);
  }
  if ("shipping_snapshot" in extra) {
    params.push(JSON.stringify(extra.shipping_snapshot));
    sets.push(`shipping_snapshot = $${params.length}::jsonb`);
  }
  if ("tray_order_id" in extra) {
    params.push(extra.tray_order_id);
    sets.push(`tray_order_id = $${params.length}`);
  }
  await query(`update public.reward_redemptions set ${sets.join(", ")} where id = $1`, params);
}

function mapRedemption(row) {
  return {
    id: row.id,
    status: row.status,
    credits_amount: Number(row.credits_amount),
    coupon_value_before_cents: Number(row.coupon_value_before_cents),
    coupon_value_after_cents: row.coupon_value_after_cents == null ? null : Number(row.coupon_value_after_cents),
    failure_reason: row.failure_reason || null,
    tray_order_id: row.tray_order_id || null,
    created_at: row.created_at,
    updated_at: row.updated_at,
  };
}

/* ─────────────────────────── Confirm (a unica coisa que debita) ─────────────────────────── */

export async function confirmRedemption(userId, { addressId, shippingOption = null, idempotencyKey } = {}, deps = {}) {
  // Kill-switch: verificado ANTES de qualquer leitura/escrita — inclusive
  // antes do lookup de idempotencia. Nenhuma query roda quando desligado.
  if (!isRewardRedemptionEnabled(deps.env)) {
    throw new RedemptionError("reward_redemption_disabled", { status: 503 });
  }

  const d = resolveDeps(deps);
  const query = d.query;

  const key = String(idempotencyKey || "").trim();
  if (!key) throw new RedemptionError("idempotency_key_required", { status: 400 });

  const existing = await findRedemptionByIdempotencyKey(query, key);
  if (existing) return { replayed: true, redemption: mapRedemption(existing) };

  const validated = await loadValidCart(userId, d);
  if (!validated.valid) throw new RedemptionError("cart_invalid", { status: 409, details: { issues: validated.issues } });

  const address = await getUserAddress(userId, addressId, deps);
  if (!address) throw new RedemptionError("address_not_found", { status: 404 });

  const balance = await getCouponBalance(userId, deps);
  const creditsAmount = validated.cart.total_nscredits;

  // Necessario para resolver/criar o customer_id Tray (a Tray nao conhece
  // users.id) — ver trayCustomerResolver.js / trayRedemptionOrder.js.
  // birth_date/cpf so existem quando o usuario ja completou o perfil de
  // resgate (rewardProfile.js) — null aqui e um estado valido, tratado
  // como bloqueio isolado se um Customer novo precisar ser criado. cpf
  // nunca e logado (PII) — so passa pelo resolver/DTO Tray.
  const userRow = await query(`select name, email, phone, birth_date, cpf from public.users where id = $1`, [userId]);
  const userProfile = {
    name: userRow.rows[0]?.name || null,
    email: userRow.rows[0]?.email || null,
    phone: userRow.rows[0]?.phone || null,
    birthDate: userRow.rows[0]?.birth_date || null,
    cpf: userRow.rows[0]?.cpf || null,
  };

  const redemption = await createRedemptionRow(query, {
    userId,
    cartId: validated.cart.id,
    addressId,
    creditsAmount,
    balanceBeforeCents: balance.balance_cents,
    couponCode: balance.coupon_code,
    trayCouponId: balance.tray_coupon_id,
    addressSnapshot: address,
    idempotencyKey: key,
  });
  await insertRedemptionItems(query, redemption.id, validated.items);
  await recordEvent(query, redemption.id, { from: null, to: "processing" });

  // Passo 1: debito atomico do cupom (unico ponto que move dinheiro/credito).
  let debit;
  try {
    debit = await applyCouponLedgerEntry(
      {
        userId,
        operation: "debit",
        amountCents: creditsAmount * 100,
        eventType: "REDEMPTION_DEBIT",
        redemptionId: redemption.id,
        idempotencyKey: `${key}:debit`,
      },
      deps
    );
  } catch (e) {
    const reason = e instanceof CouponLedgerError ? e.code : "debit_failed";
    await setStatus(query, redemption.id, "failed", { failure_reason: reason });
    await recordEvent(query, redemption.id, { from: "processing", to: "failed", reason });
    throw new RedemptionError(reason, { status: e?.status || 409 });
  }

  await setStatus(query, redemption.id, "credits_reserved", { coupon_value_after_cents: debit.balance_cents });
  await recordEvent(query, redemption.id, { from: "processing", to: "credits_reserved" });

  // Fase F (item 33): saldo local ja mudou (debito) — sincroniza o cupom
  // Tray IMEDIATAMENTE, antes mesmo de tentar o pedido. Erra do lado seguro:
  // se o pedido falhar e compensarmos depois, o cupom Tray fica
  // temporariamente MENOR que o saldo real (nunca maior) ate a segunda
  // sincronizacao abaixo — nunca abre uma janela de double-spend.
  await syncTrayCouponBestEffort(userId, d);

  // Passo 2: tentativa de pedido Tray real.
  await setStatus(query, redemption.id, "tray_order_pending", { shipping_snapshot: shippingOption });
  await recordEvent(query, redemption.id, { from: "credits_reserved", to: "tray_order_pending" });

  try {
    const orderResult = await d.createTrayRedemptionOrder({
      userId,
      redemptionId: redemption.id,
      idempotencyKey: key,
      items: validated.items,
      userProfile,
      // M7 (prova real): POST /orders exige CustomerAddress preenchido
      // (address/number/neighborhood/city/state/zip_code/country). O endereco
      // e o do proprio usuario (user_addresses), nunca inventado. Isso NAO e
      // frete -- nenhum valor/transportadora e calculado aqui.
      address,
      couponSnapshot: { coupon_code: balance.coupon_code, tray_coupon_id: balance.tray_coupon_id },
    });

    await setStatus(query, redemption.id, "confirmed", { tray_order_id: orderResult?.orderId || null });
    await recordEvent(query, redemption.id, { from: "tray_order_pending", to: "confirmed" });
    return { replayed: false, redemption: mapRedemption({ ...redemption, status: "confirmed", coupon_value_after_cents: debit.balance_cents, tray_order_id: orderResult?.orderId || null }) };
  } catch (e) {
    if (e instanceof TrayOrderAmbiguousError) {
      // Item 34: timeout/resultado ambiguo NUNCA compensa automaticamente.
      await setStatus(query, redemption.id, "reconciliation_required", { failure_reason: e.code });
      await recordEvent(query, redemption.id, { from: "tray_order_pending", to: "reconciliation_required", reason: e.code });
      return {
        replayed: false,
        redemption: mapRedemption({ ...redemption, status: "reconciliation_required", coupon_value_after_cents: debit.balance_cents, failure_reason: e.code }),
      };
    }

    // Deterministico (inclusive TrayOrderNotImplementedError,
    // TrayCustomerProfileIncompleteError, TrayCustomerAmbiguousError e
    // TrayCustomerIdentityConflictError): sabemos que nenhum pedido foi
    // criado, entao compensar imediatamente e seguro.
    const reason =
      e instanceof TrayOrderNotImplementedError ||
      e instanceof TrayCustomerProfileIncompleteError ||
      e instanceof TrayCustomerAmbiguousError ||
      e instanceof TrayCustomerIdentityConflictError
        ? e.code
        : "tray_order_failed";
    const compensation = await applyCouponLedgerEntry(
      {
        userId,
        operation: "credit",
        amountCents: creditsAmount * 100,
        eventType: "REDEMPTION_COMPENSATION",
        redemptionId: redemption.id,
        idempotencyKey: `${key}:compensation`,
      },
      deps
    );

    // M7.1: conflito de identidade (e-mail x cpf apontando pra Customers
    // Tray incompativeis) reusa o status blocked_tray_customer_ambiguous
    // -- mesma familia semantica ("resolucao de Customer Tray bloqueada,
    // precisa de auditoria humana, nunca escolhida as cegas"); o motivo
    // especifico (tray_customer_identity_conflict + reason detalhado) fica
    // em failure_reason, sem exigir uma nova migration so pra este status.
    const finalStatus = e instanceof TrayOrderNotImplementedError
      ? "blocked_tray_contract_pending"
      : e instanceof TrayCustomerProfileIncompleteError
        ? "blocked_tray_profile_incomplete"
        : e instanceof TrayCustomerAmbiguousError || e instanceof TrayCustomerIdentityConflictError
          ? "blocked_tray_customer_ambiguous"
          : "compensated";
    await setStatus(query, redemption.id, finalStatus, { coupon_value_after_cents: compensation.balance_cents, failure_reason: reason });
    await recordEvent(query, redemption.id, { from: "tray_order_pending", to: finalStatus, reason });

    // Saldo local mudou de novo (compensacao) — resincroniza o cupom Tray
    // para refletir o credito devolvido. So agora o valor volta a subir,
    // nunca antes de termos certeza de que nenhum pedido foi criado.
    await syncTrayCouponBestEffort(userId, d);

    return {
      replayed: false,
      redemption: mapRedemption({ ...redemption, status: finalStatus, coupon_value_after_cents: compensation.balance_cents, failure_reason: reason }),
    };
  }
}

/* ─────────────────────────── Consulta ─────────────────────────── */

export async function getRedemption(userId, redemptionId, deps = {}) {
  const d = resolveDeps(deps);
  const { rows } = await d.query(`select * from public.reward_redemptions where id = $1 and user_id = $2`, [redemptionId, userId]);
  if (!rows.length) return null;
  return mapRedemption(rows[0]);
}

export async function listRedemptions(userId, { page = 1, limit = 20 } = {}, deps = {}) {
  const d = resolveDeps(deps);
  const safeLimit = Math.min(Math.max(Number(limit) || 20, 1), 100);
  const safePage = Math.max(Number(page) || 1, 1);
  const offset = (safePage - 1) * safeLimit;

  const [list, count] = await Promise.all([
    d.query(`select * from public.reward_redemptions where user_id = $1 order by created_at desc limit $2 offset $3`, [userId, safeLimit, offset]),
    d.query(`select count(*) as total from public.reward_redemptions where user_id = $1`, [userId]),
  ]);

  return {
    items: list.rows.map(mapRedemption),
    paging: { page: safePage, limit: safeLimit, total: Number(count.rows[0]?.total || 0) },
  };
}
