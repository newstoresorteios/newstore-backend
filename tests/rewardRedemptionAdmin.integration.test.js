// tests/rewardRedemptionAdmin.integration.test.js
//
// Relatorios e listagem administrativa contra Postgres REAL — unico jeito
// honesto de provar a agregacao (sum ... filter, count distinct, LIMIT/
// OFFSET), que nenhum fake em JS consegue demonstrar.
//
//   TEST_DATABASE_URL=postgres://... npm test
//
// Sem TEST_DATABASE_URL os testes sao pulados (nunca banco de producao).
// Todas as fixtures sao inseridas e removidas por este arquivo; nenhuma
// linha pre-existente e alterada e nenhuma chamada a Tray acontece.
import test, { before, after, beforeEach } from "node:test";
import assert from "node:assert/strict";

import {
  listAdminRedemptions,
  getRedemptionReport,
  getRedemptionReportMetrics,
  getAdminRedemptionDetail,
} from "../src/services/rewardRedemptionAdmin.js";

const TEST_DB = process.env.TEST_DATABASE_URL || "";
const SKIP = !TEST_DB;
const skipOpts = { skip: SKIP ? "defina TEST_DATABASE_URL para rodar os testes de integracao" : false };

let pool;
let deps;
let stamp;
let userA;
let userB;

function sslFor(url) {
  try {
    const host = new URL(url).hostname;
    return { rejectUnauthorized: false, servername: /^\d+\.\d+\.\d+\.\d+$/.test(host) ? undefined : host };
  } catch {
    return { rejectUnauthorized: false };
  }
}

/** Recorte que isola as fixtures deste arquivo de qualquer linha existente. */
function scope(extra = {}) {
  return { q: stamp, ...extra };
}

async function createUser(label) {
  const { rows } = await pool.query(
    `insert into public.users (name, email, pass_hash, is_admin) values ($1,$2,'x',false) returning id`,
    [`Admin Report ${label} ${stamp}`, `admin-report-${label}-${stamp}@exemplo.local`]
  );
  return rows[0].id;
}

async function insertRedemption({ userId, status, creditsAmount, trayOrderId = null, addressSnapshot = null, createdAt = null }) {
  const { rows } = await pool.query(
    `insert into public.reward_redemptions
       (user_id, status, credits_amount, coupon_value_before_cents, coupon_value_after_cents,
        tray_order_id, address_snapshot, idempotency_key, created_at)
     values ($1,$2,$3,$4,$5,$6,$7::jsonb,$8, coalesce($9::timestamptz, now()))
     returning id`,
    [
      userId,
      status,
      creditsAmount,
      5000000,
      5000000 - creditsAmount * 100,
      trayOrderId,
      addressSnapshot ? JSON.stringify(addressSnapshot) : null,
      `admin-report-${stamp}-${Math.random().toString(36).slice(2)}`,
      createdAt,
    ]
  );
  return rows[0].id;
}

before(async () => {
  if (SKIP) return;
  const pg = (await import("pg")).default;
  pool = new pg.Pool({ connectionString: TEST_DB, ssl: sslFor(TEST_DB), max: 5 });
  deps = { query: (sql, params) => pool.query(sql, params) };

  stamp = `rrai${Date.now()}`;
  userA = await createUser("a");
  userB = await createUser("b");
});

after(async () => {
  if (SKIP || !pool) return;
  for (const userId of [userA, userB]) {
    await pool.query("delete from public.coupon_balance_history where user_id=$1", [userId]).catch(() => {});
    await pool
      .query("delete from public.reward_redemption_events where redemption_id in (select id from public.reward_redemptions where user_id=$1)", [userId])
      .catch(() => {});
    await pool
      .query("delete from public.reward_redemption_items where redemption_id in (select id from public.reward_redemptions where user_id=$1)", [userId])
      .catch(() => {});
    await pool.query("delete from public.reward_redemptions where user_id=$1", [userId]).catch(() => {});
    await pool.query("delete from public.users where id=$1", [userId]).catch(() => {});
  }
  await pool.end().catch(() => {});
});

beforeEach(async () => {
  if (SKIP) return;
  for (const userId of [userA, userB]) {
    await pool.query("delete from public.coupon_balance_history where user_id=$1", [userId]);
    await pool.query(
      "delete from public.reward_redemption_events where redemption_id in (select id from public.reward_redemptions where user_id=$1)",
      [userId]
    );
    await pool.query(
      "delete from public.reward_redemption_items where redemption_id in (select id from public.reward_redemptions where user_id=$1)",
      [userId]
    );
    await pool.query("delete from public.reward_redemptions where user_id=$1", [userId]);
  }
});

/* ─────────────── Teste financeiro critico: compensado nunca conta ─────────────── */

test("compensado NUNCA soma em NSCreditos resgatados", skipOpts, async () => {
  await insertRedemption({ userId: userA, status: "confirmed", creditsAmount: 30000, trayOrderId: "25626" });
  await insertRedemption({ userId: userB, status: "compensated", creditsAmount: 30000 });

  const m = await getRedemptionReportMetrics(scope(), deps);

  assert.equal(m.total_attempts, 2);
  assert.equal(m.confirmed_redemptions, 1);
  assert.equal(m.credits_redeemed, 30000, "60000 aqui significaria contar a tentativa compensada");
  assert.equal(m.compensated, 1);
  assert.equal(m.credits_compensated, 30000);
});

test("clientes que resgataram conta usuario distinto, nao tentativa", skipOpts, async () => {
  await insertRedemption({ userId: userA, status: "confirmed", creditsAmount: 10000, trayOrderId: "1" });
  await insertRedemption({ userId: userA, status: "confirmed", creditsAmount: 20000, trayOrderId: "2" });

  const m = await getRedemptionReportMetrics(scope(), deps);

  assert.equal(m.confirmed_redemptions, 2);
  assert.equal(m.unique_customers, 1);
  assert.equal(m.credits_redeemed, 30000);
});

test("tentativa falha nao vira cliente que resgatou", skipOpts, async () => {
  await insertRedemption({ userId: userA, status: "confirmed", creditsAmount: 10000, trayOrderId: "1" });
  await insertRedemption({ userId: userB, status: "failed", creditsAmount: 10000 });

  const m = await getRedemptionReportMetrics(scope(), deps);
  assert.equal(m.unique_customers, 1);
  assert.equal(m.failed, 1);
  assert.equal(m.credits_redeemed, 10000);
});

test("pedidos Tray criados sao contados pela coluna factual tray_order_id", skipOpts, async () => {
  await insertRedemption({ userId: userA, status: "confirmed", creditsAmount: 30000, trayOrderId: "25626" });
  await insertRedemption({ userId: userB, status: "compensated", creditsAmount: 30000 });

  const m = await getRedemptionReportMetrics(scope(), deps);
  assert.equal(m.tray_orders_created, 1);
});

test("reconciliation_required entra na metrica propria e fora dos creditos resgatados", skipOpts, async () => {
  await insertRedemption({ userId: userA, status: "reconciliation_required", creditsAmount: 45000 });

  const m = await getRedemptionReportMetrics(scope(), deps);
  assert.equal(m.reconciliation_required, 1);
  assert.equal(m.confirmed_redemptions, 0);
  assert.equal(m.credits_redeemed, 0, "credito preso em conciliacao nao e credito resgatado");
  assert.equal(m.by_status.reconciliation_required, 1);
});

test("bloqueios ficam agrupados em blocked, nunca em confirmados", skipOpts, async () => {
  await insertRedemption({ userId: userA, status: "blocked_tray_profile_incomplete", creditsAmount: 1000 });
  await insertRedemption({ userId: userB, status: "blocked_tray_customer_ambiguous", creditsAmount: 2000 });

  const m = await getRedemptionReportMetrics(scope(), deps);
  assert.equal(m.blocked, 2);
  assert.equal(m.confirmed_redemptions, 0);
  assert.equal(m.credits_redeemed, 0);
});

test("sem nenhum resgate o relatorio devolve zeros e lista vazia", skipOpts, async () => {
  const out = await getRedemptionReport(scope(), deps);
  assert.equal(out.redemptions.total_attempts, 0);
  assert.equal(out.redemptions.credits_redeemed, 0);
  assert.equal(out.redemptions.unique_customers, 0);
  assert.equal(out.recent.length, 0);
});

test("periodo recorta o relatorio pelo created_at real", skipOpts, async () => {
  await insertRedemption({ userId: userA, status: "confirmed", creditsAmount: 10000, trayOrderId: "1", createdAt: "2026-01-10T12:00:00Z" });
  await insertRedemption({ userId: userA, status: "confirmed", creditsAmount: 20000, trayOrderId: "2", createdAt: "2026-03-10T12:00:00Z" });

  const jan = await getRedemptionReportMetrics(scope({ from: "2026-01-01", to: "2026-01-31" }), deps);
  assert.equal(jan.confirmed_redemptions, 1);
  assert.equal(jan.credits_redeemed, 10000);
});

/* ─────────────────────────── Listagem ─────────────────────────── */

test("listagem pagina no banco e traz contagem de itens correta", skipOpts, async () => {
  const ids = [];
  for (let i = 0; i < 5; i += 1) {
    // eslint-disable-next-line no-await-in-loop
    ids.push(await insertRedemption({ userId: userA, status: "confirmed", creditsAmount: 1000 * (i + 1), trayOrderId: String(i) }));
  }
  await pool.query(
    `insert into public.reward_redemption_items
       (redemption_id, tray_product_id, product_name_snapshot, quantity, nscredits_unit_price_snapshot, nscredits_total_snapshot)
     values ($1,'900010','Kit Relogio',1,1000,1000), ($1,'900011','Outro',2,500,1000)`,
    [ids[0]]
  );

  const p1 = await listAdminRedemptions(scope({ page: 1, limit: 2 }), deps);
  assert.equal(p1.items.length, 2);
  assert.deepEqual(p1.paging, { page: 1, limit: 2, total: 5, total_pages: 3 });

  const p3 = await listAdminRedemptions(scope({ page: 3, limit: 2 }), deps);
  assert.equal(p3.items.length, 1);

  const withItems = await listAdminRedemptions(scope({ page: 1, limit: 50 }), deps);
  const target = withItems.items.find((r) => r.id === ids[0]);
  assert.equal(target.item_count, 2);
  assert.equal(withItems.items.filter((r) => r.item_count === 0).length, 4);
});

test("filtro de status devolve exatamente os status pedidos", skipOpts, async () => {
  await insertRedemption({ userId: userA, status: "confirmed", creditsAmount: 1000, trayOrderId: "1" });
  await insertRedemption({ userId: userA, status: "compensated", creditsAmount: 1000 });
  await insertRedemption({ userId: userB, status: "reconciliation_required", creditsAmount: 1000 });

  const out = await listAdminRedemptions(scope({ status: "compensated,reconciliation_required" }), deps);
  assert.equal(out.items.length, 2);
  assert.deepEqual(new Set(out.items.map((i) => i.status)), new Set(["compensated", "reconciliation_required"]));
});

test("busca encontra por id do resgate, pedido Tray, id do usuario e e-mail", skipOpts, async () => {
  const id = await insertRedemption({ userId: userA, status: "confirmed", creditsAmount: 1000, trayOrderId: "998877" });

  const byRedemption = await listAdminRedemptions({ q: id }, deps);
  assert.equal(byRedemption.items.length, 1);
  assert.equal(byRedemption.items[0].id, id);

  const byTrayOrder = await listAdminRedemptions({ q: "998877" }, deps);
  assert.ok(byTrayOrder.items.some((i) => i.id === id));

  const byUserId = await listAdminRedemptions({ q: String(userA) }, deps);
  assert.ok(byUserId.items.some((i) => i.id === id));

  const byEmail = await listAdminRedemptions({ q: `admin-report-a-${stamp}@exemplo.local` }, deps);
  assert.ok(byEmail.items.some((i) => i.id === id));
});

/* ─────────────────────────── Detalhe ─────────────────────────── */

test("detalhe traz o endereco do snapshot, itens, timeline e ledger reais", skipOpts, async () => {
  const id = await insertRedemption({
    userId: userA,
    status: "compensated",
    creditsAmount: 30000,
    addressSnapshot: {
      recipient_name: "Joao Pedro",
      zipcode: "01304001",
      street: "Rua Augusta",
      number: "123",
      complement: "ap 4",
      neighborhood: "Consolacao",
      city: "Sao Paulo",
      state: "SP",
      country: "BR",
    },
  });

  await pool.query(
    `insert into public.reward_redemption_items
       (redemption_id, tray_product_id, tray_variant_id, product_name_snapshot, variant_name_snapshot, image_url_snapshot, quantity, nscredits_unit_price_snapshot, nscredits_total_snapshot)
     values ($1,'900010','77','Kit Relogio','Azul','https://cdn/1.jpg',2,15000,30000)`,
    [id]
  );
  await pool.query(
    `insert into public.reward_redemption_events (redemption_id, from_status, to_status, reason, meta)
     values ($1,null,'processing',null,'{}'::jsonb),
            ($1,'processing','credits_reserved',null,'{}'::jsonb),
            ($1,'tray_order_pending','compensated','tray_order_failed','{"http_status":400,"access_token":"segredo"}'::jsonb)`,
    [id]
  );
  await pool.query(
    `insert into public.coupon_balance_history
       (user_id, delta_cents, balance_before_cents, balance_after_cents, event_type, redemption_id, meta, event_occurred_at)
     values ($1,-3000000,5000000,2000000,'REDEMPTION_DEBIT',$2,'{}'::jsonb, now()),
            ($1, 3000000,2000000,5000000,'REDEMPTION_COMPENSATION',$2,'{}'::jsonb, now())`,
    [userA, id]
  );

  const out = await getAdminRedemptionDetail(id, deps);

  assert.equal(out.redemption.status, "compensated");
  assert.equal(out.user.id, userA);
  assert.equal(out.items.length, 1);
  assert.equal(out.items[0].variant_name, "Azul");
  assert.equal(out.address.street, "Rua Augusta");
  assert.equal(out.address.complement, "ap 4");
  assert.deepEqual(out.events.map((e) => e.to_status), ["processing", "credits_reserved", "compensated"]);
  assert.ok(!JSON.stringify(out.events).includes("segredo"));
  assert.equal(out.ledger.debit_cents, 3000000);
  assert.equal(out.ledger.compensation_cents, 3000000);
  assert.equal(out.ledger.matches_expectation, true);
});
