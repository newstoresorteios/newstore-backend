import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

import {
  autoAuthorizeAndChargeExpiredPendingCaptivePreauths,
  isCaptivePreauthAutoApproveOnExpiryEnabled,
  isCaptivePreauthChargeOnAuthorizeEnabled,
  processPendingCaptivePreauthExpirations,
  shouldRequireCaptivePreauth,
} from "../src/services/autopay/captivePreauthService.js";
import {
  cleanupExpiredReservationsGlobal,
  expireReservationForNumbersCleanup,
  pendingCaptivePreauthReservationGuardSql,
  resolveCaptivePreauthAutoApproveEffectiveFrom,
} from "../src/services/reservationExpiry.js";

const TEST_EFFECTIVE_FROM = "2026-07-01T00:00:00.000Z";
process.env.CAPTIVE_PREAUTH_AUTO_APPROVE_EFFECTIVE_FROM = TEST_EFFECTIVE_FROM;

const IDS = {
  a: "00000000-0000-4000-8000-000000000001",
  b: "00000000-0000-4000-8000-000000000002",
  c: "00000000-0000-4000-8000-000000000003",
};

function authorization(id, drawId, userId, captiveNumber, amountCents = 35000) {
  return {
    id,
    draw_id: drawId,
    user_id: userId,
    captive_number: captiveNumber,
    amount_cents: amountCents,
    status: "authorized",
    autopay_number_id: id,
    created_at: "2026-07-02T00:00:00.000Z",
  };
}

function group(authorizations) {
  return {
    draw_id: authorizations[0].draw_id,
    user_id: authorizations[0].user_id,
    authorizations,
  };
}

function prepared(groups = [], extra = {}) {
  const authorized = groups.reduce((total, item) => total + item.authorizations.length, 0);
  return {
    checked: authorized,
    eligible: authorized,
    groups,
    authorized,
    charged: 0,
    failed: 0,
    expired: 0,
    skipped: 0,
    released_reservations: 0,
    draw_ids: [...new Set(groups.map((item) => item.draw_id))],
    ...extra,
  };
}

function cleanupHarness({ authorizations, reservations, numbers, effectiveFrom = TEST_EFFECTIVE_FROM }) {
  const queries = [];
  const blockingStatuses = new Set(["active", "pending", "reserved", ""]);
  const cutoff = resolveCaptivePreauthAutoApproveEffectiveFrom(effectiveFrom);
  const matchingPending = (reservation) => authorizations.some((item) =>
    cutoff.ok &&
    item.status === "pending" &&
    new Date(item.created_at).getTime() >= cutoff.timestamp &&
    Number(item.draw_id) === Number(reservation.draw_id) &&
    Number(item.user_id) === Number(reservation.user_id) &&
    reservation.numbers.map(Number).includes(Number(item.captive_number))
  );
  const expireReservation = (reservation) => {
    const expired = new Date(reservation.expires_at).getTime() < Date.now();
    if (!expired || !blockingStatuses.has(String(reservation.status || "").toLowerCase())) return false;
    if (matchingPending(reservation)) return false;
    reservation.status = "expired";
    return true;
  };
  const runQuery = async (sql, params = []) => {
    queries.push(sql);
    const normalized = sql.replace(/\s+/g, " ").trim().toLowerCase();
    if (normalized.startsWith("update public.reservations reservation") && params.length) {
      const reservation = reservations.find((item) => item.id === params[0]);
      const changed = reservation ? expireReservation(reservation) : false;
      return { rowCount: changed ? 1 : 0, rows: changed ? [{ id: reservation.id }] : [] };
    }
    if (normalized.startsWith("update reservations reservation")) {
      const changed = reservations.filter(expireReservation);
      return { rowCount: changed.length, rows: changed.map((item) => ({ id: item.id })) };
    }
    if (normalized.startsWith("update numbers n")) {
      let changed = 0;
      for (const number of numbers) {
        const reservation = reservations.find((item) => item.id === number.reservation_id);
        const active = reservation && blockingStatuses.has(String(reservation.status || "").toLowerCase());
        if (number.status === "reserved" && !active && !(reservation && matchingPending(reservation))) {
          number.status = "available";
          number.reservation_id = null;
          changed += 1;
        }
      }
      return { rowCount: changed, rows: [] };
    }
    throw new Error(`unexpected cleanup query: ${normalized}`);
  };
  return { runQuery, queries };
}

test("flags oficiais têm default ativo e o modo legado exige false explícito", async () => {
  const previousAutoApprove = process.env.CAPTIVE_PREAUTH_AUTO_APPROVE_ON_EXPIRY_ENABLED;
  const previousCharge = process.env.CAPTIVE_PREAUTH_CHARGE_ON_AUTHORIZE_ENABLED;
  delete process.env.CAPTIVE_PREAUTH_AUTO_APPROVE_ON_EXPIRY_ENABLED;
  delete process.env.CAPTIVE_PREAUTH_CHARGE_ON_AUTHORIZE_ENABLED;
  try {
    assert.equal(isCaptivePreauthAutoApproveOnExpiryEnabled(), true);
    assert.equal(isCaptivePreauthChargeOnAuthorizeEnabled(), true);
    let autoCalls = 0;
    await processPendingCaptivePreauthExpirations({
      autoProcessor: async () => {
        autoCalls += 1;
        return { charged: 0 };
      },
    });
    assert.equal(autoCalls, 1);

    let legacyCalls = 0;
    const result = await processPendingCaptivePreauthExpirations({
      autoApproveEnabled: false,
      legacyExpirer: async () => {
        legacyCalls += 1;
        return { expired_count: 1, released_reservations: 1 };
      },
    });
    assert.equal(legacyCalls, 1);
    assert.deepEqual(result, { expired_count: 1, released_reservations: 1 });
  } finally {
    if (previousAutoApprove == null) delete process.env.CAPTIVE_PREAUTH_AUTO_APPROVE_ON_EXPIRY_ENABLED;
    else process.env.CAPTIVE_PREAUTH_AUTO_APPROVE_ON_EXPIRY_ENABLED = previousAutoApprove;
    if (previousCharge == null) delete process.env.CAPTIVE_PREAUTH_CHARGE_ON_AUTHORIZE_ENABLED;
    else process.env.CAPTIVE_PREAUTH_CHARGE_ON_AUTHORIZE_ENABLED = previousCharge;
  }
});

test("sorteio de até R$ 55 continua fora da nova preauth", () => {
  assert.equal(shouldRequireCaptivePreauth({
    currentAmountCents: 5500,
    authorizedBaseAmountCents: 5500,
  }), false);
  assert.equal(shouldRequireCaptivePreauth({
    currentAmountCents: 5499,
    authorizedBaseAmountCents: 5500,
  }), false);
  assert.equal(shouldRequireCaptivePreauth({
    currentAmountCents: 5501,
    authorizedBaseAmountCents: 5500,
  }), true);
});

test("pending antes de 12h não é cobrado e mantém a reserva", async () => {
  const state = {
    authorization: { ...authorization(IDS.a, 10, 20, 33), status: "pending", expires_at: Date.now() + 60_000 },
    reservation: { status: "pending" },
  };
  let chargeCalls = 0;
  const summary = await autoAuthorizeAndChargeExpiredPendingCaptivePreauths({
    chargeEnabled: true,
    prepareGroups: async () => (
      state.authorization.status === "pending" && state.authorization.expires_at <= Date.now()
        ? prepared([group([state.authorization])])
        : prepared([])
    ),
    chargeGroup: async () => {
      chargeCalls += 1;
    },
    finalizeGroup: async () => ({ outcome: "charged", count: 1 }),
  });
  assert.equal(chargeCalls, 0);
  assert.equal(summary.authorized, 0);
  assert.equal(state.authorization.status, "pending");
  assert.equal(state.reservation.status, "pending");
});

test("cliente recusado antes de 12h nunca é autoaprovado", async () => {
  const declined = { ...authorization(IDS.a, 10, 20, 33), status: "declined", expires_at: Date.now() - 1 };
  let chargeCalls = 0;
  const summary = await autoAuthorizeAndChargeExpiredPendingCaptivePreauths({
    chargeEnabled: true,
    prepareGroups: async () => prepared(declined.status === "pending" ? [group([declined])] : []),
    chargeGroup: async () => {
      chargeCalls += 1;
    },
    finalizeGroup: async () => ({ outcome: "charged", count: 1 }),
  });
  assert.equal(chargeCalls, 0);
  assert.equal(summary.authorized, 0);
  assert.equal(declined.status, "declined");
});

test("autoaprovação não muda pending para authorized se a cobrança está desabilitada", async () => {
  let prepareCalls = 0;
  const summary = await autoAuthorizeAndChargeExpiredPendingCaptivePreauths({
    chargeEnabled: false,
    prepareGroups: async () => {
      prepareCalls += 1;
      return prepared([group([authorization(IDS.a, 10, 20, 33)])]);
    },
  });
  assert.equal(prepareCalls, 0);
  assert.equal(summary.disabled, true);
  assert.equal(summary.authorized, 0);
});

test("cutoff exclui pending criado antes do effectiveFrom sem cobrança ou payment", async () => {
  const oldPending = {
    ...authorization(IDS.a, 10, 20, 33),
    status: "pending",
    created_at: "2026-06-30T23:59:59.999Z",
    expires_at: new Date(Date.now() - 60_000).toISOString(),
  };
  const payments = [];
  let chargeCalls = 0;
  const summary = await autoAuthorizeAndChargeExpiredPendingCaptivePreauths({
    chargeEnabled: true,
    effectiveFrom: TEST_EFFECTIVE_FROM,
    prepareGroups: async ({ effectiveFrom }) => {
      const cutoff = resolveCaptivePreauthAutoApproveEffectiveFrom(effectiveFrom);
      const eligible =
        oldPending.status === "pending" &&
        new Date(oldPending.expires_at).getTime() <= Date.now() &&
        new Date(oldPending.created_at).getTime() >= cutoff.timestamp;
      return prepared(eligible ? [group([oldPending])] : []);
    },
    chargeGroup: async () => {
      chargeCalls += 1;
      payments.push({ authorization_id: oldPending.id });
    },
    finalizeGroup: async () => ({ outcome: "charged", count: 1 }),
  });
  assert.equal(chargeCalls, 0);
  assert.equal(payments.length, 0);
  assert.equal(summary.authorized, 0);
  assert.equal(oldPending.status, "pending");
});

test("cutoff inclui nova pending vencida e conclui cobrança via mock", async () => {
  const newPending = {
    ...authorization(IDS.a, 10, 20, 33),
    status: "pending",
    created_at: TEST_EFFECTIVE_FROM,
    expires_at: new Date(Date.now() - 60_000).toISOString(),
  };
  const payments = [];
  let chargeCalls = 0;
  const summary = await autoAuthorizeAndChargeExpiredPendingCaptivePreauths({
    chargeEnabled: true,
    effectiveFrom: TEST_EFFECTIVE_FROM,
    prepareGroups: async ({ effectiveFrom }) => {
      const cutoff = resolveCaptivePreauthAutoApproveEffectiveFrom(effectiveFrom);
      const eligible =
        newPending.status === "pending" &&
        new Date(newPending.expires_at).getTime() <= Date.now() &&
        new Date(newPending.created_at).getTime() >= cutoff.timestamp;
      if (!eligible) return prepared([]);
      newPending.status = "authorized";
      return prepared([group([newPending])]);
    },
    chargeGroup: async () => {
      chargeCalls += 1;
      newPending.status = "charged";
      payments.push({ id: "payment-cutoff", authorization_id: newPending.id });
      return { ok: true, charged: true, status: "charged", code: "charged_success" };
    },
    finalizeGroup: async () => ({ outcome: "charged", count: 1, released_reservations: 0 }),
  });
  assert.equal(chargeCalls, 1);
  assert.equal(payments.length, 1);
  assert.equal(summary.authorized, 1);
  assert.equal(summary.charged, 1);
  assert.equal(newPending.status, "charged");
});

test("effectiveFrom ausente ou inválido desativa autoaprovação e não chama seleção", async () => {
  const previous = process.env.CAPTIVE_PREAUTH_AUTO_APPROVE_EFFECTIVE_FROM;
  try {
    for (const invalidValue of [
      null,
      "not-an-iso-timestamp",
      "2026-02-30T00:00:00-03:00",
    ]) {
      if (invalidValue == null) delete process.env.CAPTIVE_PREAUTH_AUTO_APPROVE_EFFECTIVE_FROM;
      else process.env.CAPTIVE_PREAUTH_AUTO_APPROVE_EFFECTIVE_FROM = invalidValue;
      let prepareCalls = 0;
      let chargeCalls = 0;
      const summary = await autoAuthorizeAndChargeExpiredPendingCaptivePreauths({
        chargeEnabled: true,
        prepareGroups: async () => {
          prepareCalls += 1;
          return prepared([group([authorization(IDS.a, 10, 20, 33)])]);
        },
        chargeGroup: async () => {
          chargeCalls += 1;
        },
      });
      assert.equal(summary.disabled, true);
      assert.match(summary.reason, /auto_approve_effective_from_(missing|invalid)/);
      assert.equal(prepareCalls, 0);
      assert.equal(chargeCalls, 0);
      assert.equal(pendingCaptivePreauthReservationGuardSql("reservation"), "TRUE");
    }
  } finally {
    process.env.CAPTIVE_PREAUTH_AUTO_APPROVE_EFFECTIVE_FROM = previous || TEST_EFFECTIVE_FROM;
  }
});

test("aprovacao ou recusa explicita anterior nao e selecionada pelo scanner", async () => {
  let chargeCalls = 0;
  const summary = await autoAuthorizeAndChargeExpiredPendingCaptivePreauths({
    chargeEnabled: true,
    prepareGroups: async () => prepared([], { checked: 0, skipped: 0 }),
    chargeGroup: async () => {
      chargeCalls += 1;
    },
    finalizeGroup: async () => ({ outcome: "charged", count: 1 }),
  });
  assert.equal(chargeCalls, 0);
  assert.equal(summary.groups, 0);
  assert.equal(summary.charged, 0);
});

test("sem resposta autoriza e chama o runner uma unica vez", async () => {
  const item = group([authorization(IDS.a, 10, 20, 33)]);
  const calls = [];
  const summary = await autoAuthorizeAndChargeExpiredPendingCaptivePreauths({
    chargeEnabled: true,
    prepareGroups: async () => prepared([item]),
    chargeGroup: async (options) => {
      calls.push(options);
      return { ok: true, charged: true, status: "charged", code: "charged_success" };
    },
    finalizeGroup: async () => ({ outcome: "charged", count: 1, released_reservations: 0 }),
  });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].expiryGroup, true);
  assert.equal(calls[0].authorizationSource, "system");
  assert.equal(calls[0].authorizedByAdminId, null);
  assert.deepEqual(calls[0].expectedAuthorizationIds, [IDS.a]);
  assert.equal(summary.authorized, 1);
  assert.equal(summary.charged, 1);
});

test("dois cativos do mesmo usuario geram uma cobranca agrupada e chave estavel", async () => {
  const item = group([
    authorization(IDS.b, 10, 20, 55),
    authorization(IDS.a, 10, 20, 33),
  ]);
  const calls = [];
  await autoAuthorizeAndChargeExpiredPendingCaptivePreauths({
    chargeEnabled: true,
    prepareGroups: async () => prepared([item]),
    chargeGroup: async (options) => {
      calls.push(options);
      return { ok: true, charged: true, code: "charged_success" };
    },
    finalizeGroup: async (chargedGroup) => {
      assert.equal(chargedGroup.total_amount_cents, 70000);
      assert.equal(
        chargedGroup.idempotency_key,
        `captive-preauth-expiry:10:20:${IDS.a},${IDS.b}`
      );
      return { outcome: "charged", count: 2 };
    },
  });
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].expectedAuthorizationIds, [IDS.a, IDS.b]);
});

test("usuarios diferentes sao cobrados em grupos separados", async () => {
  const groups = [
    group([authorization(IDS.a, 10, 20, 33)]),
    group([authorization(IDS.c, 10, 21, 55)]),
  ];
  const users = [];
  const summary = await autoAuthorizeAndChargeExpiredPendingCaptivePreauths({
    chargeEnabled: true,
    prepareGroups: async () => prepared(groups),
    chargeGroup: async (options) => {
      users.push(options.userId);
      return { ok: true, charged: true, code: "charged_success" };
    },
    finalizeGroup: async () => ({ outcome: "charged", count: 1 }),
  });
  assert.deepEqual(users, [20, 21]);
  assert.equal(summary.groups, 2);
  assert.equal(summary.charged, 2);
});

test("draw fechado e corrida vencida pela recusa nao chamam cobranca", async () => {
  let chargeCalls = 0;
  const summary = await autoAuthorizeAndChargeExpiredPendingCaptivePreauths({
    chargeEnabled: true,
    prepareGroups: async () => prepared([], {
      checked: 2,
      eligible: 0,
      authorized: 0,
      expired: 1,
      skipped: 1,
      released_reservations: 1,
      draw_ids: [10],
    }),
    chargeGroup: async () => {
      chargeCalls += 1;
    },
    finalizeGroup: async () => ({ outcome: "charged", count: 1 }),
  });
  assert.equal(chargeCalls, 0);
  assert.equal(summary.expired, 1);
  assert.equal(summary.skipped, 1);
  assert.equal(summary.released_reservations, 1);
});

test("recusa financeira definitiva marca falha e libera a reserva", async () => {
  const item = group([authorization(IDS.a, 10, 20, 33)]);
  let receivedResult = null;
  const summary = await autoAuthorizeAndChargeExpiredPendingCaptivePreauths({
    chargeEnabled: true,
    prepareGroups: async () => prepared([item]),
    chargeGroup: async () => ({
      ok: false,
      charged: false,
      definitive: true,
      code: "payment_failed",
      provider_bill_id: "bill-test",
    }),
    finalizeGroup: async (_group, result) => {
      receivedResult = result;
      return { outcome: "failed", count: 1, released_reservations: 1 };
    },
  });
  assert.equal(receivedResult.definitive, true);
  assert.equal(summary.failed, 1);
  assert.equal(summary.released_reservations, 1);
});

test("resultado incerto fica para conciliacao e scanner repetido nao cobra novamente", async () => {
  const item = group([authorization(IDS.a, 10, 20, 33)]);
  let prepareCalls = 0;
  let chargeCalls = 0;
  const options = {
    chargeEnabled: true,
    prepareGroups: async () => {
      prepareCalls += 1;
      return prepareCalls === 1 ? prepared([item]) : prepared([]);
    },
    chargeGroup: async () => {
      chargeCalls += 1;
      return {
        ok: false,
        charged: false,
        definitive: false,
        code: "payment_result_unknown",
        provider_bill_id: "bill-unknown",
      };
    },
    finalizeGroup: async (_group, result) => {
      assert.equal(result.definitive, false);
      return { outcome: "unknown", count: 1, released_reservations: 0 };
    },
  };
  const first = await autoAuthorizeAndChargeExpiredPendingCaptivePreauths(options);
  const second = await autoAuthorizeAndChargeExpiredPendingCaptivePreauths(options);
  assert.equal(chargeCalls, 1);
  assert.equal(first.authorized, 1);
  assert.equal(first.failed, 0);
  assert.equal(first.released_reservations, 0);
  assert.equal(second.groups, 0);
});

test("confirmação do cliente preserva o wiring autorização → cobrança", async () => {
  const serviceSource = await readFile(
    new URL("../src/services/autopay/captivePreauthService.js", import.meta.url),
    "utf8"
  );
  assert.match(
    serviceSource,
    /authorizeCaptivePreauthForUser[\s\S]*applyAuthorizationDecision\(row, "authorize", "account"\)[\s\S]*chargeAfterAuthorizationIfEnabled/
  );
  assert.match(
    serviceSource,
    /authorizeCaptivePreauthByCode[\s\S]*applyAuthorizationDecision\(lookup\.row, "authorize", "confirmation_code"\)[\s\S]*chargeAfterAuthorizationIfEnabled/
  );
  assert.match(
    serviceSource,
    /chargeAfterAuthorizationIfEnabled[\s\S]*isCaptivePreauthChargeOnAuthorizeEnabled\(\)[\s\S]*chargeAuthorizedCaptivePreauth/
  );
});

test("race do /api/numbers preserva preauth pending e depois permite cobrança única", async () => {
  const state = {
    authorizations: [{
      ...authorization(IDS.a, 10, 20, 33),
      status: "pending",
      expires_at: new Date(Date.now() - 1).toISOString(),
    }],
    reservations: [{
      id: "10000000-0000-4000-8000-000000000001",
      draw_id: 10,
      user_id: 20,
      numbers: [33],
      status: "pending",
      expires_at: new Date(Date.now() - 1).toISOString(),
    }],
    numbers: [{
      draw_id: 10,
      n: 33,
      status: "reserved",
      reservation_id: "10000000-0000-4000-8000-000000000001",
    }],
    payments: [],
  };
  const harness = cleanupHarness(state);

  const lazyExpiry = await expireReservationForNumbersCleanup(
    state.reservations[0].id,
    harness.runQuery
  );
  assert.equal(lazyExpiry.rowCount, 0);
  assert.equal(state.reservations[0].status, "pending");
  assert.equal(state.numbers[0].status, "reserved");

  let chargeCalls = 0;
  const summary = await autoAuthorizeAndChargeExpiredPendingCaptivePreauths({
    chargeEnabled: true,
    prepareGroups: async () => {
      state.reservations[0].expires_at = new Date(Date.now() + 10 * 60_000).toISOString();
      state.authorizations[0].status = "authorized";
      return prepared([group(state.authorizations)]);
    },
    chargeGroup: async () => {
      chargeCalls += 1;
      state.authorizations[0].status = "charged";
      state.reservations[0].status = "paid";
      state.numbers[0].status = "sold";
      state.payments.push({ id: "payment-1", authorization_id: IDS.a });
      return { ok: true, charged: true, status: "charged", code: "charged_success" };
    },
    finalizeGroup: async () => ({ outcome: "charged", count: 1, released_reservations: 0 }),
  });
  assert.equal(chargeCalls, 1);
  assert.equal(state.payments.length, 1);
  assert.equal(summary.charged, 1);
  assert.equal(state.authorizations[0].status, "charged");
  assert.equal(state.reservations[0].status, "paid");
  assert.equal(state.numbers[0].status, "sold");
  assert.match(harness.queries[0], /status = 'pending'/);
  assert.match(harness.queries[0], /captive_number = ANY\(reservation\.numbers\)/);
});

test("cleanup global expira reserva comum e protege a vinculada a preauth pending", async () => {
  const state = {
    authorizations: [{
      ...authorization(IDS.a, 10, 20, 33),
      status: "pending",
    }],
    reservations: [
      {
        id: "10000000-0000-4000-8000-000000000001",
        draw_id: 10,
        user_id: 20,
        numbers: [33],
        status: "pending",
        expires_at: new Date(Date.now() - 60_000).toISOString(),
      },
      {
        id: "10000000-0000-4000-8000-000000000002",
        draw_id: 10,
        user_id: 21,
        numbers: [44],
        status: "active",
        expires_at: new Date(Date.now() - 60_000).toISOString(),
      },
    ],
    numbers: [
      {
        draw_id: 10,
        n: 33,
        status: "reserved",
        reservation_id: "10000000-0000-4000-8000-000000000001",
      },
      {
        draw_id: 10,
        n: 44,
        status: "reserved",
        reservation_id: "10000000-0000-4000-8000-000000000002",
      },
    ],
  };
  const harness = cleanupHarness(state);
  await cleanupExpiredReservationsGlobal(harness.runQuery);

  assert.equal(state.reservations[0].status, "pending");
  assert.equal(state.numbers[0].status, "reserved");
  assert.equal(state.reservations[1].status, "expired");
  assert.equal(state.numbers[1].status, "available");
  assert.equal(harness.queries.length, 2);
  for (const sql of harness.queries) {
    assert.match(sql, /pending_captive_preauth\.status = 'pending'/);
  }
});

test("cleanup não eterniza reserva de preauth pending criada antes do cutoff", async () => {
  const state = {
    effectiveFrom: TEST_EFFECTIVE_FROM,
    authorizations: [{
      ...authorization(IDS.a, 10, 20, 33),
      status: "pending",
      created_at: "2026-06-30T23:59:59.999Z",
    }],
    reservations: [{
      id: "10000000-0000-4000-8000-000000000003",
      draw_id: 10,
      user_id: 20,
      numbers: [33],
      status: "pending",
      expires_at: new Date(Date.now() - 60_000).toISOString(),
    }],
    numbers: [{
      draw_id: 10,
      n: 33,
      status: "reserved",
      reservation_id: "10000000-0000-4000-8000-000000000003",
    }],
  };
  const harness = cleanupHarness(state);
  await cleanupExpiredReservationsGlobal(harness.runQuery, TEST_EFFECTIVE_FROM);

  assert.equal(state.authorizations[0].status, "pending");
  assert.equal(state.reservations[0].status, "expired");
  assert.equal(state.numbers[0].status, "available");
  assert.match(harness.queries[0], /created_at >= '2026-07-01T00:00:00\.000Z'::timestamptz/);
});

test("autorização antiga expired permanece untouched e não cria payment", async () => {
  const oldAuthorization = {
    ...authorization(IDS.a, 10, 20, 33),
    status: "expired",
    expires_at: new Date(Date.now() - 24 * 60 * 60_000).toISOString(),
  };
  const payments = [];
  let chargeCalls = 0;
  const summary = await autoAuthorizeAndChargeExpiredPendingCaptivePreauths({
    chargeEnabled: true,
    prepareGroups: async () => prepared(
      oldAuthorization.status === "pending" ? [group([oldAuthorization])] : []
    ),
    chargeGroup: async () => {
      chargeCalls += 1;
      payments.push({ authorization_id: oldAuthorization.id });
    },
    finalizeGroup: async () => ({ outcome: "charged", count: 1 }),
  });
  assert.equal(chargeCalls, 0);
  assert.equal(payments.length, 0);
  assert.equal(summary.authorized, 0);
  assert.equal(oldAuthorization.status, "expired");
});

test("duas execuções concorrentes lógicas geram somente uma cobrança e um payment", async () => {
  const state = {
    authorization: {
      ...authorization(IDS.a, 10, 20, 33),
      status: "pending",
      expires_at: new Date(Date.now() - 1).toISOString(),
    },
    payments: new Map(),
  };
  let chargeCalls = 0;
  const options = {
    chargeEnabled: true,
    prepareGroups: async () => {
      if (state.authorization.status !== "pending") return prepared([]);
      state.authorization.status = "authorized";
      return prepared([group([state.authorization])]);
    },
    chargeGroup: async () => {
      chargeCalls += 1;
      state.authorization.status = "charged";
      state.payments.set("captive-preauth-expiry:10:20:1", {
        authorization_id: state.authorization.id,
      });
      return { ok: true, charged: true, status: "charged", code: "charged_success" };
    },
    finalizeGroup: async () => ({ outcome: "charged", count: 1, released_reservations: 0 }),
  };
  const [first, second] = await Promise.all([
    autoAuthorizeAndChargeExpiredPendingCaptivePreauths(options),
    autoAuthorizeAndChargeExpiredPendingCaptivePreauths(options),
  ]);
  assert.equal(chargeCalls, 1);
  assert.equal(state.payments.size, 1);
  assert.equal(first.charged + second.charged, 1);
});

test("selecao, lock, transicao e idempotencia permanecem explicitos no codigo", async () => {
  const serviceSource = await readFile(
    new URL("../src/services/autopay/captivePreauthService.js", import.meta.url),
    "utf8"
  );
  const runnerSource = await readFile(
    new URL("../src/services/autopayRunner.js", import.meta.url),
    "utf8"
  );
  const expiryGuardSource = await readFile(
    new URL("../src/services/reservationExpiry.js", import.meta.url),
    "utf8"
  );
  assert.match(serviceSource, /status = 'pending'[\s\S]*expires_at IS NOT NULL[\s\S]*expires_at <= now\(\)[\s\S]*FOR UPDATE SKIP LOCKED/);
  assert.match(serviceSource, /expires_at <= now\(\)[\s\S]*created_at >= \$1::timestamptz[\s\S]*FOR UPDATE SKIP LOCKED/);
  assert.match(serviceSource, /WHERE id = ANY\(\$1::uuid\[\]\)[\s\S]*AND status = 'pending'/);
  assert.match(serviceSource, /AND created_at >= \$2::timestamptz[\s\S]*RETURNING \*/);
  assert.match(serviceSource, /draw_not_open_at_auto_approval/);
  assert.match(serviceSource, /payment_result_unknown/);
  assert.match(runnerSource, /captive-preauth-expiry/);
  assert.match(runnerSource, /pg_advisory_lock\(hashtext\(\$1\)\)/);
  assert.match(runnerSource, /provider_request->>'idempotency_key' = \$3/);
  assert.match(expiryGuardSource, /pending_captive_preauth\.created_at >= \$\{cutoffSql\}/);
  assert.match(expiryGuardSource, /if \(!cutoffSql\) return "TRUE"/);
});
