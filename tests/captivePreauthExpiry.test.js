import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

import {
  autoAuthorizeAndChargeExpiredPendingCaptivePreauths,
  isCaptivePreauthAutoApproveOnExpiryEnabled,
  processPendingCaptivePreauthExpirations,
} from "../src/services/autopay/captivePreauthService.js";

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

test("flag nova tem default false e preserva o expirador legado", async () => {
  const previous = process.env.CAPTIVE_PREAUTH_AUTO_APPROVE_ON_EXPIRY_ENABLED;
  delete process.env.CAPTIVE_PREAUTH_AUTO_APPROVE_ON_EXPIRY_ENABLED;
  try {
    assert.equal(isCaptivePreauthAutoApproveOnExpiryEnabled(), false);
    let legacyCalls = 0;
    let autoCalls = 0;
    const result = await processPendingCaptivePreauthExpirations({
      autoApproveEnabled: false,
      legacyExpirer: async () => {
        legacyCalls += 1;
        return { expired_count: 1, released_reservations: 1 };
      },
      autoProcessor: async () => {
        autoCalls += 1;
      },
    });
    assert.equal(legacyCalls, 1);
    assert.equal(autoCalls, 0);
    assert.deepEqual(result, { expired_count: 1, released_reservations: 1 });
  } finally {
    if (previous == null) delete process.env.CAPTIVE_PREAUTH_AUTO_APPROVE_ON_EXPIRY_ENABLED;
    else process.env.CAPTIVE_PREAUTH_AUTO_APPROVE_ON_EXPIRY_ENABLED = previous;
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

test("selecao, lock, transicao e idempotencia permanecem explicitos no codigo", async () => {
  const serviceSource = await readFile(
    new URL("../src/services/autopay/captivePreauthService.js", import.meta.url),
    "utf8"
  );
  const runnerSource = await readFile(
    new URL("../src/services/autopayRunner.js", import.meta.url),
    "utf8"
  );
  assert.match(serviceSource, /status = 'pending'[\s\S]*expires_at IS NOT NULL[\s\S]*expires_at <= now\(\)[\s\S]*FOR UPDATE SKIP LOCKED/);
  assert.match(serviceSource, /WHERE id = ANY\(\$1::uuid\[\]\)[\s\S]*AND status = 'pending'/);
  assert.match(serviceSource, /draw_not_open_at_auto_approval/);
  assert.match(serviceSource, /payment_result_unknown/);
  assert.match(runnerSource, /captive-preauth-expiry/);
});
