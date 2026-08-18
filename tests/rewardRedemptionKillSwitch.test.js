// tests/rewardRedemptionKillSwitch.test.js
// Logica pura do kill-switch de producao (item 24/25) — sem banco.
import test from "node:test";
import assert from "node:assert/strict";

import { isRewardRedemptionEnabled } from "../src/services/rewardRedemption.js";

test("ausente = desligado (default seguro)", () => {
  assert.equal(isRewardRedemptionEnabled({}), false);
});

test("'true' liga", () => {
  assert.equal(isRewardRedemptionEnabled({ REWARD_REDEMPTION_ENABLED: "true" }), true);
});

test("'TRUE'/'True' tambem liga (case-insensitive)", () => {
  assert.equal(isRewardRedemptionEnabled({ REWARD_REDEMPTION_ENABLED: "TRUE" }), true);
  assert.equal(isRewardRedemptionEnabled({ REWARD_REDEMPTION_ENABLED: "True" }), true);
});

test("qualquer outro valor mantem desligado", () => {
  for (const v of ["false", "1", "yes", "on", " ", "true ", "", "verdadeiro"]) {
    if (v === "true ") continue; // trim cobre espaco em volta, testado separado
    assert.equal(isRewardRedemptionEnabled({ REWARD_REDEMPTION_ENABLED: v }), false, `valor "${v}" deveria manter desligado`);
  }
});

test("espacos em volta de 'true' ainda contam como ligado (trim)", () => {
  assert.equal(isRewardRedemptionEnabled({ REWARD_REDEMPTION_ENABLED: "  true  " }), true);
});

test("sem argumento usa process.env real", () => {
  const original = process.env.REWARD_REDEMPTION_ENABLED;
  try {
    delete process.env.REWARD_REDEMPTION_ENABLED;
    assert.equal(isRewardRedemptionEnabled(), false);
    process.env.REWARD_REDEMPTION_ENABLED = "true";
    assert.equal(isRewardRedemptionEnabled(), true);
  } finally {
    if (original === undefined) delete process.env.REWARD_REDEMPTION_ENABLED;
    else process.env.REWARD_REDEMPTION_ENABLED = original;
  }
});
