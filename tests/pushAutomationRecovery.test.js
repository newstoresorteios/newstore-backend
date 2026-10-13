import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";

function run(scenario) {
  const child = spawnSync(process.execPath, ["--experimental-vm-modules", fileURLToPath(new URL("./helpers/pushAutomationHarness.mjs", import.meta.url)), scenario], { encoding: "utf8" });
  assert.equal(child.status, 0, child.stderr);
  return JSON.parse(child.stdout);
}

test("rejeição de campos não consome a chave de entrega válida", () => {
  const r = run("rejected");
  assert.equal(r.first.reason, "safety_missing_occurred_at");
  assert.equal(r.ledgerSize, 0);
  assert.equal(r.second.status, "sent");
  assert.deepEqual(r.deliveries, ["S1", "S2"]);
});
test("preview não bloqueia envio de produção", () => {
  const r = run("preview");
  assert.equal(r.second.status, "sent");
  assert.equal(r.deliveries.length, 2);
});
test("falha parcial recupera somente o dispositivo que não recebeu", () => {
  const r = run("partial");
  assert.equal(r.first.status, "partial_failure");
  assert.equal(r.second.status, "sent");
  assert.deepEqual(r.deliveries, ["S1", "S2", "S2"]);
});
test("evento legado skipped pode ser recuperado dentro das proteções atuais", () => {
  const r = run("skipped");
  assert.equal(r.response.status, "sent");
  assert.equal(r.deliveries.length, 2);
});
test("processamento abandonado pode ser retomado", () => {
  const r = run("stale");
  assert.equal(r.response.status, "sent");
  assert.equal(r.deliveries.length, 2);
});
test("processamento recente não é tomado por outra execução", () => {
  const r = run("fresh");
  assert.equal(r.deliveries.length, 0);
  assert.equal(r.response.deduped, true);
});
test("reexecução depois de sucesso não duplica dispositivos", () => {
  const r = run("twice");
  assert.equal(r.response.deduped, true);
  assert.deepEqual(r.deliveries, ["S1", "S2"]);
});
test("registro antigo sent com falhas recupera apenas o dispositivo pendente", () => {
  const r = run("old-partial");
  assert.equal(r.response.status, "sent");
  assert.deepEqual(r.deliveries, ["S2"]);
});
test("worker que perdeu a posse interrompe os demais envios", () => {
  const r = run("lost-lease");
  assert.equal(r.error, "push_event_lease_lost");
  assert.deepEqual(r.deliveries, ["S1"]);
});
test("crash após último dispositivo conclui o evento na recuperação sem reenviar", () => {
  const r = run("all-already-sent");
  assert.equal(r.first.status, "sent");
  assert.equal(r.second.deduped, true);
  assert.deepEqual(r.deliveries, []);
});
test("falha WhatsApp não fica escondida por sucesso do push", () => {
  const r = run("whatsapp-failure");
  assert.equal(r.first.status, "partial_failure");
  assert.equal(r.first.failed, 1);
  assert.equal(r.second.status, "sent");
  assert.deepEqual(r.deliveries, ["S1", "S2"]);
});
test("heartbeat mantém posse durante envio longo e impede worker concorrente", () => {
  const r = run("long-send");
  assert.equal(r.competing.deduped, true);
  assert.deepEqual(r.deliveries, ["S1", "S2"]);
  assert.equal(r.timerCleared, true);
});
test("WhatsApp real recupera destinatário falho e deduplica somente após aceite", () => {
  const child = spawnSync(process.execPath, ["--experimental-vm-modules", fileURLToPath(new URL("./helpers/whatsappRecoveryHarness.mjs", import.meta.url))], { encoding: "utf8" });
  assert.equal(child.status, 0, child.stderr);
  const r = JSON.parse(child.stdout);
  assert.equal(r.first.failed, 1);
  assert.equal(r.second.accepted, 1);
  assert.equal(r.third.deduped, 1);
  assert.equal(r.attempts, 2);
});
