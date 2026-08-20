// tests/rewardRedemptionFailureMeta.test.js
//
// Observabilidade da falha definitiva da Tray (M7): o corpo do erro precisa
// chegar em reward_redemption_events.meta para diagnosticar CONTRATO, mas
// nunca pode carregar segredo nem PII.
import test from "node:test";
import assert from "node:assert/strict";

import { sanitizeTrayErrorBody, buildTrayFailureMeta } from "../src/services/rewardRedemption.js";

test("preserva o campo rejeitado e a mensagem de validacao da Tray", () => {
  const body = { code: 400, name: "Bad Request", causes: { Order: { payment_form: ["campo obrigatório"] } } };
  const out = sanitizeTrayErrorBody(body);
  assert.equal(out.code, 400);
  assert.equal(out.name, "Bad Request");
  assert.deepEqual(out.causes.Order.payment_form, ["campo obrigatório"]);
});

test("buildTrayFailureMeta monta http_status/tray_error_code/tray_body", () => {
  const e = Object.assign(new Error("tray_request_invalid"), {
    code: "tray_request_invalid",
    status: 400,
    publicDetails: { tray_body: { causes: { Order: { shipment: ["campo obrigatório"] } } } },
  });
  const meta = buildTrayFailureMeta(e);
  assert.equal(meta.http_status, 400);
  assert.equal(meta.tray_error_code, "tray_request_invalid");
  assert.deepEqual(meta.tray_body.causes.Order.shipment, ["campo obrigatório"]);
});

test("nomes de campo sensiveis sobrevivem, valores sensiveis nunca", () => {
  const body = {
    causes: { Customer: { cpf: ["Este campo não pode ser deixado em branco."] } },
    cpf: "10425415902",
    email: "jp@newstore.com",
    phone: "43998640480",
    access_token: "APP_ID-7abc",
    password: "hunter2",
    DATABASE_URL: "postgres://u:p@host/db",
  };
  const out = sanitizeTrayErrorBody(body);
  // a CHAVE rejeitada continua visivel (e o que diagnostica o contrato)
  assert.ok(out.causes.Customer.cpf);
  // os VALORES sensiveis somem
  for (const k of ["cpf", "email", "phone", "access_token", "password", "DATABASE_URL"]) {
    assert.equal(out[k], "[redacted]", `vazou ${k}`);
  }
});

test("valores que parecem CPF/e-mail/token sao redigidos mesmo fora de chave conhecida", () => {
  const out = sanitizeTrayErrorBody({ detalhe: "10425415902", contato: "jp@newstore.com", h: "Bearer abc123" });
  assert.equal(out.detalhe, "[redacted]");
  assert.equal(out.contato, "[redacted]");
  assert.equal(out.h, "[redacted]");
});

test("meta serializado nunca contem segredo/PII", () => {
  const e = Object.assign(new Error("tray_request_invalid"), {
    code: "tray_request_invalid",
    status: 400,
    publicDetails: { tray_body: { cpf: "10425415902", access_token: "APP_ID-7", email: "jp@newstore.com" } },
  });
  const serialized = JSON.stringify(buildTrayFailureMeta(e)).toLowerCase();
  for (const forbidden of ["10425415902", "app_id-7", "jp@newstore.com"]) {
    assert.equal(serialized.includes(forbidden.toLowerCase()), false, `vazou ${forbidden}`);
  }
});

test("profundidade/tamanho limitados: nunca grava blob gigante", () => {
  let deep = { v: "x" };
  for (let i = 0; i < 12; i++) deep = { nested: deep };
  const out = sanitizeTrayErrorBody(deep);
  assert.ok(JSON.stringify(out).includes("[truncated]"));

  const long = sanitizeTrayErrorBody({ msg: "a".repeat(2000) });
  assert.ok(long.msg.length <= 501);
});

test("erro sem publicDetails nao inventa meta", () => {
  assert.equal(buildTrayFailureMeta(null), null);
  assert.equal(buildTrayFailureMeta(Object.assign(new Error("x"), {})), null);
});
