// tests/rewardRedemptionTracking.test.js
//
// Acompanhamento logistico do CLIENTE: posse, gate de consulta e ausencia
// total de efeito colateral. IDs ficticios — nenhum teste depende de um
// pedido real de producao.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { getRedemptionTrayStatus, RedemptionTrackingError } from "../src/services/rewardRedemptionTracking.js";

const REDEMPTION_A = "aaaaaaaa-1111-2222-3333-444444444444";
const OWNER_ID = 418;
const OUTRO_USUARIO = 101;

/** Fake de `query` que grava o SQL e devolve linhas conforme a posse real. */
function dbFor(rows) {
  const calls = [];
  const query = async (sql, params) => {
    calls.push({ sql: String(sql).replace(/\s+/g, " ").trim(), params });
    // Reproduz a semantica do WHERE id = $1 AND user_id = $2.
    const [id, userId] = params;
    const found = rows.filter((r) => r.id === id && r.user_id === userId);
    return { rows: found };
  };
  return { calls, query };
}

const RESGATE_COM_PEDIDO = { id: REDEMPTION_A, user_id: OWNER_ID, status: "confirmed", tray_order_id: "90002" };

function pedidoEnviado() {
  return {
    id: 90002,
    status: "ENVIADO",
    OrderStatus: { type: "open" },
    date: "2026-05-27",
    modified: "2026-05-28 15:14:29",
    shipment: "Sedex",
    shipment_integrator: "Correios",
    shipment_date: "2026-05-28",
    sending_code: "AD507735291BR",
    tracking_url: "https://www.exemplo-loja.com.br/rastreio?cod_acesso=A4400C4741",
    has_shipment: "1",
    is_traceable: "1",
    estimated_delivery_date: "2026-06-17",
    store_note: "Resgate Loja NS / redemption_id=aaaa",
    Customer: { cpf: "12345678901" },
  };
}

/* ─────────────────────────── Posse ─────────────────────────── */

test("o dono consulta o proprio acompanhamento", async () => {
  const { query } = dbFor([RESGATE_COM_PEDIDO]);
  let asked = null;

  const out = await getRedemptionTrayStatus(OWNER_ID, REDEMPTION_A, {
    query,
    getTrayOrder: async (id) => {
      asked = id;
      return { raw: pedidoEnviado() };
    },
  });

  assert.equal(asked, "90002", "o tray_order_id tem que sair do BANCO");
  assert.equal(out.available, true);
  assert.equal(out.logistics.phase, "shipped");
  assert.equal(out.logistics.tracking_code, "AD507735291BR");
});

test("resgate de OUTRO usuario: 404 e nenhuma consulta a Tray", async () => {
  const { query } = dbFor([RESGATE_COM_PEDIDO]);
  let trayCalled = false;

  await assert.rejects(
    () =>
      getRedemptionTrayStatus(OUTRO_USUARIO, REDEMPTION_A, {
        query,
        getTrayOrder: async () => {
          trayCalled = true;
          return { raw: pedidoEnviado() };
        },
      }),
    (e) => e instanceof RedemptionTrackingError && e.code === "redemption_not_found" && e.status === 404
  );

  assert.equal(trayCalled, false, "nunca consultar a Tray por um resgate que nao e do usuario");
});

test("resgate de terceiro responde igual a inexistente (sem enumeracao)", async () => {
  const { query } = dbFor([RESGATE_COM_PEDIDO]);
  const dep = { query, getTrayOrder: async () => ({ raw: pedidoEnviado() }) };

  const alheio = await getRedemptionTrayStatus(OUTRO_USUARIO, REDEMPTION_A, dep).catch((e) => e);
  const inexistente = await getRedemptionTrayStatus(OUTRO_USUARIO, "bbbbbbbb-1111-2222-3333-444444444444", dep).catch((e) => e);

  assert.equal(alheio.code, inexistente.code);
  assert.equal(alheio.status, inexistente.status);
});

test("a posse entra no proprio WHERE, nunca em filtro no JS", async () => {
  const { calls, query } = dbFor([RESGATE_COM_PEDIDO]);
  await getRedemptionTrayStatus(OWNER_ID, REDEMPTION_A, { query, getTrayOrder: async () => ({ raw: pedidoEnviado() }) });

  assert.match(calls[0].sql, /where id = \$1::uuid and user_id = \$2/);
  assert.deepEqual(calls[0].params, [REDEMPTION_A, OWNER_ID]);
});

test("id malformado nem chega ao banco", async () => {
  const { calls, query } = dbFor([RESGATE_COM_PEDIDO]);
  await assert.rejects(
    () => getRedemptionTrayStatus(OWNER_ID, "1 OR 1=1", { query, getTrayOrder: async () => ({ raw: {} }) }),
    (e) => e.status === 404
  );
  assert.equal(calls.length, 0);
});

test("usuario invalido/ausente e 401, sem consulta nenhuma", async () => {
  const { calls, query } = dbFor([RESGATE_COM_PEDIDO]);
  for (const bad of [null, undefined, 0, -1, "abc"]) {
    // eslint-disable-next-line no-await-in-loop
    await assert.rejects(
      () => getRedemptionTrayStatus(bad, REDEMPTION_A, { query, getTrayOrder: async () => ({ raw: {} }) }),
      (e) => e.status === 401
    );
  }
  assert.equal(calls.length, 0);
});

/* ─────────────────────────── Gate de consulta ─────────────────────────── */

test("resgate sem pedido Tray: nenhuma chamada externa, resposta factual", async () => {
  const { query } = dbFor([{ id: REDEMPTION_A, user_id: OWNER_ID, status: "compensated", tray_order_id: null }]);
  let trayCalled = false;

  const out = await getRedemptionTrayStatus(OWNER_ID, REDEMPTION_A, {
    query,
    getTrayOrder: async () => {
      trayCalled = true;
      return { raw: pedidoEnviado() };
    },
  });

  assert.equal(trayCalled, false);
  assert.equal(out.available, false);
  assert.equal(out.reason, "tray_order_not_created");
  assert.equal(out.temporarily_unavailable, undefined, "nao ter pedido nao e indisponibilidade temporaria");
});

/* ─────────────────────────── Tray fora do ar ─────────────────────────── */

test("Tray indisponivel vira indisponibilidade temporaria, nunca erro do resgate", async () => {
  const { calls, query } = dbFor([RESGATE_COM_PEDIDO]);

  const out = await getRedemptionTrayStatus(OWNER_ID, REDEMPTION_A, {
    query,
    getTrayOrder: async () => {
      const e = new Error("tray_unreachable");
      e.code = "tray_unreachable";
      throw e;
    },
  });

  assert.equal(out.available, false);
  assert.equal(out.temporarily_unavailable, true);
  assert.equal(out.redemption_id, REDEMPTION_A);
  // Uma unica consulta: a leitura do resgate. Nada foi escrito.
  assert.equal(calls.length, 1);
  assert.match(calls[0].sql, /^select /i);
});

test("resposta invalida da Tray tambem cai no fallback seguro", async () => {
  const { query } = dbFor([RESGATE_COM_PEDIDO]);
  const out = await getRedemptionTrayStatus(OWNER_ID, REDEMPTION_A, {
    query,
    getTrayOrder: async () => ({ raw: null }),
  });

  assert.equal(out.available, false);
  assert.equal(out.temporarily_unavailable, true);
});

/* ─────────────────────────── Sem efeito colateral ─────────────────────────── */

test("a consulta nunca escreve nem toca no financeiro", async () => {
  const { calls, query } = dbFor([RESGATE_COM_PEDIDO]);
  await getRedemptionTrayStatus(OWNER_ID, REDEMPTION_A, { query, getTrayOrder: async () => ({ raw: pedidoEnviado() }) });

  assert.equal(calls.length, 1, "uma unica leitura, nenhuma escrita");
  for (const call of calls) {
    assert.ok(!/insert |update |delete /i.test(call.sql), `SQL de escrita detectado: ${call.sql}`);
  }
});

test("o codigo-fonte do acompanhamento nao chama saga, ledger nem cupom", () => {
  const source = readFileSync(
    fileURLToPath(new URL("../src/services/rewardRedemptionTracking.js", import.meta.url)),
    "utf8"
  );

  for (const forbidden of [
    "applyCouponLedgerEntry",
    "ensureTrayCouponForUser",
    "confirmRedemption",
    "prepareRedemption",
    "createTrayOrder",
    "createTrayRedemptionOrder",
    "trayMutationRequest",
  ]) {
    assert.ok(!source.includes(forbidden), `acompanhamento jamais pode chamar ${forbidden}`);
  }
  for (const pattern of [/insert\s+into/i, /update\s+public\./i, /delete\s+from/i, /method:\s*["'`]\s*(POST|PUT|DELETE)/i]) {
    assert.ok(!pattern.test(source), `padrao de mutacao detectado: ${pattern}`);
  }
});

test("nada do pedido Tray cru vaza para o cliente", async () => {
  const { query } = dbFor([RESGATE_COM_PEDIDO]);
  const out = await getRedemptionTrayStatus(OWNER_ID, REDEMPTION_A, { query, getTrayOrder: async () => ({ raw: pedidoEnviado() }) });

  const serialized = JSON.stringify(out);
  assert.ok(!serialized.includes("12345678901"), "CPF nunca");
  assert.ok(!serialized.includes("store_note"));
  assert.ok(!serialized.includes("redemption_id=aaaa"));
  assert.ok(!serialized.includes("ENVIADO"), "status comercial cru nunca vai para o cliente");
});
