import assert from "node:assert/strict";
import test from "node:test";

import {
  AUTOMATIC_EMAIL_EVENT_KEYS,
  handleAutomaticEmailEvent,
} from "../src/services/notifications/automaticEmailNotifications.js";
import {
  RESULT_EMAIL_EVENT_KEYS,
  acquireResultEventLock,
  buildResultEmail,
  canonicalResultReferenceKey,
  classifyDispatchHistory,
  handleAutomaticResultEmailEvent,
  loadDispatchHistory,
  loadResultParticipants,
  resultConfig,
  resultMessageId,
} from "../src/services/notifications/automaticResultEmailNotifications.js";

const T0 = new Date("2026-10-10T13:00:00.000Z");
const EFFECTIVE_FROM = new Date("2026-10-09T00:00:00.000Z");

function withEnv(values, run) {
  const previous = {};
  for (const [name, value] of Object.entries(values)) {
    previous[name] = process.env[name];
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
  const restore = () => {
    for (const [name, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  };
  return Promise.resolve().then(run).finally(restore);
}

const ENABLED = { NOTIFICATION_EMAIL_AUTOMATION_ENABLED: "true" };

function baseDraw(overrides = {}) {
  return {
    id: 150,
    status: "sorteado",
    draw_type: "principal",
    product_name: "Moto 0km",
    banner_title: null,
    winner_number: 7,
    winner_user_id: 11,
    winner_name: "Vencedora Teste",
    realized_at: new Date("2026-10-10T12:00:00.000Z"),
    closed_at: new Date("2026-10-08T00:00:00.000Z"),
    ...overrides,
  };
}

const WINNER = { id: 11, name: "Vencedora Teste", email: "vencedora@example.test" };
const PARTICIPANTS = [
  { id: 21, name: "Ana", email: "ana@example.test" },
  { id: 22, name: "Bruno", email: "bruno@example.test" },
  { id: 23, name: "Carla", email: "carla@example.test" },
];

function makeWorld(options = {}) {
  const world = {
    now: options.now || T0,
    draws: new Map([[150, options.draw || baseDraw()]]),
    winner: "winner" in options ? options.winner : WINNER,
    participants: options.participants || PARTICIPANTS,
    adminEmail: "admin@example.test",
    dispatches: [],
    mails: [],
    campaigns: [],
    locks: new Set(),
    smtpFails: new Set(), // e-mails para os quais o SMTP falha
    smtpDown: false,
    markAcceptedFails: false,
    nextId: 1,
    config: { effectiveFrom: EFFECTIVE_FROM, maxAgeHours: 192, pendingStaleMinutes: 20, maxAttempts: 3, adminEmail: "admin@example.test" },
  };
  world.deps = {
    now: () => world.now,
    resultConfig: () => world.config,
    loadResultDraw: async (id) => {
      const draw = world.draws.get(id);
      if (!draw) throw Object.assign(new Error("email_draw_not_found"), { code: "email_draw_not_found" });
      return { ...draw };
    },
    loadResultWinner: async (id) => (world.winner && world.winner.id === id ? world.winner : null),
    loadResultParticipants: async (_id, winnerId) => world.participants.filter((user) => user.id !== winnerId),
    loadDispatchHistory: async ({ eventKey, drawId, referenceKey, userId, recipient }) =>
      world.dispatches.map((row) => ({ ...row, final_failure: row.payload?.final_failure ? "true" : null })).filter((row) =>
        row.eventKey === eventKey &&
        row.drawId === drawId &&
        row.payload.reference_key === referenceKey &&
        (userId ? row.userId === userId : row.userId == null && row.recipient.toLowerCase() === String(recipient).toLowerCase())
      ),
    acquireResultEventLock: async (key) => {
      if (world.locks.has(key)) return null;
      world.locks.add(key);
      return async () => world.locks.delete(key);
    },
    getSmtpConfig: () => ({ fromName: "New Store", fromEmail: "contato@newstore.test", replyTo: "contato@newstore.test" }),
    createSmtpTransporter: () => ({
      sendMail: async (message) => {
        if (world.beforeSend) await world.beforeSend(message);
        if (world.smtpDown || world.smtpFails.has(message.to)) {
          throw Object.assign(new Error("smtp unavailable"), { code: "ECONNECTION" });
        }
        world.mails.push(message);
        return { messageId: message.messageId, accepted: [message.to] };
      },
    }),
    createCampaign: async (args) => {
      const campaign = { id: world.campaigns.length + 1, ...args };
      world.campaigns.push(campaign);
      return campaign;
    },
    updateCampaignAudienceCounts: async () => {},
    createDispatch: async (args) => {
      const row = { id: world.nextId++, status: "pending", created_at: world.now, ...args };
      world.dispatches.push(row);
      return row;
    },
    markDispatchAccepted: async ({ dispatchId }) => {
      if (world.markAcceptedFails) throw new Error("db unavailable");
      world.dispatches.find((row) => row.id === dispatchId).status = "accepted";
    },
    markDispatchFailed: async ({ dispatchId, status }) => {
      world.dispatches.find((row) => row.id === dispatchId).status = status || "failed";
    },
  };
  return world;
}

function event(eventKey, drawId = 150, extra = {}) {
  const referenceType = extra.referenceType ?? "draw";
  return {
    eventKey,
    referenceType,
    referenceKey: extra.referenceKey ?? canonicalResultReferenceKey(extra.drawType || "principal", drawId, eventKey),
    metadata: { draw_id: drawId, ...(extra.metadata || {}) },
    occurredAt: "2026-10-10T12:00:00.000Z",
  };
}

const run = (world, eventKey, drawId, extra) =>
  withEnv(ENABLED, () => handleAutomaticResultEmailEvent(event(eventKey, drawId, extra), world.deps));

// ---------- configuracao / chaves ----------

test("eventos de resultado estao registrados no despachante existente", () => {
  for (const key of RESULT_EMAIL_EVENT_KEYS) assert.ok(AUTOMATIC_EMAIL_EVENT_KEYS.includes(key));
  assert.ok(AUTOMATIC_EMAIL_EVENT_KEYS.includes("DRAW_CLOSED"));
  assert.ok(AUTOMATIC_EMAIL_EVENT_KEYS.includes("NEW_DRAW_PUBLISHED"));
});

test("handleAutomaticEmailEvent encaminha eventos de resultado ao tratador de resultado", async () => {
  await withEnv({ NOTIFICATION_EMAIL_AUTOMATION_ENABLED: undefined }, async () => {
    const response = await handleAutomaticEmailEvent({
      eventKey: "EMAIL_RESULT_WINNER",
      referenceType: "draw",
      referenceKey: "draw:150:result_winner_email",
      metadata: { draw_id: 150 },
    }, {});
    assert.equal(response.status, "disabled");
    assert.equal(response.event_key, "EMAIL_RESULT_WINNER");
  });
});

test("desligado por padrao: sem automacao habilitada nada e enviado", async () => {
  const world = makeWorld();
  const response = await withEnv({ NOTIFICATION_EMAIL_AUTOMATION_ENABLED: undefined }, () =>
    handleAutomaticResultEmailEvent(event("EMAIL_RESULT_WINNER"), world.deps));
  assert.equal(response.status, "disabled");
  assert.equal(world.mails.length, 0);
  assert.equal(world.dispatches.length, 0);
});

test("fail-closed: sem NOTIFICATION_EMAIL_RESULT_EFFECTIVE_FROM nenhum e-mail de resultado e enviado", async () => {
  const world = makeWorld();
  world.config = { ...world.config, effectiveFrom: null };
  const response = await run(world, "EMAIL_RESULT_WINNER");
  assert.equal(response.status, "disabled");
  assert.equal(response.reason, "result_effective_from_not_configured");
  assert.equal(world.mails.length, 0);
});

// ---------- vencedor / participantes / admin ----------

test("vencedor identificado recebe o e-mail de parabens com o numero vencedor", async () => {
  const world = makeWorld();
  const response = await run(world, "EMAIL_RESULT_WINNER", 150, { metadata: { contest_number: 2990, result_date: "2026-10-09" } });
  assert.equal(response.status, "processed");
  assert.equal(response.sent, 1);
  assert.equal(world.mails.length, 1);
  const [mail] = world.mails;
  assert.equal(mail.to, "vencedora@example.test");
  assert.match(mail.subject, /Parabéns/);
  assert.match(mail.text, /Número vencedor: 07/);
  assert.match(mail.text, /Concurso da Lotomania utilizado: 2990 \(2026-10-09\)/);
  assert.match(mail.text, /próximas instruções/);
  assert.equal(world.dispatches.length, 1);
  assert.equal(world.dispatches[0].status, "accepted");
  assert.equal(world.dispatches[0].userId, 11);
});

test("participantes nao contemplados recebem um e-mail cada, sem o vencedor e sem expor o nome dele", async () => {
  const world = makeWorld({ participants: [...PARTICIPANTS, WINNER] });
  const response = await run(world, "EMAIL_RESULT_PARTICIPANT");
  assert.equal(response.sent, 3);
  assert.deepEqual(world.mails.map((mail) => mail.to).sort(), ["ana@example.test", "bruno@example.test", "carla@example.test"]);
  for (const mail of world.mails) {
    assert.match(mail.text, /Número vencedor: 07/);
    assert.match(mail.text, /não foi contemplado/);
    assert.doesNotMatch(mail.text, /Vencedora Teste/);
    assert.doesNotMatch(mail.text, /Vencedor: -/);
  }
  assert.equal(new Set(world.dispatches.map((row) => row.userId)).size, 3);
});

test("loadResultParticipants deduplica usuarios com varias compras e exclui o vencedor", async () => {
  let captured;
  const rows = await loadResultParticipants(150, 11, async (sql, params) => {
    captured = { sql, params };
    return { rows: [
      { id: 21, name: "Ana", email: "ana@example.test" },
      { id: 21, name: "Ana", email: "ana@example.test" },
      { id: 24, name: "Ana 2", email: "ANA@example.test" },
      { id: 25, name: "Sem email valido", email: "invalido" },
    ] };
  });
  assert.deepEqual(rows.map((row) => row.id), [21]);
  assert.deepEqual(captured.params, [150, 11]);
  assert.match(captured.sql, /u\.id <> \$2/);
  assert.match(captured.sql, /r\.draw_id = \$1/);
  assert.match(captured.sql, /p\.draw_id = \$1/);
});

test("administracao recebe resultado com vencedor identificado e concurso", async () => {
  const world = makeWorld();
  const response = await run(world, "EMAIL_RESULT_ADMIN", 150, { metadata: { contest_number: 2990 } });
  assert.equal(response.sent, 1);
  const [mail] = world.mails;
  assert.equal(mail.to, "admin@example.test");
  assert.match(mail.subject, /Resultado definido/);
  assert.match(mail.text, /Concurso da Lotomania utilizado: 2990/);
  assert.match(mail.text, /vencedora@example\.test/);
  assert.equal(world.dispatches[0].userId, null);
});

// ---------- sorteio sem comprador ----------

test("sem comprador identificado: sem parabens, sem aviso aos participantes e pendencia para a administracao", async () => {
  const world = makeWorld({ draw: baseDraw({ winner_user_id: null, winner_name: null }), winner: null });
  const winner = await run(world, "EMAIL_RESULT_WINNER");
  const participants = await run(world, "EMAIL_RESULT_PARTICIPANT");
  const admin = await run(world, "EMAIL_RESULT_ADMIN");
  assert.equal(winner.reason, "no_identified_winner");
  assert.equal(participants.reason, "no_identified_winner");
  assert.equal(admin.sent, 1);
  assert.equal(world.mails.length, 1);
  const [mail] = world.mails;
  assert.equal(mail.to, "admin@example.test");
  assert.match(mail.subject, /PENDÊNCIA/);
  assert.match(mail.text, /nenhum comprador foi identificado/i);
  assert.doesNotMatch(mail.text, /Vencedor: -/);
});

// ---------- guardas de historico ----------

test("resultado ainda nao definido (draw nao sorteado) nao envia", async () => {
  for (const draw of [baseDraw({ status: "closed" }), baseDraw({ realized_at: null }), baseDraw({ winner_number: null })]) {
    const world = makeWorld({ draw });
    const response = await run(world, "EMAIL_RESULT_WINNER");
    assert.equal(response.reason, "result_not_defined");
    assert.equal(world.mails.length, 0);
  }
});

test("sorteio antigo (realizado antes do ponto de corte) nao recebe mensagens inesperadas", async () => {
  const world = makeWorld({ draw: baseDraw({ realized_at: new Date("2026-10-06T14:59:26.000Z") }) });
  for (const key of RESULT_EMAIL_EVENT_KEYS) {
    const response = await run(world, key);
    assert.equal(response.reason, "result_before_effective_from");
  }
  assert.equal(world.mails.length, 0);
  assert.equal(world.dispatches.length, 0);
});

test("participantes alem da janela de recuperacao nao recebem mais e nao geram alerta", async () => {
  const world = makeWorld({ now: new Date("2026-10-25T00:00:00.000Z") });
  const response = await run(world, "EMAIL_RESULT_PARTICIPANT");
  assert.equal(response.reason, "result_too_old");
  assert.equal(response.failed, 0);
  assert.equal(world.mails.length, 0);
  assert.equal(world.dispatches.length, 0);
});

// ---------- chaves, principal x adicional, mesmo numero ----------

test("chave canonica distingue principal de adicional e sorteios diferentes", () => {
  assert.equal(canonicalResultReferenceKey("principal", 5, "EMAIL_RESULT_WINNER"), "draw:5:result_winner_email");
  assert.equal(canonicalResultReferenceKey(null, 5, "EMAIL_RESULT_ADMIN"), "draw:5:result_admin_email");
  assert.equal(canonicalResultReferenceKey("adicional", 5, "EMAIL_RESULT_PARTICIPANT"), "additional_draw:5:result_participant_email");
  assert.equal(canonicalResultReferenceKey("secundario", 6, "EMAIL_RESULT_WINNER"), "additional_draw:6:result_winner_email");
  assert.notEqual(canonicalResultReferenceKey("principal", 5, "EMAIL_RESULT_WINNER"), canonicalResultReferenceKey("principal", 6, "EMAIL_RESULT_WINNER"));
});

test("chave de referencia divergente da canonica e rejeitada (evita duplicidade por chaves diferentes)", async () => {
  const world = makeWorld();
  await assert.rejects(
    run(world, "EMAIL_RESULT_WINNER", 150, { referenceKey: "draw:150:winner_email_v2" }),
    (error) => error.code === "email_reference_key_invalid"
  );
  await assert.rejects(
    run(world, "EMAIL_RESULT_WINNER", 150, { referenceType: "additional_draw" }),
    (error) => error.code === "email_reference_type_invalid"
  );
  assert.equal(world.mails.length, 0);
});

test("principal e adicional com o mesmo numero vencedor geram e-mails independentes por draw", async () => {
  const world = makeWorld();
  world.draws.set(151, baseDraw({ id: 151, draw_type: "adicional", winner_number: 7, winner_user_id: 11 }));
  const principal = await run(world, "EMAIL_RESULT_WINNER", 150);
  const additional = await run(world, "EMAIL_RESULT_WINNER", 151, { drawType: "adicional", referenceType: "additional_draw" });
  assert.equal(principal.sent, 1);
  assert.equal(additional.sent, 1);
  assert.equal(world.mails.length, 2);
  assert.deepEqual(
    world.dispatches.map((row) => row.payload.reference_key).sort(),
    ["additional_draw:151:result_winner_email", "draw:150:result_winner_email"]
  );
  assert.notEqual(world.mails[0].messageId, world.mails[1].messageId);
});

// ---------- falha SMTP, reenvio, dedupe ----------

test("falha de SMTP fica registrada como failed e o reenvio acontece na proxima execucao", async () => {
  const world = makeWorld();
  world.smtpDown = true;
  const first = await run(world, "EMAIL_RESULT_WINNER");
  assert.equal(first.status, "failed");
  assert.equal(first.failed, 1);
  assert.equal(world.mails.length, 0);
  assert.equal(world.dispatches[0].status, "failed");

  world.smtpDown = false;
  const second = await run(world, "EMAIL_RESULT_WINNER");
  assert.equal(second.status, "processed");
  assert.equal(world.mails.length, 1);
  assert.deepEqual(world.dispatches.map((row) => row.status), ["failed", "accepted"]);
});

test("e-mail ja enviado nao e reenviado por uma execucao normal", async () => {
  const world = makeWorld();
  await run(world, "EMAIL_RESULT_WINNER");
  const again = await run(world, "EMAIL_RESULT_WINNER");
  const third = await run(world, "EMAIL_RESULT_WINNER");
  assert.equal(world.mails.length, 1);
  assert.equal(again.status, "deduped");
  assert.equal(again.deduped, 1);
  assert.equal(third.sent, 0);
  assert.equal(world.dispatches.length, 1);
});

test("falha parcial: so os destinatarios que falharam sao reenviados", async () => {
  const world = makeWorld();
  world.smtpFails.add("bruno@example.test");
  const first = await run(world, "EMAIL_RESULT_PARTICIPANT");
  assert.equal(first.status, "partial_failure");
  assert.equal(first.sent, 2);
  assert.equal(first.failed, 1);

  world.smtpFails.clear();
  const second = await run(world, "EMAIL_RESULT_PARTICIPANT");
  assert.equal(second.sent, 1);
  assert.equal(second.deduped, 2);
  assert.deepEqual(world.mails.map((mail) => mail.to).sort(), ["ana@example.test", "bruno@example.test", "carla@example.test"]);
});

test("cinco tentativas (limite configurado) esgotam o vencedor: alerta unico, registro persistente e sem reenvio", async () => {
  const world = makeWorld();
  world.smtpDown = true;
  for (let i = 0; i < 3; i += 1) await run(world, "EMAIL_RESULT_WINNER");
  assert.equal(world.dispatches.filter((row) => row.status === "failed").length, 3);
  world.smtpDown = false; // o provedor volta, mas o limite ja foi atingido
  const alert = await run(world, "EMAIL_RESULT_WINNER");
  assert.equal(alert.status, "critical_failure");
  assert.equal(alert.failed, 1);
  assert.equal(alert.critical_alerts, 1);
  assert.equal(alert.exhausted, 1);
  assert.equal(world.mails.length, 0);
  const markers = world.dispatches.filter((row) => row.payload.final_failure);
  assert.equal(markers.length, 1);
  assert.equal(markers[0].payload.final_failure_reason, "retry_exhausted");
  assert.equal(markers[0].status, "skipped");
  assert.equal(markers[0].payload.draw_id, 150);
  assert.equal(markers[0].payload.event_key, "EMAIL_RESULT_WINNER");
});

// ---------- concorrencia e queda do processo ----------

test("concorrencia: dois processadores do mesmo evento resultam em um unico envio", async () => {
  const world = makeWorld();
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  let entered;
  const inside = new Promise((resolve) => { entered = resolve; });
  world.beforeSend = async () => { entered(); await gate; };
  const first = run(world, "EMAIL_RESULT_WINNER");
  await inside;
  const second = await run(world, "EMAIL_RESULT_WINNER");
  assert.equal(second.status, "in_progress");
  assert.equal(second.sent, 0);
  release();
  const firstResult = await first;
  assert.equal(firstResult.sent, 1);
  assert.equal(world.mails.length, 1);
  assert.equal(world.locks.size, 0);
});

test("processo interrompido apos o provedor aceitar: pending recente nao duplica; pending abandonado e recuperado uma vez", async () => {
  const world = makeWorld();
  world.markAcceptedFails = true; // simula queda do banco depois do sendMail
  await run(world, "EMAIL_RESULT_WINNER");
  assert.equal(world.mails.length, 1);
  assert.equal(world.dispatches[0].status, "pending");
  world.markAcceptedFails = false;

  // logo depois: pending recente => outro processo ainda pode estar enviando; nao reenviar
  world.now = new Date(T0.getTime() + 5 * 60 * 1000);
  const soon = await run(world, "EMAIL_RESULT_WINNER");
  assert.equal(soon.status, "nothing_to_send");
  assert.equal(soon.in_flight, 1);
  assert.equal(world.mails.length, 1);

  // depois do prazo: abandonado => encerrado como falha e reenviado com o MESMO Message-ID
  world.now = new Date(T0.getTime() + 25 * 60 * 1000);
  const recovered = await run(world, "EMAIL_RESULT_WINNER");
  assert.equal(recovered.sent, 1);
  assert.equal(world.mails.length, 2);
  assert.equal(world.mails[0].messageId, world.mails[1].messageId);
  assert.deepEqual(world.dispatches.map((row) => row.status), ["failed", "accepted"]);

  const after = await run(world, "EMAIL_RESULT_WINNER");
  assert.equal(after.status, "deduped");
  assert.equal(world.mails.length, 2);
});

test("classifyDispatchHistory decide delivered / in_flight / retry / exhausted", () => {
  const now = new Date("2026-10-10T13:00:00.000Z");
  const opts = { now, pendingStaleMinutes: 20, maxAttempts: 3 };
  assert.equal(classifyDispatchHistory([], opts).state, "retry");
  assert.equal(classifyDispatchHistory([{ id: 1, status: "accepted", created_at: now }], opts).state, "delivered");
  assert.equal(classifyDispatchHistory([{ id: 1, status: "failed", created_at: now }, { id: 2, status: "accepted", created_at: now }], opts).state, "delivered");
  assert.equal(classifyDispatchHistory([{ id: 1, status: "pending", created_at: new Date(now.getTime() - 60_000) }], opts).state, "in_flight");
  const stale = classifyDispatchHistory([{ id: 1, status: "pending", created_at: new Date(now.getTime() - 30 * 60_000) }], opts);
  assert.equal(stale.state, "retry");
  assert.deepEqual(stale.abandonedIds, [1]);
  const failures = [1, 2, 3].map((id) => ({ id, status: "failed", created_at: now }));
  assert.equal(classifyDispatchHistory(failures, opts).state, "exhausted");
  assert.equal(classifyDispatchHistory([{ id: 1, status: "skipped", created_at: now }], opts).state, "retry");
});

test("Message-ID e estavel por evento+destinatario e muda entre destinatarios e eventos", () => {
  const a = resultMessageId("draw:1:result_winner_email", 11, "contato@newstore.test");
  assert.equal(a, resultMessageId("draw:1:result_winner_email", 11, "contato@newstore.test"));
  assert.notEqual(a, resultMessageId("draw:1:result_winner_email", 12, "contato@newstore.test"));
  assert.notEqual(a, resultMessageId("draw:2:result_winner_email", 11, "contato@newstore.test"));
  assert.match(a, /^<result-[0-9a-f]{32}@newstore\.test>$/);
});

test("conteudo escapa HTML e usa o nome do sorteio", () => {
  const mail = buildResultEmail("EMAIL_RESULT_PARTICIPANT", {
    draw: baseDraw({ product_name: "<b>Moto</b>" }),
    winner: WINNER,
    recipient: { id: 1, name: "Ana <script>", email: "ana@example.test" },
  });
  assert.doesNotMatch(mail.html, /<script>/);
  assert.match(mail.html, /&lt;b&gt;Moto&lt;\/b&gt;/);
  assert.match(mail.subject, /Resultado disponível/);
});

function fakePool(lockResult) {
  const calls = [];
  const client = {
    released: 0,
    async query(sql, params) {
      calls.push({ sql: String(sql).replace(/\s+/g, " ").trim(), params });
      if (/pg_try_advisory_xact_lock/.test(sql)) return { rows: [{ locked: lockResult }] };
      return { rows: [] };
    },
    release() { client.released += 1; },
  };
  return { calls, client, provider: async () => ({ connect: async () => client }) };
}

test("trava por evento usa transacao + advisory xact lock (compativel com pooler em modo transacao)", async () => {
  const pool = fakePool(true);
  const release = await acquireResultEventLock("draw:150:result_winner_email", pool.provider);
  assert.equal(typeof release, "function");
  assert.deepEqual(pool.calls.map((call) => call.sql.split(" ")[0]), ["BEGIN", "SELECT"]);
  assert.match(pool.calls[1].sql, /pg_try_advisory_xact_lock\(hashtextextended\(\$1, 7001\)\)/);
  assert.deepEqual(pool.calls[1].params, ["draw:150:result_winner_email"]);
  assert.doesNotMatch(pool.calls.map((call) => call.sql).join(" "), /pg_try_advisory_lock\(/);
  assert.equal(pool.client.released, 0);
  await release();
  assert.equal(pool.calls.at(-1).sql, "ROLLBACK");
  assert.equal(pool.client.released, 1);
});

test("trava ocupada devolve null, encerra a transacao e devolve a conexao", async () => {
  const pool = fakePool(false);
  const release = await acquireResultEventLock("draw:150:result_winner_email", pool.provider);
  assert.equal(release, null);
  assert.equal(pool.calls.at(-1).sql, "ROLLBACK");
  assert.equal(pool.client.released, 1);
});

test("falha ao registrar a falha de um destinatario nao aborta os demais", async () => {
  const world = makeWorld();
  world.smtpFails.add("ana@example.test");
  const originalFail = world.deps.markDispatchFailed;
  let first = true;
  world.deps.markDispatchFailed = async (args) => {
    if (first) { first = false; throw new Error("db unavailable while marking failure"); }
    return originalFail(args);
  };
  const response = await run(world, "EMAIL_RESULT_PARTICIPANT");
  assert.equal(response.sent, 2);
  assert.equal(response.failed, 1);
  assert.deepEqual(world.mails.map((mail) => mail.to).sort(), ["bruno@example.test", "carla@example.test"]);
  assert.equal(world.locks.size, 0);
});

test("erro ao liberar a trava nao mascara o resultado do envio", async () => {
  const world = makeWorld();
  world.deps.acquireResultEventLock = async () => async () => { throw new Error("connection closed"); };
  const response = await run(world, "EMAIL_RESULT_WINNER");
  assert.equal(response.status, "processed");
  assert.equal(response.sent, 1);
});

test("consulta do historico usa draw_id = $2 (indice) e distingue usuario de administrador", async () => {
  const calls = [];
  const runQuery = async (sql, params) => { calls.push({ sql: sql.replace(/\s+/g, " "), params }); return { rows: [] }; };
  await loadDispatchHistory({ eventKey: "EMAIL_RESULT_WINNER", drawId: 150, referenceKey: "draw:150:result_winner_email", userId: 11, recipient: "a@x.test" }, runQuery);
  await loadDispatchHistory({ eventKey: "EMAIL_RESULT_ADMIN", drawId: 150, referenceKey: "draw:150:result_admin_email", userId: null, recipient: "Admin@X.test" }, runQuery);
  assert.match(calls[0].sql, /draw_id = \$2/);
  assert.doesNotMatch(calls[0].sql, /IS NOT DISTINCT FROM/);
  assert.match(calls[0].sql, /user_id = \$4/);
  assert.deepEqual(calls[0].params, ["EMAIL_RESULT_WINNER", 150, "draw:150:result_winner_email", 11]);
  assert.match(calls[1].sql, /user_id IS NULL AND lower\(recipient\) = lower\(\$4\)/);
  assert.equal(calls[1].params[3], "Admin@X.test");
});

// ---------- janelas, falha definitiva e alerta unico ----------

function seedAttempts(world, { eventKey, userId = 11, recipient = "vencedora@example.test", count, status = "failed", at }) {
  for (let i = 0; i < count; i += 1) {
    world.dispatches.push({
      id: world.nextId++,
      status,
      created_at: at || world.now,
      eventKey,
      drawId: 150,
      userId,
      recipient,
      payload: { reference_key: canonicalResultReferenceKey("principal", 150, eventKey), source: "automation" },
    });
  }
}

test("janela de aceitacao padrao e 192h e e configuravel", () => {
  const previous = { max: process.env.NOTIFICATION_EMAIL_RESULT_MAX_AGE_HOURS };
  delete process.env.NOTIFICATION_EMAIL_RESULT_MAX_AGE_HOURS;
  try {
    assert.equal(resultConfig().maxAgeHours, 192);
    assert.equal(resultConfig().maxAttempts, 5);
    process.env.NOTIFICATION_EMAIL_RESULT_MAX_AGE_HOURS = "240";
    assert.equal(resultConfig().maxAgeHours, 240);
    process.env.NOTIFICATION_EMAIL_RESULT_MAX_AGE_HOURS = "abc";
    assert.equal(resultConfig().maxAgeHours, 192);
  } finally {
    if (previous.max === undefined) delete process.env.NOTIFICATION_EMAIL_RESULT_MAX_AGE_HOURS;
    else process.env.NOTIFICATION_EMAIL_RESULT_MAX_AGE_HOURS = previous.max;
  }
});

test("dentro de 192h um e-mail pendente ainda e enviado; a corte de ativacao continua valendo", async () => {
  const world = makeWorld({ now: new Date("2026-10-18T11:00:00.000Z") }); // ~167h apos realized_at
  assert.equal((await run(world, "EMAIL_RESULT_WINNER")).sent, 1);
  const lateWorld = makeWorld({ now: new Date("2026-10-18T11:00:00.000Z"), draw: baseDraw({ realized_at: new Date("2026-10-06T14:59:26.000Z") }) });
  assert.equal((await run(lateWorld, "EMAIL_RESULT_WINNER")).reason, "result_before_effective_from");
  assert.equal(lateWorld.mails.length, 0);
  assert.equal(lateWorld.dispatches.length, 0);
});

test("a janela e o corte de ativacao valem na mesma execucao: antigo e anterior ao corte nunca alerta", async () => {
  const world = makeWorld({ now: new Date("2026-12-01T00:00:00.000Z"), draw: baseDraw({ realized_at: new Date("2026-10-06T14:59:26.000Z") }) });
  for (const key of RESULT_EMAIL_EVENT_KEYS) {
    const response = await run(world, key);
    assert.equal(response.reason, "result_before_effective_from");
    assert.equal(response.failed, 0);
  }
  assert.equal(world.dispatches.length, 0);
});

test("vencedor pendente alem da janela: registro persistente + alerta UNICO, sem envio e sem repeticao", async () => {
  const world = makeWorld({ now: new Date("2026-10-25T00:00:00.000Z") });
  const first = await run(world, "EMAIL_RESULT_WINNER");
  assert.equal(first.status, "critical_failure");
  assert.equal(first.failed, 1);
  assert.equal(first.critical_alerts, 1);
  assert.equal(world.mails.length, 0);
  const markers = world.dispatches.filter((row) => row.payload.final_failure);
  assert.equal(markers.length, 1);
  assert.equal(markers[0].payload.final_failure_reason, "recovery_window_expired");
  assert.equal(markers[0].payload.reference_key, "draw:150:result_winner_email");

  for (let i = 0; i < 3; i += 1) {
    const again = await run(world, "EMAIL_RESULT_WINNER");
    assert.equal(again.failed, 0);
    assert.equal(again.critical_alerts ?? 0, 0);
    assert.notEqual(again.status, "critical_failure");
  }
  assert.equal(world.dispatches.filter((row) => row.payload.final_failure).length, 1);
  assert.equal(world.mails.length, 0);
});

test("administracao pendente alem da janela tambem gera alerta unico (inclusive sem comprador)", async () => {
  const world = makeWorld({ now: new Date("2026-10-25T00:00:00.000Z"), draw: baseDraw({ winner_user_id: null }), winner: null });
  const first = await run(world, "EMAIL_RESULT_ADMIN");
  assert.equal(first.status, "critical_failure");
  assert.equal(first.critical_alerts, 1);
  const again = await run(world, "EMAIL_RESULT_ADMIN");
  assert.equal(again.failed, 0);
  assert.equal(world.dispatches.filter((row) => row.payload.final_failure).length, 1);
});

test("evento critico ja entregue alem da janela fica em silencio (sem alerta, sem reenvio)", async () => {
  const world = makeWorld();
  await run(world, "EMAIL_RESULT_WINNER");
  world.now = new Date("2026-10-30T00:00:00.000Z");
  const later = await run(world, "EMAIL_RESULT_WINNER");
  assert.equal(later.failed, 0);
  assert.equal(later.status, "deduped");
  assert.equal(world.mails.length, 1);
  assert.equal(world.dispatches.filter((row) => row.payload.final_failure).length, 0);
});

test("alem da janela com envio em andamento (pending recente) nao alerta ainda", async () => {
  const world = makeWorld({ now: new Date("2026-10-25T00:00:00.000Z") });
  seedAttempts(world, { eventKey: "EMAIL_RESULT_WINNER", count: 1, status: "pending", at: new Date("2026-10-24T23:55:00.000Z") });
  const response = await run(world, "EMAIL_RESULT_WINNER");
  assert.equal(response.failed, 0);
  assert.equal(response.in_flight, 1);
  assert.equal(world.dispatches.filter((row) => row.payload.final_failure).length, 0);
});

test("falha definitiva do vencedor: nunca reenvia depois, mesmo com o provedor de volta e em execucoes repetidas", async () => {
  const world = makeWorld();
  seedAttempts(world, { eventKey: "EMAIL_RESULT_WINNER", count: 3 });
  const alert = await run(world, "EMAIL_RESULT_WINNER");
  assert.equal(alert.status, "critical_failure");
  for (let i = 0; i < 4; i += 1) {
    const again = await run(world, "EMAIL_RESULT_WINNER");
    assert.equal(again.sent, 0);
    assert.equal(again.failed, 0);
  }
  assert.equal(world.mails.length, 0);
  assert.equal(world.dispatches.filter((row) => row.payload.final_failure).length, 1);
});

test("o marcador de falha definitiva nao conta como tentativa", () => {
  const now = new Date("2026-10-10T13:00:00.000Z");
  const attempts = [1, 2].map((id) => ({ id, status: "failed", created_at: now }));
  // o marcador e filtrado antes de classificar; classificar so as tentativas reais nao esgota (2 < 3)
  assert.equal(classifyDispatchHistory(attempts, { now, maxAttempts: 3 }).state, "retry");
});

test("participantes que esgotaram tentativas nao geram alerta nem bloqueiam os demais destinatarios", async () => {
  const world = makeWorld();
  seedAttempts(world, { eventKey: "EMAIL_RESULT_PARTICIPANT", userId: 21, recipient: "ana@example.test", count: 3 });
  const response = await run(world, "EMAIL_RESULT_PARTICIPANT");
  assert.equal(response.sent, 2);
  assert.equal(response.failed, 0);
  assert.equal(response.exhausted, 1);
  assert.equal(response.status, "processed");
  assert.deepEqual(world.mails.map((mail) => mail.to).sort(), ["bruno@example.test", "carla@example.test"]);
  assert.equal(world.dispatches.filter((row) => row.payload.final_failure).length, 0);
});

test("com o limite padrao de cinco tentativas: a quinta falha esgota, a quarta ainda reenvia", async () => {
  const four = makeWorld();
  four.config = { ...four.config, maxAttempts: 5 };
  seedAttempts(four, { eventKey: "EMAIL_RESULT_WINNER", count: 4 });
  const retry = await run(four, "EMAIL_RESULT_WINNER");
  assert.equal(retry.sent, 1);
  assert.equal(retry.failed, 0);

  const five = makeWorld();
  five.config = { ...five.config, maxAttempts: 5 };
  seedAttempts(five, { eventKey: "EMAIL_RESULT_WINNER", count: 5 });
  const exhausted = await run(five, "EMAIL_RESULT_WINNER");
  assert.equal(exhausted.status, "critical_failure");
  assert.equal(five.mails.length, 0);
});

test("logs de alerta identificam sorteio e tipo sem dados pessoais", async () => {
  const world = makeWorld();
  seedAttempts(world, { eventKey: "EMAIL_RESULT_WINNER", count: 3 });
  const logged = [];
  const original = console.error;
  console.error = (...args) => logged.push(JSON.stringify(args));
  try {
    await run(world, "EMAIL_RESULT_WINNER");
  } finally {
    console.error = original;
  }
  const line = logged.find((entry) => entry.includes("critical_notification_failed"));
  assert.ok(line);
  assert.match(line, /EMAIL_RESULT_WINNER/);
  assert.match(line, /"draw_id":150/);
  assert.doesNotMatch(line, /vencedora@example\.test/);
  assert.doesNotMatch(line, /Vencedora Teste/);
});
