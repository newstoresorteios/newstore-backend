import assert from "node:assert/strict";
import test from "node:test";

import {
  handleAutomaticEmailEvent,
  isDrawClosedForEmail,
  loadRecipients,
  resolveDrawDisplayName,
} from "../src/services/notifications/automaticEmailNotifications.js";
import { handleInternalEmailEventRequest } from "../src/routes/internal_email_events.js";

const DRAW_CLOSED_EVENT = {
  eventKey: "DRAW_CLOSED",
  referenceType: "draw",
  referenceKey: "draw:42:closed_email",
  metadata: { draw_id: 42 },
  occurredAt: "2026-07-24T21:00:00.000Z",
};

test("NEW_DRAW_PUBLISHED consulta todos os usuários sem parâmetros SQL extras", async () => {
  let capturedSql = null;
  let capturedParams = null;

  const recipients = await loadRecipients(42, "NEW_DRAW_PUBLISHED", async (sql, params) => {
    capturedSql = sql;
    capturedParams = params;
    return { rows: [{ id: 1, name: "Cliente", email: "cliente@example.test" }] };
  });

  assert.deepEqual(capturedParams, []);
  assert.doesNotMatch(capturedSql, /\$1/);
  assert.match(capturedSql, /FROM public\.users/);
  assert.equal(recipients.length, 1);
});

test("EMAIL_DRAW_REMAINING_75 consulta todos os usuários sem parâmetros SQL extras", async () => {
  let capturedParams = null;

  await loadRecipients(42, "EMAIL_DRAW_REMAINING_75", async (_sql, params) => {
    capturedParams = params;
    return { rows: [] };
  });

  assert.deepEqual(capturedParams, []);
});

test("DRAW_CLOSED consulta participantes com drawId e preserva os dois filtros SQL", async () => {
  let capturedSql = null;
  let capturedParams = null;

  const recipients = await loadRecipients(42, "DRAW_CLOSED", async (sql, params) => {
    capturedSql = sql;
    capturedParams = params;
    return {
      rows: [
        { id: 1, name: "Cliente", email: "cliente@example.test" },
        { id: 2, name: "Duplicado", email: "CLIENTE@example.test" },
        { id: 3, name: "Inválido", email: "email-invalido" },
      ],
    };
  });

  assert.deepEqual(capturedParams, [42]);
  assert.match(capturedSql, /r\.draw_id = \$1/);
  assert.match(capturedSql, /p\.draw_id = \$1/);
  assert.deepEqual(recipients.map((recipient) => recipient.id), [1]);
});

async function withEnv(name, value, run) {
  const previous = process.env[name];
  if (value === undefined) delete process.env[name];
  else process.env[name] = value;
  try {
    return await run();
  } finally {
    if (previous === undefined) delete process.env[name];
    else process.env[name] = previous;
  }
}

function drawContext(status = "closed", closedAt = "2026-07-24T21:00:00.000Z") {
  return {
    draw: { id: 42, status, draw_type: "principal", closed_at: closedAt },
    config: null,
    principalConfig: null,
    drawName: "Sorteio 42",
    drawUrl: "https://example.test/?draw_id=42",
  };
}

function users(count) {
  return Array.from({ length: count }, (_unused, index) => ({
    id: index + 1,
    name: `Cliente ${index + 1}`,
    email: `cliente${index + 1}@example.test`,
  }));
}

function automaticEmailHarness({
  recipients = users(10),
  drawStatus = "closed",
  closedAt = "2026-07-24T21:00:00.000Z",
  context = null,
  remainingNumbers = 15,
  shouldFail = () => false,
  smtpConfigurationError = false,
} = {}) {
  const acceptedUsers = new Set();
  const acceptedDispatchKeys = new Set();
  const dispatchUserById = new Map();
  const dispatchKeyById = new Map();
  const dispatchStatuses = new Map();
  const campaignUpdates = [];
  const sentMessages = [];
  let smtpCalls = 0;
  let campaignCalls = 0;
  let dispatchSequence = 0;

  const dependencies = {
    async loadDrawContext(drawId) {
      if (typeof context === "function") return context(drawId);
      return context || drawContext(drawStatus, closedAt);
    },
    async loadRecipients() {
      return recipients;
    },
    async loadRemaining() {
      return remainingNumbers;
    },
    async alreadyDispatched({ eventKey, referenceKey, drawId, userId }) {
      return acceptedDispatchKeys.has(`${eventKey}:${referenceKey}:${drawId}:${userId}`);
    },
    getSmtpConfig() {
      if (smtpConfigurationError) {
        const error = new Error("manual_email_smtp_not_configured");
        error.code = "manual_email_smtp_not_configured";
        throw error;
      }
      return {
        host: "smtp.example.test",
        port: 587,
        user: "test-user",
        pass: "test-pass",
        fromEmail: "sender@example.test",
        fromName: "New Store Test",
        replyTo: "reply@example.test",
      };
    },
    createSmtpTransporter() {
      return {
        async sendMail(message) {
          smtpCalls += 1;
          sentMessages.push(message);
          if (shouldFail({ attempt: smtpCalls, message })) {
            const error = new Error("mock_smtp_failure");
            error.code = "MOCK_SMTP_FAILURE";
            throw error;
          }
          return { messageId: `mock-${smtpCalls}`, accepted: [message.to] };
        },
      };
    },
    async createCampaign() {
      campaignCalls += 1;
      return { id: `campaign-${campaignCalls}` };
    },
    async createDispatch({ eventKey, userId, drawId, payload }) {
      dispatchSequence += 1;
      const id = `dispatch-${dispatchSequence}`;
      dispatchUserById.set(id, userId);
      dispatchKeyById.set(
        id,
        `${eventKey}:${payload?.reference_key}:${drawId}:${userId}`
      );
      dispatchStatuses.set(id, "pending");
      return { id };
    },
    async markDispatchAccepted({ dispatchId }) {
      dispatchStatuses.set(dispatchId, "accepted");
      acceptedUsers.add(dispatchUserById.get(dispatchId));
      acceptedDispatchKeys.add(dispatchKeyById.get(dispatchId));
      return { id: dispatchId, status: "accepted" };
    },
    async markDispatchFailed({ dispatchId }) {
      dispatchStatuses.set(dispatchId, "failed");
      return { id: dispatchId, status: "failed" };
    },
    async updateCampaignAudienceCounts(_pgClient, campaignId, counts) {
      campaignUpdates.push({ campaignId, counts });
      return { id: campaignId, ...counts };
    },
  };

  return {
    dependencies,
    acceptedUsers,
    dispatchStatuses,
    campaignUpdates,
    sentMessages,
    get smtpCalls() {
      return smtpCalls;
    },
    get campaignCalls() {
      return campaignCalls;
    },
  };
}

function namedDrawContext({
  drawId = 145,
  drawType = "adicional",
  databaseDrawName = null,
  status = "open",
  closedAt = null,
} = {}) {
  return {
    draw: {
      id: drawId,
      status,
      draw_type: drawType,
      product_name: databaseDrawName,
      closed_at: closedAt,
    },
    databaseDrawName,
    drawUrl: `https://example.test/?draw_id=${drawId}`,
  };
}

function automaticEvent({
  eventKey = "EMAIL_DRAW_REMAINING_15",
  drawId = 145,
  drawType = "adicional",
  drawName,
  referenceKey = `additional_draw:${drawId}:email_remaining:15`,
} = {}) {
  return {
    eventKey,
    referenceType: drawType === "principal" ? "draw" : "additional_draw",
    referenceKey,
    metadata: {
      draw_id: drawId,
      draw_type: drawType,
      ...(drawName === undefined ? {} : { draw_name: drawName }),
    },
  };
}

test("nome atual do banco prevalece sobre o nome recebido do engine", () => {
  assert.equal(
    resolveDrawDisplayName({
      drawId: 145,
      drawType: "adicional",
      databaseDrawName: "  Sorteio adicional   de créditos  ",
      payloadDrawName: "Nome antigo",
    }),
    "Sorteio adicional de créditos"
  );
});

test("banco sem nome usa draw_name recebido do engine", () => {
  assert.equal(
    resolveDrawDisplayName({
      drawId: 145,
      drawType: "adicional",
      databaseDrawName: " ",
      payloadDrawName: "  Vale-compras   New Store ",
    }),
    "Vale-compras New Store"
  );
});

test("banco e engine sem nome usam fallback por tipo e ID", () => {
  assert.equal(resolveDrawDisplayName({ drawId: 140, drawType: "principal" }), "Sorteio principal");
  assert.equal(resolveDrawDisplayName({ drawId: 145, drawType: "adicional" }), "Sorteio adicional #145");
  assert.equal(resolveDrawDisplayName({ drawId: 146, drawType: "secundario" }), "Sorteio secundário #146");
});

test("principal, adicional e secundário preservam seus nomes reais", () => {
  assert.equal(resolveDrawDisplayName({ drawId: 140, drawType: "principal", databaseDrawName: "Relógio Rolex Submariner" }), "Relógio Rolex Submariner");
  assert.equal(resolveDrawDisplayName({ drawId: 145, drawType: "adicional", databaseDrawName: "Sorteio adicional de créditos" }), "Sorteio adicional de créditos");
  assert.equal(resolveDrawDisplayName({ drawId: 146, drawType: "secundario", databaseDrawName: "Vale-compras New Store" }), "Vale-compras New Store");
});

test("assunto e conteúdo usam o nome atual do banco", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness({
      recipients: users(1),
      context: namedDrawContext({ databaseDrawName: "Sorteio adicional de créditos" }),
      remainingNumbers: 15,
    });
    const result = await handleAutomaticEmailEvent(
      automaticEvent({ drawName: "Nome antigo do engine" }),
      harness.dependencies
    );

    assert.equal(result.sent, 1);
    assert.equal(
      harness.sentMessages[0].subject,
      "Restam apenas 15 números no Sorteio adicional de créditos"
    );
    assert.match(
      harness.sentMessages[0].html,
      /Faltam apenas 15 números para completar o Sorteio adicional de créditos\./
    );
    assert.doesNotMatch(harness.sentMessages[0].subject, /Nome antigo/);
  });
});

test("evento de 50 números usa o nome real no assunto", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness({
      recipients: users(1),
      context: namedDrawContext({ databaseDrawName: "Relógio Rolex Submariner" }),
      remainingNumbers: 50,
    });
    await handleAutomaticEmailEvent(automaticEvent({
      eventKey: "EMAIL_DRAW_REMAINING_50",
      referenceKey: "additional_draw:145:email_remaining:50",
    }), harness.dependencies);

    assert.equal(
      harness.sentMessages[0].subject,
      "Restam 50 números no Relógio Rolex Submariner"
    );
  });
});

test("nome recebido do engine é usado quando o banco não possui nome", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness({
      recipients: users(1),
      context: namedDrawContext({ databaseDrawName: null }),
    });
    await handleAutomaticEmailEvent(
      automaticEvent({ drawName: "Vale-compras New Store" }),
      harness.dependencies
    );

    assert.match(harness.sentMessages[0].subject, /Vale-compras New Store/);
    assert.match(harness.sentMessages[0].text, /Vale-compras New Store/);
  });
});

test("evento antigo sem draw_name continua funcionando com fallback", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness({
      recipients: users(1),
      context: namedDrawContext({ databaseDrawName: null }),
    });
    const result = await handleAutomaticEmailEvent(
      automaticEvent({ drawName: undefined }),
      harness.dependencies
    );

    assert.equal(result.sent, 1);
    assert.match(harness.sentMessages[0].subject, /Sorteio adicional #145/);
  });
});

test("assunto de encerramento usa o nome real sem duplicar a palavra sorteio", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness({
      recipients: users(1),
      context: namedDrawContext({
        drawId: 140,
        drawType: "principal",
        databaseDrawName: "Relógio Rolex Submariner",
        status: "closed",
        closedAt: "2026-07-24T21:00:00.000Z",
      }),
    });
    await handleAutomaticEmailEvent({
      ...DRAW_CLOSED_EVENT,
      metadata: { draw_id: 140, draw_type: "principal" },
      referenceKey: "draw:140:closed_email",
    }, harness.dependencies);

    assert.equal(harness.sentMessages[0].subject, "O sorteio Relógio Rolex Submariner foi encerrado");
    assert.match(harness.sentMessages[0].html, /O sorteio <strong>Relógio Rolex Submariner<\/strong> foi encerrado\./);
  });
});

test("acentos e caracteres especiais são preservados e HTML do nome é escapado", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const unsafeName = `<script>alert("x")</script> Créditos & Prêmios`;
    const harness = automaticEmailHarness({
      recipients: users(1),
      context: namedDrawContext({ databaseDrawName: unsafeName }),
    });
    await handleAutomaticEmailEvent(automaticEvent(), harness.dependencies);

    assert.match(harness.sentMessages[0].subject, /Créditos & Prêmios/);
    assert.doesNotMatch(harness.sentMessages[0].html, /<script>/i);
    assert.match(harness.sentMessages[0].html, /&lt;script&gt;alert\(&quot;x&quot;\)&lt;\/script&gt; Créditos &amp; Prêmios/);
  });
});

test("renomear sorteio não altera a deduplicação do mesmo evento", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    let contextCalls = 0;
    const harness = automaticEmailHarness({
      recipients: users(1),
      context: () => namedDrawContext({
        databaseDrawName: contextCalls++ === 0 ? "Nome anterior" : "Nome atualizado",
      }),
    });
    const event = automaticEvent();
    const first = await handleAutomaticEmailEvent(event, harness.dependencies);
    const second = await handleAutomaticEmailEvent(event, harness.dependencies);

    assert.equal(first.sent, 1);
    assert.equal(second.status, "deduped");
    assert.equal(harness.smtpCalls, 1);
  });
});

test("dois sorteios com o mesmo nome não se confundem na deduplicação", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness({
      recipients: users(1),
      context: (drawId) => namedDrawContext({
        drawId,
        databaseDrawName: "Vale-compras New Store",
      }),
    });
    const first = await handleAutomaticEmailEvent(automaticEvent({ drawId: 145 }), harness.dependencies);
    const second = await handleAutomaticEmailEvent(automaticEvent({ drawId: 146 }), harness.dependencies);

    assert.equal(first.sent, 1);
    assert.equal(second.sent, 1);
    assert.equal(harness.smtpCalls, 2);
  });
});

test("DRAW_CLOSED usa closed_at mesmo depois de o draw virar sorteado", () => {
  assert.equal(
    isDrawClosedForEmail({
      status: "sorteado",
      closed_at: "2026-07-24T21:00:00.000Z",
    }),
    true
  );
});

test("DRAW_CLOSED rejeita draw sem fechamento efetivo", () => {
  assert.equal(isDrawClosedForEmail({ status: "closed", closed_at: null }), false);
  assert.equal(isDrawClosedForEmail({ status: "open", closed_at: null }), false);
});

test("DRAW_CLOSED aceita draw fechado com closed_at", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness({ recipients: [], drawStatus: "closed" });
    const result = await handleAutomaticEmailEvent(DRAW_CLOSED_EVENT, harness.dependencies);

    assert.equal(result.status, "no_recipients");
    assert.equal(result.sent, 0);
    assert.equal(result.failed, 0);
  });
});

test("DRAW_CLOSED aceita draw sorteado com closed_at depois do D+1", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness({ recipients: [], drawStatus: "sorteado" });
    const result = await handleAutomaticEmailEvent(DRAW_CLOSED_EVENT, harness.dependencies);

    assert.equal(result.status, "no_recipients");
    assert.equal(result.sent, 0);
    assert.equal(result.failed, 0);
  });
});

test("email automático contabiliza sucesso total por destinatário", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness();
    const result = await handleAutomaticEmailEvent(DRAW_CLOSED_EVENT, harness.dependencies);

    assert.equal(harness.smtpCalls, 10);
    assert.equal(result.status, "processed");
    assert.equal(result.sent, 10);
    assert.equal(result.failed, 0);
    assert.equal(result.skipped, 0);
    assert.equal(
      [...harness.dispatchStatuses.values()].filter((status) => status === "accepted").length,
      10
    );
  });
});

test("falha SMTP parcial não interrompe os demais destinatários", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness({
      shouldFail: ({ attempt }) => attempt === 3 || attempt === 7,
    });
    const result = await handleAutomaticEmailEvent(DRAW_CLOSED_EVENT, harness.dependencies);

    assert.equal(harness.smtpCalls, 10);
    assert.equal(result.status, "partial_failure");
    assert.equal(result.sent, 8);
    assert.equal(result.failed, 2);
    assert.equal(
      [...harness.dispatchStatuses.values()].filter((status) => status === "failed").length,
      2
    );
    assert.deepEqual(harness.campaignUpdates[0].counts, {
      created: 10,
      sent: 8,
      failed: 2,
      skipped: 0,
    });
  });
});

test("falha SMTP total é relatada como failed sem fingir sucesso", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness({ shouldFail: () => true });
    const result = await handleAutomaticEmailEvent(DRAW_CLOSED_EVENT, harness.dependencies);

    assert.equal(harness.smtpCalls, 10);
    assert.equal(result.status, "failed");
    assert.equal(result.sent, 0);
    assert.equal(result.failed, 10);
    assert.equal(
      [...harness.dispatchStatuses.values()].filter((status) => status === "failed").length,
      10
    );
  });
});

test("segunda execução deduplica dispatches aceitos e não chama SMTP", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness({ recipients: users(3) });
    const first = await handleAutomaticEmailEvent(DRAW_CLOSED_EVENT, harness.dependencies);
    const second = await handleAutomaticEmailEvent(DRAW_CLOSED_EVENT, harness.dependencies);

    assert.equal(first.sent, 3);
    assert.equal(second.status, "deduped");
    assert.equal(second.sent, 0);
    assert.equal(second.failed, 0);
    assert.equal(second.skipped, 3);
    assert.equal(second.deduped, 3);
    assert.equal(harness.smtpCalls, 3);
    assert.equal(harness.campaignCalls, 1);
  });
});

test("dispatch failed permanece elegível e é reenviado na próxima execução", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness({
      recipients: users(1),
      shouldFail: ({ attempt }) => attempt === 1,
    });
    const first = await handleAutomaticEmailEvent(DRAW_CLOSED_EVENT, harness.dependencies);
    const second = await handleAutomaticEmailEvent(DRAW_CLOSED_EVENT, harness.dependencies);

    assert.equal(first.status, "failed");
    assert.equal(first.sent, 0);
    assert.equal(first.failed, 1);
    assert.equal(second.status, "processed");
    assert.equal(second.sent, 1);
    assert.equal(second.failed, 0);
    assert.equal(harness.smtpCalls, 2);
    assert.deepEqual([...harness.dispatchStatuses.values()], ["failed", "accepted"]);
  });
});

test("automação desabilitada retorna estado explícito sem resolver destinatários nem SMTP", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "false", async () => {
    let dependencyCalls = 0;
    const result = await handleAutomaticEmailEvent(DRAW_CLOSED_EVENT, {
      async loadDrawContext() {
        dependencyCalls += 1;
        throw new Error("must_not_run");
      },
    });

    assert.equal(result.status, "disabled");
    assert.equal(result.reason, "disabled");
    assert.equal(result.sent, 0);
    assert.equal(result.failed, 0);
    assert.equal(dependencyCalls, 0);
  });
});

test("SMTP ausente retorna configuration_error explícito e não cria campanha", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness({
      recipients: users(2),
      smtpConfigurationError: true,
    });
    const result = await handleAutomaticEmailEvent(DRAW_CLOSED_EVENT, harness.dependencies);

    assert.equal(result.ok, false);
    assert.equal(result.status, "configuration_error");
    assert.equal(result.reason, "manual_email_smtp_not_configured");
    assert.equal(result.sent, 0);
    assert.equal(result.failed, 0);
    assert.equal(result.skipped, 2);
    assert.equal(harness.smtpCalls, 0);
    assert.equal(harness.campaignCalls, 0);
  });
});

function fakeResponse() {
  return {
    statusCode: 200,
    body: null,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(body) {
      this.body = body;
      return this;
    },
  };
}

async function requestWithServiceError(code, message = code) {
  const error = new Error(message);
  error.code = code;
  const req = {
    body: {
      event_key: "DRAW_CLOSED",
      reference_key: "draw:42:closed_email",
      metadata: { draw_id: 42 },
    },
    get() {
      return "expected-token";
    },
  };
  const res = fakeResponse();

  await handleInternalEmailEventRequest(req, res, async () => {
    throw error;
  });

  return res;
}

test("email_draw_id_invalid retorna HTTP 400", async () => {
  await withEnv("PUSH_INTERNAL_EVENTS_TOKEN", "expected-token", async () => {
    const res = await requestWithServiceError("email_draw_id_invalid");

    assert.equal(res.statusCode, 400);
    assert.deepEqual(res.body, { ok: false, error: "email_draw_id_invalid" });
  });
});

test("email_draw_not_found retorna HTTP 404", async () => {
  await withEnv("PUSH_INTERNAL_EVENTS_TOKEN", "expected-token", async () => {
    const res = await requestWithServiceError("email_draw_not_found");

    assert.equal(res.statusCode, 404);
    assert.deepEqual(res.body, { ok: false, error: "email_draw_not_found" });
  });
});

test("erro SQL desconhecido retorna HTTP 500", async () => {
  await withEnv("PUSH_INTERNAL_EVENTS_TOKEN", "expected-token", async () => {
    const res = await requestWithServiceError("08P01", "database failure");

    assert.equal(res.statusCode, 500);
    assert.deepEqual(res.body, { ok: false, error: "08P01" });
  });
});

test("token interno inválido recusa evento antes de campanha ou SMTP", async () => {
  await withEnv("PUSH_INTERNAL_EVENTS_TOKEN", "expected-token", async () => {
    let handlerCalls = 0;
    const req = {
      body: { event_key: "DRAW_CLOSED" },
      get() {
        return "invalid-token";
      },
    };
    const res = fakeResponse();

    await handleInternalEmailEventRequest(req, res, async () => {
      handlerCalls += 1;
      return { ok: true };
    });

    assert.equal(res.statusCode, 401);
    assert.deepEqual(res.body, { ok: false, error: "internal_email_event_unauthorized" });
    assert.equal(handlerCalls, 0);
  });
});

test("payload interno inválido não inicia campanha nem SMTP", async () => {
  await withEnv("PUSH_INTERNAL_EVENTS_TOKEN", "expected-token", async () => {
    let handlerCalls = 0;
    const req = {
      body: { event_key: "NOT_ALLOWED" },
      get() {
        return "expected-token";
      },
    };
    const res = fakeResponse();

    await handleInternalEmailEventRequest(req, res, async () => {
      handlerCalls += 1;
      return { ok: true };
    });

    assert.equal(res.statusCode, 400);
    assert.deepEqual(res.body, { ok: false, error: "email_event_not_allowed" });
    assert.equal(handlerCalls, 0);
  });
});

test("rota interna aceita draw_name opcional no payload sem alterar os identificadores", async () => {
  await withEnv("PUSH_INTERNAL_EVENTS_TOKEN", "expected-token", async () => {
    let receivedEvent = null;
    const req = {
      body: {
        event_key: "EMAIL_DRAW_REMAINING_15",
        reference_key: "additional_draw:145:email_remaining:15",
        draw_id: 145,
        draw_type: "adicional",
        draw_name: "Sorteio adicional de créditos",
      },
      get() {
        return "expected-token";
      },
    };
    const res = fakeResponse();

    await handleInternalEmailEventRequest(req, res, async (event) => {
      receivedEvent = event;
      return { ok: true, status: "processed" };
    });

    assert.equal(res.statusCode, 200);
    assert.equal(receivedEvent.referenceKey, "additional_draw:145:email_remaining:15");
    assert.deepEqual(receivedEvent.metadata, {
      draw_id: 145,
      draw_type: "adicional",
      draw_name: "Sorteio adicional de créditos",
    });
  });
});
