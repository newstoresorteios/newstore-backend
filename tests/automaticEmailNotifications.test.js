import assert from "node:assert/strict";
import test from "node:test";

import {
  handleAutomaticEmailEvent,
  isDrawClosedForEmail,
} from "../src/services/notifications/automaticEmailNotifications.js";
import { handleInternalEmailEventRequest } from "../src/routes/internal_email_events.js";

const DRAW_CLOSED_EVENT = {
  eventKey: "DRAW_CLOSED",
  referenceType: "draw",
  referenceKey: "draw:42:closed_email",
  metadata: { draw_id: 42 },
  occurredAt: "2026-07-24T21:00:00.000Z",
};

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
  shouldFail = () => false,
  smtpConfigurationError = false,
} = {}) {
  const acceptedUsers = new Set();
  const dispatchUserById = new Map();
  const dispatchStatuses = new Map();
  const campaignUpdates = [];
  let smtpCalls = 0;
  let campaignCalls = 0;
  let dispatchSequence = 0;

  const dependencies = {
    async loadDrawContext() {
      return drawContext(drawStatus, closedAt);
    },
    async loadRecipients() {
      return recipients;
    },
    async alreadyDispatched({ userId }) {
      return acceptedUsers.has(userId);
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
    async createDispatch({ userId }) {
      dispatchSequence += 1;
      const id = `dispatch-${dispatchSequence}`;
      dispatchUserById.set(id, userId);
      dispatchStatuses.set(id, "pending");
      return { id };
    },
    async markDispatchAccepted({ dispatchId }) {
      dispatchStatuses.set(dispatchId, "accepted");
      acceptedUsers.add(dispatchUserById.get(dispatchId));
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
    get smtpCalls() {
      return smtpCalls;
    },
    get campaignCalls() {
      return campaignCalls;
    },
  };
}

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
