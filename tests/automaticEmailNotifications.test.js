import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  handleAutomaticEmailEvent,
  isDrawClosedForEmail,
  loadDrawContext,
  loadRecipients,
  resolveDrawDisplayName,
  resolveDrawTypeLabel,
} from "../src/services/notifications/automaticEmailNotifications.js";
import {
  balanceStageMatches,
  buildBalanceReferenceKey,
  expiredBalanceIsEligible,
  formatBalanceExpiryDate,
  formatBalanceValue,
  loadBalanceExpiryContext,
  previousCalendarDateKey,
} from "../src/services/notifications/automaticBalanceEmailNotifications.js";
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
  balanceContext = null,
  shouldFail = () => false,
  smtpConfigurationError = false,
} = {}) {
  const acceptedUsers = new Set();
  const acceptedDispatchKeys = new Set();
  const dispatchUserById = new Map();
  const dispatchKeyById = new Map();
  const dispatchStatuses = new Map();
  const campaignUpdates = [];
  const campaigns = [];
  const dispatches = [];
  const acceptedResults = [];
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
    async loadBalanceContext() {
      return typeof balanceContext === "function" ? balanceContext() : balanceContext;
    },
    async alreadyDispatched({ eventKey, referenceKey, legacyReferenceKey, drawId, userId }) {
      if (acceptedDispatchKeys.has(`${eventKey}:${referenceKey}:${drawId}:${userId}`)) return true;
      if (legacyReferenceKey && acceptedDispatchKeys.has(`${eventKey}:${legacyReferenceKey}:${drawId}:${userId}`)) {
        return true;
      }
      return false;
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
    async createCampaign(input) {
      campaignCalls += 1;
      campaigns.push(input);
      return { id: `campaign-${campaignCalls}` };
    },
    async createDispatch(input) {
      const { eventKey, userId, drawId, payload } = input;
      dispatchSequence += 1;
      const id = `dispatch-${dispatchSequence}`;
      dispatches.push({ ...input, id });
      dispatchUserById.set(id, userId);
      dispatchKeyById.set(
        id,
        `${eventKey}:${payload?.reference_key}:${drawId}:${userId}`
      );
      dispatchStatuses.set(id, "pending");
      return { id };
    },
    async markDispatchAccepted({ dispatchId, result }) {
      dispatchStatuses.set(dispatchId, "accepted");
      acceptedResults.push(result);
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
    campaigns,
    dispatches,
    acceptedResults,
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

function currentBalanceContext({
  userId = 123,
  name = "Maria Cliente",
  email = "maria@example.test",
  balanceCents = 15000,
  expiresOn = "2026-09-05",
  daysToExpire = 30,
  expirySource = "last_approved_purchase",
  balanceReferenceAt = "2026-03-05T15:00:00.000Z",
} = {}) {
  return {
    user_id: userId,
    name,
    email,
    balance_cents: balanceCents,
    balance_reference_at: balanceReferenceAt,
    expires_at: expiresOn ? `${expiresOn}T03:00:00.000Z` : null,
    expires_on: expiresOn,
    days_to_expire: daysToExpire,
    expiry_source: expirySource,
  };
}

function balanceEvent({
  eventKey = "EMAIL_BALANCE_EXPIRING_30_DAYS",
  userId = 123,
  referenceKey = "engine-reference-is-not-authoritative",
} = {}) {
  return {
    eventKey,
    referenceType: "user_balance",
    referenceKey,
    metadata: {
      user_id: userId,
      balance_cents: 1,
      expires_at: "2000-01-01T00:00:00.000Z",
      days_to_expire: -999,
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
  assert.equal(resolveDrawDisplayName({ drawId: 140, drawType: "principal" }), "Sorteio principal #140");
  assert.equal(resolveDrawDisplayName({ drawId: 145, drawType: "adicional" }), "Sorteio adicional #145");
  assert.equal(resolveDrawDisplayName({ drawId: 146, drawType: "secundario" }), "Sorteio secundário #146");
});

test("principal, adicional e secundário preservam seus nomes reais", () => {
  assert.equal(resolveDrawDisplayName({ drawId: 140, drawType: "principal", databaseDrawName: "Relógio Rolex Submariner" }), "Relógio Rolex Submariner");
  assert.equal(resolveDrawDisplayName({ drawId: 145, drawType: "adicional", databaseDrawName: "Sorteio adicional de créditos" }), "Sorteio adicional de créditos");
  assert.equal(resolveDrawDisplayName({ drawId: 146, drawType: "secundario", databaseDrawName: "Vale-compras New Store" }), "Vale-compras New Store");
});

test("contexto do sorteio consulta product_name e usa banner_title distinto como descrição", async () => {
  const context = await loadDrawContext(145, async (sql, params) => {
    if (/FROM public\.draws/.test(sql)) {
      assert.deepEqual(params, [145]);
      return {
        rows: [{
          id: 145,
          status: "open",
          draw_type: "adicional",
          product_name: "R$ 2.500 em compras no site",
          product_link: null,
          opened_at: "2026-08-01T00:00:00.000Z",
          closed_at: null,
        }],
      };
    }
    if (/FROM public\.app_config_new/.test(sql)) {
      assert.deepEqual(params, ["145"]);
      return { rows: [{ id: "145", banner_title: "Vale-compras para usar na New Store" }] };
    }
    throw new Error(`unexpected query: ${sql}`);
  });

  assert.equal(context.drawName, "R$ 2.500 em compras no site");
  assert.equal(context.drawDescription, "Vale-compras para usar na New Store");
  assert.equal(context.drawTypeLabel, "Sorteio adicional");
  assert.equal(context.drawDisplayTitle, "Sorteio adicional — R$ 2.500 em compras no site");
  assert.equal(context.drawStatusLabel, "Aberto");
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
      "Restam 15 números no sorteio adicional — Sorteio adicional de créditos"
    );
    assert.match(
      harness.sentMessages[0].html,
      /Restam <strong>15 números<\/strong> no Sorteio adicional — Sorteio adicional de créditos\./
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
      "Restam 50 números no sorteio adicional — Relógio Rolex Submariner"
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

test("assunto de encerramento distingue tipo e usa o nome real", async () => {
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

    assert.equal(harness.sentMessages[0].subject, "Sorteio principal — Relógio Rolex Submariner — encerrado");
    assert.match(harness.sentMessages[0].html, /O <strong>Sorteio principal — Relógio Rolex Submariner<\/strong> foi encerrado\./);
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

const BALANCE_STAGE_CASES = [
  ["EMAIL_BALANCE_EXPIRING_30_DAYS", 30, "Seu saldo de R$ 150,00 vence em 30 dias"],
  ["EMAIL_BALANCE_EXPIRING_20_DAYS", 20, "Faltam 20 dias para usar seu saldo de R$ 150,00"],
  ["EMAIL_BALANCE_EXPIRING_10_DAYS", 10, "Atenção: seu saldo vence em 10 dias"],
  ["EMAIL_BALANCE_EXPIRING_7_DAYS", 7, "Seu saldo vence em 7 dias"],
  ["EMAIL_BALANCE_EXPIRING_3_DAYS", 3, "Últimos 3 dias para usar seu saldo de R$ 150,00"],
];

for (const [eventKey, daysToExpire, expectedSubject] of BALANCE_STAGE_CASES) {
  test(`${eventKey} envia somente para o usuário do saldo`, async () => {
    await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
      const harness = automaticEmailHarness({
        balanceContext: currentBalanceContext({ daysToExpire }),
      });
      const result = await handleAutomaticEmailEvent(balanceEvent({ eventKey }), harness.dependencies);

      assert.equal(result.status, "processed");
      assert.equal(result.sent, 1);
      assert.equal(harness.smtpCalls, 1);
      assert.equal(harness.sentMessages[0].to, "maria@example.test");
      assert.equal(harness.sentMessages[0].subject, expectedSubject);
      assert.match(harness.sentMessages[0].html, /05\/09\/2026/);
      assert.match(harness.sentMessages[0].text, /R\$ 150,00/);
    });
  });
}

test("EMAIL_BALANCE_EXPIRED envia quando o vencimento respeita effective_from", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    await withEnv("EMAIL_BALANCE_AUTOMATION_EFFECTIVE_FROM", "2026-09-01", async () => {
      await withEnv("EMAIL_BALANCE_EXPIRED_BACKFILL_ENABLED", undefined, async () => {
        const harness = automaticEmailHarness({
          balanceContext: currentBalanceContext({ daysToExpire: -1 }),
        });
        const result = await handleAutomaticEmailEvent(
          balanceEvent({ eventKey: "EMAIL_BALANCE_EXPIRED" }),
          harness.dependencies
        );

        assert.equal(result.status, "processed");
        assert.equal(harness.sentMessages[0].subject, "O prazo do seu saldo de R$ 150,00 terminou");
        assert.match(harness.sentMessages[0].text, /terminou em 05\/09\/2026/);
      });
    });
  });
});

test("evento de saldo sem saldo atual é ignorado", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness({ balanceContext: null });
    const result = await handleAutomaticEmailEvent(balanceEvent(), harness.dependencies);
    assert.equal(result.status, "skipped");
    assert.equal(result.reason, "balance_not_positive");
    assert.equal(harness.smtpCalls, 0);
  });
});

for (const email of [null, "email-invalido"]) {
  test(`evento de saldo rejeita destinatário ${email === null ? "sem e-mail" : "com e-mail inválido"}`, async () => {
    await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
      const harness = automaticEmailHarness({
        balanceContext: currentBalanceContext({ email }),
      });
      const result = await handleAutomaticEmailEvent(balanceEvent(), harness.dependencies);
      assert.equal(result.status, "skipped");
      assert.equal(result.reason, "balance_email_invalid");
      assert.equal(harness.smtpCalls, 0);
    });
  });
}

test("usuário sem fonte determinística de vencimento é auditado e não recebe e-mail", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness({
      balanceContext: currentBalanceContext({
        expiresOn: null,
        daysToExpire: null,
        expirySource: null,
        balanceReferenceAt: null,
      }),
    });
    const result = await handleAutomaticEmailEvent(balanceEvent(), harness.dependencies);
    assert.equal(result.status, "skipped");
    assert.equal(result.reason, "balance_expiry_source_missing");
    assert.equal(harness.smtpCalls, 0);
  });
});

test("estágio recebido diferente do vencimento atual é ignorado", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness({
      balanceContext: currentBalanceContext({ daysToExpire: 20 }),
    });
    const result = await handleAutomaticEmailEvent(balanceEvent(), harness.dependencies);
    assert.equal(result.status, "skipped");
    assert.equal(result.reason, "balance_stage_mismatch");
    assert.equal(harness.smtpCalls, 0);
  });
});

test("deduplicação de saldo usa usuário, data de vencimento e estágio com draw_id nulo", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness({
      balanceContext: currentBalanceContext(),
    });
    const first = await handleAutomaticEmailEvent(balanceEvent(), harness.dependencies);
    const second = await handleAutomaticEmailEvent(balanceEvent(), harness.dependencies);

    assert.equal(first.reference_key, "user_balance:123:expires:2026-09-05:email:30_days");
    assert.equal(second.status, "deduped");
    assert.equal(harness.smtpCalls, 1);
    assert.equal(harness.dispatches[0].drawId, null);
  });
});

test("nova data de validade permite uma nova sequência para o mesmo usuário", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    let expiresOn = "2026-09-05";
    const harness = automaticEmailHarness({
      balanceContext: () => currentBalanceContext({ expiresOn }),
    });
    const first = await handleAutomaticEmailEvent(balanceEvent(), harness.dependencies);
    expiresOn = "2026-10-05";
    const second = await handleAutomaticEmailEvent(balanceEvent(), harness.dependencies);

    assert.equal(first.sent, 1);
    assert.equal(second.sent, 1);
    assert.notEqual(first.reference_key, second.reference_key);
    assert.equal(harness.smtpCalls, 2);
  });
});

test("vencido anterior ao effective_from é ignorado com backfill desativado por padrão", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    await withEnv("EMAIL_BALANCE_AUTOMATION_EFFECTIVE_FROM", "2026-10-01", async () => {
      await withEnv("EMAIL_BALANCE_EXPIRED_BACKFILL_ENABLED", undefined, async () => {
        const harness = automaticEmailHarness({
          balanceContext: currentBalanceContext({ daysToExpire: -20 }),
        });
        const result = await handleAutomaticEmailEvent(
          balanceEvent({ eventKey: "EMAIL_BALANCE_EXPIRED" }),
          harness.dependencies
        );
        assert.equal(result.status, "skipped");
        assert.equal(result.reason, "balance_expired_before_effective_from");
        assert.equal(harness.smtpCalls, 0);
      });
    });
  });
});

test("backfill de vencidos exige ativação explícita", async () => {
  await withEnv("EMAIL_BALANCE_AUTOMATION_EFFECTIVE_FROM", undefined, async () => {
    await withEnv("EMAIL_BALANCE_EXPIRED_BACKFILL_ENABLED", undefined, async () => {
      assert.equal(expiredBalanceIsEligible("2026-09-05"), false);
    });
    await withEnv("EMAIL_BALANCE_EXPIRED_BACKFILL_ENABLED", "true", async () => {
      assert.equal(expiredBalanceIsEligible("2026-09-05"), true);
    });
  });
});

test("campanha de saldo registra audiência unitária e snapshots canônicos", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness({ balanceContext: currentBalanceContext() });
    await handleAutomaticEmailEvent(balanceEvent(), harness.dependencies);

    assert.equal(harness.campaigns[0].audienceCountExpected, 1);
    assert.equal(harness.campaigns[0].audienceFilter, "specific_user");
    assert.equal(harness.campaigns[0].payload.balance_cents, 15000);
    assert.equal(harness.campaigns[0].payload.expires_date, "05/09/2026");
    assert.equal(harness.dispatches[0].userId, 123);
    assert.equal(harness.dispatches[0].drawId, null);
    assert.equal(harness.acceptedResults[0].delivery_status, "unknown");
  });
});

test("formatadores de saldo usam reais e data brasileira", () => {
  assert.equal(formatBalanceValue(15000), "R$ 150,00");
  assert.equal(formatBalanceExpiryDate("2026-09-05"), "05/09/2026");
  assert.equal(balanceStageMatches("EMAIL_BALANCE_EXPIRING_7_DAYS", 7), true);
  assert.equal(balanceStageMatches("EMAIL_BALANCE_EXPIRED", -1), true);
  assert.equal(
    buildBalanceReferenceKey({ userId: 123, expiresOn: "2026-09-05", eventKey: "EMAIL_BALANCE_EXPIRING_3_DAYS" }),
    "user_balance:123:expires:2026-09-05:email:3_days"
  );
});

test("view canônica é consultada por user_id e não por e-mail", async () => {
  let capturedSql = null;
  let capturedParams = null;
  await loadBalanceExpiryContext(123, async (sql, params) => {
    capturedSql = sql;
    capturedParams = params;
    return { rows: [] };
  });
  assert.match(capturedSql, /public\.user_coupon_balance_expiry/);
  assert.match(capturedSql, /WHERE user_id = \$1/);
  assert.doesNotMatch(capturedSql, /WHERE email/);
  assert.deepEqual(capturedParams, [123]);
});

test("expires_on é trazido como texto pelo SQL (sem virar objeto Date sujeito a fuso)", async () => {
  let capturedSql = null;
  await loadBalanceExpiryContext(326, async (sql) => {
    capturedSql = sql;
    return { rows: [] };
  });
  assert.match(capturedSql, /expires_on::text/);
});

test("expires_on = 2026-08-09 permanece 2026-08-09", () => {
  assert.equal(formatBalanceExpiryDate("2026-08-09").split("/").reverse().join("-"), "2026-08-09");
});

test("reference key não recua um dia (caso real user 326)", () => {
  const referenceKey = buildBalanceReferenceKey({
    userId: 326,
    expiresOn: "2026-08-09",
    eventKey: "EMAIL_BALANCE_EXPIRING_3_DAYS",
  });
  assert.equal(referenceKey, "user_balance:326:expires:2026-08-09:email:3_days");
  assert.notEqual(referenceKey, "user_balance:326:expires:2026-08-08:email:3_days");
});

test("data brasileira aparece como 09/08/2026", () => {
  assert.equal(formatBalanceExpiryDate("2026-08-09"), "09/08/2026");
});

test("expires_at em UTC não substitui expires_on quando ambos estão presentes", () => {
  // 2026-08-09T02:00:00Z equivale a 2026-08-08 23:00 em America/Sao_Paulo:
  // se o texto do e-mail usasse expires_at como fonte, sairia 08/08/2026.
  const context = { expires_on: "2026-08-09", expires_at: "2026-08-09T02:00:00.000Z" };
  assert.equal(formatBalanceExpiryDate(context.expires_on || context.expires_at), "09/08/2026");
});

test("previousCalendarDateKey desloca um dia por aritmética de calendário pura, sem fuso", () => {
  assert.equal(previousCalendarDateKey("2026-08-09"), "2026-08-08");
  assert.equal(previousCalendarDateKey("2026-08-01"), "2026-07-31");
  assert.equal(previousCalendarDateKey("2027-01-01"), "2026-12-31");
});

for (const [userId, expiresOn, daysToExpire, eventKey, referenceStage] of [
  [326, "2026-08-09", 3, "EMAIL_BALANCE_EXPIRING_3_DAYS", "3_days"],
  [284, "2026-08-26", 20, "EMAIL_BALANCE_EXPIRING_20_DAYS", "20_days"],
  [315, "2026-09-05", 30, "EMAIL_BALANCE_EXPIRING_30_DAYS", "30_days"],
]) {
  test(`usuário ${userId} permanece no estágio ${referenceStage} com a data canônica correta`, async () => {
    const harness = automaticEmailHarness({
      balanceContext: currentBalanceContext({ userId, expiresOn, daysToExpire }),
    });
    await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
      const result = await handleAutomaticEmailEvent(balanceEvent({ userId, eventKey }), harness.dependencies);
      assert.equal(result.status, "processed");
      assert.equal(result.reference_key, `user_balance:${userId}:expires:${expiresOn}:email:${referenceStage}`);
    });
  });
}

test("dispatch histórico com a chave anterior (um dia a menos) é deduplicado e não reenvia", async () => {
  const harness = automaticEmailHarness({
    balanceContext: currentBalanceContext({ userId: 326, expiresOn: "2026-08-09", daysToExpire: 3, balanceCents: 22000 }),
  });
  const legacyReferenceKey = "user_balance:326:expires:2026-08-08:email:3_days";
  const seeded = await harness.dependencies.createDispatch({
    eventKey: "EMAIL_BALANCE_EXPIRING_3_DAYS",
    userId: 326,
    drawId: null,
    payload: { reference_key: legacyReferenceKey },
  });
  await harness.dependencies.markDispatchAccepted({ dispatchId: seeded.id, result: { ok: true } });

  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const result = await handleAutomaticEmailEvent(
      balanceEvent({ userId: 326, eventKey: "EMAIL_BALANCE_EXPIRING_3_DAYS" }),
      harness.dependencies
    );
    assert.equal(result.status, "deduped");
  });
  assert.equal(harness.smtpCalls, 0);
});

test("retry após a correção não gera novo envio para dispatch histórico legado", async () => {
  const harness = automaticEmailHarness({
    balanceContext: currentBalanceContext({ userId: 284, expiresOn: "2026-08-26", daysToExpire: 20, balanceCents: 38500 }),
  });
  const legacyReferenceKey = "user_balance:284:expires:2026-08-25:email:20_days";
  const seeded = await harness.dependencies.createDispatch({
    eventKey: "EMAIL_BALANCE_EXPIRING_20_DAYS",
    userId: 284,
    drawId: null,
    payload: { reference_key: legacyReferenceKey },
  });
  await harness.dependencies.markDispatchAccepted({ dispatchId: seeded.id, result: { ok: true } });

  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    for (let attempt = 0; attempt < 2; attempt += 1) {
      const result = await handleAutomaticEmailEvent(
        balanceEvent({ userId: 284, eventKey: "EMAIL_BALANCE_EXPIRING_20_DAYS" }),
        harness.dependencies
      );
      assert.equal(result.status, "deduped");
    }
  });
  assert.equal(harness.smtpCalls, 0);
});

test("eventos de sorteio permanecem intactos após a correção da data de saldo", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness();
    const result = await handleAutomaticEmailEvent(DRAW_CLOSED_EVENT, harness.dependencies);
    assert.equal(result.status, "processed");
    assert.equal(result.sent, 10);
    assert.equal(result.failed, 0);
  });
});

test("migration centraliza seis meses e não usa fallback móvel com NOW", async () => {
  const sql = await readFile(new URL("../src/migrations/027_user_coupon_balance_expiry.sql", import.meta.url), "utf8");
  assert.match(sql, /INTERVAL '6 months'/);
  assert.match(sql, /America\/Sao_Paulo/g);
  assert.match(sql, /coupon_value_cents/);
  assert.doesNotMatch(sql, /COALESCE\([^)]*NOW\(\)[^)]*\)\s*\+\s*INTERVAL '6 months'/i);
});

for (const [drawType, expectedLabel] of [
  ["principal", "Sorteio principal"],
  ["adicional", "Sorteio adicional"],
  ["secundario", "Sorteio secundário"],
]) {
  test(`${drawType} usa label, nome, descrição e situação atuais`, async () => {
    await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
      const harness = automaticEmailHarness({
        recipients: users(1),
        context: {
          ...namedDrawContext({ drawType, databaseDrawName: "Prêmio atual" }),
          drawDescription: "Descrição atual do banco",
          drawStatusLabel: "Aberto",
        },
        remainingNumbers: 15,
      });
      await handleAutomaticEmailEvent(automaticEvent({ drawType }), harness.dependencies);

      assert.equal(resolveDrawTypeLabel(drawType), expectedLabel);
      assert.match(harness.sentMessages[0].subject, new RegExp(expectedLabel.toLocaleLowerCase("pt-BR")));
      assert.match(harness.sentMessages[0].html, /Prêmio atual/);
      assert.match(harness.sentMessages[0].html, /Descrição atual do banco/);
      assert.match(harness.sentMessages[0].html, /Situação:<\/strong> Aberto/);
      assert.equal(harness.campaigns[0].payload.draw_type_label, expectedLabel);
      assert.equal(harness.dispatches[0].payload.draw_description, "Descrição atual do banco");
    });
  });
}

test("nome atualizado no banco aparece no próximo evento sem alterar reference_key", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    let name = "Nome antigo";
    const harness = automaticEmailHarness({
      recipients: users(1),
      context: () => namedDrawContext({ databaseDrawName: name }),
    });
    const first = await handleAutomaticEmailEvent(automaticEvent(), harness.dependencies);
    name = "Nome atual do banco";
    const second = await handleAutomaticEmailEvent(
      automaticEvent({ referenceKey: "additional_draw:145:email_remaining:15:new-cycle" }),
      harness.dependencies
    );

    assert.equal(first.reference_key, "additional_draw:145:email_remaining:15");
    assert.match(harness.sentMessages[1].subject, /Nome atual do banco/);
    assert.doesNotMatch(harness.sentMessages[1].subject, /Nome antigo/);
    assert.equal(second.sent, 1);
  });
});

test("retry após falha SMTP de saldo cria nova tentativa sem duplicar aceite", async () => {
  await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
    const harness = automaticEmailHarness({
      balanceContext: currentBalanceContext(),
      shouldFail: ({ attempt }) => attempt === 1,
    });
    const first = await handleAutomaticEmailEvent(balanceEvent(), harness.dependencies);
    const second = await handleAutomaticEmailEvent(balanceEvent(), harness.dependencies);
    const third = await handleAutomaticEmailEvent(balanceEvent(), harness.dependencies);

    assert.equal(first.status, "failed");
    assert.equal(second.status, "processed");
    assert.equal(third.status, "deduped");
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

test("rota interna aceita evento individual de user_balance sem exigir draw_id", async () => {
  await withEnv("PUSH_INTERNAL_EVENTS_TOKEN", "expected-token", async () => {
    let receivedEvent = null;
    const req = {
      body: {
        event_key: "EMAIL_BALANCE_EXPIRING_30_DAYS",
        reference_type: "user_balance",
        reference_key: "user_balance:123:expires:2026-09-05:email:30_days",
        user_id: 123,
        balance_cents: 15000,
        expires_at: "2026-09-05T03:00:00.000Z",
        days_to_expire: 30,
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
    assert.equal(receivedEvent.referenceType, "user_balance");
    assert.equal(receivedEvent.metadata.user_id, 123);
    assert.equal(receivedEvent.metadata.draw_id, undefined);
  });
});

test("evento de saldo com user_id inválido retorna HTTP 400", async () => {
  await withEnv("PUSH_INTERNAL_EVENTS_TOKEN", "expected-token", async () => {
    await withEnv("NOTIFICATION_EMAIL_AUTOMATION_ENABLED", "true", async () => {
      const req = {
        body: {
          event_key: "EMAIL_BALANCE_EXPIRING_30_DAYS",
          reference_type: "user_balance",
          reference_key: "invalid-user",
          metadata: { user_id: "abc" },
        },
        get() {
          return "expected-token";
        },
      };
      const res = fakeResponse();
      await handleInternalEmailEventRequest(req, res);
      assert.equal(res.statusCode, 400);
      assert.deepEqual(res.body, { ok: false, error: "email_user_id_invalid" });
    });
  });
});
