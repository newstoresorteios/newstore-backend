import assert from "node:assert/strict";
import { readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import {
  DEFAULT_PUBLIC_SITE_URL,
  PUBLIC_SITE_URL_ENV_ORDER,
  resolvePublicSiteUrl,
} from "../src/config/publicSiteUrl.js";
import {
  handleAutomaticEmailEvent,
  loadDrawContext,
} from "../src/services/notifications/automaticEmailNotifications.js";
import { handleAutomaticBalanceEmailEvent } from "../src/services/notifications/automaticBalanceEmailNotifications.js";
import {
  buildResultEmail,
  handleAutomaticResultEmailEvent,
} from "../src/services/notifications/automaticResultEmailNotifications.js";

const OFFICIAL = "https://www.sorteionewstore.com.br";
const LEGACY = /xnamai/i;
const SITE_ENV = ["PUBLIC_APP_URL", "APP_PUBLIC_URL", "FRONTEND_URL", "SITE_URL"];

async function withEnv(values, run) {
  const names = new Set([...SITE_ENV, "NOTIFICATION_EMAIL_AUTOMATION_ENABLED", ...Object.keys(values)]);
  const previous = {};
  for (const name of names) previous[name] = process.env[name];
  for (const name of SITE_ENV) delete process.env[name];
  for (const [name, value] of Object.entries(values)) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
  try {
    return await run();
  } finally {
    for (const [name, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}

// ---------- resolvedor ----------

test("padrao oficial e o dominio NewStore", () => {
  assert.equal(DEFAULT_PUBLIC_SITE_URL, OFFICIAL);
  assert.equal(resolvePublicSiteUrl({}), OFFICIAL);
});

test("fallback quando as variaveis estao ausentes, vazias ou invalidas", () => {
  assert.equal(resolvePublicSiteUrl({ PUBLIC_APP_URL: "", FRONTEND_URL: "   ", SITE_URL: "nao-e-url" }), OFFICIAL);
  assert.equal(resolvePublicSiteUrl({ PUBLIC_APP_URL: "ftp://x.test" }), OFFICIAL);
});

test("precedencia PUBLIC_APP_URL > APP_PUBLIC_URL > FRONTEND_URL > SITE_URL", () => {
  assert.deepEqual([...PUBLIC_SITE_URL_ENV_ORDER], SITE_ENV);
  const all = {
    PUBLIC_APP_URL: "https://a.example.test",
    APP_PUBLIC_URL: "https://b.example.test",
    FRONTEND_URL: "https://c.example.test",
    SITE_URL: "https://d.example.test",
  };
  assert.equal(resolvePublicSiteUrl(all), "https://a.example.test");
  delete all.PUBLIC_APP_URL;
  assert.equal(resolvePublicSiteUrl(all), "https://b.example.test");
  delete all.APP_PUBLIC_URL;
  assert.equal(resolvePublicSiteUrl(all), "https://c.example.test");
  delete all.FRONTEND_URL;
  assert.equal(resolvePublicSiteUrl(all), "https://d.example.test");
});

test("normaliza barra final e permite http em desenvolvimento/homologacao", () => {
  assert.equal(resolvePublicSiteUrl({ PUBLIC_APP_URL: "https://stage.example.test///" }), "https://stage.example.test");
  assert.equal(resolvePublicSiteUrl({ PUBLIC_APP_URL: "http://localhost:3000/" }), "http://localhost:3000");
  assert.equal(resolvePublicSiteUrl({ PUBLIC_APP_URL: " https://www.sorteionewstore.com.br/ " }), OFFICIAL);
});

test("variavel apontando para a marca antiga e ignorada e cai para a proxima valida ou para o padrao", () => {
  const warn = console.warn;
  console.warn = () => {};
  try {
    assert.equal(resolvePublicSiteUrl({ PUBLIC_APP_URL: "https://sorteiosxnamai.com.br" }), OFFICIAL);
    assert.equal(resolvePublicSiteUrl({ PUBLIC_APP_URL: "https://www.clubxnamai.com.br", FRONTEND_URL: "https://stage.example.test" }), "https://stage.example.test");
    assert.equal(resolvePublicSiteUrl({ SITE_URL: "https://XNAMAI.example.test/x" }), OFFICIAL);
  } finally {
    console.warn = warn;
  }
});

test("PUBLIC_URL (URL do backend para webhooks de pagamento) nao interfere na URL do site", () => {
  assert.equal(resolvePublicSiteUrl({ PUBLIC_URL: "https://api.example.test" }), OFFICIAL);
});

// ---------- links reais nos e-mails ----------

function hrefs(html) {
  return [...String(html).matchAll(/href="([^"]+)"/g)].map((match) => match[1]);
}

function drawQuery(draw) {
  return async (sql) => {
    if (/FROM public\.draws/.test(sql)) return { rows: [draw] };
    if (/app_config_new/.test(sql)) return { rows: [{ id: String(draw.id), banner_title: "SORTEIO DE R$ 10.000,00 EM COMPRAS NO SITE." }] };
    return { rows: [] };
  };
}

function mailHarness(draw) {
  const mails = [];
  const dependencies = {
    loadDrawContext: (id) => loadDrawContext(id, drawQuery(draw)),
    loadRecipients: async () => [{ id: 1, name: "Ana", email: "ana@example.test" }],
    loadRemaining: async () => 47,
    alreadyDispatched: async () => false,
    getSmtpConfig: () => ({ fromName: "New Store Sorteios", fromEmail: "contato@newstore.test", replyTo: "contato@newstore.test" }),
    createSmtpTransporter: () => ({ sendMail: async (message) => { mails.push(message); return { messageId: "m1", accepted: [message.to] }; } }),
    createCampaign: async () => ({ id: "c1" }),
    createDispatch: async () => ({ id: "d1" }),
    markDispatchAccepted: async () => {},
    markDispatchFailed: async () => {},
    updateCampaignAudienceCounts: async () => {},
  };
  return { mails, dependencies };
}

const OPEN_PRINCIPAL = { id: 149, status: "open", draw_type: "principal", product_name: "Premio principal", product_link: null, opened_at: new Date(), closed_at: null };
const OPEN_ADDITIONAL = { id: 148, status: "open", draw_type: "adicional", product_name: "SORTEIO DE R$ 10.000,00 EM COMPRAS NO SITE.", product_link: null, opened_at: new Date(), closed_at: null };
const CLOSED_PRINCIPAL = { ...OPEN_PRINCIPAL, status: "closed", closed_at: new Date() };

async function sendDrawEvent(eventKey, draw, env = {}) {
  const { mails, dependencies } = mailHarness(draw);
  const prefix = draw.draw_type === "principal" ? "draw" : "additional_draw";
  await withEnv({ NOTIFICATION_EMAIL_AUTOMATION_ENABLED: "true", ...env }, () =>
    handleAutomaticEmailEvent({
      eventKey,
      referenceType: prefix,
      referenceKey: `${prefix}:${draw.id}:${eventKey}`,
      metadata: { draw_id: draw.id },
    }, dependencies));
  assert.equal(mails.length, 1);
  return mails[0];
}

for (const [label, draw] of [["principal", OPEN_PRINCIPAL], ["adicional", OPEN_ADDITIONAL]]) {
  for (const eventKey of ["NEW_DRAW_PUBLISHED", "EMAIL_DRAW_REMAINING_75", "EMAIL_DRAW_REMAINING_50"]) {
    test(`${eventKey} (${label}): botao e texto levam ao sorteio no site NewStore (sem variaveis)`, async () => {
      const mail = await sendDrawEvent(eventKey, draw);
      const expected = `${OFFICIAL}/?draw_id=${draw.id}`;
      assert.ok(hrefs(mail.html).includes(expected), `html sem ${expected}: ${hrefs(mail.html)}`);
      assert.ok(mail.text.includes(expected));
      assert.doesNotMatch(mail.html, LEGACY);
      assert.doesNotMatch(mail.text, LEGACY);
      assert.doesNotMatch(mail.subject, LEGACY);
    });
  }
}

test("link do e-mail usa PUBLIC_APP_URL quando definida (homologacao)", async () => {
  const mail = await sendDrawEvent("EMAIL_DRAW_REMAINING_75", OPEN_ADDITIONAL, { PUBLIC_APP_URL: "https://stage.example.test/" });
  assert.ok(hrefs(mail.html).includes("https://stage.example.test/?draw_id=148"));
  assert.ok(mail.text.includes("https://stage.example.test/?draw_id=148"));
});

test("PUBLIC_APP_URL herdada da marca antiga nao chega ao cliente", async () => {
  const warn = console.warn;
  console.warn = () => {};
  try {
    const mail = await sendDrawEvent("NEW_DRAW_PUBLISHED", OPEN_PRINCIPAL, { PUBLIC_APP_URL: "https://sorteiosxnamai.com.br" });
    assert.ok(hrefs(mail.html).includes(`${OFFICIAL}/?draw_id=149`));
    assert.doesNotMatch(mail.html + mail.text, LEGACY);
  } finally {
    console.warn = warn;
  }
});

test("DRAW_CLOSED: sem link de outra marca e com o canal oficial da Caixa", async () => {
  const mail = await sendDrawEvent("DRAW_CLOSED", CLOSED_PRINCIPAL);
  assert.ok(hrefs(mail.html).every((href) => !LEGACY.test(href)));
  assert.ok(mail.html.includes("youtube.com/@caixa"));
  assert.doesNotMatch(mail.text, LEGACY);
});

test("e-mail de saldo e cupom leva a /conta no site NewStore", async () => {
  const mails = [];
  const dependencies = {
    loadBalanceContext: async () => ({
      user_id: 7, name: "Ana", email: "ana@example.test", balance_cents: 5000,
      balance_reference_at: new Date("2026-10-01T00:00:00Z"), expires_at: new Date("2026-11-08T00:00:00Z"),
      expires_on: "2026-11-08", days_to_expire: 30, expiry_source: "coupon",
    }),
    alreadyDispatched: async () => false,
    getSmtpConfig: () => ({ fromName: "New Store Sorteios", fromEmail: "contato@newstore.test", replyTo: "contato@newstore.test" }),
    createSmtpTransporter: () => ({ sendMail: async (message) => { mails.push(message); return { messageId: "m", accepted: [message.to] }; } }),
    createCampaign: async () => ({ id: "c" }),
    createDispatch: async () => ({ id: "d" }),
    markDispatchAccepted: async () => {},
    markDispatchFailed: async () => {},
    updateCampaignAudienceCounts: async () => {},
  };
  await withEnv({ NOTIFICATION_EMAIL_AUTOMATION_ENABLED: "true" }, () =>
    handleAutomaticBalanceEmailEvent({ eventKey: "EMAIL_BALANCE_EXPIRING_30_DAYS", metadata: { user_id: 7 } }, dependencies));
  assert.equal(mails.length, 1);
  assert.ok(hrefs(mails[0].html).includes(`${OFFICIAL}/conta`), hrefs(mails[0].html).join(","));
  assert.ok(mails[0].text.includes(`${OFFICIAL}/conta`));
  assert.doesNotMatch(mails[0].html + mails[0].text, LEGACY);
});

test("e-mails de resultado (vencedor, nao contemplado, administracao): links corretos, principal e adicional", async () => {
  for (const draw of [
    { id: 150, status: "sorteado", draw_type: "principal", product_name: "Moto", winner_number: 7, winner_user_id: 11, realized_at: new Date() },
    { id: 151, status: "sorteado", draw_type: "adicional", product_name: "Compras", winner_number: 7, winner_user_id: 11, realized_at: new Date() },
  ]) {
    await withEnv({}, async () => {
      const participant = buildResultEmail("EMAIL_RESULT_PARTICIPANT", { draw, winner: { id: 11 }, recipient: { name: "Ana" } });
      assert.ok(hrefs(participant.html).length === 0 || hrefs(participant.html).every((href) => !LEGACY.test(href)));
      assert.ok(participant.text.includes(`${OFFICIAL}/?draw_id=${draw.id}`));
      assert.doesNotMatch(participant.text + participant.html, LEGACY);
      for (const key of ["EMAIL_RESULT_WINNER", "EMAIL_RESULT_ADMIN"]) {
        const mail = buildResultEmail(key, { draw, winner: { id: 11, name: "V", email: "v@example.test" }, recipient: { name: "V" } });
        assert.doesNotMatch(mail.text + mail.html + mail.subject, LEGACY);
      }
    });
  }
});

test("evento de resultado completo: nenhum link da marca antiga no e-mail enviado", async () => {
  const mails = [];
  const draw = { id: 150, status: "sorteado", draw_type: "principal", product_name: "Moto", banner_title: null, winner_number: 7, winner_user_id: 11, realized_at: new Date(), closed_at: new Date() };
  const dependencies = {
    now: () => new Date(),
    resultConfig: () => ({ effectiveFrom: new Date("2026-01-01T00:00:00Z"), maxAgeHours: 192, pendingStaleMinutes: 20, maxAttempts: 5, adminEmail: "admin@example.test" }),
    loadResultDraw: async () => ({ ...draw }),
    loadResultWinner: async () => ({ id: 11, name: "V", email: "v@example.test" }),
    loadResultParticipants: async () => [{ id: 21, name: "Ana", email: "ana@example.test" }],
    loadDispatchHistory: async () => [],
    acquireResultEventLock: async () => async () => {},
    getSmtpConfig: () => ({ fromName: "New Store Sorteios", fromEmail: "contato@newstore.test", replyTo: "contato@newstore.test" }),
    createSmtpTransporter: () => ({ sendMail: async (message) => { mails.push(message); return { messageId: "m", accepted: [message.to] }; } }),
    createCampaign: async () => ({ id: "c" }),
    createDispatch: async () => ({ id: "d" }),
    markDispatchAccepted: async () => {},
    markDispatchFailed: async () => {},
    updateCampaignAudienceCounts: async () => {},
  };
  await withEnv({ NOTIFICATION_EMAIL_AUTOMATION_ENABLED: "true" }, async () => {
    for (const key of ["EMAIL_RESULT_WINNER", "EMAIL_RESULT_PARTICIPANT", "EMAIL_RESULT_ADMIN"]) {
      await handleAutomaticResultEmailEvent({
        eventKey: key,
        referenceType: "draw",
        referenceKey: `draw:150:${{ EMAIL_RESULT_WINNER: "result_winner_email", EMAIL_RESULT_PARTICIPANT: "result_participant_email", EMAIL_RESULT_ADMIN: "result_admin_email" }[key]}`,
        metadata: { draw_id: 150 },
      }, dependencies);
    }
  });
  assert.equal(mails.length, 3);
  for (const mail of mails) assert.doesNotMatch(`${mail.subject}${mail.text}${mail.html}`, LEGACY);
  assert.ok(mails.find((mail) => mail.to === "ana@example.test").text.includes(`${OFFICIAL}/?draw_id=150`));
});

// ---------- trava estatica ----------

test("nenhum arquivo de src referencia dominios da marca antiga (exceto o detector em publicSiteUrl.js)", () => {
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "src");
  const offenders = [];
  const walk = (dir) => {
    for (const entry of readdirSync(dir)) {
      const full = path.join(dir, entry);
      if (statSync(full).isDirectory()) walk(full);
      else if (/\.(js|json|html|md|sql)$/.test(entry) && !full.endsWith(path.join("config", "publicSiteUrl.js"))) {
        if (LEGACY.test(readFileSync(full, "utf8"))) offenders.push(path.relative(root, full));
      }
    }
  };
  walk(root);
  assert.deepEqual(offenders, []);
});
