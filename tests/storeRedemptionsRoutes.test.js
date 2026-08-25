// tests/storeRedemptionsRoutes.test.js
// Guardas do resgate real. FASE E BLOQUEADA — ver relatorio: confirm
// existe e e seguro (nunca perde credito), mas nao cria pedido Tray real
// ainda.
import test, { before, after } from "node:test";
import assert from "node:assert/strict";

process.env.JWT_SECRET = process.env.JWT_SECRET || "test-secret-redemptions";

let server;
let baseUrl;
let jwt;

before(async () => {
  const [{ default: express }, { default: jsonwebtoken }, redemptions] = await Promise.all([
    import("express"),
    import("jsonwebtoken"),
    import("../src/routes/store_redemptions.js"),
  ]);
  jwt = jsonwebtoken;

  const app = express();
  app.use(express.json());
  app.use("/api/store/redemptions", redemptions.default);

  await new Promise((resolve) => {
    server = app.listen(0, "127.0.0.1", resolve);
  });
  baseUrl = `http://127.0.0.1:${server.address().port}`;
});

after(async () => {
  if (server) await new Promise((resolve) => server.close(resolve));
});

const tokenFor = (payload) => jwt.sign(payload, process.env.JWT_SECRET, { expiresIn: "5m" });

async function call(method, path, { token, body } = {}) {
  return fetch(`${baseUrl}${path}`, {
    method,
    headers: { "Content-Type": "application/json", ...(token ? { Authorization: `Bearer ${token}` } : {}) },
    body: method === "GET" ? undefined : JSON.stringify(body || {}),
  });
}

const REDEMPTION_ROUTES = [
  ["POST", "/api/store/redemptions/prepare"],
  ["POST", "/api/store/redemptions/confirm"],
  ["GET", "/api/store/redemptions"],
  ["GET", "/api/store/redemptions/abc"],
  // Acompanhamento logistico do pedido (read-only, sob demanda).
  ["GET", "/api/store/redemptions/abc/tray-status"],
];

test("toda rota de resgate exige autenticacao", async () => {
  for (const [method, path] of REDEMPTION_ROUTES) {
    const r = await call(method, path);
    assert.equal(r.status, 401, `${method} ${path} deveria exigir autenticacao`);
  }
});

test("o corpo do confirm nunca aceita user_id nem balance vindos do navegador", async () => {
  const { readFileSync } = await import("node:fs");
  const routeSource = readFileSync(new URL("../src/routes/store_redemptions.js", import.meta.url), "utf8");
  assert.ok(!/req\.body\?*\.user_id/.test(routeSource), "user_id nunca pode vir do body");
  assert.ok(!/req\.body\?*\.balance/.test(routeSource), "balance nunca pode vir do body");
  assert.ok(/req\.user\.id/.test(routeSource), "o user_id tem que vir do token em toda rota");
});

test("confirm exige idempotency_key explicito — nao inventa um sozinho", async () => {
  const { readFileSync } = await import("node:fs");
  const serviceSource = readFileSync(new URL("../src/services/rewardRedemption.js", import.meta.url), "utf8");
  assert.ok(/idempotency_key_required/.test(serviceSource));
});

test("todas as rotas tem requireAuth no stack", async () => {
  const { default: router } = await import("../src/routes/store_redemptions.js");
  const layers = router.stack.filter((l) => l.route);
  assert.ok(layers.length >= REDEMPTION_ROUTES.length);
  for (const layer of layers) {
    const names = layer.route.stack.map((s) => s.name);
    assert.ok(names.includes("requireAuth"), `${layer.route.path} sem requireAuth`);
  }
});

test("o acompanhamento logistico nunca aceita tray_order_id do navegador", async () => {
  const { readFileSync } = await import("node:fs");
  const routeSource = readFileSync(new URL("../src/routes/store_redemptions.js", import.meta.url), "utf8");
  const serviceSource = readFileSync(new URL("../src/services/rewardRedemptionTracking.js", import.meta.url), "utf8");

  // A rota so passa adiante o usuario do token e o ID do resgate da URL.
  assert.ok(/getRedemptionTrayStatus\(req\.user\.id, req\.params\.id\)/.test(routeSource));
  // E o servico nunca le tray_order_id de query/body: ele sai do banco.
  assert.ok(!/req\.(query|body)/.test(serviceSource));
  assert.ok(/select id, status, tray_order_id/.test(serviceSource));
  assert.ok(/where id = \$1::uuid and user_id = \$2/.test(serviceSource));
});
