// tests/nscreditRoutes.test.js
// Guardas de autenticacao/autorizacao das rotas de NSCreditos.
import test, { before, after } from "node:test";
import assert from "node:assert/strict";

process.env.JWT_SECRET = process.env.JWT_SECRET || "test-secret-nscredits";

let server;
let baseUrl;
let jwt;

before(async () => {
  const [{ default: express }, { default: jsonwebtoken }, adminNsCredits, me] = await Promise.all([
    import("express"),
    import("jsonwebtoken"),
    import("../src/routes/admin_nscredits.js"),
    import("../src/routes/me.js"),
  ]);
  jwt = jsonwebtoken;

  const app = express();
  app.use(express.json());
  app.use("/api/admin/store/nscredits", adminNsCredits.default);
  app.use("/api/me", me.default);

  await new Promise((resolve) => {
    server = app.listen(0, "127.0.0.1", resolve);
  });
  baseUrl = `http://127.0.0.1:${server.address().port}`;
});

after(async () => {
  if (server) await new Promise((resolve) => server.close(resolve));
});

const tokenFor = (payload) => jwt.sign(payload, process.env.JWT_SECRET, { expiresIn: "5m" });

const ADMIN_ROUTES = [
  ["GET", "/api/admin/store/nscredits/users"],
  ["GET", "/api/admin/store/nscredits/users/123"],
  ["POST", "/api/admin/store/nscredits/users/123/transactions"],
];

async function call(method, path, { token, body } = {}) {
  return fetch(`${baseUrl}${path}`, {
    method,
    headers: { "Content-Type": "application/json", ...(token ? { Authorization: `Bearer ${token}` } : {}) },
    body: method === "GET" ? undefined : JSON.stringify(body || {}),
  });
}

test("toda rota admin de NSCreditos exige autenticacao", async () => {
  for (const [method, path] of ADMIN_ROUTES) {
    const r = await call(method, path);
    assert.equal(r.status, 401, `${method} ${path} deveria exigir autenticacao`);
  }
});

test("toda rota admin de NSCreditos exige administrador", async () => {
  const token = tokenFor({ id: 42, email: "cliente@exemplo.com", role: "user" });
  for (const [method, path] of ADMIN_ROUTES) {
    const r = await call(method, path, { token });
    assert.equal(r.status, 403, `${method} ${path} deveria exigir administrador`);
    assert.deepEqual(await r.json(), { error: "forbidden" });
  }
});

test("a carteira do proprio usuario exige autenticacao", async () => {
  const r = await call("GET", "/api/me/nscredits");
  assert.equal(r.status, 401);
});

test("nao existe rota publica de debito para o cliente", async () => {
  const token = tokenFor({ id: 42, role: "user" });
  for (const path of ["/api/me/nscredits/debit", "/api/me/nscredits/spend", "/api/me/nscredits/transactions"]) {
    const r = await call("POST", path, { token, body: { amount: 5000 } });
    assert.ok(r.status === 404 || r.status === 405, `${path} nao pode existir (status ${r.status})`);
  }
});

test("nao existe rota que sobrescreva saldo diretamente", async () => {
  const token = tokenFor({ id: 1, role: "admin" });
  for (const [method, path] of [
    ["PATCH", "/api/admin/store/nscredits/users/123"],
    ["PUT", "/api/admin/store/nscredits/users/123"],
    ["PATCH", "/api/admin/store/nscredits/users/123/balance"],
    ["PUT", "/api/admin/store/nscredits/users/123/balance"],
  ]) {
    const r = await call(method, path, { token, body: { balance: 999999 } });
    assert.ok(r.status === 404 || r.status === 405, `${method} ${path} nao pode existir (status ${r.status})`);
  }
});

test("todas as rotas admin tem requireAuth e requireAdmin no stack", async () => {
  const { default: router } = await import("../src/routes/admin_nscredits.js");
  const layers = router.stack.filter((l) => l.route);
  assert.ok(layers.length >= ADMIN_ROUTES.length);

  for (const layer of layers) {
    const names = layer.route.stack.map((s) => s.name);
    assert.ok(names.includes("requireAuth"), `${layer.route.path} sem requireAuth`);
    assert.ok(names.includes("requireAdmin"), `${layer.route.path} sem requireAdmin`);
  }
});

test("a rota da carteira do cliente tem requireAuth mas NAO requireAdmin", async () => {
  const { default: router } = await import("../src/routes/me.js");
  const layer = router.stack.find((l) => l.route?.path === "/nscredits");
  assert.ok(layer, "esperada a rota GET /nscredits em /api/me");

  const names = layer.route.stack.map((s) => s.name);
  assert.ok(names.includes("requireAuth"), "cliente precisa estar autenticado");
  assert.ok(!names.includes("requireAdmin"), "o cliente comum tem que conseguir ler a propria carteira");
});
