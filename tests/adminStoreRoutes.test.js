// tests/adminStoreRoutes.test.js
// Guardas de autenticacao/autorizacao das rotas da Loja de Premios.
import test, { before, after } from "node:test";
import assert from "node:assert/strict";

process.env.JWT_SECRET = process.env.JWT_SECRET || "test-secret-loja-premios";

let server;
let baseUrl;
let jwt;

before(async () => {
  const [{ default: express }, { default: jsonwebtoken }, adminStore, store] = await Promise.all([
    import("express"),
    import("jsonwebtoken"),
    import("../src/routes/admin_store.js"),
    import("../src/routes/store.js"),
  ]);
  jwt = jsonwebtoken;

  const app = express();
  app.use(express.json());
  app.use("/api/admin/store", adminStore.default);
  app.use("/api/store", store.default);

  await new Promise((resolve) => {
    server = app.listen(0, "127.0.0.1", resolve);
  });
  baseUrl = `http://127.0.0.1:${server.address().port}`;
});

after(async () => {
  if (server) await new Promise((resolve) => server.close(resolve));
});

function tokenFor(payload) {
  return jwt.sign(payload, process.env.JWT_SECRET, { expiresIn: "5m" });
}

const ADMIN_ROUTES = [
  ["GET", "/api/admin/store/tray-products"],
  ["GET", "/api/admin/store/tray-products/123"],
  ["GET", "/api/admin/store/tray-brands"],
  ["GET", "/api/admin/store/products"],
  ["GET", "/api/admin/store/status"],
  ["POST", "/api/admin/store/products/publish"],
  ["POST", "/api/admin/store/products/sync"],
  ["PATCH", "/api/admin/store/products/123"],
];

test("toda rota admin da loja exige autenticacao (401 sem token)", async () => {
  for (const [method, path] of ADMIN_ROUTES) {
    const r = await fetch(`${baseUrl}${path}`, {
      method,
      headers: { "Content-Type": "application/json" },
      body: method === "GET" ? undefined : "{}",
    });
    assert.equal(r.status, 401, `${method} ${path} deveria exigir autenticacao`);
    assert.deepEqual(await r.json(), { error: "unauthorized" });
  }
});

test("toda rota admin da loja exige administrador (403 para usuario comum)", async () => {
  const token = tokenFor({ id: 42, email: "cliente@exemplo.com", role: "user" });

  for (const [method, path] of ADMIN_ROUTES) {
    const r = await fetch(`${baseUrl}${path}`, {
      method,
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
      body: method === "GET" ? undefined : "{}",
    });
    assert.equal(r.status, 403, `${method} ${path} deveria exigir administrador`);
    assert.deepEqual(await r.json(), { error: "forbidden" });
  }
});

test("token invalido nao passa como admin", async () => {
  const r = await fetch(`${baseUrl}/api/admin/store/products`, {
    headers: { Authorization: "Bearer token-forjado" },
  });
  assert.equal(r.status, 401);
});

test("rota publica da loja nao tem guarda de autenticacao no stack", async () => {
  // Nao chamamos a rota por HTTP aqui: ela consulta o PostgreSQL, que nao existe
  // no ambiente de teste unitario. Verificamos a estrutura do router.
  const { default: storeRouter } = await import("../src/routes/store.js");

  const layer = storeRouter.stack.find((l) => l.route?.path === "/products");
  assert.ok(layer, "esperada a rota GET /products no router publico");

  const handlerNames = layer.route.stack.map((s) => s.name);
  assert.ok(!handlerNames.includes("requireAuth"), "rota publica nao pode exigir autenticacao");
  assert.ok(!handlerNames.includes("requireAdmin"), "rota publica nao pode exigir administrador");
  assert.equal(layer.route.stack.length, 1, "rota publica deve ter apenas o handler");
});

test("todas as rotas admin da loja tem requireAuth e requireAdmin no stack", async () => {
  const { default: adminStoreRouter } = await import("../src/routes/admin_store.js");

  const routeLayers = adminStoreRouter.stack.filter((l) => l.route);
  assert.ok(routeLayers.length >= ADMIN_ROUTES.length);

  for (const layer of routeLayers) {
    const handlerNames = layer.route.stack.map((s) => s.name);
    assert.ok(handlerNames.includes("requireAuth"), `${layer.route.path} sem requireAuth`);
    assert.ok(handlerNames.includes("requireAdmin"), `${layer.route.path} sem requireAdmin`);
  }
});
