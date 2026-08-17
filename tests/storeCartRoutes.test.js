// tests/storeCartRoutes.test.js
// Guardas das rotas do carrinho e ausencia de rotas de pedido/checkout.
import test, { before, after } from "node:test";
import assert from "node:assert/strict";

process.env.JWT_SECRET = process.env.JWT_SECRET || "test-secret-cart";

let server;
let baseUrl;
let jwt;

before(async () => {
  const [{ default: express }, { default: jsonwebtoken }, cart, store] = await Promise.all([
    import("express"),
    import("jsonwebtoken"),
    import("../src/routes/store_cart.js"),
    import("../src/routes/store.js"),
  ]);
  jwt = jsonwebtoken;

  const app = express();
  app.use(express.json());
  app.use("/api/store/cart", cart.default);
  app.use("/api/store", store.default);

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
    body: method === "GET" || method === "DELETE" ? undefined : JSON.stringify(body || {}),
  });
}

const CART_ROUTES = [
  ["GET", "/api/store/cart"],
  ["POST", "/api/store/cart/items"],
  ["PATCH", "/api/store/cart/items/abc"],
  ["DELETE", "/api/store/cart/items/abc"],
  ["DELETE", "/api/store/cart"],
  ["POST", "/api/store/cart/validate"],
];

test("toda rota de carrinho exige autenticacao", async () => {
  for (const [method, path] of CART_ROUTES) {
    const r = await call(method, path);
    assert.equal(r.status, 401, `${method} ${path} deveria exigir autenticacao`);
  }
});

test("todas as rotas de carrinho tem requireAuth no stack", async () => {
  const { default: router } = await import("../src/routes/store_cart.js");
  const layers = router.stack.filter((l) => l.route);
  assert.ok(layers.length >= CART_ROUTES.length);
  for (const layer of layers) {
    const names = layer.route.stack.map((s) => s.name);
    assert.ok(names.includes("requireAuth"), `${layer.route.path} sem requireAuth`);
  }
});

test("o catalogo publico continua sem exigir autenticacao", async () => {
  const { default: router } = await import("../src/routes/store.js");
  for (const path of ["/products", "/products/:trayProductId"]) {
    const layer = router.stack.find((l) => l.route?.path === path);
    assert.ok(layer, `esperada a rota ${path}`);
    const names = layer.route.stack.map((s) => s.name);
    assert.ok(!names.includes("requireAuth"), `${path} nao pode exigir autenticacao`);
  }
});

test("nao existe rota de pedido, checkout ou resgate nesta fase", async () => {
  const token = tokenFor({ id: 42, role: "user" });
  for (const [method, path] of [
    ["POST", "/api/store/cart/checkout"],
    ["POST", "/api/store/cart/redeem"],
    ["POST", "/api/store/orders"],
    ["POST", "/api/store/cart/confirm"],
    ["POST", "/api/store/cart/finish"],
  ]) {
    const r = await call(method, path, { token, body: {} });
    assert.ok(r.status === 404 || r.status === 405, `${method} ${path} nao pode existir (status ${r.status})`);
  }
});

test("o carrinho nao aceita user_id do navegador", async () => {
  const { readFileSync } = await import("node:fs");
  const source = readFileSync(new URL("../src/routes/store_cart.js", import.meta.url), "utf8");
  assert.ok(!/req\.body\?*\.user_id/.test(source), "user_id nunca pode vir do body");
  assert.ok(/req\.user\.id/.test(source), "o user_id tem que vir do token");
});

test("o carrinho nao aceita preco vindo do navegador", async () => {
  const { readFileSync } = await import("node:fs");
  const source = readFileSync(new URL("../src/routes/store_cart.js", import.meta.url), "utf8");
  for (const proibido of ["nscredits_price", "nscredits_unit_price", "price"]) {
    assert.ok(
      !new RegExp(`req\\.body\\?*\\.${proibido}`).test(source),
      `${proibido} nao pode ser lido do body`
    );
  }
});
