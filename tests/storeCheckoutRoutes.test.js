// tests/storeCheckoutRoutes.test.js
// Guardas das rotas de checkout (endereco + cotacao de frete).
//
// O resgate em si (prepare/confirm) mora em store_redemptions.js — ver
// storeRedemptionsRoutes.test.js. Este router (/checkout) so cuida de
// endereco e cotacao de frete; nunca debita nada.
import test, { before, after } from "node:test";
import assert from "node:assert/strict";

process.env.JWT_SECRET = process.env.JWT_SECRET || "test-secret-checkout";

let server;
let baseUrl;
let jwt;

before(async () => {
  const [{ default: express }, { default: jsonwebtoken }, checkout] = await Promise.all([
    import("express"),
    import("jsonwebtoken"),
    import("../src/routes/store_checkout.js"),
  ]);
  jwt = jsonwebtoken;

  const app = express();
  app.use(express.json());
  app.use("/api/store/checkout", checkout.default);

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

const CHECKOUT_ROUTES = [
  ["GET", "/api/store/checkout"],
  ["GET", "/api/store/checkout/addresses"],
  ["POST", "/api/store/checkout/addresses"],
  ["DELETE", "/api/store/checkout/addresses/abc"],
  ["POST", "/api/store/checkout/shipping"],
];

test("toda rota de checkout exige autenticacao", async () => {
  for (const [method, path] of CHECKOUT_ROUTES) {
    const r = await call(method, path);
    assert.equal(r.status, 401, `${method} ${path} deveria exigir autenticacao`);
  }
});

test("o router de checkout nao tem rota de confirmacao/resgate — isso mora em store_redemptions.js", async () => {
  const token = tokenFor({ id: 1, role: "user" });
  const candidates = [
    ["POST", "/api/store/checkout/confirm"],
    ["POST", "/api/store/checkout/redeem"],
  ];
  for (const [method, path] of candidates) {
    const r = await call(method, path, { token });
    assert.ok(r.status === 404, `${method} ${path} nao pode existir no router de checkout (status ${r.status})`);
  }
});
