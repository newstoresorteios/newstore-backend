import test from "node:test";
import assert from "node:assert/strict";

import {
  TrayAdapterClient,
  TrayAdapterConfigError,
  TrayAdapterResourceNotFoundError,
  TrayAdapterUnauthorizedError,
  TrayAdapterUnavailableError,
} from "../src/services/trayAdapterClient.js";

const originalFetch = globalThis.fetch;

function jsonResponse(body = { success: true, products: [] }, status = 200) {
  return {
    ok: status >= 200 && status < 300,
    status,
    async json() { return body; },
  };
}

test.after(() => {
  globalThis.fetch = originalFetch;
});

test("TrayAdapter client sends Bearer token, normalized URL and params", async () => {
  let request;
  globalThis.fetch = async (url, options) => {
    request = { url: new URL(url), options };
    return jsonResponse({ success: true, products: [] });
  };

  const client = new TrayAdapterClient({
    baseUrl: "https://adapter.example///",
    token: "secret-token",
  });
  const result = await client.searchProducts({
    name: "Widget",
    reference: null,
    available: false,
    limit: 5,
    page: 2,
    stock: undefined,
  });

  assert.deepEqual(result, { success: true, products: [] });
  assert.equal(request.url.toString(), "https://adapter.example/internal/products?name=Widget&available=false&limit=5&page=2");
  assert.equal(request.options.headers.Authorization, "Bearer secret-token");
  assert.equal(request.options.headers.Accept, "application/json");
  assert.equal(request.options.method, "GET");
});

test("TrayAdapter client maps every supported method to the adapter route", async () => {
  const requests = [];
  globalThis.fetch = async (url) => {
    requests.push(new URL(url).pathname);
    return jsonResponse();
  };
  const client = new TrayAdapterClient({ baseUrl: "https://adapter.example", token: "token" });

  await client.searchProducts();
  await client.getProduct(10);
  await client.getProductStock(10);
  await client.listBrands();
  await client.getBrand(11);
  await client.listKits();
  await client.listCustomers();
  await client.getCustomer(12);
  await client.listCustomerAddresses();
  await client.getCustomerAddress(13);
  await client.listCoupons();
  await client.getCoupon(14);
  await client.listUsers();
  await client.listDistributionCenters();
  await client.getDistributionCenter(15);
  await client.getProductDistributionInventory(16);

  assert.deepEqual(requests, [
    "/internal/products",
    "/internal/products/10",
    "/internal/products/10/stock",
    "/internal/brands",
    "/internal/brands/11",
    "/internal/kits",
    "/internal/customers",
    "/internal/customers/12",
    "/internal/customer-addresses",
    "/internal/customer-addresses/13",
    "/internal/coupons",
    "/internal/coupons/14",
    "/internal/users",
    "/internal/inventory/distribution-centers",
    "/internal/inventory/distribution-centers/15",
    "/internal/inventory/products/16/distribution-centers",
  ]);
});

test("TrayAdapter client maps upstream errors without exposing the token", async (t) => {
  for (const [status, ErrorType] of [
    [401, TrayAdapterUnauthorizedError],
    [403, TrayAdapterUnauthorizedError],
    [404, TrayAdapterResourceNotFoundError],
    [429, TrayAdapterUnavailableError],
    [500, TrayAdapterUnavailableError],
  ]) {
    await t.test(`status ${status}`, async () => {
      globalThis.fetch = async () => jsonResponse({}, status);
      const client = new TrayAdapterClient({ baseUrl: "https://adapter.example", token: "do-not-leak" });
      await assert.rejects(client.getProduct(1), (error) => {
        assert.ok(error instanceof ErrorType);
        assert.equal(error.message.includes("do-not-leak"), false);
        return true;
      });
    });
  }
});

test("TrayAdapter client maps timeout and connection failures", async () => {
  globalThis.fetch = async (_url, { signal }) => new Promise((_resolve, reject) => {
    signal.addEventListener("abort", () => {
      const error = new Error("aborted");
      error.name = "AbortError";
      reject(error);
    }, { once: true });
  });
  const timeoutClient = new TrayAdapterClient({
    baseUrl: "https://adapter.example",
    token: "secret",
    timeoutMs: 5,
  });
  await assert.rejects(timeoutClient.getProduct(1), (error) => {
    assert.equal(error.code, "tray_adapter_timeout");
    assert.equal(error.message.includes("secret"), false);
    return true;
  });

  globalThis.fetch = async () => { throw new TypeError("connection refused secret"); };
  const connectionClient = new TrayAdapterClient({ baseUrl: "https://adapter.example", token: "secret" });
  await assert.rejects(connectionClient.getProduct(1), (error) => {
    assert.equal(error.code, "tray_adapter_connection_error");
    assert.equal(error.message.includes("secret"), false);
    return true;
  });
});

test("TrayAdapter client requires only adapter configuration", async () => {
  const client = new TrayAdapterClient({ baseUrl: "", token: "" });
  await assert.rejects(client.searchProducts(), (error) => {
    assert.ok(error instanceof TrayAdapterConfigError);
    return true;
  });
});
