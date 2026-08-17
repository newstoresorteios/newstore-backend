const DEFAULT_TIMEOUT_MS = 12_000;

export class TrayAdapterError extends Error {
  constructor(message, { code = "tray_adapter_error", status = null } = {}) {
    super(message);
    this.name = "TrayAdapterError";
    this.code = code;
    this.status = status;
  }
}

export class TrayAdapterConfigError extends TrayAdapterError {
  constructor(message = "TrayAdapter configuration is incomplete") {
    super(message, { code: "tray_adapter_config_error" });
    this.name = "TrayAdapterConfigError";
  }
}

export class TrayAdapterUnauthorizedError extends TrayAdapterError {
  constructor(status = 401) {
    super("TrayAdapter authorization failed", {
      code: "tray_adapter_unauthorized",
      status,
    });
    this.name = "TrayAdapterUnauthorizedError";
  }
}

export class TrayAdapterResourceNotFoundError extends TrayAdapterError {
  constructor(status = 404) {
    super("TrayAdapter resource not found", {
      code: "tray_adapter_not_found",
      status,
    });
    this.name = "TrayAdapterResourceNotFoundError";
  }
}

export class TrayAdapterUnavailableError extends TrayAdapterError {
  constructor(message = "TrayAdapter is unavailable", { status = null, code = "tray_adapter_unavailable" } = {}) {
    super(message, { code, status });
    this.name = "TrayAdapterUnavailableError";
  }
}

function cleanBaseUrl(value) {
  return String(value || "").trim().replace(/\/+$/, "");
}

function removeNullParams(params) {
  return Object.fromEntries(
    Object.entries(params || {}).filter(([, value]) => value !== undefined && value !== null)
  );
}

function adapterConfig(overrides = {}) {
  const baseUrl = cleanBaseUrl(overrides.baseUrl ?? process.env.TRAY_ADAPTER_URL);
  const token = String(overrides.token ?? process.env.TRAY_ADAPTER_TOKEN ?? "").trim();
  if (!baseUrl || !token) throw new TrayAdapterConfigError();
  return {
    baseUrl,
    token,
    timeoutMs: Number(overrides.timeoutMs ?? DEFAULT_TIMEOUT_MS) || DEFAULT_TIMEOUT_MS,
  };
}

function isAbortError(error) {
  return error?.name === "AbortError" || error?.code === "ABORT_ERR";
}

export class TrayAdapterClient {
  constructor(options = {}) {
    this.options = options;
  }

  async request(path, { params = {}, signal } = {}) {
    const config = adapterConfig(this.options);
    const url = new URL(path, `${config.baseUrl}/`);
    for (const [key, value] of Object.entries(removeNullParams(params))) {
      url.searchParams.set(key, String(value));
    }

    const controller = new AbortController();
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, config.timeoutMs);
    const forwardAbort = () => controller.abort();
    signal?.addEventListener?.("abort", forwardAbort, { once: true });

    try {
      const response = await fetch(url, {
        method: "GET",
        headers: {
          Accept: "application/json",
          Authorization: `Bearer ${config.token}`,
        },
        signal: controller.signal,
      });

      if (response.status === 401 || response.status === 403) {
        throw new TrayAdapterUnauthorizedError(response.status);
      }
      if (response.status === 404) {
        throw new TrayAdapterResourceNotFoundError(response.status);
      }
      if (response.status === 429 || response.status >= 500) {
        throw new TrayAdapterUnavailableError("TrayAdapter upstream request failed", {
          status: response.status,
          code: response.status === 429 ? "tray_adapter_rate_limited" : "tray_adapter_upstream_error",
        });
      }
      if (!response.ok) {
        throw new TrayAdapterError("TrayAdapter request failed", {
          code: "tray_adapter_request_failed",
          status: response.status,
        });
      }

      try {
        return await response.json();
      } catch {
        throw new TrayAdapterError("TrayAdapter returned an invalid response", {
          code: "tray_adapter_invalid_response",
          status: response.status,
        });
      }
    } catch (error) {
      if (error instanceof TrayAdapterError) throw error;
      if (timedOut || isAbortError(error)) {
        throw new TrayAdapterUnavailableError("TrayAdapter request timed out", {
          code: "tray_adapter_timeout",
        });
      }
      throw new TrayAdapterUnavailableError("TrayAdapter connection failed", {
        code: "tray_adapter_connection_error",
      });
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener?.("abort", forwardAbort);
    }
  }

  searchProducts(params = {}) { return this.request("/internal/products", { params }); }
  getProduct(productId) { return this.request(`/internal/products/${encodeURIComponent(productId)}`); }
  getProductStock(productId) { return this.request(`/internal/products/${encodeURIComponent(productId)}/stock`); }

  listBrands(params = {}) { return this.request("/internal/brands", { params }); }
  getBrand(brandId) { return this.request(`/internal/brands/${encodeURIComponent(brandId)}`); }

  listKits(params = {}) { return this.request("/internal/kits", { params }); }

  listCustomers(params = {}) { return this.request("/internal/customers", { params }); }
  getCustomer(customerId) { return this.request(`/internal/customers/${encodeURIComponent(customerId)}`); }

  listCustomerAddresses(params = {}) { return this.request("/internal/customer-addresses", { params }); }
  getCustomerAddress(addressId) { return this.request(`/internal/customer-addresses/${encodeURIComponent(addressId)}`); }

  listCoupons(params = {}) { return this.request("/internal/coupons", { params }); }
  getCoupon(couponId) { return this.request(`/internal/coupons/${encodeURIComponent(couponId)}`); }

  listUsers(params = {}) { return this.request("/internal/users", { params }); }

  listDistributionCenters(params = {}) { return this.request("/internal/inventory/distribution-centers", { params }); }
  getDistributionCenter(centerId) {
    return this.request(`/internal/inventory/distribution-centers/${encodeURIComponent(centerId)}`);
  }
  getProductDistributionInventory(productId) {
    return this.request(`/internal/inventory/products/${encodeURIComponent(productId)}/distribution-centers`);
  }

  // Aliases matching the integration contract naming used by upstream callers.
  search_products(params = {}) { return this.searchProducts(params); }
  get_product(productId) { return this.getProduct(productId); }
  get_product_stock(productId) { return this.getProductStock(productId); }
  list_brands(params = {}) { return this.listBrands(params); }
  get_brand(brandId) { return this.getBrand(brandId); }
  list_kits(params = {}) { return this.listKits(params); }
  list_customers(params = {}) { return this.listCustomers(params); }
  get_customer(customerId) { return this.getCustomer(customerId); }
  list_customer_addresses(params = {}) { return this.listCustomerAddresses(params); }
  get_customer_address(addressId) { return this.getCustomerAddress(addressId); }
  list_coupons(params = {}) { return this.listCoupons(params); }
  get_coupon(couponId) { return this.getCoupon(couponId); }
  list_users(params = {}) { return this.listUsers(params); }
  list_distribution_centers(params = {}) { return this.listDistributionCenters(params); }
  get_distribution_center(centerId) { return this.getDistributionCenter(centerId); }
  get_product_distribution_inventory(productId) { return this.getProductDistributionInventory(productId); }
}

export const trayAdapterClient = new TrayAdapterClient();
