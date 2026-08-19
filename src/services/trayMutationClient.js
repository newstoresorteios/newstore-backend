// src/services/trayMutationClient.js
//
// Cliente HTTP da Tray para MUTAÇÕES — bloqueado por padrão.
//
// Ao contrário de trayCatalogClient.js (somente GET, sempre), esta camada
// pode emitir POST/PUT/PATCH/DELETE — mas SOMENTE para operações nomeadas
// e explicitamente autorizadas em ALLOWED_MUTATIONS (trayCatalogClient.js).
// Qualquer operação fora dessa lista falha antes da rede.
//
// Reusa a mesma autenticação/token de trayCatalogClient.js — não duplica
// nem contorna o fluxo OAuth existente.

import { assertAllowedTrayMutation, TrayCatalogError, sanitizeTrayUrl } from "./trayCatalogClient.js";

const DEFAULT_TIMEOUT_MS = Number(process.env.TRAY_MUTATION_TIMEOUT_MS || 20000);

async function defaultDeps() {
  const [{ trayToken }, { getTrayApiBase }] = await Promise.all([
    import("./tray.js"),
    import("./trayConfig.js"),
  ]);
  return {
    fetchImpl: (url, options) => fetch(url, options),
    getToken: (opts) => trayToken(opts),
    getApiBase: () => getTrayApiBase(),
  };
}

async function readBody(response) {
  const contentType = String(response.headers?.get?.("content-type") || "").toLowerCase();
  if (contentType.includes("application/json")) {
    return await response.json().catch(() => null);
  }
  const text = await response.text().catch(() => "");
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

function errorForStatus(response, body) {
  const status = Number(response.status);
  if (status === 429) return new TrayCatalogError("tray_rate_limited", { status: 429 });
  if (status === 401 || status === 403) return new TrayCatalogError("tray_auth_failed", { status: 502 });
  if (status === 400) return new TrayCatalogError("tray_request_invalid", { status: 400, publicDetails: { tray_body: body } });
  if (status === 404) return new TrayCatalogError("tray_resource_not_found", { status: 404 });
  if (status >= 500) return new TrayCatalogError("tray_unavailable", { status: 503 });
  return new TrayCatalogError("tray_request_failed", { status: 502, publicDetails: { tray_status: status, tray_body: body } });
}

/**
 * Mutação Tray nomeada e autorizada. Nunca chamar com um `operation` que
 * não exista em ALLOWED_MUTATIONS — a guarda derruba antes da rede.
 *
 * @param {string} operation nome estável da operação (ex.: "TRAY_ORDER_CREATE")
 * @param {string} method HTTP method exato exigido pela operação
 * @param {string} path caminho relativo (ex.: "/orders")
 * @param {object} body corpo JSON — enviado como está, sem transformação
 */
export async function trayMutationRequest(operation, method, path, body, options = {}) {
  const m = assertAllowedTrayMutation(operation, method);
  const deps = options.deps || (await defaultDeps());
  const timeoutMs = Number(options.timeoutMs) > 0 ? Number(options.timeoutMs) : DEFAULT_TIMEOUT_MS;

  const apiBase = String(await deps.getApiBase()).replace(/\/+$/, "");
  const token = await deps.getToken({ signal: options.signal });
  const url = `${apiBase}${path}?access_token=${encodeURIComponent(String(token))}`;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  if (options.signal) {
    if (options.signal.aborted) controller.abort();
    else options.signal.addEventListener("abort", () => controller.abort(), { once: true });
  }

  let response;
  try {
    response = await deps.fetchImpl(url, {
      method: m,
      headers: { "Content-Type": "application/json" },
      body: body != null ? JSON.stringify(body) : undefined,
      signal: controller.signal,
    });
  } catch (e) {
    if (e?.name === "AbortError") throw new TrayCatalogError("tray_timeout", { status: 503 });
    throw new TrayCatalogError("tray_unreachable", { status: 503 });
  } finally {
    clearTimeout(timer);
  }

  const responseBody = await readBody(response);

  if (!response?.ok) {
    const error = errorForStatus(response || { status: 0 }, responseBody);
    console.warn("[tray.mutation] request failed", {
      operation,
      path: sanitizeTrayUrl(url),
      tray_status: response?.status ?? null,
      code: error.code,
    });
    throw error;
  }

  console.log("[tray.mutation] ok", { operation, path: sanitizeTrayUrl(url) });
  return responseBody;
}
