// src/lib/api.js
const RAW_BASE =
  process.env.REACT_APP_API_BASE_URL ||
  process.env.REACT_APP_API_BASE ||
  "/api";

// Garante que, se for uma URL absoluta, termine com /api
function normalizeBase(b) {
  let s = String(b || "/api").replace(/\/+$/, "");
  if (/^https?:\/\//i.test(s) && !/\/api$/i.test(s)) s += "/api";
  return s;
}

const API_BASE = normalizeBase(RAW_BASE);

// junta a base com o path
export const apiJoin = (p) => {
  const path = p.startsWith("/") ? p : `/${p}`;
  // evita /api/api/...
  if (API_BASE.endsWith("/api") && path.startsWith("/api/")) {
    return `${API_BASE}${path.slice(4)}`;
  }
  return `${API_BASE}${path}`;
};

// token util
export const getStoredToken = () =>
  (localStorage.getItem("ns_auth_token") ||
    sessionStorage.getItem("ns_auth_token") ||
    "")
    .replace(/^Bearer\s+/i, "")
    .replace(/^["']|["']$/g, "");

export const authHeaders = () => {
  const t = getStoredToken();
  return t ? { Authorization: `Bearer ${t}` } : {};
};

export async function getJSON(pathOrUrl, opts = {}) {
  const url = /^https?:\/\//i.test(pathOrUrl) ? pathOrUrl : apiJoin(pathOrUrl);
  const r = await fetch(url, {
    ...opts,
    headers: {
      "Content-Type": "application/json",
      ...(opts.headers || {}),
      ...authHeaders(),
    },
    credentials: "omit",
  });
  if (!r.ok) {
    let err = "request_failed";
    try {
      const j = await r.json();
      err = j?.error || err;
    } catch {}
    throw new Error(err + ":" + r.status);
  }
  return r.json();
}

export async function postJSON(path, body) {
  return getJSON(path, { method: "POST", body: JSON.stringify(body || {}) });
}
