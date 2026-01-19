// src/scripts/tray_healthcheck.js
// Uso:
//   node src/scripts/tray_healthcheck.js
// Requer envs:
//   TRAY_CONSUMER_KEY, TRAY_CONSUMER_SECRET, TRAY_API_BASE/TRAY_API_ADDRESS
// E um refresh_token persistido (kv_store key 'tray_refresh_token') ou TRAY_CODE (bootstrap 1x).

import { trayHealthCheck } from "../services/tray.js";

async function main() {
  const res = await trayHealthCheck();
  console.log("[tray.healthcheck] result:", {
    ok: res.ok,
    status: res.status,
    bodyKeys: res.body && typeof res.body === "object" ? Object.keys(res.body) : null,
  });
  process.exit(res.ok ? 0 : 1);
}

main().catch((e) => {
  console.error("[tray.healthcheck] error:", e?.message || e);
  process.exit(1);
});


