// src/services/trayCouponEnsure.js
// Gatilho de cupom no login (best-effort) + endpoint /api/coupons/ensure
// Regras:
// - Nunca bloquear UX por falha Tray
// - Sempre logar para auditoria (Render)
// - Idempotente por code (find antes de create)

import { query } from "../db.js";
import { trayToken, trayFindCouponByCode, trayCreateCoupon } from "./tray.js";

const VALID_DAYS = Number(process.env.TRAY_COUPON_VALID_DAYS || 180);

function fmtDate(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function makeUserCouponCode(userId) {
  const id = Number(userId || 0);
  const base = `NSU-${String(id).padStart(4, "0")}`;
  const salt = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  const tail = salt[(id * 7) % salt.length] + salt[(id * 13) % salt.length];
  return `${base}-${tail}`;
}

async function ensureCouponTraySyncTable() {
  try {
    await query(`
      create table if not exists coupon_tray_sync (
        user_id int4 primary key,
        code text not null,
        tray_coupon_id text null,
        tray_sync_status text not null default 'PENDING',
        tray_last_error text null,
        tray_synced_at timestamptz null,
        updated_at timestamptz default now(),
        created_at timestamptz default now()
      )
    `);
    await query(`create index if not exists coupon_tray_sync_status_idx on coupon_tray_sync(tray_sync_status, updated_at desc)`);
  } catch {}
}

async function upsertCouponTraySync({ userId, code, trayCouponId = null, status, lastError = null, syncedAt = null }) {
  await ensureCouponTraySyncTable();
  await query(
    `insert into coupon_tray_sync (user_id, code, tray_coupon_id, tray_sync_status, tray_last_error, tray_synced_at, updated_at)
     values ($1,$2,$3,$4,$5,$6,now())
     on conflict (user_id) do update
       set code=excluded.code,
           tray_coupon_id=coalesce(excluded.tray_coupon_id, coupon_tray_sync.tray_coupon_id),
           tray_sync_status=excluded.tray_sync_status,
           tray_last_error=excluded.tray_last_error,
           tray_synced_at=coalesce(excluded.tray_synced_at, coupon_tray_sync.tray_synced_at),
           updated_at=now()`,
    [Number(userId), String(code), trayCouponId ? String(trayCouponId) : null, String(status), lastError ? String(lastError) : null, syncedAt]
  );
}

function authFlagsFromEnv() {
  return {
    hasCKEY: !!process.env.TRAY_CONSUMER_KEY,
    hasCSECRET: !!process.env.TRAY_CONSUMER_SECRET,
    hasCode: !!process.env.TRAY_CODE,
    hasRefreshEnv: !!process.env.TRAY_REFRESH_TOKEN,
    // hasRefreshKV é inferido pelo trayToken() (já loga [tray.auth] env/missing), aqui mantemos "unknown"
    hasRefreshKV: "unknown",
  };
}

function normalizeErrForLog(e) {
  return {
    msg: e?.message || String(e),
    status: e?.status || e?.provider_status || null,
    body: e?.body ?? e?.response ?? null,
  };
}

/**
 * ensureTrayCouponForUser(userId)
 * - Sempre retorna rapidamente e nunca joga erro para o caller (best-effort)
 * - Usa timeout interno para chamadas Tray (AbortController)
 */
export async function ensureTrayCouponForUser(userId, { timeoutMs = 5000 } = {}) {
  const uid = Number(userId);
  const rid = Math.random().toString(36).slice(2, 8);

  console.log(`[tray.coupon.ensure] start user=${uid} rid=${rid}`);

  // Carrega cupom na nossa base (mesma regra do coupons.sync: users.coupon_code e coupon_value_cents)
  let code = null;
  let valueCents = 0;
  try {
    const r = await query(
      `select id, coupon_code, COALESCE(coupon_value_cents,0)::int as coupon_value_cents
         from users where id=$1 limit 1`,
      [uid]
    );
    if (!r.rows.length) {
      console.log(`[tray.coupon.ensure] user_not_found user=${uid} rid=${rid}`);
      return { ok: false, status: "USER_NOT_FOUND" };
    }
    code = (r.rows[0].coupon_code && String(r.rows[0].coupon_code).trim()) || makeUserCouponCode(uid);
    valueCents = Number(r.rows[0].coupon_value_cents || 0);

    if (!r.rows[0].coupon_code) {
      try {
        await query(`update users set coupon_code=$2, coupon_updated_at=now() where id=$1 and coupon_code is null`, [uid, code]);
      } catch {}
    }
  } catch (e) {
    console.log(`[tray.coupon.ensure] user_load_failed user=${uid} rid=${rid} msg=${e?.message || e}`);
    return { ok: false, status: "USER_LOAD_FAILED" };
  }

  const startsAt = fmtDate(new Date());
  const endsAt = fmtDate(new Date(Date.now() + VALID_DAYS * 86400000));

  console.log(
    `[tray.coupon.ensure] computed user=${uid} rid=${rid} code=${code} value=${valueCents} starts=${startsAt} ends=${endsAt}`
  );

  // Status tracking (best-effort)
  try {
    await upsertCouponTraySync({ userId: uid, code, status: "PENDING", lastError: null, trayCouponId: null, syncedAt: null });
  } catch {}

  const shortTimeoutMs = Math.max(1000, Number(timeoutMs || 5000)); // token/find
  const createTimeoutMs = 20_000; // (2) obrigatório: POST cupom 20s
  const pollMaxMs = 25_000; // (3) obrigatório: polling até 25s

  const withAbort = async (ms, fn) => {
    const c = new AbortController();
    const t = setTimeout(() => c.abort(new Error("timeout")), ms);
    try {
      return await fn(c.signal);
    } finally {
      clearTimeout(t);
    }
  };

  const pollFindAfterTimeout = async () => {
    const started = Date.now();
    const maxAttempts = 5;
    for (let i = 0; i < maxAttempts; i++) {
      // eslint-disable-next-line no-await-in-loop
      await new Promise((r) => setTimeout(r, i === 0 ? 0 : 5000));
      // eslint-disable-next-line no-await-in-loop
      const found = await withAbort(shortTimeoutMs, (signal) => trayFindCouponByCode(code, { maxPages: 3, signal })).catch(() => null);
      const trayId = found?.coupon?.id ?? null;
      if (found?.found) return { found: true, trayId };
      if (Date.now() - started > pollMaxMs) break;
    }
    return { found: false, trayId: null };
  };

  try {
    // 1) token (se faltar bootstrap, não falhar UX)
    try {
      await withAbort(shortTimeoutMs, (signal) => trayToken({ signal }));
    } catch (e) {
      if (String(e?.message || "").includes("tray_no_refresh_and_no_code")) {
        const flags = authFlagsFromEnv();
        console.log(`[tray.auth] missing hasCKEY=${flags.hasCKEY} hasCSECRET=${flags.hasCSECRET} hasCode=${flags.hasCode} hasRefreshEnv=${flags.hasRefreshEnv} hasRefreshKV=${flags.hasRefreshKV}`);
        await upsertCouponTraySync({ userId: uid, code, status: "FAILED", lastError: "PENDING_AUTH", trayCouponId: null, syncedAt: null }).catch(() => {});
        return { ok: true, status: "PENDING_AUTH", action: "pending_auth", code };
      }
      if (e?.code === "tray_code_invalid_or_expired" || String(e?.message || "").includes("tray_code_invalid_or_expired")) {
        console.log(`[tray.coupon.ensure] user=${uid} rid=${rid} action=pending_auth reason=code_invalid_or_expired`);
        await upsertCouponTraySync({ userId: uid, code, status: "FAILED", lastError: "PENDING_AUTH_CODE_INVALID", trayCouponId: null, syncedAt: null }).catch(() => {});
        return { ok: true, status: "PENDING_AUTH", action: "pending_auth", reason: "code_invalid_or_expired", code };
      }
      throw e;
    }

    // 2) find (idempotência)
    const found = await withAbort(shortTimeoutMs, (signal) => trayFindCouponByCode(code, { maxPages: 3, signal }));
    if (found?.found) {
      const trayId = found?.coupon?.id ?? null;
      console.log(`[tray.coupon.ensure] user=${uid} rid=${rid} code=${code} action=already_exists trayId=${trayId || ""}`);
      await upsertCouponTraySync({ userId: uid, code, status: "SYNCED", lastError: null, trayCouponId: trayId, syncedAt: new Date().toISOString() }).catch(() => {});
      return { ok: true, status: "SYNCED", action: "already_exists", code, trayId };
    }

    // 3) create
    console.log(`[tray.coupon.create] user=${uid} rid=${rid} code=${code} value=${valueCents} starts=${startsAt} ends=${endsAt}`);
    let created = null;
    try {
      created = await withAbort(createTimeoutMs, (signal) =>
        trayCreateCoupon({
          code,
          value: valueCents / 100,
          startsAt,
          endsAt,
          description: `Crédito do cliente ${uid} - New Store`,
          signal,
        })
      );
    } catch (e) {
      const aborted = e?.name === "AbortError" || String(e?.message || "").includes("timeout");
      if (!aborted) throw e;

      // (3) obrigatório: não finalizar com timeout sem confirmar via GET
      console.log(`[tray.coupon.ensure] user=${uid} rid=${rid} code=${code} action=create_timeout_confirming`);
      const confirmed = await pollFindAfterTimeout();
      if (confirmed.found) {
        console.log(`[tray.coupon.ensure] user=${uid} rid=${rid} code=${code} action=created_confirmed_after_timeout trayId=${confirmed.trayId || ""}`);
        await upsertCouponTraySync({ userId: uid, code, status: "SYNCED", lastError: null, trayCouponId: confirmed.trayId, syncedAt: new Date().toISOString() }).catch(() => {});
        return { ok: true, status: "SYNCED", action: "created_confirmed_after_timeout", code, trayId: confirmed.trayId };
      }
      console.log(`[tray.coupon.ensure] user=${uid} rid=${rid} code=${code} action=failed status=timeout_not_confirmed`);
      await upsertCouponTraySync({ userId: uid, code, status: "FAILED", lastError: "timeout_not_confirmed", trayCouponId: null, syncedAt: null }).catch(() => {});
      return { ok: true, status: "FAILED", action: "failed", code };
    }

    const trayId = created?.id ?? null;
    if (trayId) {
      console.log(`[tray.coupon.ensure] user=${uid} rid=${rid} code=${code} action=created trayId=${trayId || ""}`);
      await upsertCouponTraySync({ userId: uid, code, status: "SYNCED", lastError: null, trayCouponId: trayId, syncedAt: new Date().toISOString() }).catch(() => {});
      return { ok: true, status: "SYNCED", action: "created", code, trayId };
    }

    // Sem id: confirma por GET antes de falhar
    console.log(`[tray.coupon.ensure] user=${uid} rid=${rid} code=${code} action=create_no_id_confirming`);
    const confirmed = await pollFindAfterTimeout();
    if (confirmed.found) {
      console.log(`[tray.coupon.ensure] user=${uid} rid=${rid} code=${code} action=created_confirmed_after_timeout trayId=${confirmed.trayId || ""}`);
      await upsertCouponTraySync({ userId: uid, code, status: "SYNCED", lastError: null, trayCouponId: confirmed.trayId, syncedAt: new Date().toISOString() }).catch(() => {});
      return { ok: true, status: "SYNCED", action: "created_confirmed_after_timeout", code, trayId: confirmed.trayId };
    }

    console.log(`[tray.coupon.ensure] user=${uid} rid=${rid} code=${code} action=failed status=create_no_id_not_confirmed`);
    await upsertCouponTraySync({ userId: uid, code, status: "FAILED", lastError: "create_no_id_not_confirmed", trayCouponId: null, syncedAt: null }).catch(() => {});
    return { ok: true, status: "FAILED", action: "failed", code };
  } catch (e) {
    const info = normalizeErrForLog(e);
    const aborted = e?.name === "AbortError" || String(e?.message || "").includes("timeout");
    if (aborted) {
      // Nunca retornar "timeout" sem confirmação -> aqui tratamos como falha controlada
      console.log(`[tray.coupon.ensure] user=${uid} rid=${rid} code=${code} action=failed status=timeout`);
      await upsertCouponTraySync({ userId: uid, code, status: "FAILED", lastError: "TIMEOUT", trayCouponId: null, syncedAt: null }).catch(() => {});
      return { ok: true, status: "FAILED", action: "failed", code };
    }

    console.log(
      `[tray.coupon.ensure] user=${uid} rid=${rid} code=${code} action=failed status=${info.status || ""} msg=${info.msg}`
    );
    if (info.body) {
      console.log("[tray.coupon.ensure] body", info.body);
    }
    await upsertCouponTraySync({ userId: uid, code, status: "FAILED", lastError: info.msg, trayCouponId: null, syncedAt: null }).catch(() => {});
    return { ok: true, status: "FAILED", action: "failed", code };
  }
}


