// src/services/trayCouponEnsure.js
// Gatilho de cupom no login (best-effort) + endpoint /api/coupons/ensure
// Regras:
// - Nunca bloquear UX por falha Tray
// - Sempre logar para auditoria (Render)
// - Idempotente por code (find antes de create)

import { query } from "../db.js";
import { trayToken, trayFindCouponByCode, trayCreateCoupon, trayGetCouponById, trayUpdateCouponById } from "./tray.js";

const VALID_DAYS = Number(process.env.TRAY_COUPON_VALID_DAYS || 180);

function fmtDate(d) {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

// Gera um cupom determinístico por usuário (mesma lógica usada no login /auth e /api/coupons/sync)
function makeUserCouponCode(userId) {
  const id = Number(userId || 0);
  const base = `NSU-${String(id).padStart(4, "0")}`;
  const salt = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
  const tail = salt[(id * 7) % salt.length] + salt[(id * 13) % salt.length];
  return `${base}-${tail}`;
}

async function loadCouponSystemRow(userId) {
  const uid = Number(userId);
  // Pedido do requisito: primeiro tenta public.coupon_tray_system (se existir).
  try {
    const r = await query(
      `SELECT coupon_code,
              tray_coupon_id,
              COALESCE(coupon_value_cents,0)::int AS coupon_value_cents
         FROM public.coupon_tray_system
        WHERE id = $1
        LIMIT 1`,
      [uid]
    );
    if (r.rows?.length) return { source: "coupon_tray_system", row: r.rows[0] };
  } catch {}

  // Fallback compatível com o repo atual: users
  const r2 = await query(
    `SELECT coupon_code,
            tray_coupon_id,
            COALESCE(coupon_value_cents,0)::int AS coupon_value_cents
       FROM users
      WHERE id=$1
      LIMIT 1`,
    [uid]
  );
  if (!r2.rows?.length) return { source: "none", row: null };
  return { source: "users", row: r2.rows[0] };
}

async function persistCouponSystemFields({ userId, code = null, trayCouponId = null }) {
  const uid = Number(userId);
  const c = code != null ? String(code) : null;
  const t = trayCouponId != null ? String(trayCouponId) : null;

  // Atualiza primeiro coupon_tray_system (se existir), senão cai pro users.
  try {
    if (c && t) {
      await query(
        `UPDATE public.coupon_tray_system
            SET coupon_code = COALESCE(coupon_code, $2),
                tray_coupon_id = $3,
                coupon_updated_at = NOW()
          WHERE id = $1`,
        [uid, c, t]
      );
      return;
    }
    if (t) {
      await query(
        `UPDATE public.coupon_tray_system
            SET tray_coupon_id = $2,
                coupon_updated_at = NOW()
          WHERE id = $1`,
        [uid, t]
      );
      return;
    }
    if (c) {
      await query(
        `UPDATE public.coupon_tray_system
            SET coupon_code = COALESCE(coupon_code, $2),
                coupon_updated_at = COALESCE(coupon_updated_at, NOW())
          WHERE id = $1`,
        [uid, c]
      );
      return;
    }
  } catch {}

  // users fallback
  try {
    if (c && t) {
      await query(`UPDATE users SET coupon_code=COALESCE(coupon_code,$2), tray_coupon_id=$3, coupon_updated_at=NOW() WHERE id=$1`, [uid, c, t]);
      return;
    }
    if (t) {
      await query(`UPDATE users SET tray_coupon_id=$2, coupon_updated_at=NOW() WHERE id=$1`, [uid, t]);
      return;
    }
    if (c) {
      await query(`UPDATE users SET coupon_code=COALESCE(coupon_code,$2), coupon_updated_at=COALESCE(coupon_updated_at,NOW()) WHERE id=$1`, [uid, c]);
      return;
    }
  } catch {}
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

function addMonthsClamped(date, months) {
  const d = new Date(date.getTime());
  const day = d.getUTCDate();
  // move to first day to avoid overflow
  d.setUTCDate(1);
  d.setUTCMonth(d.getUTCMonth() + Number(months || 0));
  const year = d.getUTCFullYear();
  const month = d.getUTCMonth();
  const lastDay = new Date(Date.UTC(year, month + 1, 0)).getUTCDate();
  d.setUTCDate(Math.min(day, lastDay));
  return d;
}

async function hasColumn(table, column, schema = "public") {
  const { rows } = await query(
    `SELECT 1
       FROM information_schema.columns
      WHERE table_schema=$1 AND table_name=$2 AND column_name=$3
      LIMIT 1`,
    [schema, table, column]
  );
  return !!rows.length;
}

async function getUserLastApprovedPurchaseDate(userId) {
  // status real do sistema (ampliado)
  const statuses = ["approved", "paid", "pago", "completed"];
  const parts = [];
  if (await hasColumn("payments", "paid_at")) parts.push("COALESCE(paid_at, to_timestamp(0))");
  if (await hasColumn("payments", "approved_at")) parts.push("COALESCE(approved_at, to_timestamp(0))");
  parts.push("COALESCE(created_at, to_timestamp(0))");
  const tExpr = parts.length === 1 ? parts[0] : `GREATEST(${Array.from(new Set(parts)).join(", ")})`;

  const r = await query(
    `select ${tExpr} as t
       from payments
      where user_id=$1
        and lower(status) = any($2::text[])
      order by ${tExpr} desc
      limit 1`,
    [Number(userId), statuses]
  );
  const t = r.rows?.[0]?.t || null;
  return t ? new Date(t) : null;
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

  // Carrega cupom na nossa base (prioridade: coupon_tray_system; fallback: users)
  let code = null;
  let valueCents = 0;
  let dbTrayCouponId = null;
  let dbSource = "unknown";
  try {
    const loaded = await loadCouponSystemRow(uid);
    dbSource = loaded.source;
    const row = loaded.row;
    if (!row) {
      console.log(`[tray.coupon.ensure] user_not_found user=${uid} rid=${rid}`);
      return { ok: false, status: "USER_NOT_FOUND" };
    }
    const dbCode = row.coupon_code != null ? String(row.coupon_code).trim() : "";
    dbTrayCouponId = row.tray_coupon_id != null ? String(row.tray_coupon_id) : null;
    valueCents = Number(row.coupon_value_cents || 0);

    console.log("[tray.coupon.ensure] db coupon", {
      user: uid,
      source: dbSource,
      coupon_code: dbCode || null,
      tray_coupon_id: dbTrayCouponId || null,
      coupon_value_cents: valueCents,
    });

    // Regra: respeitar coupon_code persistido; só gera se estiver vazio.
    if (dbCode) {
      code = dbCode;
    } else {
      code = makeUserCouponCode(uid);
      await persistCouponSystemFields({ userId: uid, code }).catch(() => {});
    }
  } catch (e) {
    console.log(`[tray.coupon.ensure] user_load_failed user=${uid} rid=${rid} msg=${e?.message || e}`);
    return { ok: false, status: "USER_LOAD_FAILED" };
  }

  // REGRAS NOVAS: starts_at = última compra aprovada; ends_at = starts + 6 meses
  const lastPurchaseAt = await getUserLastApprovedPurchaseDate(uid).catch(() => null);
  console.log(`[tray.coupon.ensure] lastPurchase user=${uid} rid=${rid} lastPurchaseAt=${lastPurchaseAt ? lastPurchaseAt.toISOString() : "null"}`);
  if (!lastPurchaseAt) {
    console.log(`[tray.coupon.ensure] user=${uid} rid=${rid} action=no_purchase`);
    return { ok: true, status: "NO_PURCHASE", action: "no_purchase", code };
  }

  const startsAt = fmtDate(lastPurchaseAt);
  const endsAt = fmtDate(addMonthsClamped(lastPurchaseAt, 6));
  const giftValueBRL = Number((valueCents / 100).toFixed(2));

  function getMinPurchaseByGiftValue(v) {
    if (!Number.isFinite(v)) return null;
    if (v >= 50 && v <= 250) return 1500;
    if (v >= 251 && v <= 600) return 3500;
    if (v >= 601 && v <= 800) return 5500;
    if (v >= 801 && v <= 1100) return 7500;
    if (v >= 1101 && v <= 2100) return 15000;
    if (v >= 2101 && v <= 3100) return 22500;
    if (v >= 3101 && v <= 4200) return 30000;
    return null;
  }

  // Regra obrigatória: GC < 50 => não criar/atualizar cupom (evita rejeições e cupom inválido)
  if (Number.isFinite(giftValueBRL) && giftValueBRL < 50) {
    console.warn(`[tray.coupon.rules] WARN giftValueBRL=${giftValueBRL.toFixed(2)} abaixo do mínimo (50.00) -> skip_below_min`);
    console.log(
      `[tray.coupon.ensure] computed user=${uid} rid=${rid} code=${code} value=${valueCents} giftValueBRL=${giftValueBRL.toFixed(2)} value_start=SKIP lastPurchaseAt=${lastPurchaseAt.toISOString()} starts=${startsAt} ends=${endsAt} usage_counter_limit=1 usage_counter_limit_customer=1`
    );
    await upsertCouponTraySync({ userId: uid, code, status: "FAILED", lastError: "skip_below_min", trayCouponId: null, syncedAt: null }).catch(() => {});
    return { ok: true, status: "SKIPPED", action: "skip_below_min", code };
  }

  let minPurchase = getMinPurchaseByGiftValue(giftValueBRL);
  if (minPurchase == null) {
    console.warn(`[tray.coupon.rules] WARN giftValueBRL=${giftValueBRL.toFixed(2)} fora da tabela -> value_start=30000.00`);
    minPurchase = 30000;
  }

  console.log(
    `[tray.coupon.ensure] computed user=${uid} rid=${rid} code=${code} value=${valueCents} giftValueBRL=${giftValueBRL.toFixed(2)} value_start=${Number(minPurchase).toFixed(2)} lastPurchaseAt=${lastPurchaseAt.toISOString()} starts=${startsAt} ends=${endsAt} usage_counter_limit=1 usage_counter_limit_customer=1`
  );

  // Status tracking (best-effort)
  try {
    await upsertCouponTraySync({ userId: uid, code, status: "PENDING", lastError: null, trayCouponId: dbTrayCouponId, syncedAt: null });
  } catch {}

  const shortTimeoutMs = Math.max(1000, Number(timeoutMs || 5000)); // token/find
  const createTimeoutMs = 30_000; // (5) obrigatório: POST cupom 30s
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

  const confirmAndValidate = async (trayId, expected) => {
    const conf = await withAbort(shortTimeoutMs, (signal) => trayGetCouponById(trayId, { signal })).catch(() => null);
    const c = conf?.coupon || null;
    if (!conf?.ok || !c) return { ok: false, reason: "confirm_failed", coupon: c };

    const normDate = (v) => (v ? String(v).slice(0, 10) : null);
    const normNum2 = (v) => {
      const n = Number(String(v).replace(",", "."));
      return Number.isFinite(n) ? n.toFixed(2) : null;
    };

    const got = {
      starts_at: normDate(c.starts_at),
      ends_at: normDate(c.ends_at),
      value: normNum2(c.value),
      value_start: normNum2(c.value_start),
      usage_counter_limit: String(c.usage_counter_limit || ""),
      usage_counter_limit_customer: String(c.usage_counter_limit_customer || ""),
    };

    const exp = {
      starts_at: expected.startsAt,
      ends_at: expected.endsAt,
      value: Number(expected.valueBRL).toFixed(2),
      value_start: Number(expected.valueStartBRL).toFixed(2),
      usage_counter_limit: "1",
      usage_counter_limit_customer: "1",
    };

    console.log("[tray.coupon.confirm]", {
      id: String(trayId),
      ok: true,
      starts: got.starts_at,
      ends: got.ends_at,
      value: got.value,
      value_start: got.value_start,
      usage_counter_limit: got.usage_counter_limit,
      usage_counter_limit_customer: got.usage_counter_limit_customer,
    });

    const matches =
      got.starts_at === exp.starts_at &&
      got.ends_at === exp.ends_at &&
      got.value === exp.value &&
      got.value_start === exp.value_start &&
      got.usage_counter_limit === exp.usage_counter_limit &&
      got.usage_counter_limit_customer === exp.usage_counter_limit_customer;

    if (!matches) {
      console.warn("[tray.coupon.confirm] WARN mismatch", { expected: exp, got });
    }
    return { ok: matches, reason: matches ? null : "mismatch", coupon: c, expected: exp, got };
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
      const existingStarts = found?.coupon?.starts_at || null;
      const existingEnds = found?.coupon?.ends_at || null;
      const existingValue = found?.coupon?.value || null;
      const existingValueStart = found?.coupon?.value_start || null;
      const desiredValue = giftValueBRL.toFixed(2);
      const desiredValueStart = Number(minPurchase).toFixed(2);

      const needsUpdate =
        (existingStarts && existingStarts !== startsAt) ||
        (existingEnds && existingEnds !== endsAt) ||
        (existingValue && String(existingValue) !== desiredValue) ||
        (existingValueStart && String(existingValueStart) !== desiredValueStart);

      if (!needsUpdate) {
        console.log(`[tray.coupon.ensure] user=${uid} rid=${rid} code=${code} action=exists trayId=${trayId || ""}`);
        if (trayId) await persistCouponSystemFields({ userId: uid, code, trayCouponId: trayId }).catch(() => {});
        await upsertCouponTraySync({ userId: uid, code, status: "SYNCED", lastError: null, trayCouponId: trayId, syncedAt: new Date().toISOString() }).catch(() => {});
        return { ok: true, status: "SYNCED", action: "exists", code, trayId };
      }

      console.log(`[tray.coupon.update] user=${uid} rid=${rid} id=${trayId} code=${code} starts=${startsAt} ends=${endsAt} value=${desiredValue} value_start=${minPurchase != null ? Number(minPurchase).toFixed(2) : ""}`);
      let updated = null;
      try {
        updated = await withAbort(createTimeoutMs, (signal) =>
          trayUpdateCouponById(trayId, {
            startsAt,
            endsAt,
            valueBRL: giftValueBRL,
            minPurchaseBRL: minPurchase,
            description: `Crédito do cliente ${uid} - New Store`,
            signal,
          })
        );
      } catch (e) {
        const aborted = e?.name === "AbortError" || String(e?.message || "").includes("timeout");
        if (!aborted) throw e;
        console.log(`[tray.coupon.ensure] user=${uid} rid=${rid} code=${code} action=update_timeout_confirming`);
        const confirmed = await pollFindAfterTimeout();
        if (confirmed.found) {
          console.log(`[tray.coupon.ensure] user=${uid} rid=${rid} code=${code} action=updated_confirmed_after_timeout trayId=${confirmed.trayId || ""}`);
          await upsertCouponTraySync({ userId: uid, code, status: "SYNCED", lastError: null, trayCouponId: confirmed.trayId, syncedAt: new Date().toISOString() }).catch(() => {});
          return { ok: true, status: "SYNCED", action: "updated_confirmed_after_timeout", code, trayId: confirmed.trayId };
        }
        console.log(`[tray.coupon.ensure] user=${uid} rid=${rid} code=${code} action=failed status=update_timeout_not_confirmed`);
        await upsertCouponTraySync({ userId: uid, code, status: "FAILED", lastError: "update_timeout_not_confirmed", trayCouponId: trayId, syncedAt: null }).catch(() => {});
        return { ok: true, status: "FAILED", action: "failed", code };
      }

      console.log(`[tray.coupon.update.resp] user=${uid} rid=${rid} status=${updated?.status ?? ""} ok=${!!updated?.ok} id=${trayId}`);

      // Confirmar e validar regras antes de marcar updated
      const conf = await confirmAndValidate(trayId, {
        startsAt,
        endsAt,
        valueBRL: giftValueBRL,
        valueStartBRL: minPurchase,
      });
      if (updated?.ok || conf.ok) {
        console.log(`[tray.coupon.ensure] user=${uid} rid=${rid} code=${code} action=updated trayId=${trayId || ""}`);
        if (trayId) await persistCouponSystemFields({ userId: uid, code, trayCouponId: trayId }).catch(() => {});
        await upsertCouponTraySync({ userId: uid, code, status: "SYNCED", lastError: null, trayCouponId: trayId, syncedAt: new Date().toISOString() }).catch(() => {});
        return { ok: true, status: "SYNCED", action: "updated", code, trayId };
      }

      console.log(`[tray.coupon.ensure] user=${uid} rid=${rid} code=${code} action=failed status=update_not_confirmed`);
      await upsertCouponTraySync({ userId: uid, code, status: "FAILED", lastError: "update_not_confirmed", trayCouponId: trayId, syncedAt: null }).catch(() => {});
      return { ok: true, status: "FAILED", action: "failed", code };
    }

    // 3) create
    console.log(`[tray.coupon.create] user=${uid} rid=${rid} code=${code} value=${valueCents} starts=${startsAt} ends=${endsAt}`);
    let created = null;
    try {
      created = await withAbort(createTimeoutMs, (signal) =>
        trayCreateCoupon({
          code,
          valueBRL: giftValueBRL,
          valueStartBRL: minPurchase,
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
      console.log("[tray.coupon.create] success", { user: uid, code, tray_coupon_id: String(trayId) });
      const conf = await confirmAndValidate(trayId, {
        startsAt,
        endsAt,
        valueBRL: giftValueBRL,
        valueStartBRL: minPurchase,
      });
      if (conf.ok) {
        await persistCouponSystemFields({ userId: uid, code, trayCouponId: trayId }).catch(() => {});
        await upsertCouponTraySync({ userId: uid, code, status: "SYNCED", lastError: null, trayCouponId: trayId, syncedAt: new Date().toISOString() }).catch(() => {});
        return { ok: true, status: "SYNCED", action: "created", code, trayId };
      }

      console.log(`[tray.coupon.ensure] user=${uid} rid=${rid} code=${code} action=failed status=create_not_confirmed`);
      await upsertCouponTraySync({ userId: uid, code, status: "FAILED", lastError: "create_not_confirmed", trayCouponId: trayId, syncedAt: null }).catch(() => {});
      return { ok: true, status: "FAILED", action: "failed", code };
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


