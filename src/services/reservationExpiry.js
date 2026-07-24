const SQL_ALIAS_RE = /^[a-z_][a-z0-9_]*$/i;
const ISO_TIMESTAMP_RE =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(Z|([+-])(\d{2}):(\d{2}))$/;
const EFFECTIVE_FROM_ENV = "CAPTIVE_PREAUTH_AUTO_APPROVE_EFFECTIVE_FROM";
const loggedInvalidEffectiveFrom = new Set();

function assertSqlAlias(alias) {
  const value = String(alias || "").trim();
  if (!SQL_ALIAS_RE.test(value)) throw new Error("invalid_sql_alias");
  return value;
}

function warnInvalidEffectiveFromOnce(code, raw) {
  const key = `${code}:${String(raw || "")}`;
  if (loggedInvalidEffectiveFrom.has(key)) return;
  loggedInvalidEffectiveFrom.add(key);
  console.warn("[captive-preauth] auto_approve_effective_from_invalid", {
    config: EFFECTIVE_FROM_ENV,
    configured: Boolean(String(raw || "").trim()),
    reason: code,
    auto_approval_enabled: false,
  });
}

function isValidIsoTimestamp(raw) {
  const match = ISO_TIMESTAMP_RE.exec(raw);
  if (!match) return false;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  const offsetHour = match[10] == null ? 0 : Number(match[10]);
  const offsetMinute = match[11] == null ? 0 : Number(match[11]);
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const daysInMonth = [
    31,
    leapYear ? 29 : 28,
    31,
    30,
    31,
    30,
    31,
    31,
    30,
    31,
    30,
    31,
  ];
  return (
    year >= 1 &&
    month >= 1 &&
    month <= 12 &&
    day >= 1 &&
    day <= daysInMonth[month - 1] &&
    hour >= 0 &&
    hour <= 23 &&
    minute >= 0 &&
    minute <= 59 &&
    second >= 0 &&
    second <= 59 &&
    offsetHour >= 0 &&
    offsetHour <= 23 &&
    offsetMinute >= 0 &&
    offsetMinute <= 59
  );
}

export function resolveCaptivePreauthAutoApproveEffectiveFrom(
  value = process.env[EFFECTIVE_FROM_ENV]
) {
  const raw =
    value instanceof Date && Number.isFinite(value.getTime())
      ? value.toISOString()
      : String(value || "").trim();
  if (!raw) {
    const code = "auto_approve_effective_from_missing";
    warnInvalidEffectiveFromOnce(code, raw);
    return { ok: false, code, iso: null, timestamp: null };
  }
  if (!isValidIsoTimestamp(raw)) {
    const code = "auto_approve_effective_from_invalid";
    warnInvalidEffectiveFromOnce(code, raw);
    return { ok: false, code, iso: null, timestamp: null };
  }
  const timestamp = Date.parse(raw);
  if (!Number.isFinite(timestamp)) {
    const code = "auto_approve_effective_from_invalid";
    warnInvalidEffectiveFromOnce(code, raw);
    return { ok: false, code, iso: null, timestamp: null };
  }
  return {
    ok: true,
    code: null,
    iso: new Date(timestamp).toISOString(),
    timestamp,
  };
}

function effectiveFromSqlLiteral(effectiveFrom) {
  const resolved = resolveCaptivePreauthAutoApproveEffectiveFrom(effectiveFrom);
  if (!resolved.ok) return null;
  // `iso` is produced by Date#toISOString, so it cannot contain arbitrary SQL.
  return `'${resolved.iso}'::timestamptz`;
}

export function pendingCaptivePreauthReservationGuardSql(
  reservationAlias = "r",
  effectiveFrom = process.env[EFFECTIVE_FROM_ENV]
) {
  const alias = assertSqlAlias(reservationAlias);
  const cutoffSql = effectiveFromSqlLiteral(effectiveFrom);
  if (!cutoffSql) return "TRUE";
  return `NOT EXISTS (
          SELECT 1
            FROM public.autopay_draw_authorizations pending_captive_preauth
           WHERE pending_captive_preauth.status = 'pending'
             AND pending_captive_preauth.created_at >= ${cutoffSql}
             AND pending_captive_preauth.draw_id = ${alias}.draw_id
             AND pending_captive_preauth.user_id = ${alias}.user_id
             AND pending_captive_preauth.captive_number = ANY(${alias}.numbers)
        )`;
}

export function pendingCaptivePreauthNumberGuardSql(
  numberAlias = "n",
  reservationAlias = "protected_reservation",
  effectiveFrom = process.env[EFFECTIVE_FROM_ENV]
) {
  const number = assertSqlAlias(numberAlias);
  const reservation = assertSqlAlias(reservationAlias);
  const cutoffSql = effectiveFromSqlLiteral(effectiveFrom);
  if (!cutoffSql) return "TRUE";
  return `NOT EXISTS (
          SELECT 1
            FROM public.reservations ${reservation}
            JOIN public.autopay_draw_authorizations pending_captive_preauth
              ON pending_captive_preauth.status = 'pending'
             AND pending_captive_preauth.created_at >= ${cutoffSql}
             AND pending_captive_preauth.draw_id = ${reservation}.draw_id
             AND pending_captive_preauth.user_id = ${reservation}.user_id
             AND pending_captive_preauth.captive_number = ANY(${reservation}.numbers)
           WHERE ${reservation}.id = ${number}.reservation_id
        )`;
}

export async function expireReservationForNumbersCleanup(
  reservationId,
  runQuery,
  effectiveFrom = process.env[EFFECTIVE_FROM_ENV]
) {
  if (typeof runQuery !== "function") throw new Error("reservation_expiry_query_required");
  return runQuery(
    `UPDATE public.reservations reservation
        SET status = 'expired'
      WHERE reservation.id = $1
        AND reservation.expires_at IS NOT NULL
        AND reservation.expires_at < now()
        AND lower(coalesce(reservation.status, '')) IN ('active', 'pending', 'reserved', '')
        AND ${pendingCaptivePreauthReservationGuardSql("reservation", effectiveFrom)}
      RETURNING reservation.id`,
    [reservationId]
  );
}

export async function cleanupExpiredReservationsGlobal(
  runQuery,
  effectiveFrom = process.env[EFFECTIVE_FROM_ENV]
) {
  if (typeof runQuery !== "function") throw new Error("reservation_expiry_query_required");
  await runQuery(
    `UPDATE reservations reservation
        SET status = 'expired'
      WHERE reservation.expires_at IS NOT NULL
        AND reservation.expires_at < NOW()
        AND lower(coalesce(reservation.status,'')) IN ('active','pending','reserved','')
        AND ${pendingCaptivePreauthReservationGuardSql("reservation", effectiveFrom)}`
  );

  await runQuery(
    `UPDATE numbers n
        SET status = 'available',
            reservation_id = NULL
      WHERE n.status = 'reserved'
        AND NOT EXISTS (
              SELECT 1
                FROM reservations r
               WHERE r.id = n.reservation_id
                 AND lower(coalesce(r.status,'')) IN ('active','pending','reserved','')
            )
        AND ${pendingCaptivePreauthNumberGuardSql("n", "protected_reservation", effectiveFrom)}`
  );
}
