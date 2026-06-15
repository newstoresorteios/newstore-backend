import { Router } from "express";
import { query } from "../db.js";
import { requireAuth, requireAdmin } from "../middleware/auth.js";

const router = Router();

router.use(requireAuth, requireAdmin);

function getQueryValue(value) {
  if (Array.isArray(value)) return value[0];
  return value;
}

function parsePage(value) {
  const parsed = Number(getQueryValue(value));
  if (!Number.isFinite(parsed)) return 1;
  return Math.max(1, Math.trunc(parsed));
}

function parsePageSize(value, defaultValue = 50, maxValue = 100) {
  const parsed = Number(getQueryValue(value));
  if (!Number.isFinite(parsed)) return defaultValue;
  return Math.min(maxValue, Math.max(1, Math.trunc(parsed)));
}

function normalizedText(value) {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return text || null;
}

function centsToBrl(value) {
  const cents = Number(value || 0);
  return Number((cents / 100).toFixed(2));
}

function safeMeta(meta) {
  if (!meta || typeof meta !== "object" || Array.isArray(meta)) return {};

  return {
    ...(meta.source !== undefined && { source: meta.source }),
    ...(meta.assignment_source !== undefined && { assignment_source: meta.assignment_source }),
    ...(meta.admin_user_id !== undefined && { admin_user_id: meta.admin_user_id }),
    ...(meta.credit_coupon !== undefined && { credit_coupon: meta.credit_coupon }),
    ...(meta.payment_status !== undefined && { payment_status: meta.payment_status }),
  };
}

function getAdminUserId(meta) {
  if (!meta || typeof meta !== "object" || Array.isArray(meta)) return null;
  const raw = meta.admin_user_id;
  if (raw === null || raw === undefined) return null;
  const text = String(raw).trim();
  if (!/^\d+$/.test(text)) return null;
  const id = Number(text);
  return Number.isSafeInteger(id) && id > 0 ? id : null;
}

function isAdminAssignedNumber(meta) {
  if (!meta || typeof meta !== "object" || Array.isArray(meta)) return false;

  return [meta.source, meta.assignment_source].some((value) => {
    const text = String(value || "").toLowerCase();
    return (
      text.includes("admin_assign_numbers") ||
      text.includes("adminusers.assignnumbers")
    );
  });
}

function interpretMovement(row) {
  const eventType = row.event_type || null;
  const channel = row.channel || null;
  const deltaCents = Number(row.delta_cents || 0);
  const meta = row.meta || {};

  if (eventType === "CREDIT_PURCHASE" && channel === "PIX") {
    return {
      movement_type: "PURCHASE_CASHBACK",
      movement_label: "Crédito de compra",
      origin_label: "Pagamento via PIX",
      description: "Cashback gerado por compra paga via PIX",
    };
  }

  if (eventType === "CREDIT_PURCHASE" && channel === "VINDI") {
    return {
      movement_type: "VINDI_AUTOPAY_CASHBACK",
      movement_label: "Crédito de compra",
      origin_label: "Cobrança automática Vindi",
      description: "Cashback gerado por cobrança automática no cartão",
    };
  }

  if (eventType === "CREDIT_PURCHASE" && channel === "ADMIN" && isAdminAssignedNumber(meta)) {
    return {
      movement_type: "ADMIN_ASSIGNED_NUMBER_CREDIT",
      movement_label: "Crédito administrativo",
      origin_label: "Número atribuído pelo administrador",
      description: "Crédito gerado por atribuição administrativa de número",
    };
  }

  if (eventType === "ADMIN_BALANCE_ADJUSTMENT" && deltaCents > 0) {
    return {
      movement_type: "ADMIN_BALANCE_CREDIT",
      movement_label: "Saldo adicionado",
      origin_label: "Ajuste manual do administrador",
      description: "Saldo adicionado manualmente pelo administrador",
    };
  }

  if (eventType === "ADMIN_BALANCE_ADJUSTMENT" && deltaCents < 0) {
    return {
      movement_type: "ADMIN_BALANCE_DEBIT",
      movement_label: "Saldo removido",
      origin_label: "Ajuste manual do administrador",
      description: "Saldo removido manualmente pelo administrador",
    };
  }

  return {
    movement_type: eventType,
    movement_label: eventType,
    origin_label: channel,
    description: "Movimentação registrada no sistema",
  };
}

router.get("/users", async (req, res) => {
  const search = String(getQueryValue(req.query.q) || "").trim().slice(0, 120);
  const page = parsePage(req.query.page);
  const pageSize = parsePageSize(req.query.pageSize);
  const offset = (page - 1) * pageSize;

  const params = [];
  let searchFilter = "";

  if (search) {
    params.push(`%${search}%`);
    searchFilter = `
      AND (
        COALESCE(u.name, '') ILIKE $${params.length}
        OR COALESCE(u.email, '') ILIKE $${params.length}
        OR COALESCE(u.coupon_code, '') ILIKE $${params.length}
        OR COALESCE(cts.code, '') ILIKE $${params.length}
        OR CAST(u.id AS TEXT) ILIKE $${params.length}
      )`;
  }

  params.push(pageSize);
  const limitParam = `$${params.length}`;
  params.push(offset);
  const offsetParam = `$${params.length}`;

  try {
    const { rows } = await query(
      `
      WITH history_stats AS (
        SELECT
          user_id,
          COUNT(*) AS movements_count,
          MAX(created_at) AS last_movement_at
        FROM public.coupon_balance_history
        GROUP BY user_id
      ),
      filtered_users AS (
        SELECT
          u.id AS user_id,
          u.name,
          u.email,
          u.created_at,
          u.coupon_code,
          u.coupon_value_cents AS balance_cents,
          hs.last_movement_at,
          COALESCE(hs.movements_count, 0) AS movements_count,
          cts.code AS tray_code,
          cts.tray_coupon_id,
          cts.tray_sync_status,
          cts.tray_last_error,
          cts.tray_synced_at,
          COALESCE(hs.last_movement_at, u.created_at) AS sort_at
        FROM public.users u
        LEFT JOIN history_stats hs
          ON hs.user_id = u.id
        LEFT JOIN public.coupon_tray_sync cts
          ON cts.user_id = u.id
        WHERE COALESCE(u.is_admin, false) = false
        ${searchFilter}
      ),
      total AS (
        SELECT COUNT(*) AS total_count
        FROM filtered_users
      )
      SELECT
        fu.user_id,
        fu.name,
        fu.email,
        fu.created_at,
        fu.coupon_code,
        fu.balance_cents,
        fu.last_movement_at,
        fu.movements_count,
        fu.tray_code,
        fu.tray_coupon_id,
        fu.tray_sync_status,
        fu.tray_last_error,
        fu.tray_synced_at,
        total.total_count
      FROM total
      LEFT JOIN LATERAL (
        SELECT *
        FROM filtered_users
        ORDER BY sort_at DESC, user_id DESC
        LIMIT ${limitParam}
        OFFSET ${offsetParam}
      ) fu ON true
      ORDER BY fu.sort_at DESC, fu.user_id DESC
      `,
      params
    );

    const total = Number(rows[0]?.total_count || 0);
    const users = rows
      .filter((row) => row.user_id !== null && row.user_id !== undefined)
      .map((row) => {
        const balanceCents = Number(row.balance_cents || 0);
        const movementsCount = Number(row.movements_count || 0);
        const userCouponCode = normalizedText(row.coupon_code);
        const trayCode = normalizedText(row.tray_code);

        return {
          user_id: Number(row.user_id),
          name: row.name || row.email || "-",
          email: row.email || null,
          created_at: row.created_at || null,
          coupon_code: userCouponCode || trayCode,
          balance_cents: balanceCents,
          balance_brl: Number((balanceCents / 100).toFixed(2)),
          last_movement_at: row.last_movement_at || null,
          movements_count: movementsCount,
          tray_code: trayCode,
          tray_coupon_id: row.tray_coupon_id ?? null,
          tray_sync_status: row.tray_sync_status || null,
          tray_last_error: row.tray_last_error || null,
          tray_synced_at: row.tray_synced_at || null,
        };
      });

    const totalPages = Math.ceil(total / pageSize);

    return res.json({
      ok: true,
      users,
      pagination: {
        page,
        page_size: pageSize,
        total,
        total_pages: totalPages,
        has_previous: page > 1,
        has_next: page < totalPages,
      },
      search,
    });
  } catch (error) {
    console.error("[admin/balance-history/users] error:", {
      code: error?.code,
      message: error?.message,
    });

    return res.status(500).json({
      ok: false,
      error: "balance_history_users_list_failed",
    });
  }
});

router.get("/users/:userId/movements", async (req, res) => {
  const userId = Number(req.params.userId);
  if (!Number.isSafeInteger(userId) || userId <= 0) {
    return res.status(400).json({
      ok: false,
      error: "invalid_user_id",
    });
  }

  const page = parsePage(req.query.page);
  const pageSize = parsePageSize(req.query.pageSize, 100, 200);
  const offset = (page - 1) * pageSize;

  try {
    const userResult = await query(
      `
      SELECT
        u.id AS user_id,
        u.name,
        u.email,
        u.created_at,
        u.coupon_code,
        u.coupon_value_cents AS balance_cents,
        cts.code AS tray_code,
        cts.tray_coupon_id,
        cts.tray_sync_status,
        cts.tray_last_error,
        cts.tray_synced_at
      FROM public.users u
      LEFT JOIN public.coupon_tray_sync cts
        ON cts.user_id = u.id
      WHERE u.id = $1
        AND COALESCE(u.is_admin, false) = false
      LIMIT 1
      `,
      [userId]
    );

    const userRow = userResult.rows[0];
    if (!userRow) {
      return res.status(404).json({
        ok: false,
        error: "user_not_found",
      });
    }

    const movementsResult = await query(
      `
      WITH filtered_movements AS (
        SELECT
          h.id,
          h.user_id,
          h.payment_id,
          h.delta_cents,
          h.balance_before_cents,
          h.balance_after_cents,
          h.event_type,
          h.channel,
          h.status,
          h.draw_id,
          h.run_trace_id,
          h.meta,
          h.created_at,
          h.created_at AS history_created_at,
          COALESCE(p.paid_at, p.created_at, reservation_event.created_at, h.created_at) AS event_date,
          admin_user.id AS admin_id,
          admin_user.name AS admin_name,
          admin_user.email AS admin_email
        FROM public.coupon_balance_history h
        LEFT JOIN public.payments p
          ON p.id = h.payment_id
        LEFT JOIN LATERAL (
          SELECT r.created_at
          FROM public.reservations r
          WHERE r.payment_id = h.payment_id
          ORDER BY r.created_at ASC
          LIMIT 1
        ) reservation_event ON true
        LEFT JOIN public.users admin_user
          ON admin_user.id = CASE
            WHEN h.meta->>'admin_user_id' ~ '^[0-9]+$'
            THEN (h.meta->>'admin_user_id')::integer
            ELSE NULL
          END
        WHERE h.user_id = $1
      ),
      total AS (
        SELECT COUNT(*) AS total_count
        FROM filtered_movements
      )
      SELECT
        fm.id,
        fm.user_id,
        fm.payment_id,
        fm.delta_cents,
        fm.balance_before_cents,
        fm.balance_after_cents,
        fm.event_type,
        fm.channel,
        fm.status,
        fm.draw_id,
        fm.run_trace_id,
        fm.meta,
        fm.created_at,
        fm.history_created_at,
        fm.event_date,
        fm.admin_id,
        fm.admin_name,
        fm.admin_email,
        total.total_count
      FROM total
      LEFT JOIN LATERAL (
        SELECT *
        FROM filtered_movements
        ORDER BY event_date DESC, id DESC
        LIMIT $2
        OFFSET $3
      ) fm ON true
      ORDER BY fm.event_date DESC, fm.id DESC
      `,
      [userId, pageSize, offset]
    );

    const balanceCents = Number(userRow.balance_cents || 0);
    const userCouponCode = normalizedText(userRow.coupon_code);
    const trayCode = normalizedText(userRow.tray_code);
    const total = Number(movementsResult.rows[0]?.total_count || 0);
    const totalPages = Math.ceil(total / pageSize);

    const movements = movementsResult.rows
      .filter((row) => row.id !== null && row.id !== undefined)
      .map((row) => {
        const deltaCents = Number(row.delta_cents || 0);
        const balanceBeforeCents = Number(row.balance_before_cents || 0);
        const balanceAfterCents = Number(row.balance_after_cents || 0);
        const meta = row.meta || {};
        const adminUserId = getAdminUserId(meta);
        const interpreted = interpretMovement(row);

        return {
          id: row.id,
          created_at: row.created_at || null,
          history_created_at: row.history_created_at || null,
          event_date: row.event_date || null,
          event_type: row.event_type || null,
          movement_type: interpreted.movement_type,
          movement_label: interpreted.movement_label,
          channel: row.channel || null,
          origin_label: interpreted.origin_label,
          description: interpreted.description,
          delta_cents: deltaCents,
          delta_brl: centsToBrl(deltaCents),
          balance_before_cents: balanceBeforeCents,
          balance_before_brl: centsToBrl(balanceBeforeCents),
          balance_after_cents: balanceAfterCents,
          balance_after_brl: centsToBrl(balanceAfterCents),
          status: row.status || null,
          payment_id: row.payment_id || null,
          draw_id: row.draw_id ?? null,
          run_trace_id: row.run_trace_id || null,
          admin: adminUserId
            ? {
                id: adminUserId,
                name: row.admin_name || null,
                email: row.admin_email || null,
              }
            : null,
          meta: safeMeta(meta),
        };
      });

    return res.json({
      ok: true,
      user: {
        user_id: Number(userRow.user_id),
        name: userRow.name || userRow.email || "-",
        email: userRow.email || null,
        coupon_code: userCouponCode || trayCode,
        balance_cents: balanceCents,
        balance_brl: centsToBrl(balanceCents),
        created_at: userRow.created_at || null,
      },
      tray: {
        tray_code: trayCode,
        tray_coupon_id: userRow.tray_coupon_id ?? null,
        tray_sync_status: userRow.tray_sync_status || null,
        tray_last_error: userRow.tray_last_error || null,
        tray_synced_at: userRow.tray_synced_at || null,
      },
      movements,
      pagination: {
        page,
        page_size: pageSize,
        total,
        total_pages: totalPages,
        has_previous: page > 1,
        has_next: page < totalPages,
      },
    });
  } catch (error) {
    console.error("[admin/balance-history/user-movements] error:", {
      code: error?.code,
      message: error?.message,
    });

    return res.status(500).json({
      ok: false,
      error: "balance_history_movements_failed",
    });
  }
});

export default router;
