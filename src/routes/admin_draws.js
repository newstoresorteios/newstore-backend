// backend/src/routes/admin_draws.js
import { Router } from "express";
import { query } from "../db.js";
import { requireAuth } from "../middleware/auth.js";
import { runAutopayForDraw } from "../services/autopayRunner.js";
import {
  createCaptivePreAuthorizationsForDraw,
  isCaptivePreauthEnabled,
  resolveCaptivePreauthDrawRequirement,
} from "../services/autopay/captivePreauthService.js";
import { handlePushAutomationEvent } from "../services/notifications/pushAutomationEvents.js";
import { handleAutomaticEmailEvent } from "../services/notifications/automaticEmailNotifications.js";

const router = Router();

async function emitAdminNewDrawPublished(drawId) {
  if (process.env.PUSH_ALLOW_ENGINE_EVENTS !== "true") return;
  try {
    await handlePushAutomationEvent({
      eventKey: "NEW_DRAW_PUBLISHED",
      source: "admin",
      referenceType: "draw",
      referenceKey: `draw:${drawId}`,
      metadata: {
        draw_id: Number(drawId),
        origin: "admin",
      },
      actor: { type: "admin_draws" },
      dryRun: process.env.PUSH_ENGINE_DRY_RUN !== "false",
    });
  } catch (error) {
    console.warn("[admin/draws] push automation event skipped", {
      draw_id: drawId,
      code: error?.code || null,
      message: error?.message || null,
    });
  }
}

async function emitAdminNewDrawEmail(draw) {
  try {
    await handleAutomaticEmailEvent({
      eventKey: "NEW_DRAW_PUBLISHED",
      referenceType: "draw",
      referenceKey: `draw:${draw.id}:published_email`,
      metadata: { draw_id: Number(draw.id), draw_type: "principal", product_name: draw.product_name || null },
      occurredAt: new Date().toISOString(),
    });
  } catch (error) {
    console.warn("[admin/draws] email automation skipped", {
      event_key: "NEW_DRAW_PUBLISHED",
      reference_key: `draw:${draw.id}:published_email`,
      draw_id: Number(draw.id),
      code: error?.code || null,
    });
  }
}

async function ensureNumbersForDraw(drawId) {
  await query(
    `insert into public.numbers(draw_id, n, status, reservation_id)
     select $1, gs::int2, 'available', null
       from generate_series(0,99) as gs
      where not exists (
        select 1 from public.numbers n where n.draw_id=$1 and n.n = gs::int2
      )`,
    [drawId]
  );
}

async function createCaptivePreauthIfEnabled(drawId, adminUserId, context) {
  if (!isCaptivePreauthEnabled()) {
    return { ok: true, skipped: true, reason: "captive_preauth_disabled" };
  }
  try {
    const requirement = await resolveCaptivePreauthDrawRequirement(drawId);
    if (!requirement.required) {
      return {
        ok: true,
        skipped: true,
        reason: "amount_not_above_default",
        ...requirement,
      };
    }
    return await createCaptivePreAuthorizationsForDraw(drawId, { adminUserId });
  } catch (error) {
    console.error(`[${context}] captive preauth failed`, {
      draw_id: drawId,
      admin_user_id: adminUserId || null,
      message: error?.message || null,
      code: error?.code || null,
    });
    return {
      ok: false,
      error: error?.message || "captive_preauth_create_failed",
      code: error?.code || null,
    };
  }
}

async function requireAdmin(req, res, next) {
  try {
    const userId = req?.user?.id;
    if (!userId) return res.status(401).json({ error: "unauthorized" });
    const r = await query("select is_admin from users where id = $1", [userId]);
    if (!r.rows.length || !r.rows[0].is_admin) {
      return res.status(403).json({ error: "forbidden" });
    }
    return next();
  } catch (e) {
    console.error("[admin check] error", e);
    return res.status(500).json({ error: "admin_check_failed" });
  }
}

/* ------------------------------------------------------------------ *
 * ADMIN: criar sorteio + rodar Autopay
 * ------------------------------------------------------------------ */
router.post("/new", requireAuth, requireAdmin, async (req, res) => {
  try {
    const product_name = String(req.body?.product_name || "").slice(0, 255) || null;
    const product_link = String(req.body?.product_link || "").slice(0, 1024) || null;

    const ins = await query(
      `insert into draws (status, opened_at, product_name, product_link, autopay_ran_at)
       values ('open', now(), $1, $2, null)
       returning id, status, product_name, product_link`,
      [product_name, product_link]
    );
    if (!ins.rowCount) return res.status(500).json({ error: "create_failed" });

    const draw = ins.rows[0];
    console.log("[admin/draws/new] novo draw id =", draw.id);

    // Garante tabela numbers 00..99 (runner também garante, mas aqui evita corrida inicial)
    try {
      await ensureNumbersForDraw(draw.id);
    } catch (e) {
      console.warn("[admin/draws/new] falha ao garantir numbers 00..99 (seguindo mesmo assim)", {
        draw_id: draw.id,
        msg: e?.message || e,
      });
    }

    const captivePreauth = await createCaptivePreauthIfEnabled(
      draw.id,
      req.user?.id ?? null,
      "admin/draws/new"
    );
    if (!captivePreauth?.ok) {
      return res.status(500).json({
        error: "captive_preauth_create_failed",
        draw_id: draw.id,
        draw,
        captive_preauth: captivePreauth,
      });
    }

    const result = await runAutopayForDraw(draw.id);
    if (!result?.ok) {
      return res.status(500).json({
        error: "autopay_run_failed",
        draw_id: draw.id,
        draw,
        captive_preauth: captivePreauth,
        autopay: result || null,
      });
    }
    await emitAdminNewDrawPublished(draw.id);
    await emitAdminNewDrawEmail(draw);
    return res.json({ ok: true, draw_id: draw.id, draw, captive_preauth: captivePreauth, autopay: result });
  } catch (e) {
    console.error("[admin/draws/new] error", e);
    return res.status(500).json({ error: "create_failed" });
  }
});

/* ------------------------------------------------------------------ *
 * Histórico (fechados)
 * ------------------------------------------------------------------ */
router.get("/history", requireAuth, requireAdmin, async (_req, res) => {
  try {
    const r = await query(`
      select
        d.id,
        d.status,
        coalesce(d.opened_at, d.created_at) as opened_at,
        d.closed_at,
        d.realized_at,
        round(extract(epoch from (coalesce(d.closed_at, now()) - coalesce(d.opened_at, d.created_at))) / 86400.0)::int as days_open,
        coalesce(d.winner_name, '-') as winner_name
      from draws d
      where d.status = 'closed' or d.closed_at is not null
      order by d.id desc
    `);
    res.json({ history: r.rows || [] });
  } catch (e) {
    console.error("[admin/draws/history] error", e);
    res.status(500).json({ error: "list_failed" });
  }
});

/* ------------------------------------------------------------------ *
 * Participantes pagos (reservations)
 * ------------------------------------------------------------------ */
router.get("/:id/participants", requireAuth, requireAdmin, async (req, res) => {
  try {
    const drawId = Number(req.params.id);
    if (!Number.isFinite(drawId)) return res.status(400).json({ error: "invalid_draw_id" });

    const sql = `
      select
        r.id as reservation_id,
        r.draw_id,
        r.user_id,
        num as number,
        r.status as status,
        r.created_at,
        coalesce(nullif(u.name,''), u.email, '-') as user_name,
        u.email as user_email
      from reservations r
      left join users u on u.id = r.user_id
      left join payments p on p.id = r.payment_id
      cross join lateral unnest(coalesce(r.numbers, '{}'::int[])) as num
      where r.draw_id = $1
        and (
          lower(coalesce(r.status,'')) in ('paid', 'pago', 'approved')
          or lower(coalesce(p.status,'')) in ('approved', 'paid', 'pago')
        )
      order by user_name asc, number asc
    `;
    const r = await query(sql, [drawId]);
    res.json({ draw_id: drawId, participants: r.rows || [] });
  } catch (e) {
    console.error("[admin/draws/:id/participants] error", e);
    res.status(500).json({ error: "participants_failed" });
  }
});

/* Alias /players */
router.get("/:id/players", requireAuth, requireAdmin, async (req, res) => {
  try {
    const drawId = Number(req.params.id);
    if (!Number.isFinite(drawId)) return res.status(400).json({ error: "invalid_draw_id" });

    const sql = `
      select
        r.id as reservation_id,
        r.draw_id,
        r.user_id,
        num as number,
        r.status as status,
        r.created_at,
        coalesce(nullif(u.name,''), u.email, '-') as user_name,
        u.email as user_email
      from reservations r
      left join users u on u.id = r.user_id
      left join payments p on p.id = r.payment_id
      cross join lateral unnest(coalesce(r.numbers, '{}'::int[])) as num
      where r.draw_id = $1
        and (
          lower(coalesce(r.status,'')) in ('paid', 'pago', 'approved')
          or lower(coalesce(p.status,'')) in ('approved', 'paid', 'pago')
        )
      order by user_name asc, number asc
    `;
    const r = await query(sql, [drawId]);
    res.json({ draw_id: drawId, participants: r.rows || [] });
  } catch (e) {
    console.error("[admin/draws/:id/players] error", e);
    res.status(500).json({ error: "participants_failed" });
  }
});

/* ------------------------------------------------------------------ *
 * Reabrir + rodar Autopay
 * ------------------------------------------------------------------ */
router.post("/:id/open", requireAuth, requireAdmin, async (req, res) => {
  const drawId = Number(req.params.id);
  if (!Number.isFinite(drawId)) return res.status(400).json({ error: "invalid_draw_id" });

  try {
    const up = await query(
      `update draws
          set status='open',
              opened_at = coalesce(opened_at, now()),
              closed_at = null,
              realized_at = null,
              autopay_ran_at = null
        where id = $1
        returning id, status`,
      [drawId]
    );
    if (!up.rowCount) return res.status(404).json({ error: "draw_not_found" });
    try {
      await ensureNumbersForDraw(drawId);
    } catch (e) {
      console.warn("[admin/draws/:id/open] falha ao garantir numbers 00..99 (seguindo mesmo assim)", {
        draw_id: drawId,
        msg: e?.message || e,
      });
    }
  } catch (e) {
    console.error("[admin/draws/:id/open] error", e);
    return res.status(500).json({ error: "open_failed" });
  }

  const captivePreauth = await createCaptivePreauthIfEnabled(
    drawId,
    req.user?.id ?? null,
    "admin/draws/:id/open"
  );
  if (!captivePreauth?.ok) {
    return res.status(500).json({
      error: "captive_preauth_create_failed",
      draw_id: drawId,
      captive_preauth: captivePreauth,
    });
  }

  const result = await runAutopayForDraw(drawId);
  if (!result?.ok) return res.status(500).json({ ...result, captive_preauth: captivePreauth });
  await emitAdminNewDrawPublished(drawId);
  await emitAdminNewDrawEmail({ id: drawId });
  return res.json({ ...result, captive_preauth: captivePreauth });
});

/* ------------------------------------------------------------------ *
 * Rodar Autopay manualmente
 * ------------------------------------------------------------------ */
router.post("/:id/autopay-run", requireAuth, requireAdmin, async (req, res) => {
  const drawId = Number(req.params.id);
  if (!Number.isFinite(drawId)) return res.status(400).json({ error: "invalid_draw_id" });

  const result = await runAutopayForDraw(drawId);
  if (!result?.ok) return res.status(500).json(result);
  return res.json(result);
});

export default router;
