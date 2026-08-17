import assert from "node:assert/strict";
import test from "node:test";

import { closeDrawIfSoldOut } from "../src/services/drawLifecycle.js";

function lifecycleDb({ stats, draw }) {
  const calls = [];
  return {
    calls,
    async query(sql) {
      calls.push(String(sql));
      if (String(sql).includes("COUNT(*)::int AS total")) {
        return { rows: [stats], rowCount: 1 };
      }
      if (String(sql).includes("SELECT id, status, closed_at")) {
        return { rows: [draw], rowCount: 1 };
      }
      if (String(sql).includes("UPDATE public.draws")) {
        return {
          rows: [{
            ...draw,
            status: "closed",
            closed_at: draw.closed_at || "2026-07-24T21:00:00.000Z",
          }],
          rowCount: 1,
        };
      }
      throw new Error(`unexpected_query:${sql}`);
    },
  };
}

test("fecha draw open quando todos os números estão sold", async () => {
  const db = lifecycleDb({
    stats: { total: 100, sold: 100, available: 0, reserved: 0 },
    draw: { id: 42, status: "open", closed_at: null },
  });

  const result = await closeDrawIfSoldOut(42, db);

  assert.equal(result.ok, true);
  assert.equal(result.closed, true);
  assert.equal(result.draw.status, "closed");
  assert.ok(result.draw.closed_at);
  assert.equal(db.calls.filter((sql) => sql.includes("UPDATE public.draws")).length, 1);
});

test("não fecha enquanto ainda existe reserva", async () => {
  const db = lifecycleDb({
    stats: { total: 100, sold: 99, available: 0, reserved: 1 },
    draw: { id: 42, status: "open", closed_at: null },
  });

  const result = await closeDrawIfSoldOut(42, db);

  assert.equal(result.ok, true);
  assert.equal(result.closed, false);
  assert.equal(db.calls.some((sql) => sql.includes("UPDATE public.draws")), false);
});

test("não fecha enquanto ainda existe número disponível", async () => {
  const db = lifecycleDb({
    stats: { total: 100, sold: 99, available: 1, reserved: 0 },
    draw: { id: 42, status: "open", closed_at: null },
  });

  const result = await closeDrawIfSoldOut(42, db);

  assert.equal(result.ok, true);
  assert.equal(result.closed, false);
  assert.equal(db.calls.some((sql) => sql.includes("UPDATE public.draws")), false);
});
