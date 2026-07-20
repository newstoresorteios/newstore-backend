const OPEN_DRAW_SLOT_LOCK = "newstore_open_draw_slots";

function createLimitError(error, message) {
  return Object.assign(new Error(message), {
    status: 409,
    publicError: error,
    openDrawLimit: true,
  });
}

export async function lockOpenDrawSlots(client) {
  await client.query(
    "SELECT pg_advisory_xact_lock(hashtext($1))",
    [OPEN_DRAW_SLOT_LOCK]
  );
}

export async function assertCanOpenAdditionalDraw(client) {
  const result = await client.query(
    `SELECT
       COUNT(*) FILTER (
         WHERE COALESCE(draw_type, 'principal') = 'principal'
       ) AS principal_count
       FROM public.draws
      WHERE status = 'open'`
  );

  const counts = {
    principal: Number(result.rows[0]?.principal_count || 0),
  };

  if (counts.principal > 1) {
    throw createLimitError(
      "open_draw_state_inconsistent",
      "Existe mais de um sorteio principal em andamento."
    );
  }
  return counts;
}

export function getOpenDrawLimitResponse(error) {
  if (!error?.openDrawLimit || error?.status !== 409 || !error?.publicError) {
    return null;
  }
  return {
    error: error.publicError,
    message: error.message,
  };
}

export function isOneOpenPerTypeConstraint(error) {
  return (
    error?.code === "23505" &&
    error?.constraint === "ux_draws_one_open_per_type"
  );
}

export function isDrawTypeConstraintViolation(error) {
  return (
    error?.code === "23514" &&
    typeof error?.constraint === "string" &&
    error.constraint.toLowerCase().includes("draw_type")
  );
}
