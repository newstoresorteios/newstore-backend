export const JWT_SECRET =
  process.env.JWT_SECRET ||
  process.env.JWT_SECRET_KEY ||
  process.env.SUPABASE_JWT_SECRET;

if (!JWT_SECRET) {
  throw new Error(
    "JWT_SECRET missing. Configure JWT_SECRET, JWT_SECRET_KEY, or SUPABASE_JWT_SECRET."
  );
}

