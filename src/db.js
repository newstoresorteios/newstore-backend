import pg from 'pg';
const { Pool } = pg;

// Bypass TLS only in development to avoid SELF_SIGNED_CERT_IN_CHAIN on Windows
if ((process.env.NODE_ENV || 'development') === 'development') {
  process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
  pg.defaults.ssl = { rejectUnauthorized: false };
}

const connectionString = process.env.POSTGRES_URL || process.env.DATABASE_URL;

export const pool = new Pool({
  connectionString,
  ssl: { rejectUnauthorized: false }
});

export const query = (text, params) => pool.query(text, params);
