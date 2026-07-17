import { getPool, query } from './db/pg.js';
import { hashPassword } from './utils.js';

async function seedInitialDrawAndNumbers() {
  const existingDraws = await query(
    'select count(*)::int as total from public.draws'
  );
  const totalDraws = Number(existingDraws.rows[0]?.total || 0);

  if (totalDraws > 0) {
    console.log('[seed] existing_draws_preserved', {
      total_draws: totalDraws,
      action: 'skip_data_seed',
    });
    return;
  }

  const pool = await getPool();
  const client = await pool.connect();
  let transactionOpen = false;

  try {
    await client.query('begin');
    transactionOpen = true;

    await client.query(
      "select pg_advisory_xact_lock(hashtext('newstore_initial_seed'))"
    );

    const lockedDraws = await client.query(
      'select count(*)::int as total from public.draws'
    );
    const lockedTotalDraws = Number(lockedDraws.rows[0]?.total || 0);

    if (lockedTotalDraws > 0) {
      console.log('[seed] existing_draws_preserved', {
        total_draws: lockedTotalDraws,
        action: 'skip_data_seed',
      });
      await client.query('commit');
      transactionOpen = false;
      return;
    }

    const drawTypeColumn = await client.query(
      `select 1
         from information_schema.columns
        where table_schema = 'public'
          and table_name = 'draws'
          and column_name = 'draw_type'
        limit 1`
    );

    const insertedDraw = drawTypeColumn.rows.length
      ? await client.query(
          `insert into public.draws(status, draw_type)
           values('open', 'principal')
           returning id`
        )
      : await client.query(
          `insert into public.draws(status)
           values('open')
           returning id`
        );
    const drawId = insertedDraw.rows[0].id;

    await client.query(
      `insert into public.numbers(draw_id, n, status, reservation_id)
       select $1, n, 'available', null
         from generate_series(0, 99) as generated(n)`,
      [drawId]
    );

    await client.query('commit');
    transactionOpen = false;
  } catch (error) {
    if (transactionOpen) {
      await client.query('rollback').catch(() => {});
    }
    throw error;
  } finally {
    client.release();
  }
}

export async function ensureSchema() {
  // Tabelas
  await query(`
    create table if not exists users (
      id serial primary key,
      name text not null,
      email text unique not null,
      pass_hash text not null,
      is_admin boolean default false,
      created_at timestamptz default now()
    );

    create table if not exists draws (
      id serial primary key,
      status text not null default 'open',
      opened_at timestamptz default now(),
      closed_at timestamptz
    );

    create table if not exists numbers (
      draw_id int references draws(id) on delete cascade,
      n smallint not null,
      status text not null default 'available',
      reservation_id uuid,
      primary key (draw_id, n)
    );

    create table if not exists reservations (
      id uuid primary key,
      user_id int references users(id) on delete cascade,
      draw_id int references draws(id) on delete cascade,
      numbers int[] not null,
      status text not null default 'active',
      expires_at timestamptz not null,
      payment_id text,
      created_at timestamptz default now()
    );

    create table if not exists payments (
      id text primary key,
      user_id int references users(id) on delete set null,
      draw_id int references draws(id) on delete set null,
      numbers int[] not null,
      amount_cents int not null,
      status text not null,
      qr_code text,
      qr_code_base64 text,
      created_at timestamptz default now(),
      paid_at timestamptz
    );
  `);

  // Cria o sorteio inicial somente quando o banco ainda não possui draws.
  await seedInitialDrawAndNumbers();

  // Usuário de teste
  const email = 'teste@newstore.com';
  const exists = await query('select 1 from users where email=$1', [email]);
  if (!exists.rows.length) {
    const pass = await hashPassword('123456');
    await query(
      'insert into users(name, email, pass_hash, is_admin) values($1,$2,$3,$4)',
      ['Teste', email, pass, true]
    );
  }
}
