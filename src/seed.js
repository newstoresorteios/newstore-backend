import { query } from './db/pg.js';
import { hashPassword } from './utils.js';

export async function ensureSchema() {
  // Tabelas
  await query(`
  `);

  // Sorteio aberto
  const open = await query(`select id from draws where status='open' order by id desc limit 1`);
  let drawId;
  if (open.rows.length) {
    drawId = open.rows[0].id;
  } else {
    //const ins = await query(`insert into draws(status) values('open') returning id`);
    //drawId = ins.rows[0].id;
  }

  // Garante 100 números (00-99)
  const count = await query(`select count(*)::int as c from numbers where draw_id=$1`, [drawId]);
  if (count.rows[0].c < 100) {
    await query('delete from numbers where draw_id=$1', [drawId]);
    const tuples = [];
    for (let i = 0; i < 100; i++) {
      tuples.push(`($1, ${i}, 'available', null)`);
    }
    const sql = `insert into numbers(draw_id, n, status, reservation_id) values ${tuples.join(', ')}`;
    await query(sql, [drawId]);
  }

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
