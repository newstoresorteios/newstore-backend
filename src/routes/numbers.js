import { Router } from 'express';
import { query } from '../db.js';

const router = Router();

router.get('/', async (_req, res) => {
  const dr = await query(`select id from draws where status='open' order by id desc limit 1`);
  if (!dr.rows.length) return res.json({ drawId: null, numbers: [] });

  const drawId = dr.rows[0].id;
  const r = await query('select n, status from numbers where draw_id=$1 order by n asc', [drawId]);
  const numbers = r.rows.map((x) => ({ n: x.n, status: x.status }));
  res.json({ drawId, numbers });
});

export default router;
