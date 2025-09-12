import { Router } from 'express';
import { query } from '../db.js';

const router = Router();

router.get('/current', async (_req, res) => {
  const r = await query(`select id, status, opened_at, closed_at from draws where status='open' order by id desc limit 1`);
  if (!r.rows.length) return res.json(null);
  res.json(r.rows[0]);
});

export default router;
