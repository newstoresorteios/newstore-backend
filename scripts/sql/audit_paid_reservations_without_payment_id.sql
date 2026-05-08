-- Audit: paid reservations without payment_id
-- No automatic credit/payment creation here.

SELECT
  r.id AS reservation_id,
  r.user_id,
  u.name,
  u.email,
  r.draw_id,
  r.numbers,
  COALESCE(array_length(r.numbers, 1), 0)::int AS qty,
  r.created_at,
  r.expires_at
FROM public.reservations r
LEFT JOIN public.users u ON u.id = r.user_id
WHERE lower(COALESCE(r.status, '')) = 'paid'
  AND r.payment_id IS NULL
ORDER BY r.created_at DESC, r.id DESC;

