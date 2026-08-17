-- Auditoria somente leitura do checkout agrupado.

-- 1. Total do batch diferente da soma dos itens.
SELECT b.id AS batch_id, b.amount_cents AS batch_amount_cents,
       COALESCE(SUM(i.amount_cents), 0)::int AS items_amount_cents
  FROM public.checkout_batches b
  LEFT JOIN public.checkout_batch_items i ON i.batch_id = b.id
 GROUP BY b.id, b.amount_cents
HAVING b.amount_cents <> COALESCE(SUM(i.amount_cents), 0);

-- 2. Itens sem reserva.
SELECT i.* FROM public.checkout_batch_items i
LEFT JOIN public.reservations r ON r.id = i.reservation_id
WHERE r.id IS NULL;

-- 3. Draw do item diferente da reserva.
SELECT i.id AS item_id, i.draw_id AS item_draw_id, r.draw_id AS reservation_draw_id
FROM public.checkout_batch_items i
JOIN public.reservations r ON r.id = i.reservation_id
WHERE i.draw_id <> r.draw_id;

-- 4. Numeros do item diferentes da reserva (ordem ignorada).
SELECT i.id AS item_id, i.numbers AS item_numbers, r.numbers AS reservation_numbers
FROM public.checkout_batch_items i
JOIN public.reservations r ON r.id = i.reservation_id
WHERE ARRAY(SELECT unnest(i.numbers) ORDER BY 1)
   <> ARRAY(SELECT unnest(r.numbers) ORDER BY 1);

-- 5. Pagamentos filhos sinteticos sem item.
SELECT p.* FROM public.payments p
LEFT JOIN public.checkout_batch_items i ON i.child_payment_id = p.id
WHERE p.id LIKE 'batch:%' AND i.id IS NULL;

-- 6. Batches liquidados sem pagamento filho em todos os itens.
SELECT b.id AS batch_id, COUNT(*)::int AS item_count,
       COUNT(i.child_payment_id)::int AS child_payment_count
FROM public.checkout_batches b
JOIN public.checkout_batch_items i ON i.batch_id = b.id
WHERE b.status = 'settled' OR b.settled_at IS NOT NULL
GROUP BY b.id
HAVING COUNT(*) <> COUNT(i.child_payment_id);

-- 7. Soma dos pagamentos filhos diferente do batch.
SELECT b.id AS batch_id, b.amount_cents AS batch_amount_cents,
       COALESCE(SUM(p.amount_cents), 0)::int AS child_amount_cents
FROM public.checkout_batches b
LEFT JOIN public.checkout_batch_items i ON i.batch_id = b.id
LEFT JOIN public.payments p ON p.id = i.child_payment_id
GROUP BY b.id, b.amount_cents
HAVING b.amount_cents <> COALESCE(SUM(p.amount_cents), 0)
   AND COUNT(i.child_payment_id) > 0;

-- 8. Reserva paga apontando para filho incorreto.
SELECT i.id AS item_id, i.reservation_id, i.child_payment_id, r.payment_id
FROM public.checkout_batch_items i
JOIN public.reservations r ON r.id = i.reservation_id
WHERE lower(r.status) IN ('paid', 'pago')
  AND r.payment_id IS DISTINCT FROM i.child_payment_id;

-- 9. Numeros vendidos que nao correspondem aos itens/pagamentos filhos.
SELECT i.id AS item_id, expected.n,
       n.status AS number_status, p.id AS child_payment_id
FROM public.checkout_batch_items i
CROSS JOIN LATERAL unnest(i.numbers) AS expected(n)
LEFT JOIN public.numbers n ON n.draw_id = i.draw_id AND n.n = expected.n
LEFT JOIN public.payments p ON p.id = i.child_payment_id
JOIN public.checkout_batches b ON b.id = i.batch_id
WHERE (b.status = 'settled' OR b.settled_at IS NOT NULL)
  AND (n.status IS DISTINCT FROM 'sold'
       OR p.id IS NULL
       OR NOT (expected.n = ANY(p.numbers))
       OR p.draw_id <> i.draw_id);

-- 10. Batches aprovados ainda nao liquidados.
SELECT * FROM public.checkout_batches
WHERE status = 'approved' AND settled_at IS NULL;

-- 11. Batches expirados ainda com numeros reservados para suas reservas.
SELECT b.id AS batch_id, i.id AS item_id, n.draw_id, n.n, n.reservation_id
FROM public.checkout_batches b
JOIN public.checkout_batch_items i ON i.batch_id = b.id
JOIN public.numbers n ON n.reservation_id = i.reservation_id
WHERE b.status = 'expired' AND n.status = 'reserved';

-- 12. Pagamentos filhos com usuario diferente do batch.
SELECT b.id AS batch_id, b.user_id AS batch_user_id,
       p.id AS child_payment_id, p.user_id AS payment_user_id
FROM public.checkout_batches b
JOIN public.checkout_batch_items i ON i.batch_id = b.id
JOIN public.payments p ON p.id = i.child_payment_id
WHERE p.user_id IS DISTINCT FROM b.user_id;
