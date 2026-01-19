-- 006_coupon_tray_sync.sql
-- Tabela auxiliar para acompanhar consistência eventual do cupom na Tray.

create table if not exists public.coupon_tray_sync (
  user_id int4 primary key,
  code text not null,
  tray_coupon_id text null,
  tray_sync_status text not null default 'PENDING',
  tray_last_error text null,
  tray_synced_at timestamptz null,
  updated_at timestamptz default now(),
  created_at timestamptz default now()
);

create index if not exists coupon_tray_sync_status_idx
  on public.coupon_tray_sync(tray_sync_status, updated_at desc);


