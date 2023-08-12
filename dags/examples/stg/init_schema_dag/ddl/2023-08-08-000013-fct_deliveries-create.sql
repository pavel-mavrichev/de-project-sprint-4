
create table if not exists dds.fct_deliveries (
	id serial4 not null,
	delivery_id varchar not null,
	courier_id int4 not null references dds.dm_couriers (id),
	order_ts timestamp not null,
	order_id int4 not null references dds.dm_orders (id),
	rate int4 not null,
	tip_sum numeric(19, 5) not null default 0,
	constraint fct_deliveries_delivery_id_key unique (delivery_id),
	constraint fct_deliveries_pkey primary key (id),
	constraint fct_deliveries_rate_check check ((rate > 0))
);