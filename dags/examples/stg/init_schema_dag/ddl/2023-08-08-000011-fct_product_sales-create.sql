create table if not exists dds.fct_product_sales (
	id serial4 not null primary key,
	product_id int not null references dds.dm_products (id),
	order_id int not null references dds.dm_orders (id),
	"count" int not null,
	price numeric(19, 5) not null,
	total_sum numeric(19, 5) not null,
	bonus_payment numeric(19, 5) not null,
	bonus_grant numeric(19, 5) not null,
	constraint fct_product_sales_product_id_order_id_key unique (product_id, order_id)	
);