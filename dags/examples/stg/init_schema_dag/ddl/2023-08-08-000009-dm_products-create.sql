
create table if not exists dds.dm_products (
	id serial4 not null primary key,
	product_id varchar not null unique,
	product_name varchar not null, 
	product_price numeric(19, 5) not null,
	active_from timestamp not null,
	active_to timestamp not null,
	restaurant_id int not null references dds.dm_restaurants (id)
);