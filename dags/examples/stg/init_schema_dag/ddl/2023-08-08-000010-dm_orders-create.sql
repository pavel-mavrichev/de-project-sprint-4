create table if not exists dds.dm_orders (
	id serial4 not null primary key,
	order_key varchar not null unique,
	order_status varchar not null,
	restaurant_id int not null references dds.dm_restaurants(id),
	timestamp_id int not null references dds.dm_timestamps(id),
	user_id int not null references dds.dm_users(id)
);