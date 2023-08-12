
CREATE TABLE IF NOT EXISTS stg.deliverysystem_deliveries (
	id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT deliverysystem_deliveries_object_id_uindex UNIQUE (object_id),
	CONSTRAINT deliverysystem_deliveries_pkey PRIMARY KEY (id)
);