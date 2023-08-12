CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
	id serial4 NOT NULL,
	ts timestamp NOT NULL,
	"year" int4 NOT NULL,
	"month" int4 NOT NULL,
	"day" int4 NOT NULL,
	"time" time NOT NULL,
	"date" date NOT NULL,
	CONSTRAINT dm_timestamps_pkey PRIMARY KEY (id),
	CONSTRAINT dm_timestamps_ts_key UNIQUE (ts)
);