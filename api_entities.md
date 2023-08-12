**Состав витрины cdm.dm_courier_ledger:**
1. **id** — идентификатор записи. **serial**
2. **courier_id** — ID курьера, которому перечисляем. **int**
3. **courier_name** — Ф. И. О. курьера. **varchar**
4. **settlement_year** — год отчёта. **int** Проверка на 2020 - 2099
5. **settlement_month** — месяц отчёта, где 1 — январь и 12 — декабрь. **int** Проверка на 1-12
6. **orders_count** — количество заказов за период (месяц). **int** Проверка > 0
7. **orders_total_sum** — общая стоимость заказов. **numeric(19, 5)** Проверка > 0
8. **rate_avg** — средний рейтинг курьера по оценкам пользователей. **numeric(3, 2)** Проверка 0-5
9. **order_processing_fee** — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25. **numeric(19, 5)** Проверка > 0
10. **courier_order_sum** — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга. **numeric(19, 5)** Проверка > 0
11. **courier_tips_sum** — сумма, которую пользователи оставили курьеру в качестве чаевых. **numeric(19, 5)** Проверка > 0
12. **courier_reward_sum** — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа). **numeric(19, 5)** Проверка > 0

**Источники cdm.dm_courier_ledger:**
1. **id** - генерируется
2. **courier_id** - *dm_couriers.id* (создать, связать с *fct_deliveries* по courier_id)
3. **courier_name** -  *dm_couriers.courier_name* (создать)
4. **settlement_year** - extract year from *fct_deliveries.order_ts* (создать)
5. **settlement_month** - extract month from *fct_deliveries.order_ts* (создать)
6. **orders_count** - count distinct *fct_deliveries.order_id* group by courier_id (создать)
7. **orders_total_sum** - sum fct_product_sales.total_sum (связать с fct_deliveries по order_id)
8. **rate_avg** - avg *fct_deliveries.rate* (cоздать)
9. **order_processing_fee** - расчет
10. **courier_order_sum** - расчет
11. **courier_tips_sum** - sum *fct_deliveries.tip_sum*
12. **courier_reward_sum** - расчет

**Что потребуется использовать:**
1. Создать **dm_couriers**, куда загружать (через stg) информацию о курьерах - конкретно имя
2. Создать **fct_deliveries**, куда загружать (через stg) информацию о доставках

**cdm.dm_courier_ledger DDL**
```
CREATE TABLE if not exists cdm.dm_courier_ledger (
	id serial4 NOT NULL,
	courier_id int4 NOT NULL references dds.dm_couriers (id),
	courier_name varchar NOT NULL,
	settlement_year int4 NOT NULL,
	settlement_month int4 NOT NULL,
	orders_count int4 NOT NULL,
	orders_total_sum numeric(19, 5) NOT NULL,
	rate_avg numeric(2, 1) NOT NULL,
	order_processing_fee numeric(19, 5) NOT NULL,
	courier_order_sum numeric(19, 5) NOT NULL,
	courier_tips_sum numeric(19, 5) NOT NULL,
	courier_reward_sum numeric(19, 5) NOT NULL,
	CONSTRAINT dm_courier_ledger_courier_order_sum_check CHECK ((courier_order_sum > (0)::numeric)),
	CONSTRAINT dm_courier_ledger_courier_reward_sum_check CHECK ((courier_reward_sum > (0)::numeric)),
	CONSTRAINT dm_courier_ledger_courier_tips_sum_check CHECK ((courier_tips_sum > (0)::numeric)),
	CONSTRAINT dm_courier_ledger_order_processing_fee_check CHECK ((order_processing_fee > (0)::numeric)),
	CONSTRAINT dm_courier_ledger_orders_count_check CHECK ((orders_count >= 0)),
	CONSTRAINT dm_courier_ledger_orders_total_sum_check CHECK ((orders_total_sum >= (0)::numeric)),
	CONSTRAINT dm_courier_ledger_pkey PRIMARY KEY (id),
	CONSTRAINT dm_courier_ledger_rate_avg_check CHECK ((rate_avg > (0)::numeric)),
	CONSTRAINT dm_courier_ledger_settlement_month_check CHECK (((settlement_month >= 1) AND (settlement_month <= 12))),
	CONSTRAINT dm_courier_ledger_settlement_year_check CHECK (((settlement_year >= 2020) AND (settlement_year <= 2099)))
);
ALTER TABLE cdm.dm_courier_ledger ADD CONSTRAINT dm_courier_ledger_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id);
```

**dds.dm_couriers DDL**
```
CREATE TABLE if not exists dds.dm_couriers (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	"name" varchar NOT NULL,
	CONSTRAINT dm_couriers_courier_id_key UNIQUE (courier_id),
	CONSTRAINT dm_couriers_pkey PRIMARY KEY (id)
);
```

**dds.fct_deliveries DDL**
```
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
```

**cdm.dm_courier_ledger mart**
```
delete from cdm.dm_courier_ledger dcl
where dcl.settlement_year > extract(year from %(first_day_last_month)s::timestamp)
	or (dcl.settlement_year = extract(year from %(first_day_last_month)s::timestamp)
		and dcl.settlement_month >= extract(month from %(first_day_last_month)s::timestamp));

insert into cdm.dm_courier_ledger (courier_id, 
									courier_name, 
									settlement_year, 
									settlement_month, 
									orders_count,
									orders_total_sum,
									rate_avg,
									order_processing_fee,
									courier_order_sum,
									courier_tips_sum,
									courier_reward_sum) (
with 
orders_sum as (
	select fps.order_id, sum(fps.total_sum) total_sum
	from dds.fct_product_sales fps
		join dds.dm_orders do2 on fps.order_id = do2.id 
		join dds.dm_timestamps dt on do2.timestamp_id = dt.id
	where dt.ts >= %(first_day_last_month)s::timestamp
	group by fps.order_id
), 
orders_details as (
select fd.courier_id courier_id ,
		dc."name" courier_name ,
		extract (year from fd.order_ts) settlement_year,
		extract (month from fd.order_ts) settlement_month,
		count(distinct fd.order_id) orders_count,
		sum(os.total_sum) orders_total_sum,
		avg(fd.rate)::numeric(3,1) rate_avg,
		sum(os.total_sum::float * 0.25) order_processing_fee,
		sum(fd.tip_sum) courier_tips_sum
from dds.fct_deliveries fd
	join dds.dm_couriers dc on fd.courier_id =dc.id
	join orders_sum os on fd.order_id = os.order_id
group by fd.courier_id, dc."name", extract (year from fd.order_ts), extract (month from fd.order_ts))
select courier_id,
		courier_name,
		settlement_year,
		settlement_month,
		orders_count,
		orders_total_sum,
		rate_avg::numeric(3,1),
		order_processing_fee,
		case when rate_avg < 4 then greatest(orders_total_sum * 0.05, 100)
			when rate_avg >= 4 and rate_avg < 4.5 then greatest(orders_total_sum * 0.07, 150)
			when rate_avg >= 4.5 and rate_avg < 4.9 then greatest(orders_total_sum * 0.08, 175)
			when rate_avg >= 4.9 then greatest(orders_total_sum * 0.1, 200)
		end courier_order_sum, 
		courier_tips_sum,
		(case when rate_avg < 4 then greatest(orders_total_sum * 0.05, 100)
			when rate_avg >= 4 and rate_avg < 4.5 then greatest(orders_total_sum * 0.07, 150)
			when rate_avg >= 4.5 and rate_avg < 4.9 then greatest(orders_total_sum * 0.08, 175)
			when rate_avg >= 4.9 then greatest(orders_total_sum * 0.1, 200)
		end + courier_tips_sum) * 0.95 courier_reward_sum 
from orders_details);
	
```


