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
	