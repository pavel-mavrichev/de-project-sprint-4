delete from cdm.dm_courier_ledger dcl
where dcl.settlement_year > extract(year from %(first_day_last_month)s::timestamp)
	or (dcl.settlement_year = extract(year from %(first_day_last_month)s::timestamp)
		and dcl.settlement_month >= extract(month from %(first_day_last_month)s::timestamp));