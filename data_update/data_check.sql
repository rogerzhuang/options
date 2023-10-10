select p.ticker, p.exch_time, o.option_type, o.strike, o.expiry, p.[close], p.volume from hist_price_1d p
inner join securities s on p.ticker=s.ticker
inner join options o on o.ticker = s.ticker
where s.underlying_ticker = 'SHEL' and o.expiry = '2023-02-03' and exch_time = '2023-01-27' --and o.strike = 155 and o.option_type = 'put'
order by expiry, option_type, strike, exch_time

select * from hist_price_1d where ticker = 'FISV' and exch_time = '2022-12-15 00:00:00.000'

select s.ticker, o.option_type, o.strike, o.expiry from securities s
inner join options o on o.ticker = s.ticker
where s.underlying_ticker = 'SHEL' and o.expiry = '2023-01-27'
order by expiry, option_type, strike
