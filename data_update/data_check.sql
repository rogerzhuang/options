select p.ticker, p.exch_time, o.option_type, o.strike, o.expiry, p.[close], p.volume from hist_price_1d p
inner join securities s on p.ticker=s.ticker
inner join options o on o.ticker = s.ticker
where s.underlying_ticker = 'MS' and o.expiry = '2023-11-03' and exch_time = '2023-10-18' and o.strike = 71 and o.option_type = 'call'
order by expiry, option_type, strike, exch_time

select * from hist_price_1d where ticker = 'GME' and exch_time = '2021-11-19 00:00:00.000'

select s.ticker, o.option_type, o.strike, o.expiry from securities s
inner join options o on o.ticker = s.ticker
where s.underlying_ticker = 'KEY' and o.expiry = '2023-03-03'
order by expiry, option_type, strike
