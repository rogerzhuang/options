select p.ticker, p.exch_time, o.option_type, o.strike, o.expiry, p.[close], p.volume from hist_price_1d p
inner join securities s on p.ticker=s.ticker
inner join options o on o.ticker = s.ticker
where s.underlying_ticker = 'MS' and o.expiry = '2023-11-03' and exch_time = '2023-10-18' and o.strike = 71 and o.option_type = 'call'
order by expiry, option_type, strike, exch_time

select * from hist_price_1d where ticker = 'MSFT' and exch_time = '2023-11-03 00:00:00.000'

select s.ticker, o.option_type, o.strike, o.expiry from securities s
inner join options o on o.ticker = s.ticker
where s.underlying_ticker = 'KEY' and o.expiry = '2023-03-03'
order by expiry, option_type, strike

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2023-11-06' and n.exch_time <= '2023-11-09'
and n.author = 'Zacks Equity Research' and securities.ticker in ('SE', 'M', 'AAP', 'MPW', 'SIRI', 'XPEV', 'LCID', 'BBWI', 'SOFI', 'RIVN', 'NIO', 'PANW', 'COIN', 'TGT', 'XP', 'SEDG', 'RBLX', 'SMCI', 'JD', 'BILI', 'WSM', 'CHWY', 'BILL', 'DKNG', 'GME', 'PARA', 'MRNA', 'SNAP', 'PLTR', 'ENPH')
group by securities.ticker
order by sentiment
