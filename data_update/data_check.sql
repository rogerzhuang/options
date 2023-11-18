select p.ticker, p.exch_time, o.option_type, o.strike, o.expiry, p.[close], p.volume from hist_price_1d p
inner join securities s on p.ticker=s.ticker
inner join options o on o.ticker = s.ticker
where s.underlying_ticker = 'MSFT' and o.expiry = '2023-11-24' and exch_time = '2023-11-17' and o.strike = 71 and o.option_type = 'call'
order by expiry, option_type, strike, exch_time

select * from hist_price_1d where ticker = 'MSFT' and exch_time = '2023-11-03 00:00:00.000'

select * from options where ticker like '%AAPL%' and expiry = '2023-12-15'

select s.ticker, o.option_type, o.strike, o.expiry from securities s
inner join options o on o.ticker = s.ticker
where s.underlying_ticker = 'KEY' and o.expiry = '2023-03-03'
order by expiry, option_type, strike

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2023-10-23' and n.exch_time <= '2023-10-26'
and n.author = 'Zacks Equity Research' 
and securities.ticker in ('SRPT','SOFI','SMCI','BILL','ROKU','SQ','PLTR','COIN','SEDG','NET','DKNG','ETSY','TEAM','GNRC','PINS','PARA','SHOP','MPW','MRNA','VFC','FSLR','Z','SIRI','CROX','PYPL','ABNB','MLCO','DASH','ANET','MTCH','CAR','PLUG','NCLH','EXPE','LCID','MELI','RKT','XPEV','ON','RIVN')
group by securities.ticker
order by sentiment

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2023-10-30' and n.exch_time <= '2023-11-02'
and n.author = 'Zacks Equity Research' 
and securities.ticker in ('PSNY','LCID','SIRI','RBLX','CELH','U','PLUG','TOST','BROS','TTD','RIVN','TWLO','DDOG','ILMN','COTY','HOOD','SE','UWMC','WBD','MPW','LI','TEVA','XPEV','TPR','CPNG','COIN','GTLB','UBER','BILL','NIO','EBAY','TTWO','SEDG','IQ','SMCI','GME','SOFI','BILI','MGM','RKT')
group by securities.ticker
order by sentiment

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2023-11-06' and n.exch_time <= '2023-11-09'
and n.author = 'Zacks Equity Research' 
and securities.ticker in ('SE','PLUG','M','AAP','SIRI','BBWI','LCID','MPW','XPEV','PANW','XP','TGT','NU','NIO','SEDG','RIVN','SOFI','LII','ILMN','COIN','WSM','IQ','GME','BILI','JD','U','SMCI','RBLX','DKNG','KEY','CHWY','TOST','FITB','GTLB','PLTR','AGNC','BILL','RKT','AAL','MRNA')
group by securities.ticker
order by sentiment

