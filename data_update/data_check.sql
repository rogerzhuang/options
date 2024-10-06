select p.ticker, p.exch_time, o.option_type, o.strike, o.expiry, p.[close], p.volume from hist_price_1d p
inner join securities s on p.ticker=s.ticker
inner join options o on o.ticker = s.ticker
where s.underlying_ticker = 'MSFT' and o.expiry = '2023-11-24' and exch_time = '2023-11-17' and o.strike = 71 and o.option_type = 'call'
order by expiry, option_type, strike, exch_time

select * from hist_price_1d where ticker = 'MSFT' and exch_time = '2023-11-03 00:00:00.000'

select * from Polygon.dbo.options where ticker like '%AAPL%' and expiry = '2023-12-15'

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

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2023-11-13' and n.exch_time <= '2023-11-16'
-- and n.author = 'Zacks Equity Research' 
and securities.ticker in ('YPF','PLUG','BURL','SIRI','MPW','ZM','DKS','LCID','NIO','XPEV','SOFI','NVDA','SMCI','RIVN','FUTU','BIDU','BBY','PSNY','ADSK','COIN','PARA','ROST','LI','GME','SEDG','GTLB','AAP','NCLH','CHWY','ENPH','MRNA','EDU',
'SE','KMX','PLTR','KEY','U','WBD','ALB','BILL')
group by securities.ticker
order by sentiment

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2023-11-20' and n.exch_time <= '2023-11-23'
and n.author = 'Zacks Equity Research' 
and securities.ticker in ('PLUG','PATH','OKTA','ASO','MPW','BILI','PDD','SNOW','ZS','NIO','DLTR','MRVL','XPEV','SIRI','GME','CRWD','LCID','ULTA','FUTU','SOFI','RIVN','DELL','ALB','IQ','WDAY','COIN','PLTR','SMCI','APA','CHWY','WBD','PARA','ENPH','AAP','NTAP','LI','KR','AA','VFC','SNAP')
group by securities.ticker
order by sentiment

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2023-11-27' and n.exch_time <= '2023-11-30'
and n.author = 'Zacks Equity Research' 
and securities.ticker in ('GME','CHWY','GTLB','DOCU','PLUG','S','RH','MDB','NIO','MPW','SIRI','DG','LCID','XPEV','SOFI','RIVN','SEDG','COIN','BILI','LULU','KEY','NTAP','PARA','VFC','U','SMCI','KMX','HES','HOOD','YPF','ILMN','BILL','CELH','CPNG','ALB','ENPH','PLTR','MRNA','SE','ASO')
group by securities.ticker
order by sentiment

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2023-12-04' and n.exch_time <= '2023-12-07'
and n.author = 'Zacks Equity Research' 
and securities.ticker in ('PLUG','PSNY','MPW','SIRI','LCID','GME','CHWY','YPF','COIN','SEDG','NTAP','HOOD','PARA','RIVN','XPEV','NIO','SOFI','NOC','TOST','ALB','M','XP','SRPT','ENPH','SMCI','NOK','PLTR','RKT','U','MRNA','ADBE','LEN','VFC','LI','X','ORCL','S','CAR','AA','CMA')
group by securities.ticker
order by sentiment

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2023-12-11' and n.exch_time <= '2023-12-14'
and n.author = 'Zacks Equity Research' 
and securities.ticker in ('PLUG','LCID','SEDG','TOL','MPW','KMX','EDU','SIRI','CCL','NIO','XPEV','ENPH','RIVN','NTAP','SOFI','GME','COIN','RKT','CHWY','DOCU','HOOD','PLTR','PARA','CLF','AA','PATH','U','M','COF','SMCI','SRPT','ROKU','S','NKE','SNAP','FSLR','BIIB','SE','BBWI','AAP')
group by securities.ticker
order by sentiment

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2023-10-16' and n.exch_time <= '2023-10-20'
-- and n.author = 'Zacks Equity Research' 
and securities.ticker in ('SNAP','SRPT','MPW','ENPH','ALGN','SPOT','SEDG','PSNY','LCID','META','PLUG','HOG','XPEV','RCL','NIO','OKTA','CLF','RIVN','EDU','SMCI','CMG','URI','SOFI','YPF','GME','INTC','NCLH','AMZN','COIN','NOW','STX','PLTR','LUV','SPLK','CMA','TOST','NET','BILI','FSLR','ROKU')
group by securities.ticker
order by sentiment

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2023-10-09' and n.exch_time <= '2023-10-12'
and n.author = 'Zacks Equity Research' 
and securities.ticker in ('MPW','SRPT','LCID','PSNY','SIRI','ISRG','CMA','PLUG','AA','NFLX','XPEV','NOK','SMCI','AAL','SEDG','ALLY','SOFI','SCHW','UAL','NIO','COIN','KEY','RIVN','TSLA','BILI','LRCX','LI','PLTR','EQT','CHWY','USB','ROKU','PARA','CCL','NET','NCLH','DFS','GTLB','AAP','WBD')
group by securities.ticker
order by sentiment

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2023-10-02' and n.exch_time <= '2023-10-05'
and n.author = 'Zacks Equity Research' 
and securities.ticker in ('KGC','SRPT','PLUG','RIVN','WBA','MPW','SOFI','DPZ','SIRI','KEY','XPEV','GME','LCID','S','NIO','COIN','PLTR','SEDG','OKTA','ALLY','SMCI','PATH','CCL','BURL','RKT','GTLB','NET','BILI','RBLX','HOOD','DAL','ROKU','ENPH','TOST','BEKE','APA','CELH','EDU','CMA','FSLR')
group by securities.ticker
order by sentiment

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2023-09-25' and n.exch_time <= '2023-09-28'
and n.author = 'Zacks Equity Research' 
and securities.ticker in ('MDT','SIRI','MPW','SRPT','NIO','LCID','RIVN','XPEV','NU','PLUG','GME','CCL','TSLA','COIN','PLTR','SOFI','S','NCLH','SMCI','CHWY','KEY','NET','DKNG','LI','WBD','RBLX','SEDG','ROKU','SNAP','ALLY','BILL','GTLB','T','U','CZR','CLF','SQ','ENPH','SE','SHOP')
group by securities.ticker
order by sentiment

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2023-09-18' and n.exch_time <= '2023-09-21'
and n.author = 'Zacks Equity Research' 
and securities.ticker in ('KMX','CCL','MPW','PSNY','RIVN','XPEV','NIO','COIN','HOOD','LCID','IQ','SOFI','PLUG','NCLH','PLTR','SMCI','CHWY','GME','U','DKNG','TOST','NKE','MU','PARA','ROKU','SE','KEY','TSLA','GNRC','NET','SEDG','RBLX','BILI','PATH','CELH','AA','DDOG','SQ','CMA','ENPH')
group by securities.ticker
order by sentiment

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2023-09-11' and n.exch_time <= '2023-09-14'
and n.author = 'Zacks Equity Research' 
and securities.ticker in ('HOG','PSNY','HSBC','MPW','PINS','GME','XPEV','SIRI','LCID','NIO','PLUG','SNAP','FDX','COIN','SMCI','PATH','RIVN','U','PLTR','AGNC','SOFI','KEY','BILI','ROKU','F','GTLB','ALLY','NET','CCL','TFC','CSX','AAP','CHWY','RBLX','Z','DKNG','LI','AAL','FUTU','CAR')
group by securities.ticker
order by sentiment

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2023-12-09' and n.exch_time <= '2023-12-22'
and n.author = 'Zacks Equity Research' 
and securities.ticker in ('PSNY','PLUG','HES','COIN','SEDG','NIO','LCID','BILI','SIRI','KEY','YPF','IQ','XPEV','CHWY','SOFI','GME','MPW','RKT','HOOD','SMCI','ENPH','RIVN','U','S','PARA','EDU','SRPT','VFC','CZR','DKS','AR','SE','PATH','AA','BILL','X','NCLH','TOST','ROKU','BROS','FSLR','FUTU','KGC','ALB','CCL','JD','AAP','GTLB','RH','SNAP')
group by securities.ticker
order by sentiment

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2023-12-16' and n.exch_time <= '2023-12-29'
and n.author = 'Zacks Equity Research' 
and securities.ticker in ('PLUG','WBA','COIN','LCID','MPW','NIO','RIVN','XPEV','PATH','SEDG','BILI','SIRI','SOFI','GME','AA','CHWY','ENPH','SE','U','SMCI','HOOD','ALB','CCL','WBD','BP','AAP','CELH','ROKU','FUTU','TSLA','MRNA','PLTR','SRPT','PBR','EDU','PDD','CLF','LI','PARA','SQ','AGNC','NCLH','ISRG','URI','DKNG','AAL','FSLR','RH','RRC','JD')
group by securities.ticker
order by sentiment

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2023-12-23' and n.exch_time <= '2024-01-05'
and n.author = 'Zacks Equity Research' 
and securities.ticker in ('MPW','PLUG','COIN','SRPT','LCID','X','CROX','XPEV','SEDG','SOFI','NIO','MRNA','RIVN','SWN','ILMN','AR','BILI','ENPH','HOOD','CHWY','CELH','AA','GME','U','VFC','YPF','ALB','ROKU','SE','WYNN','SNAP','ET','PLTR','BILL','PARA','SMCI','NCLH','ISRG','SQ','WBD','LI','AAP','DAL','AGNC','RH','Z','CCJ','AAL','EDU','CZR')
group by securities.ticker
order by sentiment

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2024-01-27' and n.exch_time <= '2024-02-09'
and n.author = 'Zacks Equity Research' 
and securities.ticker in ('CART','VFS','ROKU','TTD','TOST','SHOP','TWLO','COIN','ZI','DDOG','HOOD','GNRC','DKNG','ANET','CROX','CAR','ARM','LCID','ALB','Z','AFRM','DASH','CVNA','TAL','MSTR','RKT','AR','NIO','ABNB','XPEV','WAL','SMCI','BBIO','GDDY','W','APLS','RIVN','SONY','REGN','PLTR','CYTK','SNAP','SWN','ZION','SOFI','U','EDU','MRNA','MGM','XP')
group by securities.ticker
order by sentiment

select securities.ticker, avg(ns.sentiment) as sentiment, count(*) as count
from news n inner join news_securities ns on n.id = ns.news_id
inner join securities on ns.ticker = securities.ticker
where n.exch_time >= '2024-05-04' and n.exch_time <= '2024-05-17'
and n.author = 'Zacks Equity Research' 
and securities.ticker in ('VFS', 'ELF', 'VFC', 'X', 'XPEV', 'LCID', 'MSTR', 'SNOW', 'PANW', 'LI', 'SMCI', 'NVDA', 'BEKE', 'PDD', 'ZM', 'NIO', 'RIVN', 'NTES', 'XP', 'RKT', 'WHR', 'COIN', 'HOOD', 'AFRM', 'YPF', 'WDAY', 'TGT', 'TOST', 'CHWY', 'DELL', 'PARA', 'ROST', 'CART', 'VRT', 'ARM', 'CVNA', 'WBD', 'SOFI', 'TOL', 'NOK', 'S', 'TAL', 'NCLH', 'SNAP', 'CAH', 'DXCM', 'PATH', 'W', 'BBY', 'CAVA')
group by securities.ticker
order by sentiment

SELECT n.title, n.exch_time, ns.sentiment, n.article_url, n.author, n.content
FROM news n 
INNER JOIN news_securities ns ON n.id = ns.news_id
INNER JOIN securities ON ns.ticker = securities.ticker
WHERE n.exch_time >= '2024-06-29' AND n.exch_time <= '2024-07-12'
AND securities.ticker = 'NIO';

select distinct publisher_name from news
The Motley Fool

SELECT TOP (1000) [id]
      ,[exch_time]
      ,[published_utc]
      ,[publisher_name]
      ,[title]
      ,[author]
      ,[article_url]
      ,[content]
  FROM [Polygon].[dbo].[news]
  where exch_time >= '2024-05-10' and exch_time <= '2024-05-12'
  and publisher_name = 'Zacks Investment Research' and content is null

delete from news where id in 
('l6cP3KqB9A3dXYd83BHMOzCObOVs6L0bx7RqFT6hBbY'
)

delete from news_securities where news_id in 
('I3Mz0Rq0PMNt_0848eKv5dGma0CpPTTpH_6cfqbJsNE'
)


DELETE FROM news_securities
WHERE EXISTS (
    SELECT 1
    FROM news
    WHERE news.id = news_securities.news_id
    AND news.exch_time >= '2024-05-03'
    AND news.exch_time <= '2024-05-04'
    AND news_securities.sentiment IS NULL
);


SELECT TOP 10 s.ticker, n.title, n.article_url, n.content, n.published_utc, ns.sentiment
FROM stocks s
INNER JOIN news_securities ns ON s.ticker = ns.ticker
INNER JOIN news n ON n.id = ns.news_id
WHERE len(n.id) < 10 and ns.sentiment IS NOT NULL and s.ticker = 'MU'
ORDER BY n.published_utc DESC;