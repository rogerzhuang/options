from sqlalchemy import Column, String, ForeignKey, Date, Float, DateTime, Integer, PickleType
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import Text

Base = declarative_base()


class Securities(Base):
    __tablename__ = 'securities'  # It's good practice to explicitly name your table
    ticker = Column(String(40), primary_key=True)
    security_name = Column(String(255))
    security_type = Column(String(25))
    underlying_ticker = Column(String(40), ForeignKey('securities.ticker'))
    stocks = relationship('Stocks', backref='security')
    options = relationship('Options', backref='security')
    hist_prices = relationship('HistPrice1D', backref='security')
    iv_surf = relationship('IvSurf', backref='security')
    news_articles = relationship(
        'NewsSecurities', back_populates='security')


class Stocks(Base):
    __tablename__ = 'stocks'
    ticker = Column(String(40), ForeignKey(
        'securities.ticker'), primary_key=True)
    gics = Column(String(255))


class Options(Base):
    __tablename__ = 'options'
    ticker = Column(String(40), ForeignKey(
        'securities.ticker'), primary_key=True)
    option_type = Column(String(25))
    option_style = Column(String(25))
    expiry = Column(Date)
    strike = Column(Float)


class HistPrice1D(Base):
    __tablename__ = 'hist_price_1d'
    ticker = Column(String(40), ForeignKey(
        'securities.ticker'), primary_key=True)
    exch_time = Column(DateTime, primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    open_adj = Column(Float)
    high_adj = Column(Float)
    low_adj = Column(Float)
    close_adj = Column(Float)
    volume = Column(Integer)


class IvSurf(Base):
    __tablename__ = 'iv_surf'
    ticker = Column(String(40), ForeignKey(
        'securities.ticker'), primary_key=True)
    exch_time = Column(DateTime, primary_key=True)
    iv_surf_data = Column(PickleType)


class News(Base):
    __tablename__ = 'news'

    id = Column(String(255), primary_key=True)
    exch_time = Column(DateTime)
    published_utc = Column(DateTime)
    publisher_name = Column(String(255))
    title = Column(Text)
    author = Column(String(255))
    article_url = Column(Text)
    content = Column(Text)
    tickers = relationship(
        'NewsSecurities', back_populates='news', lazy=True)


class NewsSecurities(Base):
    __tablename__ = 'news_securities'

    news_id = Column(String(255), ForeignKey(
        'news.id'), primary_key=True)
    ticker = Column(String(40), ForeignKey(
        'securities.ticker'), primary_key=True)
    sentiment = Column(Integer)  # Sentiment score column

    # Relationships
    news = relationship('News', back_populates='tickers')
    security = relationship('Securities', back_populates='news_articles')
