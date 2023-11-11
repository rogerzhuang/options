from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


class Securities(db.Model):
    ticker = db.Column(db.String(40), primary_key=True)
    security_name = db.Column(db.String(255))
    security_type = db.Column(db.String(25))
    underlying_ticker = db.Column(
        db.String(40), db.ForeignKey('securities.ticker'))
    stocks = db.relationship('Stocks', backref='security', lazy=True)
    options = db.relationship('Options', backref='security', lazy=True)
    hist_prices = db.relationship('HistPrice1D', backref='security', lazy=True)
    iv_surf = db.relationship('IvSurf', backref='security', lazy=True)
    news_articles = db.relationship(
        'NewsSecurities', back_populates='security', lazy=True)


class Stocks(db.Model):
    ticker = db.Column(db.String(40), db.ForeignKey(
        'securities.ticker'), primary_key=True)
    gics = db.Column(db.String(255))


class Options(db.Model):
    ticker = db.Column(db.String(40), db.ForeignKey(
        'securities.ticker'), primary_key=True)
    option_type = db.Column(db.String(25))
    option_style = db.Column(db.String(25))
    expiry = db.Column(db.Date)
    strike = db.Column(db.Float)


class HistPrice1D(db.Model):
    __tablename__ = 'hist_price_1d'

    ticker = db.Column(db.String(40), db.ForeignKey(
        'securities.ticker'), primary_key=True)
    exch_time = db.Column(db.DateTime, primary_key=True)
    open = db.Column(db.Float)
    high = db.Column(db.Float)
    low = db.Column(db.Float)
    close = db.Column(db.Float)
    open_adj = db.Column(db.Float)
    high_adj = db.Column(db.Float)
    low_adj = db.Column(db.Float)
    close_adj = db.Column(db.Float)
    volume = db.Column(db.Integer)


class IvSurf(db.Model):
    __tablename__ = 'iv_surf'

    ticker = db.Column(db.String(40), db.ForeignKey(
        'securities.ticker'), primary_key=True)
    exch_time = db.Column(db.DateTime, primary_key=True)
    iv_surf_data = db.Column(db.PickleType)


class News(db.Model):
    __tablename__ = 'news'

    id = db.Column(db.String(255), primary_key=True)
    exch_time = db.Column(db.DateTime)
    published_utc = db.Column(db.DateTime)
    publisher_name = db.Column(db.String(255))
    title = db.Column(db.String(255))
    author = db.Column(db.String(255))
    article_url = db.Column(db.String(255))
    content = db.Column(db.Text)
    tickers = db.relationship(
        'NewsSecurities', back_populates='news', lazy=True)


class NewsSecurities(db.Model):
    __tablename__ = 'news_securities'

    news_id = db.Column(db.String(255), db.ForeignKey(
        'news.id'), primary_key=True)
    ticker = db.Column(db.String(40), db.ForeignKey(
        'securities.ticker'), primary_key=True)
    sentiment = db.Column(db.Integer)  # Sentiment score column

    # Relationships
    news = db.relationship('News', back_populates='tickers')
    security = db.relationship('Securities', back_populates='news_articles')
