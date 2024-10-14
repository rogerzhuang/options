import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, func, distinct
from sqlalchemy.orm import sessionmaker, aliased
from models import News, NewsSecurities, Securities, HistPrice1D, IvSurf, Options
from datetime import datetime, timedelta

# Load environment variables from .env file
load_dotenv()

# Get database connection details from environment variables
DB_SERVER = os.getenv('DB_SERVER')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Create the connection string
connection_string = f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"

# Create a database engine and session
engine = create_engine(connection_string)
Session = sessionmaker(bind=engine)
session = Session()

# 1. Options data analysis
SecuritiesOption = aliased(Securities)
SecuritiesStock = aliased(Securities)

options_query = (
    session.query(
        SecuritiesStock.ticker.label('stock_ticker'),
        Options.option_type,
        HistPrice1D.exch_time,
        func.count(Options.ticker).label('option_count')
    )
    .select_from(SecuritiesOption)
    .join(Options, SecuritiesOption.ticker == Options.ticker)
    .join(SecuritiesStock, SecuritiesOption.underlying_ticker == SecuritiesStock.ticker)
    .join(HistPrice1D, (HistPrice1D.ticker == Options.ticker) & (HistPrice1D.exch_time <= Options.expiry))
    .filter(SecuritiesOption.security_type == 'opt')
    .group_by(SecuritiesStock.ticker, Options.option_type, HistPrice1D.exch_time)
)

options_df = pd.read_sql(options_query.statement, session.bind)

# Calculate average number of options per day for each stock and option type
options_avg = options_df.groupby(['stock_ticker', 'option_type'])[
    'option_count'].mean().unstack(fill_value=0)

# Rename columns for clarity
options_avg = options_avg.rename(
    columns={'call': 'average_calls', 'put': 'average_puts'})

print("\n1. Time Series Average Number of Options for Each Stock:")
print(options_avg)

# 2. IV Surface data analysis
iv_surf_query = (
    session.query(
        Securities.ticker,
        func.count(distinct(IvSurf.exch_time)).label('days_with_iv_surf'),
        func.count(distinct(HistPrice1D.exch_time)).label('total_trading_days')
    )
    .join(IvSurf, Securities.ticker == IvSurf.ticker, isouter=True)
    .join(HistPrice1D, Securities.ticker == HistPrice1D.ticker)
    .filter(Securities.security_type == 'stk')
    .group_by(Securities.ticker)
)

iv_surf_df = pd.read_sql(iv_surf_query.statement, session.bind)

print("\n2. IV Surface Data Analysis:")

# Calculate the proportion of days with IV surface data
iv_surf_df['iv_surf_proportion'] = iv_surf_df['days_with_iv_surf'] / iv_surf_df['total_trading_days']

# Focus on stocks with incomplete data
incomplete_data = iv_surf_df[iv_surf_df['iv_surf_proportion'] < 1]
print("\nDescriptive Statistics of Stocks with Incomplete IV Surface Data:")
print(incomplete_data['iv_surf_proportion'].describe())

print(f"\nNumber of stocks with complete IV surface data: {(iv_surf_df['iv_surf_proportion'] == 1).sum()}")
print(f"Number of stocks with incomplete IV surface data: {len(incomplete_data)}")

print("\nBottom 20 Stocks with Lowest Proportion of IV Surface Data:")
print(iv_surf_df.nsmallest(20, 'iv_surf_proportion')[['ticker', 'iv_surf_proportion']])


# 3. News data analysis
news_query = (
    session.query(News, NewsSecurities, Securities)
    .select_from(News)
    .join(NewsSecurities, News.id == NewsSecurities.news_id)
    .join(Securities, NewsSecurities.ticker == Securities.ticker)
)
df = pd.read_sql(news_query.statement, session.bind)

print("\n3. News Sentiment Distribution:")
print(df['sentiment'].describe())

# 4. Top 10 most mentioned tickers in news
top_tickers = df['ticker'].value_counts().head(10)
print("\n4. Top 10 Most Mentioned Tickers:")
print(top_tickers)

# 5. Average sentiment for top 10 most mentioned tickers
top_10_tickers = top_tickers.index
avg_sentiment = df[df['ticker'].isin(top_10_tickers)].groupby(
    'ticker')['sentiment'].mean().sort_values(ascending=False)
print("\n5. Average Sentiment for Top 10 Most Mentioned Tickers:")
print(avg_sentiment)

# 6. News volume over time
df['date'] = pd.to_datetime(df['published_utc']).dt.date
news_volume = df.groupby('date').size()
print("\n6. News Volume Over Time:")
print(news_volume.describe())

# 7. Descriptive stats about related tickers per news
tickers_per_news = df.groupby('id')['ticker'].count()
print("\n7. Descriptive Stats of Related Tickers per News Article:")
print(tickers_per_news.describe())

# 8. Analysis of news-ticker pairs with and without sentiment scores
print("\n8. Analysis of News-Ticker Pairs With and Without Sentiment Scores:")
print(f"Total number of news-ticker pairs: {len(df)}")
print(
    f"Number of news-ticker pairs with sentiment scores: {df['sentiment'].notnull().sum()}")
print(
    f"Number of news-ticker pairs without sentiment scores: {df['sentiment'].isnull().sum()}")

# Close the session
session.close()
