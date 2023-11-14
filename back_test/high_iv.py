from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import date
import requests
import pickle
import base64
import numpy as np
from models import Stocks, HistPrice1D
from dotenv import load_dotenv
import os

load_dotenv()

db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')
db_server = os.environ.get('DB_SERVER')
db_name = os.environ.get('DB_NAME')


driver_name = "ODBC Driver 17 for SQL Server"
DATABASE_URL = f'mssql+pyodbc://{db_user}:{db_password}@{db_server}/{db_name}?\
    driver={driver_name}'
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()


def get_stock_list_with_high_put_iv(input_date):
    # Fetching available stocks for the given day
    stocks = (session.query(Stocks)
              .join(HistPrice1D, HistPrice1D.ticker == Stocks.ticker)
              .filter(HistPrice1D.exch_time == input_date)
              .all())

    # Define the strike ratio (90% of stock price)
    strike_ratio = 0.9

    # Preparing request data
    tickers = [stock.ticker for stock in stocks]
    str_input_date = input_date.strftime('%Y-%m-%d')

    # Fetch IV surfaces
    response = requests.post('http://127.0.0.1:5000/get_iv_surfs', json={
        'tickers': tickers,
        'start_date': str_input_date,
        'end_date': str_input_date
    })
    iv_surfs = response.json()

    # Calculate IV for each stock and collect results
    stock_ivs = []
    for stock in stocks:
        if not iv_surfs[stock.ticker]:
            continue

        iv_surf_data = pickle.loads(base64.b64decode(
            iv_surfs[stock.ticker][str_input_date]))
        T = 7.0 / 365.0
        iv = iv_surf_data(strike_ratio, T)

        if iv and not np.isnan(iv):
            stock_ivs.append((stock.ticker, iv))

    # Sort stocks based on IV
    stock_ivs.sort(key=lambda x: x[1], reverse=True)

    # Return list of tickers with highest put IVs
    return ','.join([ticker for ticker, _ in stock_ivs[:40]])


# Example usage
input_date = date(2023, 11, 3)
high_put_iv_stocks = get_stock_list_with_high_put_iv(input_date)
print("Stocks with highest put IVs:", high_put_iv_stocks)
