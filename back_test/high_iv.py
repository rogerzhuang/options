from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import requests
import pickle
import base64
import numpy as np
from models import Stocks, HistPrice1D
from dotenv import load_dotenv
import os
import json
import argparse

def get_stock_list_with_high_put_iv(input_date, strike_ratio, expiry_days, n_stocks, session):
    T = expiry_days / 365.0

    # Load date ranges and tickers from JSON
    with open('ticker_date_ranges.json', 'r') as file:
        date_ranges = json.load(file)

    # Convert input_date to string for comparison
    str_input_date = input_date.strftime('%Y-%m-%d')

    # Filter date ranges to find matching range for input_date
    filtered_tickers = None
    for date_range in date_ranges:
        if date_range["start_date"] <= str_input_date <= date_range["end_date"]:
            filtered_tickers = set(date_range["tickers"])
            break

    # Fetching available stocks for the given day
    stocks = (session.query(Stocks)
              .join(HistPrice1D, HistPrice1D.ticker == Stocks.ticker)
              .filter(HistPrice1D.exch_time == input_date)
              .all())

    # If filtered_tickers is not None, filter stocks based on the tickers list
    if filtered_tickers is not None:
        stocks = [stock for stock in stocks if stock.ticker in filtered_tickers]

    # Preparing request data
    tickers = [stock.ticker for stock in stocks]

    # Fetch IV surfaces
    response = requests.post('http://127.0.0.1:8000/get_iv_surfs', json={
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
        iv = iv_surf_data(strike_ratio, T)

        if iv and not np.isnan(iv):
            stock_ivs.append((stock.ticker, iv))

    # Sort stocks based on IV
    stock_ivs.sort(key=lambda x: x[1], reverse=True)

    # Return list of tickers with highest put IVs
    return [ticker for ticker, _ in stock_ivs[:n_stocks]]


if __name__ == '__main__':
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Fetch stocks with the highest put IV.')
    parser.add_argument('input_date', type=str, help='Date to fetch data for (format YYYY-MM-DD)')
    parser.add_argument('strike_ratio', type=float, help='Strike price ratio for IV calculation')
    parser.add_argument('expiry_days', type=float, help='Days to expiry for the option')
    parser.add_argument('n_stocks', type=int, help='Number of stocks to return with highest put IVs')

    # Parse the command line arguments
    args = parser.parse_args()

    # Convert input_date argument to a date object
    input_date = datetime.strptime(args.input_date, '%Y-%m-%d').date()

    # Load environment variables
    load_dotenv()

    db_user = os.environ.get('DB_USER')
    db_password = os.environ.get('DB_PASSWORD')
    db_server = os.environ.get('DB_SERVER').replace('\\\\', '\\')
    db_name = os.environ.get('DB_NAME')

    driver_name = "ODBC Driver 17 for SQL Server"
    DATABASE_URL = f'mssql+pyodbc://{db_user}:{db_password}@{db_server}/{db_name}?driver={driver_name}'
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Get the list of stocks with high put IV
    high_put_iv_stocks = ','.join(
        get_stock_list_with_high_put_iv(input_date, args.strike_ratio, args.expiry_days, args.n_stocks, session)
    )
    print(f"Stocks with highest put IVs on {input_date}: {high_put_iv_stocks}")
