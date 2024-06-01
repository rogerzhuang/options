from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import date, timedelta
from high_iv import get_stock_list_with_high_put_iv
import pandas_market_calendars as mcal
import requests
import os
from dotenv import load_dotenv

# Function to get stock prices on a specific day


def get_stock_prices_on_day(tickers, day):
    str_day = day.strftime('%Y-%m-%d')
    response = requests.post('http://127.0.0.1:8000/get_stock_price', json={
        'tickers': tickers,
        'start_date': str_day,
        'end_date': str_day
    })
    return response.json()

# Function to find the last trading day of a week given a date


def get_last_trading_day_of_week(date, valid_days):
    current_year, current_week = date.isocalendar()[:2]
    days_of_current_week = [day for day in valid_days if day.isocalendar(
    )[0] == current_year and day.isocalendar()[1] == current_week]
    return max(days_of_current_week) if days_of_current_week else None

# Function to filter stocks based on return criteria


def filter_stocks_based_on_return(input_date, n_stocks, session):
    # First, get the list of high IV stocks
    high_iv_stocks = get_stock_list_with_high_put_iv(
        input_date, n_stocks, session)

    # Calculate 8 weeks before input_date
    eight_weeks_before = input_date - timedelta(weeks=8)

    # Get valid trading days within the range
    nyse = mcal.get_calendar('NYSE')
    valid_days = nyse.valid_days(
        start_date=eight_weeks_before, end_date=input_date).date.tolist()

    # Find the last trading day 8 weeks before
    last_trading_day_8_weeks_before = get_last_trading_day_of_week(
        eight_weeks_before, valid_days)

    # Fetch prices for both dates for all high IV stocks
    prices_on_input_date = get_stock_prices_on_day(high_iv_stocks, input_date)
    prices_8_weeks_before = get_stock_prices_on_day(
        high_iv_stocks, last_trading_day_8_weeks_before)

    # Filter stocks based on the return criteria (<20% and >-20%)
    filtered_stocks = []
    for ticker in high_iv_stocks:
        if ticker in prices_on_input_date and ticker in prices_8_weeks_before:
            price_now = prices_on_input_date[ticker][input_date.strftime(
                '%Y-%m-%d')]['close_adj']
            price_before = prices_8_weeks_before[ticker][last_trading_day_8_weeks_before.strftime(
                '%Y-%m-%d')]['close_adj']
            return_percentage = (
                (price_now - price_before) / price_before) * 100
            if return_percentage < 20 and return_percentage > -20:
                filtered_stocks.append(ticker)

    return filtered_stocks


# Example usage
if __name__ == '__main__':
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

    input_date = date(2024, 1, 26)
    filtered_stocks = filter_stocks_based_on_return(input_date, 70, session)
    print("Filtered Stocks with return criteria:", ','.join(filtered_stocks))
