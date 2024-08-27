from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import pandas_market_calendars as mcal
from iv_surf.iv_surf import get_option_price
from datetime import date, timedelta, datetime
import matplotlib.pyplot as plt
from models import Stocks, HistPrice1D, Securities
import numpy as np
import requests
import pickle
import base64
from dotenv import load_dotenv
import os
import json

load_dotenv()

db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')
db_server = os.environ.get('DB_SERVER').replace('\\\\', '\\')
db_name = os.environ.get('DB_NAME')

Base = declarative_base()

driver_name = "ODBC Driver 17 for SQL Server"
DATABASE_URL = f'mssql+pyodbc://{db_user}:{db_password}@{db_server}/{db_name}?driver={driver_name}'
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

CAPITAL = 1000000  # Total capital
strike_ratio = 0.9


# Helper function to check if a date falls within any of the specified ranges
def is_date_in_range(date, date_ranges):
    for range in date_ranges:
        if range['start_date'] <= date <= range['end_date']:
            return range['tickers']
    return None


def get_last_trading_day_of_week(date, valid_days):
    current_year, current_week = date.isocalendar()[:2]
    days_of_current_week = [day for day in valid_days if day.isocalendar(
    )[0] == current_year and day.isocalendar()[1] == current_week]
    return max(days_of_current_week) if days_of_current_week else None


def get_last_trading_day_of_future_week(asof_date, valid_days, days_ahead):
    return get_last_trading_day_of_week(asof_date + timedelta(days=days_ahead), valid_days)


def backtest_strategy(start_date, end_date):
    nyse = mcal.get_calendar('NYSE')
    valid_days = nyse.valid_days(
        start_date=start_date - timedelta(days=56), end_date=end_date + timedelta(days=28)).date.tolist()
    last_trading_days = list(
        set([get_last_trading_day_of_week(day, valid_days) for day in valid_days]))
    last_trading_days.sort()
    backtest_days = [day for day in last_trading_days if day >=
                     start_date and day <= end_date]

    cumulative_return = 0
    returns = []
    premiums = []
    option_counts = []
    exercised_ratios = []
    recovery_ratios = []
    holdings = {}

    # Load the JSON file
    with open('ticker_date_ranges.json', 'r') as file:
        ticker_date_ranges = json.load(file)

    for day in backtest_days:
        print(f"Backtesting on {day}")
        stocks = (session.query(Stocks)
                  .join(Securities, Securities.ticker == Stocks.ticker)
                  .join(HistPrice1D, HistPrice1D.ticker == Securities.ticker)
                  .filter(HistPrice1D.exch_time == day)
                  .all())

        # Check if the current day falls within any date range and get the corresponding tickers
        valid_tickers = is_date_in_range(
            day.strftime('%Y-%m-%d'), ticker_date_ranges)
        if valid_tickers is not None:
            stocks = [stock for stock in stocks if stock.ticker in valid_tickers]

        next_week_expiry = get_last_trading_day_of_future_week(
            day, valid_days, 7)
        str_day = datetime.strftime(day, '%Y-%m-%d')
        str_next_week_expiry = datetime.strftime(next_week_expiry, '%Y-%m-%d')

        response = requests.post('http://127.0.0.1:8000/get_iv_surfs', json={
            'tickers': [stock.ticker for stock in stocks],
            'start_date': str_day,
            'end_date': str_day
        })
        iv_surfs = response.json()

        response = requests.post('http://127.0.0.1:8000/get_stock_price', json={
            'tickers': [stock.ticker for stock in stocks],
            'start_date': str_day,
            'end_date': str_day
        })
        stock_prices = response.json()

        two_months_ago = day - timedelta(days=56)
        two_months_ago_trading_day = get_last_trading_day_of_week(
            two_months_ago, valid_days)

        # Retrieve historical prices two months ago
        str_two_months_ago = datetime.strftime(
            two_months_ago_trading_day, '%Y-%m-%d')
        response = requests.post('http://127.0.0.1:8000/get_stock_price', json={
            'tickers': [stock.ticker for stock in stocks],
            'start_date': str_two_months_ago,
            'end_date': str_two_months_ago
        })
        historical_prices = response.json()

        valid_options_iv_check = []
        T = (next_week_expiry - day).days / 365.0
        for stock in stocks:
            # Check if historical and current prices are available and valid
            if stock.ticker not in historical_prices or stock.ticker not in stock_prices:
                continue

            past_price_data = historical_prices[stock.ticker].get(
                str_two_months_ago)
            current_price_data = stock_prices[stock.ticker].get(str_day)
            if not past_price_data or not current_price_data:
                continue

            past_price = past_price_data['close_adj']
            current_price = current_price_data['close_adj']

            # Calculate return over the past two months
            stock_return = (current_price - past_price) / past_price

            # Check if the stock's return is less than 10%
            if stock_return > 0.1:
                continue

            if not iv_surfs[stock.ticker]:
                continue
            iv_surf = pickle.loads(base64.b64decode(
                iv_surfs[stock.ticker][str_day]))
            iv = iv_surf(strike_ratio, T)
            if not iv or np.isnan(iv) or iv < 0.58:
                continue
            valid_options_iv_check.append(stock)
        print(
            f"Valid options on {day}: {[stock.ticker for stock in valid_options_iv_check]}")

        capital_per_option = CAPITAL / \
            len(valid_options_iv_check) if valid_options_iv_check else 0
        capital_per_option = min(capital_per_option, CAPITAL / 20)

        tickers_to_check = [stock.ticker for stock in valid_options_iv_check]
        response = requests.post('http://127.0.0.1:8000/get_stock_price', json={
            'tickers': tickers_to_check,
            'start_date': str_next_week_expiry,
            'end_date': str_next_week_expiry
        })
        stock_prices_on_expiry = response.json()

        # Handling stocks that were exercised and held
        exercised_tickers = [
            ticker for ticker, info in holdings.items() if day >= info["expiry_date"]]
        recovery_count = 0
        if exercised_tickers:
            expiry_date = holdings[exercised_tickers[0]]["expiry_date"]
            three_weeks_later = last_trading_days[last_trading_days.index(
                expiry_date) + 3]
            response = requests.post('http://127.0.0.1:8000/get_stock_price', json={
                'tickers': exercised_tickers,
                'start_date': datetime.strftime(expiry_date, '%Y-%m-%d'),
                'end_date': datetime.strftime(three_weeks_later, '%Y-%m-%d')
            })
            prices_after_exercise_data = response.json()

            print("Existing holdings:")
            for ticker in exercised_tickers:
                prices_after_exercise = [
                    price for _, price in prices_after_exercise_data[ticker].items()]
                max_high = max([prices_after_exercise[0]['close_adj']] +
                               [price['high_adj'] for price in prices_after_exercise[1:]])
                strike_price = holdings[ticker]["strike"]

                if max_high > strike_price:
                    recovery_count += 1
                    pnl = 0  # Assume we sell the stock at strike price
                    print(
                        f"Ticker: {ticker} | Exercised on: {expiry_date.strftime('%Y-%m-%d')} | Sold at: {round(strike_price, 2)} | PnL: {round(pnl, 2)}")
                else:
                    final_price = prices_after_exercise[-1]['close_adj']
                    pnl = (final_price - strike_price) * \
                        100 * holdings[ticker]["contracts"]
                    print(
                        f"Ticker: {ticker} | Exercised on: {expiry_date.strftime('%Y-%m-%d')} | Sold at: {round(final_price, 2)} | PnL: {round(pnl, 2)}")

                cumulative_return += pnl
                del holdings[ticker]
            print(
                f"After liquidating existing holdings, the cumulative pnl became: {round(cumulative_return, 2)}")

        total_premium = 0
        for stock in valid_options_iv_check:
            stock_close_price = stock_prices[stock.ticker][str_day]['close_adj']
            strike_price = stock_close_price * strike_ratio
            iv_surf = pickle.loads(base64.b64decode(
                iv_surfs[stock.ticker][str_day]))
            option_premium = get_option_price(
                str_day, iv_surf, stock_close_price, 'put', str_next_week_expiry, strike_price)
            contracts_to_short = round(
                capital_per_option / (stock_close_price * 100), 2)
            total_premium += option_premium * 100 * contracts_to_short
            # print(f"Sold {contracts_to_short} contracts of {stock.ticker} put option with strike price {strike_price} and expiry {next_week_expiry} for a premium of {option_premium} with pnl of {option_premium * 100 * contracts_to_short}")

            if stock.ticker in stock_prices_on_expiry and stock_prices_on_expiry[stock.ticker][str_next_week_expiry]['close_adj'] < strike_price:
                holdings[stock.ticker] = {
                    "strike": strike_price,
                    "expiry_date": next_week_expiry,
                    "contracts": contracts_to_short
                }

        cumulative_return += total_premium
        returns.append(cumulative_return)
        average_premium = total_premium / CAPITAL * 100
        premiums.append(average_premium)
        option_counts.append(len(valid_options_iv_check))
        recovery_ratios.append(
            recovery_count / len(exercised_tickers) if len(exercised_tickers) else 0)
        exercised_ratios.append(
            len(holdings) / len(valid_options_iv_check) if len(valid_options_iv_check) else 0)

        print(
            f"After selling options, the cumulative pnl became: {round(cumulative_return, 2)}, with the average premium being {average_premium:.2f}%")
        print("Next holdings:")
        for ticker, details in holdings.items():
            formatted_expiry = details['expiry_date'].strftime('%Y-%m-%d')
            formatted_strike = round(details['strike'], 2)
            formatted_contracts = round(details['contracts'], 2)
            print(
                f"Ticker: {ticker} | Strike: {formatted_strike} | Expiry Date: {formatted_expiry} | Contracts: {formatted_contracts}")

    print("returns: " + ", ".join(f"{ret:.2f}" for ret in returns))
    print("premiums: " + ", ".join(f"{premium:.2f}" for premium in premiums))
    print("option_counts: " +
          ", ".join(f"{option_count}" for option_count in option_counts))
    print("recovery_ratios: " +
          ", ".join(f"{recovery_ratio:.2f}" for recovery_ratio in recovery_ratios))
    print("exercised_ratios: " +
          ", ".join(f"{exercised_ratio:.2f}" for exercised_ratio in exercised_ratios))

    plt.plot(backtest_days, returns)
    plt.xlabel("Date")
    plt.ylabel("Cumulative Return")
    plt.title("Strategy Cumulative Returns Over Time")
    plt.show()


start_date = date(2021, 12, 17)
end_date = date(2024, 7, 12)
backtest_strategy(start_date, end_date)
