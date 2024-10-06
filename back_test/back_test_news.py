from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from datetime import date, timedelta, datetime
import pandas_market_calendars as mcal
from models import News, NewsSecurities
from high_iv import get_stock_list_with_high_put_iv
# from high_iv_wfilter import filter_stocks_based_on_return
from dotenv import load_dotenv
import requests
import os
import argparse


def get_last_trading_day_of_week(date, valid_days):
    current_year, current_week = date.isocalendar()[:2]
    days_of_current_week = [day for day in valid_days if day.isocalendar(
    )[0] == current_year and day.isocalendar()[1] == current_week]
    return max(days_of_current_week) if days_of_current_week else None


# Function to get average sentiment scores
def get_sentiment_scores(tickers, start_date, end_date, session):
    sentiment_scores = (session.query(
        NewsSecurities.ticker,
        func.avg(NewsSecurities.sentiment).label('average_sentiment')
    )
        .join(News, News.id == NewsSecurities.news_id)
        .filter(News.exch_time.between(start_date, end_date))
        .filter(NewsSecurities.ticker.in_(tickers))
        # .filter(News.author == 'Zacks Equity Research')
        .filter(News.publisher_name == 'Yahoo')
        # .filter(NewsSecurities.ticker.not_in(['PXD']))
        .group_by(NewsSecurities.ticker)
        .all())
    return {score.ticker: score.average_sentiment for score in sentiment_scores}

# Function to backtest sentiment-based strategy


# def get_stock_prices_on_day(tickers, day):
#     str_day = datetime.strftime(day, '%Y-%m-%d')
#     response = requests.post('http://127.0.0.1:5000/get_stock_price', json={
#         'tickers': tickers,
#         'start_date': str_day,
#         'end_date': str_day
#     })
#     return response.json()
def get_stock_prices_on_day(tickers, day):
    str_day = datetime.strftime(day, '%Y-%m-%d')
    try:
        response = requests.post('http://127.0.0.1:5000/get_stock_price', json={
            'tickers': tickers,
            'start_date': str_day,
            'end_date': str_day
        })
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)

        data = response.json()
        if not data:
            raise ValueError("No data returned from the server.")

        return data
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching stock prices: {e}")
        return None
    except ValueError as e:
        print(f"An error occurred: {e}")
        return None


def execute_trades(trade_info, trade_day):
    long_tickers = [info['ticker']
                    for info in trade_info if info['position'] == 'long']
    short_tickers = [info['ticker']
                     for info in trade_info if info['position'] == 'short']
    print(
        f"Executing trades on {trade_day} with long tickers {long_tickers} and short tickers {short_tickers}")

    long_prices = get_stock_prices_on_day(long_tickers, trade_day)
    short_prices = get_stock_prices_on_day(short_tickers, trade_day)

    return {
        "long": {ticker: long_prices[ticker] for ticker in long_tickers},
        "short": {ticker: short_prices[ticker] for ticker in short_tickers},
        "trade_day": trade_day
    }


def calculate_returns(portfolio, trade_day, unwind_day):
    # Formatting the trade and unwind days
    trade_day_str = datetime.strftime(trade_day, '%Y-%m-%d')
    unwind_day_str = datetime.strftime(unwind_day, '%Y-%m-%d')

    # Get stock prices on the unwind day
    long_prices_end = get_stock_prices_on_day(
        list(portfolio["long"].keys()), unwind_day)
    short_prices_end = get_stock_prices_on_day(
        list(portfolio["short"].keys()), unwind_day)

    if long_prices_end is None or short_prices_end is None:
        print("Error: Failed to fetch stock prices.")
        return None

    # Calculate returns for long and short positions
    long_returns = sum((long_prices_end[ticker][unwind_day_str]['close_adj'] - portfolio["long"][ticker][trade_day_str]['close_adj']) / portfolio["long"][ticker][trade_day_str]['close_adj']
                       for ticker in portfolio["long"]) / len(portfolio["long"])
    short_returns = sum((portfolio["short"][ticker][trade_day_str]['close_adj'] - short_prices_end[ticker][unwind_day_str]['close_adj']) / portfolio["short"][ticker][trade_day_str]['close_adj']
                        for ticker in portfolio["short"]) / len(portfolio["short"])
    print(f"individual long returns: {[((long_prices_end[ticker][unwind_day_str]['close_adj'] - portfolio['long'][ticker][trade_day_str]['close_adj']) / portfolio['long'][ticker][trade_day_str]['close_adj']) for ticker in portfolio['long']]}")
    print(f"individual short returns: {[((portfolio['short'][ticker][trade_day_str]['close_adj'] - short_prices_end[ticker][unwind_day_str]['close_adj']) / portfolio['short'][ticker][trade_day_str]['close_adj']) for ticker in portfolio['short']]}")
    print(
        f"Long returns: {long_returns:.2%}, Short returns: {short_returns:.2%}")
    total_return = long_returns + short_returns
    return total_return


def get_friday_of_week(date):
    """Get the Friday of the week for the given date."""
    return date + timedelta(days=(4 - date.weekday()))


def backtest_sentiment_strategy(start_date, end_date, start_days_sentiment, end_days_sentiment, k_stocks, m_weeks_hold, p_weeks_delay, frac, session):
    nyse = mcal.get_calendar('NYSE')

    # Adjust start_date to the previous trading day if it's not a trading day
    while start_date not in nyse.valid_days(start_date=start_date, end_date=start_date).date:
        start_date -= timedelta(days=1)

    extended_end_date = end_date + \
        timedelta(weeks=m_weeks_hold + p_weeks_delay)
    valid_days = nyse.valid_days(
        start_date=start_date, end_date=extended_end_date).date.tolist()
    backtest_days = nyse.valid_days(
        start_date=start_date, end_date=end_date).date.tolist()

    trades_to_execute = []
    returns_list = []

    for day in backtest_days:
        last_trading_day = get_last_trading_day_of_week(day, valid_days)
        if day == last_trading_day:
            friday_of_week = get_friday_of_week(day)

            # Always use expiry_days = 7, adjustments are handled inside the function
            high_iv_stocks = get_stock_list_with_high_put_iv(
                friday_of_week, 0.9, 7, k_stocks, session)

            sentiment_start_date = friday_of_week - \
                timedelta(days=start_days_sentiment)
            sentiment_end_date = friday_of_week + \
                timedelta(days=end_days_sentiment)

            sentiment_scores = get_sentiment_scores(
                high_iv_stocks, sentiment_start_date, sentiment_end_date, session)
            sentiment_scores = {ticker: score for ticker,
                                score in sentiment_scores.items() if score}

            sorted_stocks = sorted(
                sentiment_scores.items(), key=lambda x: x[1], reverse=True)
            num_stocks = len(sorted_stocks) // frac
            long_stocks = [{'ticker': stock[0], 'position': 'long'}
                           for stock in sorted_stocks[:num_stocks]]
            short_stocks = [{'ticker': stock[0], 'position': 'short'}
                            for stock in sorted_stocks[-num_stocks:]]

            delay_days = timedelta(weeks=p_weeks_delay)
            trade_week = day + delay_days
            trade_day = get_last_trading_day_of_week(trade_week, valid_days)

            trades_to_execute.append((long_stocks + short_stocks, trade_day))

    for trade, trade_day in trades_to_execute:
        if trade_day:
            portfolio = execute_trades(trade, trade_day)

            hold_days = timedelta(weeks=m_weeks_hold)
            unwind_week = trade_day + hold_days
            unwind_day = get_last_trading_day_of_week(unwind_week, valid_days)
            if unwind_day:
                total_return = calculate_returns(
                    portfolio, trade_day, unwind_day)
                print(
                    f"Unwinding on {unwind_day} with return {total_return:.2%}")
                returns_list.append(
                    {"date": unwind_day, "return": total_return})
            else:
                print(f"No unwind day found for trade executed on {trade_day}")
        else:
            print(f"No trade day found for trades scheduled after {day}")

    return returns_list


# Example usage of the backtest function
if __name__ == '__main__':
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description='Backtest sentiment-based strategy.')
    parser.add_argument('start_date', type=str,
                        help='Start date of the backtest (format YYYY-MM-DD)')
    parser.add_argument('end_date', type=str,
                        help='End date of the backtest (format YYYY-MM-DD)')
    parser.add_argument('start_days_sentiment', type=int,
                        help='Number of days before the trade day to start checking sentiment')
    parser.add_argument('end_days_sentiment', type=int,
                        help='Number of days after the trade day to end checking sentiment')
    parser.add_argument('k_stocks', type=int,
                        help='Number of stocks to analyze with high put IV')
    parser.add_argument('m_weeks_hold', type=int,
                        help='Number of weeks to hold the trades')
    parser.add_argument('p_weeks_delay', type=int,
                        help='Number of weeks delay before executing the trade')
    parser.add_argument(
        'frac', type=int, help='Fraction of stocks to go long and short')

    args = parser.parse_args()

    # Parse the command-line arguments and set up dates
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()

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

    # Execute the backtest
    returns = backtest_sentiment_strategy(
        start_date, end_date, args.start_days_sentiment, args.end_days_sentiment, args.k_stocks, args.m_weeks_hold, args.p_weeks_delay, args.frac, session
    )

    # Print the results
    for ret in returns:
        return_value = ret['return']
        if return_value is None:
            formatted_return = "N/A"
        else:
            formatted_return = f"{return_value:.2%}"
        print(
            f"Date: {ret['date'].strftime('%Y-%m-%d')}, Return: {formatted_return}")
