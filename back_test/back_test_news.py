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
        .filter(News.author == 'Zacks Equity Research')
        # .filter(NewsSecurities.ticker.not_in(['PXD']))
        .group_by(NewsSecurities.ticker)
        .all())
    return {score.ticker: score.average_sentiment for score in sentiment_scores}

# Function to backtest sentiment-based strategy


def get_stock_prices_on_day(tickers, day):
    str_day = datetime.strftime(day, '%Y-%m-%d')
    response = requests.post('http://127.0.0.1:8000/get_stock_price', json={
        'tickers': tickers,
        'start_date': str_day,
        'end_date': str_day
    })
    return response.json()


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


def backtest_sentiment_strategy(start_date, end_date, n_days_sentiment, k_stocks, m_weeks_hold, p_weeks_delay, frac, session):
    nyse = mcal.get_calendar('NYSE')
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

        # On each last trading day of the week, form the portfolio
        if day == last_trading_day:
            next_last_trading_day = get_last_trading_day_of_week(
                last_trading_day + timedelta(weeks=1), valid_days)
            high_iv_stocks = get_stock_list_with_high_put_iv(
                day, 0.9, (next_last_trading_day - last_trading_day).days/365.0, k_stocks, session)
            # high_iv_stocks = filter_stocks_based_on_return(
            #     day, k_stocks, session)
            # sentiment_scores = get_sentiment_scores(
            #     high_iv_stocks, day - timedelta(days=n_days_sentiment), day, session)
            sentiment_scores = get_sentiment_scores(
                high_iv_stocks, day - timedelta(days=n_days_sentiment//2), day + timedelta(days=n_days_sentiment//2), session)
            # Exclude items with None values
            sentiment_scores = {ticker: score for ticker,
                                score in sentiment_scores.items() if score}

            sorted_stocks = sorted(
                sentiment_scores.items(), key=lambda x: x[1], reverse=True)
            long_stocks = [{'ticker': stock[0], 'position': 'long'}
                           for stock in sorted_stocks[:len(sorted_stocks) // frac]]
            short_stocks = [{'ticker': stock[0], 'position': 'short'}
                            for stock in sorted_stocks[-(len(sorted_stocks) // frac):]]

            # Set trade day as the next last trading day of the week
            next_week = day + timedelta(weeks=p_weeks_delay)
            next_trade_day = get_last_trading_day_of_week(
                next_week, valid_days)
            trades_to_execute.append(
                (long_stocks + short_stocks, next_trade_day))

    # Execute trades and calculate returns
    for trade, trade_day in trades_to_execute:
        portfolio = execute_trades(trade, trade_day)

        unwind_day = get_last_trading_day_of_week(
            trade_day + timedelta(weeks=m_weeks_hold), valid_days)
        if unwind_day:
            total_return = calculate_returns(portfolio, trade_day, unwind_day)
            print(f"Unwinding on {unwind_day} with return {total_return}")
            returns_list.append({"date": unwind_day, "return": total_return})

    return returns_list


# Example usage of the backtest function
if __name__ == '__main__':
    # Database connection setup
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

    start_date = date(2024, 5, 17)
    end_date = date(2024, 5, 17)
    n_days_sentiment = 13
    k_stocks = 70
    m_weeks_hold = 1
    p_weeks_delay = 1
    frac = 2
    returns = backtest_sentiment_strategy(
        start_date, end_date, n_days_sentiment, k_stocks, m_weeks_hold, p_weeks_delay, frac, session)
    for ret in returns:
        print(
            f"Date: {ret['date'].strftime('%Y-%m-%d')}, Return: {ret['return']:.2%}")
