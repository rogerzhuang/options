from back_test.high_iv import get_stock_list_with_high_put_iv
import argparse
from datetime import datetime, timedelta
import requests
import sys
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def get_fridays(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() == 4:  # Friday is 4
            yield current_date
        current_date += timedelta(days=1)

def update_news(tickers, start_date, end_date):
    url = "http://127.0.0.1:5000/populate_news"
    payload = {
        "tickers": tickers,
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d')
    }
    response = requests.post(url, json=payload)
    return response.json()

def update_sentiments(tickers, start_date, end_date):
    url = "http://127.0.0.1:5000/update_sentiment_scores"
    payload = {
        "tickers": tickers,
        "start_date": start_date.strftime('%Y-%m-%d'),
        "end_date": end_date.strftime('%Y-%m-%d')
    }
    response = requests.post(url, json=payload)
    return response.json()

def main(start_date, end_date, update_news_flag, update_sentiments_flag):
    # Load environment variables
    load_dotenv()

    # Set up database connection
    db_user = os.environ.get('DB_USER')
    db_password = os.environ.get('DB_PASSWORD')
    db_server = os.environ.get('DB_SERVER').replace('\\\\', '\\')
    db_name = os.environ.get('DB_NAME')

    driver_name = "ODBC Driver 17 for SQL Server"
    DATABASE_URL = f'mssql+pyodbc://{db_user}:{db_password}@{db_server}/{db_name}?driver={driver_name}'
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    db_session = Session()

    # Change to the back_test directory before calling get_stock_list_with_high_put_iv
    original_dir = os.getcwd()
    back_test_dir = os.path.join(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))), 'back_test')
    os.chdir(back_test_dir)

    try:
        for friday in get_fridays(start_date, end_date):
            print(f"Processing Friday: {friday}")

            # Get high IV tickers
            high_iv_tickers = get_stock_list_with_high_put_iv(
                friday, 0.9, 7, 70, db_session)

            # Calculate date range for news and sentiments
            news_start_date = friday - timedelta(days=13)  # Two Saturdays ago
            news_end_date = friday + timedelta(days=6)  # Next Thursday or today

            if update_news_flag:
                print(f"Updating news for {len(high_iv_tickers)} tickers from {news_start_date} to {news_end_date}")
                result = update_news(high_iv_tickers, news_start_date, news_end_date)
                print(f"News update result: {result}")

            if update_sentiments_flag:
                print(f"Updating sentiments for {len(high_iv_tickers)} tickers from {news_start_date} to {news_end_date}")
                result = update_sentiments(high_iv_tickers, news_start_date, news_end_date)
                print(f"Sentiment update result: {result}")

    finally:
        # Change back to the original directory
        os.chdir(original_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update historical news and sentiments for high IV stocks")
    parser.add_argument("start_date", type=lambda s: datetime.strptime(
        s, '%Y-%m-%d').date(), help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", type=lambda s: datetime.strptime(
        s, '%Y-%m-%d').date(), help="End date (YYYY-MM-DD)")
    parser.add_argument("--news", action="store_true", help="Update news")
    parser.add_argument("--sentiments", action="store_true",
                        help="Update sentiments")

    args = parser.parse_args()

    if not (args.news or args.sentiments):
        parser.error(
            "At least one of --news or --sentiments must be specified.")

    main(args.start_date, args.end_date, args.news, args.sentiments)
