import argparse
import requests
import json
import logging
import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from back_test.high_iv import get_stock_list_with_high_put_iv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def update_news(tickers, start_date, end_date):
    url = "http://127.0.0.1:5000/populate_news"
    headers = {
        "Content-Type": "application/json"
    }
    payload = {
        "tickers": tickers,
        "start_date": start_date,
        "end_date": end_date
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        logger.info("News updated successfully!")
        logger.info(f"Response: {response.json()}")
    else:
        logger.error("Failed to update news.")
        logger.error(f"Status Code: {response.status_code}")
        logger.error(f"Response: {response.text}")

def update_sentiments(tickers, start_date, end_date):
    url = "http://127.0.0.1:5000/update_sentiment_scores"
    headers = {
        "Content-Type": "application/json"
    }
    payload = {
        "tickers": tickers,
        "start_date": start_date,
        "end_date": end_date
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))

    if response.status_code == 200:
        logger.info("Sentiments updated successfully!")
        logger.info(f"Response: {response.json()}")
    else:
        logger.error("Failed to update sentiments.")
        logger.error(f"Status Code: {response.status_code}")
        logger.error(f"Response: {response.text}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update news and sentiments for high IV stocks")
    parser.add_argument("high_iv_date", type=str, help="Date to fetch high IV stocks (YYYY-MM-DD)")
    parser.add_argument("start_date", type=str, help="Start date for news update (YYYY-MM-DD)")
    parser.add_argument("end_date", type=str, help="End date for news update (YYYY-MM-DD)")
    parser.add_argument("--strike_ratio", type=float, default=0.9, help="Strike price ratio for IV calculation")
    parser.add_argument("--expiry_days", type=float, default=7, help="Days to expiry for the option")
    parser.add_argument("--n_stocks", type=int, default=70, help="Number of stocks to return with highest put IVs")

    args = parser.parse_args()

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
    session = Session()

    # Change the working directory to the back_test folder
    back_test_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'back_test')
    os.chdir(back_test_dir)

    # Get high IV stocks
    high_iv_date = datetime.strptime(args.high_iv_date, '%Y-%m-%d').date()
    high_iv_stocks = get_stock_list_with_high_put_iv(
        high_iv_date, 
        args.strike_ratio, 
        args.expiry_days, 
        args.n_stocks, 
        session
    )

    # Change back to the original directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    logger.info(f"Updating news and sentiments for {len(high_iv_stocks)} stocks")

    # Update news
    update_news(high_iv_stocks, args.start_date, args.end_date)

    # Update sentiments
    update_sentiments(high_iv_stocks, args.start_date, args.end_date)

    logger.info("News and sentiment update completed")
