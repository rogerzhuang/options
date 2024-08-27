import os
import psycopg2
from dotenv import load_dotenv
import yfinance as yf
import pandas as pd
from scipy.stats import zscore
from tqdm import tqdm  # For displaying progress
from datetime import datetime, timedelta
import json


def load_environment_variables():
    load_dotenv()
    return {
        "host": os.environ.get("PGHOST"),
        "user": os.environ.get("PGUSER"),
        "password": os.environ.get("PGPASSWORD"),
        "dbname": os.environ.get("PGDATABASE"),
        "port": os.environ.get("PGPORT")
    }


def fetch_stock_data(tickers):
    def fetch_data(ticker):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info

            # Check if the exchange is in the US and marketCap is available and non-zero
            if info.get('marketCap', 0) > 0 and 'exchange' in info and info['exchange'] in ['NMS', 'NYQ', 'ASE']:
                hist = stock.history(period="3mo")
                market_cap = info['marketCap']
                avg_turnover = hist['Volume'].mean(
                ) * hist['Close'].mean() if not hist.empty else 0
                return ticker, market_cap, avg_turnover
            else:
                raise ValueError(
                    f"{ticker} is not a US-listed stock or marketCap is not available")
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
            return ticker, 0, 0

    return [fetch_data(ticker) for ticker in tqdm(tickers, desc="Fetching data")]


def calculate_scores_and_rank(data):
    df = pd.DataFrame(data, columns=['Ticker', 'MarketCap', 'AvgTurnover'])
    df['MarketCapZ'] = zscore(df['MarketCap'])
    df['AvgTurnoverZ'] = zscore(df['AvgTurnover'])
    df['FinalScore'] = (df['MarketCapZ'] + df['AvgTurnoverZ']) / 2
    pd.set_option('display.max_rows', None)
    print(pd.DataFrame(df, columns=[
          'Ticker', 'MarketCap', 'MarketCapZ', 'AvgTurnover', 'AvgTurnoverZ', 'FinalScore']))
    return df.sort_values(by='FinalScore', ascending=False).head(800)['Ticker'].tolist()


def group_stocks_by_zacks_rank(conn_params, tickers, start_date):
    end_date = (datetime.strptime(start_date, "%Y-%m-%d") +
                timedelta(days=1)).strftime("%Y-%m-%d")
    sql_query = """
    SELECT DISTINCT s.ticker, z.zacksrank
    FROM stocks s
    JOIN zacks_rankings z ON s.id = z.stock_id
    WHERE z.zacksrank IN (1, 5)
    AND z.updatedat >= %s
    AND z.updatedat < %s
    AND s.ticker = ANY(%s)
    ORDER BY zacksrank, ticker;
    """
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_query, (start_date, end_date, tickers))
            result = cur.fetchall()

    long_stocks = [row[0] for row in result if row[1] == 1]
    short_stocks = [row[0] for row in result if row[1] == 5]

    return {
        "long": long_stocks,
        "short": short_stocks
    }


def main(start_date):
    conn_params = load_environment_variables()
    with open('stock_universe.json', 'r') as file:
        data = json.load(file)
    tickers = data['tickers']

    stock_data = fetch_stock_data(tickers)
    top_stocks = calculate_scores_and_rank(stock_data)
    grouped_stocks = group_stocks_by_zacks_rank(
        conn_params, top_stocks, start_date)

    print(json.dumps(grouped_stocks, indent=2))


# Example usage
if __name__ == "__main__":
    main("2024-08-23")
