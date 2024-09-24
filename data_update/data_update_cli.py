import argparse
import requests
import json
import csv
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def read_tickers_from_csv(file_path):
    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        tickers = [row[0] for row in csv_reader]
    return tickers


def update_tickers(tickers, start_date, end_date):
    url = "http://127.0.0.1:5000/populate_tickers"
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
        logger.info("Tickers updated successfully!")
        logger.info(f"Response: {response.json()}")
    else:
        logger.error("Failed to update tickers.")
        logger.error(f"Status Code: {response.status_code}")
        logger.error(f"Response: {response.text}")


def update_prices(tickers, start_date, end_date):
    url = "http://127.0.0.1:5000/populate_prices"
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
        logger.info("Prices updated successfully!")
        logger.info(f"Response: {response.json()}")
    else:
        logger.error("Failed to update prices.")
        logger.error(f"Status Code: {response.status_code}")
        logger.error(f"Response: {response.text}")


def update_iv_surfs(tickers, start_date, end_date):
    url = "http://127.0.0.1:5000/populate_iv_surfs"
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
        logger.info("IV Surfaces updated successfully!")
        logger.info(f"Response: {response.json()}")
    else:
        logger.error("Failed to update IV surfaces.")
        logger.error(f"Status Code: {response.status_code}")
        logger.error(f"Response: {response.text}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update tickers, prices, and IV surfaces via command line")
    parser.add_argument("csv_file", type=str,
                        help="Path to the CSV file containing tickers")
    parser.add_argument("start_date", type=str,
                        help="Start date in YYYY-MM-DD format")
    parser.add_argument("end_date", type=str,
                        help="End date in YYYY-MM-DD format")

    args = parser.parse_args()

    tickers = read_tickers_from_csv(args.csv_file)

    update_tickers(tickers, args.start_date, args.end_date)
    update_prices(tickers, args.start_date, args.end_date)
    update_iv_surfs(tickers, args.start_date, args.end_date)
