import json
import yfinance as yf
import numpy as np
import pandas as pd
import argparse

def get_target_positions(filename, capital):
    with open(filename, "r") as file:
        portfolios = json.load(file)

    total_capital = capital

    all_tickers = set()
    for portfolio in portfolios.values():
        all_tickers.update(portfolio["long"])
        all_tickers.update(portfolio["short"])

    data = yf.download(list(all_tickers), period="1d", interval="1d")

    results = {}

    for portfolio_name, portfolio in portfolios.items():
        weight = portfolio["weight"]
        portfolio_capital = total_capital * weight

        long_stocks = portfolio["long"]
        short_stocks = portfolio["short"]

        long_capital = portfolio_capital / 2
        short_capital = portfolio_capital / 2

        long_allocation_per_stock = long_capital / len(long_stocks)
        short_allocation_per_stock = short_capital / len(short_stocks)

        for stock in long_stocks:
            price = data['Adj Close'][stock].iloc[-1] if stock in data['Adj Close'] else np.nan
            if not np.isnan(price):
                results[stock] = results.get(
                    stock, 0) + np.round(long_allocation_per_stock / price)

        for stock in short_stocks:
            price = data['Adj Close'][stock].iloc[-1] if stock in data['Adj Close'] else np.nan
            if not np.isnan(price):
                results[stock] = results.get(
                    stock, 0) - np.round(short_allocation_per_stock / price)

    df_results = pd.DataFrame(list(results.items()), columns=['Symbol', 'Quantity'])
    df_results = df_results[['Quantity', 'Symbol']]  # Reorder columns
    df_results['Quantity'] = df_results['Quantity'].astype(int)  # Convert quantities to integers

    return df_results


if __name__ == "__main__":
    # Create an argument parser
    parser = argparse.ArgumentParser(description="Calculate target positions from a JSON file.")
    parser.add_argument("filename", type=str, help="The name of the JSON file with portfolio data.")
    parser.add_argument("capital", type=float, help="The total capital to allocate.")

    # Parse the command line arguments
    args = parser.parse_args()

    # Call the function with arguments from the command line
    pd.set_option('display.max_rows', None)
    df_results = get_target_positions(args.filename, args.capital)
    print(df_results.to_csv(index=False))  # Print in the desired format
