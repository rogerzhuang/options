import json
import yfinance as yf
import numpy as np
import pandas as pd

# Load the JSON data with added weight attribute for each portfolio
with open("news_ls.json", "r") as file:
    portfolios = json.load(file)

total_capital = 600_000  # Example total capital

all_tickers = set()
for portfolio in portfolios.values():
    all_tickers.update(portfolio["long"])
    all_tickers.update(portfolio["short"])

# Fetch latest stock prices using yfinance
data = yf.download(list(all_tickers), period="1d", interval="1d")

results = {}

for portfolio_name, portfolio in portfolios.items():
    # Use the portfolio weight to determine capital allocation
    weight = portfolio["weight"]
    portfolio_capital = total_capital * weight

    long_stocks = portfolio["long"]
    short_stocks = portfolio["short"]

    # Allocation to long and short positions within each portfolio remains equal
    long_capital = portfolio_capital / 2
    short_capital = portfolio_capital / 2

    # Equal allocation to each stock within long or short positions
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

# Convert the results dictionary to a DataFrame for better visualization and management
df_results = pd.DataFrame(list(results.items()), columns=[
                          'Stock', 'Net_Quantity'])

pd.set_option('display.max_rows', None)
print(df_results)
