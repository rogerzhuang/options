def aggregate_returns(file_path):
    # Initialize dictionaries for long and short returns
    long_returns = {}
    short_returns = {}

    # Read the content of the file
    with open(file_path, 'r') as file:
        content = file.read()

    # Split the content into blocks of trades
    trade_blocks = content.split('Executing trades on ')
    # Skip the first split which is empty or header text
    for block in trade_blocks[1:]:
        # Extracting the part with tickers and returns
        lines = block.split('\n')

        # Processing long tickers and returns
        long_tickers_line = lines[0].split('with long tickers ')[
            1].split(' and short tickers ')[0]
        # Safe since we control the input
        long_tickers = eval(long_tickers_line)
        num_long_tickers = len(long_tickers)

        long_returns_line = lines[1].split('individual long returns: ')[1]
        # Safe since we control the input
        individual_long_returns = eval(long_returns_line)

        for ticker, ret in zip(long_tickers, individual_long_returns):
            # Adjust return by dividing by the number of long tickers
            adjusted_ret = ret / num_long_tickers
            if ticker in long_returns:
                long_returns[ticker] += adjusted_ret
            else:
                long_returns[ticker] = adjusted_ret

        # Processing short tickers and returns
        short_tickers_line = lines[0].split('and short tickers ')[1]
        # Safe since we control the input
        short_tickers = eval(short_tickers_line)
        num_short_tickers = len(short_tickers)

        short_returns_line = lines[2].split('individual short returns: ')[1]
        # Safe since we control the input
        individual_short_returns = eval(short_returns_line)

        for ticker, ret in zip(short_tickers, individual_short_returns):
            # Adjust return by dividing by the number of short tickers
            adjusted_ret = ret / num_short_tickers
            if ticker in short_returns:
                short_returns[ticker] += adjusted_ret
            else:
                short_returns[ticker] = adjusted_ret

    return {'long': long_returns, 'short': short_returns}


# Example usage
file_path = 'news_trades.txt'
result = aggregate_returns(file_path)
print(result)
