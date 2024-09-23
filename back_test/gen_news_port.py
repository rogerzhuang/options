import datetime
import subprocess
import re
import json

# Step 1: Calculate Nearest and Previous Fridays


def get_past_fridays(reference_date, count=3):
    ref_date = datetime.datetime.strptime(reference_date, "%Y-%m-%d")
    fridays = []
    for i in range(count):
        nearest_friday = ref_date - \
            datetime.timedelta((ref_date.weekday() - 4) %
                               7) - datetime.timedelta(7 * i)
        fridays.append(nearest_friday.strftime("%Y-%m-%d"))
    return fridays

# Step 2: Run Back-Testing Commands and Capture Real Output


def run_backtest(date1, date2):
    commands = [
        f"python back_test_news.py {date1} {date1} 13 0 50 1 0 2",
        f"python back_test_news.py {date2} {date2} 13 0 50 1 1 2",
        f"python back_test_news.py {date2} {date2} 6 6 70 1 1 2"
    ]

    outputs = []

    for cmd in commands:
        print(f"Running command: {cmd}")
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True)
        output = result.stdout
        print(f"Command output:\n{output}")
        outputs.append(output)

    return outputs

# Step 3: Extract Long and Short Stocks


def extract_tickers(output):
    # Find long and short tickers using regex
    long_tickers = re.findall(r"long tickers \[(.*?)\]", output)
    short_tickers = re.findall(r"short tickers \[(.*?)\]", output)

    # Convert the tickers from strings to lists and clean up single quotes
    long_tickers = [ticker.strip().replace("'", "")
                    for ticker in long_tickers[0].split(',')] if long_tickers else []
    short_tickers = [ticker.strip().replace("'", "")
                     for ticker in short_tickers[0].split(',')] if short_tickers else []

    return long_tickers, short_tickers

# Step 4: Write all portfolios to JSON at once


def write_all_portfolios_to_json(portfolios, output_file="news_ls_auto.json"):
    news_ls = {}

    # Add each portfolio to the JSON structure
    for i, portfolio in enumerate(portfolios):
        # Use different weights for backtest and Zacks portfolios
        if i < 3:
            weight = 0.22222222222  # For the first three portfolios (backtest)
        else:
            # For the Zacks portfolios (portfolio3, portfolio4, portfolio5)
            weight = 0.11111111111

        news_ls[f"portfolio{i}"] = {
            "weight": weight,
            "long": portfolio["long"],
            "short": portfolio["short"]
        }

    with open(output_file, "w") as file:
        json.dump(news_ls, file, indent=4)

# Step 5: Run gen_zacks_sample.py and capture long/short tickers


def run_zacks_sample(date):
    cmd = f"python gen_zacks_sample.py {date}"
    print(f"Running command: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    output = result.stdout
    print(f"Zacks sample output for {date}:\n{output}")

    # Use regex to extract the long/short block from the lengthy output
    long_short_match = re.search(
        r"\{\s*\"long\":\s*\[.*?\],\s*\"short\":\s*\[.*?\]\s*\}", output, re.DOTALL)

    if long_short_match:
        # Convert the matched block to a Python dictionary
        long_short_dict = json.loads(long_short_match.group())
        return long_short_dict['long'], long_short_dict['short']
    else:
        print(
            f"Error: Could not extract long/short tickers from Zacks sample output for {date}")
        return [], []

# Main automation function


def automate_process():
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    nearest_friday, previous_friday = get_past_fridays(today, count=2)[:2]

    # Run backtest and capture output
    outputs = run_backtest(nearest_friday, previous_friday)

    # Collect all portfolios
    portfolios = []

    for output in outputs:
        # Extract tickers from each output
        long_tickers, short_tickers = extract_tickers(output)

        if long_tickers and short_tickers:
            portfolios.append({
                "long": long_tickers,
                "short": short_tickers
            })

    # Add three more portfolios from running gen_zacks_sample.py
    zacks_fridays = get_past_fridays(today, count=3)

    for i, zacks_friday in enumerate(zacks_fridays):
        long_tickers, short_tickers = run_zacks_sample(zacks_friday)

        if long_tickers and short_tickers:
            portfolios.append({
                "long": long_tickers,
                "short": short_tickers
            })

    # Write all portfolios to news_ls_auto.json at once
    if portfolios:
        write_all_portfolios_to_json(portfolios)


# Run the automation
automate_process()
