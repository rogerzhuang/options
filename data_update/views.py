from flask import Blueprint, current_app as app, request, jsonify, render_template
import requests
from datetime import datetime, timedelta
from pytz import timezone
import pytz
from models import db, Securities, Stocks, Options, HistPrice1D, IvSurf, News, NewsSecurities
import pandas_market_calendars as mcal
from sqlalchemy.exc import IntegrityError
import asyncio
import aiohttp
import pickle
from iv_surf.iv_surf import get_iv_surf, get_option_price
import base64
import numpy as np
from bs4 import BeautifulSoup
from news import get_largest_text_block
import math
from dotenv import load_dotenv
import os
from openai import AsyncOpenAI
import openai
import re

load_dotenv()

main = Blueprint('main', __name__)

POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
BASE_URL = 'https://api.polygon.io/v3/reference/options/contracts'


def get_last_trading_day_of_week(date, valid_days):
    """Return the last trading day of the week for the given date."""
    current_year, current_week = date.isocalendar()[:2]

    # Filter valid_days for the current week and year
    days_of_current_week = [day for day in valid_days if day.isocalendar(
    )[0] == current_year and day.isocalendar()[1] == current_week]

    # Return the maximum date from days_of_current_week
    return max(days_of_current_week) if days_of_current_week else None


def get_expiry_dates(asof_date, valid_days):
    """Return the expiry dates based on the asof_date."""
    one_week_ahead = get_last_trading_day_of_week(
        asof_date + timedelta(days=7), valid_days)
    two_weeks_ahead = get_last_trading_day_of_week(
        asof_date + timedelta(days=14), valid_days)
    one_month_ahead = get_last_trading_day_of_week(
        asof_date + timedelta(days=28), valid_days)
    return [one_week_ahead, two_weeks_ahead, one_month_ahead]


def instance_to_dict(instance):
    return {c.key: getattr(instance, c.key) for c in instance.__table__.columns}


MAX_CONCURRENT_REQUESTS = 2000  # Adjust this based on what you find optimal
RETRY_ATTEMPTS = 5
REQUEST_TIMEOUT = 10  # seconds
timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
BATCH_SIZE = 2000  # Adjust based on your needs


async def fetch_data(session, url, semaphore):
    async with semaphore:
        for attempt in range(RETRY_ATTEMPTS):
            try:
                async with session.get(url, timeout=timeout) as response:
                    if response.status != 200:
                        return {'error': f"Received status {response.status} for URL: {url}", 'url': url}
                    return await response.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt < RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(1)  # wait a second before retrying
                    continue
                else:
                    return {'error': str(e), 'url': url}


async def get_concurrent_stock_data(ticker, start_date, end_date):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        stock_url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}?adjusted=false&sort=asc&limit=5000&apiKey={POLYGON_API_KEY}"
        stock_adj_url = stock_url.replace("adjusted=false", "adjusted=true")

        unadjusted_data, adjusted_data = await asyncio.gather(
            fetch_data(session, stock_url, semaphore),
            fetch_data(session, stock_adj_url, semaphore)
        )

    return unadjusted_data, adjusted_data


async def get_concurrent_option_data(option_data):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        tasks = []

        for option_ticker, expiry in option_data:
            start_date_option = expiry - timedelta(days=28)
            option_url = f"https://api.polygon.io/v2/aggs/ticker/{option_ticker}/range/1/day/{start_date_option.strftime('%Y-%m-%d')}/{expiry.strftime('%Y-%m-%d')}?adjusted=false&sort=asc&limit=120&apiKey={POLYGON_API_KEY}"
            tasks.append(fetch_data(session, option_url, semaphore))

        return await asyncio.gather(*tasks)


@main.route('/populate_tickers', methods=['POST'])
async def populate_tickers():
    tickers = request.json.get('tickers', [])
    start_date = datetime.strptime(request.json.get(
        'start_date', (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')), '%Y-%m-%d')
    end_date = datetime.strptime(request.json.get(
        'end_date', datetime.now().strftime('%Y-%m-%d')), '%Y-%m-%d')

    nyse = mcal.get_calendar('NYSE')
    valid_days = nyse.valid_days(
        start_date=start_date, end_date=end_date).date.tolist()
    extended_end_date = end_date + timedelta(days=28)
    extended_valid_days = nyse.valid_days(
        start_date=start_date, end_date=extended_end_date).date.tolist()

    securities_to_add = []
    stocks_to_add = []
    options_to_add = []

    securities_to_insert = set()
    stocks_to_insert = set()
    options_to_insert = set()

    total_operations = len(tickers) * len(valid_days)
    completed_operations = 0

    all_existing_securities = {x[0] for x in db.session.query(
        Securities.ticker).filter(Securities.ticker.in_(tickers)).all()}
    all_existing_stocks = {x[0] for x in db.session.query(
        Stocks.ticker).filter(Stocks.ticker.in_(tickers)).all()}
    all_existing_options = set()

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        for ticker in tickers:
            tasks = []
            response = requests.get(
                f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={POLYGON_API_KEY}").json()
            if response['status'] != 'OK':
                continue
            security_name = response["results"]["name"]
            gics = response["results"]["sic_description"] if "sic_description" in response["results"] else None

            if ticker not in all_existing_securities and ticker not in securities_to_insert:
                security = Securities(
                    ticker=ticker, security_name=security_name, security_type='stk')
                securities_to_add.append(security)
                securities_to_insert.add(ticker)

            if ticker not in all_existing_stocks and ticker not in stocks_to_insert:
                stock = Stocks(ticker=ticker, gics=gics)
                stocks_to_add.append(stock)
                stocks_to_insert.add(ticker)

            for asof_date in valid_days:
                if asof_date == get_last_trading_day_of_week(asof_date, valid_days):
                    expiry_dates = get_expiry_dates(
                        asof_date, extended_valid_days)
                    for expiry_date in expiry_dates:
                        params = {
                            'underlying_ticker': ticker,
                            'expiration_date': expiry_date.strftime('%Y-%m-%d'),
                            'as_of': asof_date.strftime('%Y-%m-%d'),
                            'limit': 1000,
                            'sort': 'strike_price',
                            'apiKey': POLYGON_API_KEY
                        }
                        url = BASE_URL + "?" + \
                            "&".join(f"{key}={value}" for key,
                                     value in params.items())
                        task = fetch_data(session, url, semaphore)
                        tasks.append(task)

                completed_operations += 1
                progress = (completed_operations / total_operations) * 100
                app.socketio.emit('ticker_progress', {'progress': progress})

            results = await asyncio.gather(*tasks)

            # Check for errors in results
            if any('error' in result for result in results):
                print([result['error']
                      for result in results if 'error' in result])
                continue

            option_tickers_from_results = set()
            for data in results:
                if data.get('status') == 'OK':
                    option_tickers = {option_data['ticker']
                                      for option_data in data['results']}
                    option_tickers_from_results.update(option_tickers)

            existing_option_tickers = set()
            # Batch processing for existing option tickers checks
            for i in range(0, len(option_tickers_from_results), BATCH_SIZE):
                batch_tickers = list(option_tickers_from_results)[
                    i:i+BATCH_SIZE]
                existing_option_tickers.update([ticker[0] for ticker in db.session.query(
                    Options.ticker).filter(Options.ticker.in_(batch_tickers)).all()])

            all_existing_options.update(existing_option_tickers)

            # Process results for this ticker
            for data in results:
                if data.get('status') == 'OK':
                    for option_data in data['results']:
                        option_ticker = option_data['ticker']
                        if option_ticker not in all_existing_options and option_ticker not in options_to_insert:
                            security = Securities(
                                ticker=option_ticker, security_name=option_ticker, security_type='opt', underlying_ticker=ticker)
                            securities_to_add.append(security)
                            securities_to_insert.add(option_ticker)

                            option = Options(
                                ticker=option_ticker,
                                option_type=option_data['contract_type'],
                                option_style=option_data['exercise_style'],
                                expiry=option_data['expiration_date'],
                                strike=option_data['strike_price']
                            )
                            options_to_add.append(option)
                            options_to_insert.add(option_ticker)

            securities_dicts = [instance_to_dict(
                sec) for sec in securities_to_add]
            stocks_dicts = [instance_to_dict(stk) for stk in stocks_to_add]
            options_dicts = [instance_to_dict(opt) for opt in options_to_add]

            try:
                db.session.bulk_insert_mappings(Securities, securities_dicts)
                db.session.bulk_insert_mappings(Stocks, stocks_dicts)
                db.session.bulk_insert_mappings(Options, options_dicts)
                db.session.commit()
            except IntegrityError:
                db.session.rollback()
                return jsonify({"message": f"Data insertion failed for ticker {ticker} due to integrity constraints!"})

            securities_to_add.clear()
            stocks_to_add.clear()
            options_to_add.clear()

    return jsonify({"message": "Data populated successfully!"})


@main.route('/populate_prices', methods=['POST'])
async def populate_prices():
    tickers = request.json.get('tickers', [])
    start_date = datetime.strptime(request.json.get(
        'start_date', (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')), '%Y-%m-%d')
    end_date = datetime.strptime(request.json.get(
        'end_date', datetime.now().strftime('%Y-%m-%d')), '%Y-%m-%d')
    extended_end_date = end_date + timedelta(days=28)

    # Calculate average options per stock for progress
    # Filter options between start_date and extended_end_date
    options_counts = (
        db.session.query(Securities.underlying_ticker,
                         db.func.count(Options.ticker))
        .join(Options, Options.ticker == Securities.ticker)
        .filter(Securities.underlying_ticker.in_(tickers), Options.expiry.between(start_date, extended_end_date))
        .group_by(Securities.underlying_ticker)
        .all()
    )
    options_counts_dict = dict(options_counts)
    total_options = sum(options_counts_dict.get(ticker, 0)
                        for ticker in tickers)
    percent_per_option = 100 / total_options
    completed_operations = 0

    # Cache existing data records for stocks and options
    # existing_prices = set([(price.ticker, price.exch_time) for price in db.session.query(HistPrice1D.ticker, HistPrice1D.exch_time).all()])

    for ticker in tickers:
        prices_to_add = []

        # Fetch stock historical data
        stock_response, stock_adj_response = await get_concurrent_stock_data(ticker, start_date, end_date)

        # Initializing existing_prices with the existing prices of the stock ticker
        existing_prices = set([(price.ticker, price.exch_time) for price in db.session.query(
            HistPrice1D.ticker, HistPrice1D.exch_time).filter(HistPrice1D.ticker == ticker).all()])

        for record, adj_record in zip(stock_response.get('results', []), stock_adj_response.get('results', [])):
            t = datetime.fromtimestamp(
                record['t'] / 1000).astimezone(timezone('US/Eastern')).replace(tzinfo=None)
            if (ticker, t) not in existing_prices:
                price = HistPrice1D(
                    ticker=ticker,
                    exch_time=t,
                    open=record['o'],
                    high=record['h'],
                    low=record['l'],
                    close=record['c'],
                    volume=record['v'],
                    open_adj=adj_record['o'],
                    high_adj=adj_record['h'],
                    low_adj=adj_record['l'],
                    close_adj=adj_record['c']
                )
                prices_to_add.append(price)

        # Get option tickers and their expiry dates with the stock as the underlying security
        option_data = [
            (option.ticker, option.expiry) for option in
            db.session.query(Options.ticker, Options.expiry)
            .join(Securities, Securities.ticker == Options.ticker)
            .filter(Securities.underlying_ticker == ticker, Options.expiry.between(start_date, extended_end_date))
            .all()
        ]

        # Batch processing for existing price checks
        for i in range(0, len(option_data), BATCH_SIZE):
            batch_tickers = [item[0] for item in option_data[i:i+BATCH_SIZE]]
            existing_prices.update([(price.ticker, price.exch_time) for price in db.session.query(
                HistPrice1D.ticker, HistPrice1D.exch_time).filter(HistPrice1D.ticker.in_(batch_tickers)).all()])

        # Fetch option data concurrently
        option_responses = await get_concurrent_option_data(option_data)

        for i, option_response in enumerate(option_responses):
            if 'error' in option_response:
                # or handle the error in a way that's appropriate for your application
                print(option_response['error'])
                continue

            option_ticker, expiry = option_data[i]

            for record in option_response.get('results', []):
                t = datetime.fromtimestamp(
                    record['t'] / 1000).astimezone(timezone('US/Eastern')).replace(tzinfo=None)
                if (option_ticker, t) not in existing_prices:
                    price = HistPrice1D(
                        ticker=option_ticker,
                        exch_time=t,
                        open=record['o'],
                        high=record['h'],
                        low=record['l'],
                        close=record['c'],
                        volume=record['v']
                    )
                    prices_to_add.append(price)

            # Update progress for options
            completed_operations += 1
            progress = completed_operations * percent_per_option
            app.socketio.emit('price_progress', {'progress': progress})

        # Bulk insert
        prices_dicts = [instance_to_dict(price) for price in prices_to_add]

        try:
            db.session.bulk_insert_mappings(HistPrice1D, prices_dicts)
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            return jsonify({"message": f"Data insertion failed for {ticker} due to integrity constraints!"})

    return jsonify({"message": "Price data populated successfully!"})


@main.route('/populate_iv_surfs', methods=['POST'])
def populate_iv_surfs():
    tickers = request.json.get('tickers', [])
    start_date = datetime.strptime(request.json.get(
        'start_date', (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')), '%Y-%m-%d')
    end_date = datetime.strptime(request.json.get(
        'end_date', datetime.now().strftime('%Y-%m-%d')), '%Y-%m-%d')

    # Get valid trading days between start_date and end_date
    nyse = mcal.get_calendar('NYSE')
    valid_days = nyse.valid_days(
        start_date=start_date, end_date=end_date).date.tolist()

    total_operations = len(tickers) * len(valid_days)
    operations_completed = 0

    for ticker in tickers:
        # Check which valid_days already have data for the ticker
        existing_dates = [iv_surf.exch_time.date()
                          for iv_surf in IvSurf.query.filter_by(ticker=ticker).all()]

        days_to_update = [
            day for day in valid_days if day not in existing_dates]
        days_to_update_as_datetime_set = set(
            [datetime.combine(day, datetime.min.time()) for day in days_to_update])

        iv_surf_data_to_insert = []
        for day in valid_days:
            current_datetime = datetime.combine(day, datetime.min.time())
            if current_datetime in days_to_update_as_datetime_set:
                try:
                    iv_surface = get_iv_surf(ticker, day.strftime('%Y-%m-%d'))
                    serialized_iv_surf = pickle.dumps(iv_surface)
                    iv_surf_data_to_insert.append({
                        'ticker': ticker,
                        'exch_time': current_datetime,
                        'iv_surf_data': serialized_iv_surf
                    })
                except Exception as e:
                    # Log the error and continue with the next date
                    app.logger.error(
                        f"Error getting IV surface for ticker {ticker} on date {current_datetime}: {e}")

            # Update the operations completed count and emit progress
            operations_completed += 1
            progress = (operations_completed / total_operations) * 100
            app.socketio.emit('iv_surf_progress', {'progress': progress})

        if iv_surf_data_to_insert:
            db.session.bulk_insert_mappings(IvSurf, iv_surf_data_to_insert)
            db.session.commit()

    return jsonify({'message': 'IV Surface data updated successfully!'})


@main.route('/get_iv_surfs', methods=['POST'])
def get_iv_surfs():
    tickers = request.json.get('tickers', [])
    start_date = datetime.strptime(request.json.get(
        'start_date', (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')), '%Y-%m-%d')
    end_date = datetime.strptime(request.json.get(
        'end_date', datetime.now().strftime('%Y-%m-%d')), '%Y-%m-%d')

    # Query all the required data for the given tickers within the specified date range in a single query
    entries = IvSurf.query.filter(IvSurf.ticker.in_(
        tickers), IvSurf.exch_time.between(start_date, end_date)).all()

    data = {ticker: {} for ticker in tickers}
    for entry in entries:
        # Convert the binary data to a base64 encoded string
        encoded_iv_surf_data = base64.b64encode(
            entry.iv_surf_data).decode('utf-8')
        date_str = entry.exch_time.strftime('%Y-%m-%d')
        if entry.ticker in data:
            data[entry.ticker][date_str] = encoded_iv_surf_data

    return jsonify(data)


@main.route('/get_stock_price', methods=['POST'])
def get_stock_price():
    tickers = request.json.get('tickers', [])
    start_date = datetime.strptime(request.json.get(
        'start_date', (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')), '%Y-%m-%d')
    end_date = datetime.strptime(request.json.get(
        'end_date', datetime.now().strftime('%Y-%m-%d')), '%Y-%m-%d')

    data = {}

    prices = HistPrice1D.query.filter(
        HistPrice1D.ticker.in_(tickers),
        HistPrice1D.exch_time.between(start_date, end_date)
    ).all()

    for price in prices:
        ticker = price.ticker
        date_str = price.exch_time.strftime('%Y-%m-%d')
        if ticker not in data:
            data[ticker] = {}
        data[ticker][date_str] = {
            'open': price.open,
            'high': price.high,
            'low': price.low,
            'close': price.close,
            'open_adj': price.open_adj,
            'high_adj': price.high_adj,
            'low_adj': price.low_adj,
            'close_adj': price.close_adj,
            'volume': price.volume
        }

    return jsonify(data)


@main.route('/calculate_option_price', methods=['POST'])
def calculate_option_price():
    data = request.get_json()
    serialized_iv_surf = data['iv_surf']
    iv_surf = pickle.loads(base64.b64decode(serialized_iv_surf))
    trade_date = data['trade_date']
    price = data['price']
    option_type = data['option_type']
    expiry_date = data['expiry_date']
    strike = data['strike']
    option_price = get_option_price(
        trade_date, iv_surf, price, option_type, expiry_date, strike)
    return jsonify({'option_price': option_price})


@main.route('/get_iv_surface_data', methods=['POST'])
def get_iv_surface_data():
    data = request.get_json()
    serialized_iv_surf = data['iv_surf']
    iv_surf = pickle.loads(base64.b64decode(serialized_iv_surf))
    strike_ratios = np.linspace(0.8, 1.2, 100)
    T_range = np.linspace(1/365, 28/365, 100)
    X, Y = np.meshgrid(strike_ratios, T_range)
    Z = iv_surf(X, Y)
    # Replace NaN values with None (which will become null in JSON)
    Z = np.where(np.isnan(Z), None, Z)
    return jsonify({'X': X.tolist(), 'Y': Y.tolist(), 'Z': Z.tolist()})


async def fetch_news(session, base_url, semaphore):
    all_news = []
    while base_url:
        async with semaphore:
            for attempt in range(RETRY_ATTEMPTS):
                try:
                    async with session.get(base_url, timeout=timeout) as response:
                        if response.status != 200:
                            raise aiohttp.ClientResponseError(
                                response, message=f"Received status {response.status}")
                        data = await response.json()
                        all_news.extend(data['results'])
                        base_url = data.get(
                            'next_url') + f"&apiKey={POLYGON_API_KEY}" if 'next_url' in data else None
                        break  # Break the attempt loop if successful
                except (aiohttp.ClientError, aiohttp.ClientResponseError, asyncio.TimeoutError) as e:
                    if attempt < RETRY_ATTEMPTS - 1:
                        await asyncio.sleep(1)  # wait a second before retrying
                        continue
                    else:
                        return {'error': str(e), 'url': base_url}
    return all_news


# Function to get sentiment score using GPT-3.5 turbo model
async def get_sentiment_score(content, ticker, max_retries=5):
    async_openai_client = AsyncOpenAI(
        api_key=OPENAI_API_KEY, timeout=6, max_retries=3)

    # Construct the prompt for the sentiment analysis
    prompt = f"Calculate the integer sentiment score for the following news article related to the stock ticker {ticker} on a scale from 1 to 100, where 1 is most negative and 100 is most positive. IN YOUR RESPONSE, PLEASE PRODUCE THE SCORE ONLY:\n\n{content}"

    attempt = 0
    while attempt < max_retries:
        try:
            # Call the OpenAI API
            response = await async_openai_client.chat.completions.create(
                messages=[
                    {
                        "role": "system",
                        "content": "You are a sentiment analysis AI."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                model="gpt-4-1106-preview",
            )

            # Extract the sentiment score from the response
            sentiment_text = response.choices[0].message.content.strip()
            # Use regular expression to find an integer in the response
            match = re.search(r'\b\d+\b', sentiment_text)
            if match:
                # Convert the matched number to an integer
                sentiment_score = int(match.group())
                # Ensure the score is within the expected range
                if 1 <= sentiment_score <= 100:
                    print(f"Sentiment score: {sentiment_score}")
                    return sentiment_score
            # If no integer is found or the score is out of range, handle the error
            raise ValueError

        except openai.RateLimitError as e:
            attempt += 1
            wait_time = min(3 ** attempt, 60)
            print(
                f"Rate limit reached: {e}, waiting for {wait_time} seconds before retrying...")
            await asyncio.sleep(wait_time)
        except ValueError:
            # If the sentiment score could not be parsed, return None
            print(
                f"Could not parse a valid sentiment score from the response for ticker {ticker}: {sentiment_text}")
            break
        except openai.APIError as e:
            # Handle API error here, e.g. retry or log
            print(f"OpenAI API returned an API Error: {e}")
            break
        except openai.APIConnectionError as e:
            # Handle connection error here
            print(f"Failed to connect to OpenAI API: {e}")
            break
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break

    # If all retries failed, return a default sentiment score or raise an exception
    return None  # or raise an exception


async def worker(session, queue, semaphore, contents, progress_data):
    while True:
        article = await queue.get()
        content = await get_content(session, article['article_url'], semaphore)
        # Append a tuple with the ticker, article, and content (or None)
        contents.append((article, content))
        queue.task_done()

        # Update progress
        progress_data['articles_processed'] += 1
        progress = (progress_data['articles_processed'] /
                    progress_data['total_articles']) * 100
        print(f"Progress: {progress}")
        app.socketio.emit('news_update_progress', {'progress': progress})


async def get_content(session, url, semaphore):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    async with semaphore:
        for attempt in range(RETRY_ATTEMPTS):
            try:
                async with session.get(url, headers=headers, timeout=timeout) as response:
                    if response.status != 200:
                        raise aiohttp.ClientResponseError(
                            request_info=response.request_info,
                            history=(),
                            status=response.status,
                            message=f"Received status {response.status}",
                            headers=response.headers
                        )
                    content = await response.text()
                    soup = BeautifulSoup(content, 'html.parser')
                    largest_text, tag = get_largest_text_block(soup)
                    return largest_text
            except (aiohttp.ClientError, aiohttp.ClientResponseError, asyncio.TimeoutError) as e:
                if attempt < RETRY_ATTEMPTS - 1:
                    # Exponential backoff
                    backoff_time = math.pow(2, attempt) * 0.2
                    print(
                        f"Attempt {attempt + 1}: Waiting {backoff_time} seconds before retrying for URL {url}")
                    await asyncio.sleep(backoff_time)
                    continue
                else:
                    # Log the error, return None or handle it as appropriate
                    print(f"Failed to fetch content for URL {url}: {e}")
                    return None

MAX_WORKERS = 50


@main.route('/populate_news', methods=['POST'])
async def populate_news():
    data = request.json
    tickers = data.get('tickers', [])
    start_date = datetime.strptime(request.json.get(
        'start_date', (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')), '%Y-%m-%d')
    end_date = datetime.strptime(request.json.get(
        'end_date', datetime.now().strftime('%Y-%m-%d')), '%Y-%m-%d')

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for ticker in tickers:
            url = f"https://api.polygon.io/v2/reference/news?ticker={ticker}&published_utc.gte={start_date.strftime('%Y-%m-%d')}&published_utc.lte={end_date.strftime('%Y-%m-%d')}&limit=50&apiKey={POLYGON_API_KEY}"
            tasks.append(fetch_news(session, url, semaphore))

        responses = await asyncio.gather(*tasks)

        total_articles = sum(len(articles) for articles in responses)
        progress_data = {'articles_processed': 0,
                         'total_articles': total_articles}

        # For each article_url, fetch the content.
        # This list will now store tuples of (ticker, article, content)
        contents = []
        queue = asyncio.Queue()

        for response in responses:
            for article in response:
                # Put a tuple of ticker and article
                await queue.put(article)

        workers = [asyncio.create_task(
            worker(session, queue, semaphore, contents, progress_data)) for _ in range(MAX_WORKERS)]

        await queue.join()  # Wait until the queue is fully processed

        for w in workers:
            w.cancel()

        # Collect all unique article IDs and tickers from the responses
        all_article_ids = {article['id']
                           for response in responses for article in response}
        all_tickers = {ticker for ticker in tickers}

        # Query the database in bulk for existing articles and tickers
        existing_articles = News.query.filter(
            News.id.in_(all_article_ids)).all()
        existing_tickers = Securities.query.filter(
            Securities.ticker.in_(all_tickers)).all()

        # Create sets of existing article IDs for efficient look-up
        existing_article_ids = {article.id for article in existing_articles}

        # Create a dictionary to map ticker symbols to Securities objects
        ticker_to_security = {
            ticker.ticker: ticker for ticker in existing_tickers}

        # Insert the news data into the database
        for article, content in contents:
            if article['id'] not in existing_article_ids:
                # Create a new News entry if the article does not exist
                published_utc = pytz.utc.localize(datetime.strptime(
                    article['published_utc'], '%Y-%m-%dT%H:%M:%SZ'))
                exch_time = published_utc.astimezone(timezone(
                    'US/Eastern')).replace(tzinfo=None).replace(hour=0, minute=0, second=0, microsecond=0)

                news_entry = News(
                    id=article['id'],
                    exch_time=exch_time,
                    published_utc=published_utc.replace(tzinfo=None),
                    publisher_name=article['publisher']['name'],
                    title=article['title'],
                    author=article['author'],
                    article_url=article['article_url'],
                    content=content
                )
                db.session.add(news_entry)

                # Add related tickers to the news_entry
                for ticker in set(article['tickers']):
                    security_entry = ticker_to_security.get(ticker)
                    if security_entry:
                        # Get the sentiment score for the article with respect to the ticker
                        print(f"Getting sentiment score for {ticker}...")
                        sentiment_score = await get_sentiment_score(content, ticker)

                        # Create a NewsSecurities association object
                        news_security_entry = NewsSecurities(
                            news_id=article['id'],
                            ticker=security_entry.ticker,
                            sentiment=sentiment_score
                        )
                        db.session.add(news_security_entry)

                # Update the existing_article_ids set to include the new article
                existing_article_ids.add(article['id'])

        db.session.commit()

    return jsonify({'status': 'success', 'message': 'News data updated.'})


@main.route('/update/tickers', methods=['GET'])
def update_tickers():
    return render_template('update_tickers.html')


@main.route('/update/prices', methods=['GET'])
def update_prices():
    return render_template('update_prices.html')


@main.route('/update/iv_surfs', methods=['GET'])
def update_iv_surfs():
    return render_template('update_iv_surf.html')


@main.route('/view/price', methods=['GET'])
def view_price():
    return render_template('view_price.html')


@main.route('/update/news', methods=['GET'])
def update_news():
    return render_template('update_news.html')


@main.route('/view/news', methods=['GET'])
def view_news():
    return render_template('view_news.html')


@main.route('/get_news', methods=['GET'])
def get_news():
    tickers = request.args.get('tickers')
    ticker_list = tickers.split(',')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    news_articles = db.session.query(
        News, NewsSecurities.sentiment, NewsSecurities.ticker
    ).join(
        NewsSecurities, News.id == NewsSecurities.news_id
    ).filter(
        NewsSecurities.ticker.in_(ticker_list),
        News.exch_time.between(start_date, end_date)
    ).order_by(
        NewsSecurities.ticker, News.exch_time
    ).all()

    # Organize news articles by ticker
    organized_articles = {}
    for article in news_articles:
        ticker = article.ticker
        if ticker not in organized_articles:
            organized_articles[ticker] = []
        organized_articles[ticker].append({
            'title': article.News.title,
            'author': article.News.author,
            'url': article.News.article_url,
            'sentiment': article.sentiment,
            'exch_time': article.News.exch_time.isoformat()
        })

    return jsonify(organized_articles)
