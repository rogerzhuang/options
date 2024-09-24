import requests
from bs4 import BeautifulSoup
import json
from news import get_largest_text_block
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def get_article_content(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # First, get the redirect URL
    response = requests.get(url, headers=headers, allow_redirects=False)
    if response.status_code == 302:
        redirect_url = response.headers['Location']
    else:
        logger.error(
            f"Error: Unexpected status code {response.status_code} for URL: {url}")
        return None

    # Now, fetch the actual article
    response = requests.get(redirect_url, headers=headers)
    if response.status_code != 200:
        logger.error(f"Error fetching the webpage: {redirect_url}")
        return None

    soup = BeautifulSoup(response.content, 'html.parser')
    largest_text, tag = get_largest_text_block(soup)

    if largest_text:
        return largest_text.strip()
    else:
        logger.error(f"Error extracting the content from: {redirect_url}")
        return None


def fetch_articles(api_url, source):
    response = requests.get(api_url)
    if response.status_code != 200:
        logger.error(f"Error fetching data from API: {response.status_code}")
        return

    articles = json.loads(response.text)
    for article in articles:
        if article['source'] != source:
            continue
        logger.info(f"Processing article: {article['headline']}")
        content = get_article_content(article['url'])
        if content:
            logger.info(f"Content:\n{content}\n{'='*50}\n")
        else:
            logger.warning(
                f"Failed to retrieve content for: {article['url']}\n{'='*50}\n")


# Your API URL
api_url = "https://finnhub.io/api/v1/company-news?symbol=NIO&from=2024-08-08&to=2024-08-09&token=cqm4789r01qoqqs7up3gcqm4789r01qoqqs7up40"
source = "Yahoo"

fetch_articles(api_url, source)
