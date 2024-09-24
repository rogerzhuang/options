import requests
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)

def get_largest_text_block(soup):
    ignored_classes = ["footer", "sidebar", "header", "ad", "menu"]
    all_texts = []

    # find_all(True) retrieves all tags in the soup
    for tag in soup.find_all(True):
        if tag.name in ['script', 'style']:  # ignore script and style tags
            continue

        # filter out unwanted classes
        if any(cls in tag.get('class', []) for cls in ignored_classes):
            continue

        # filter out unwanted ids
        if any(id_str in tag.get('id', '') for id_str in ignored_classes):
            continue

        # Calculate combined text for child p tags
        child_p_tags = tag.find_all('p')
        combined_text = " ".join(p.get_text(
            separator=" ", strip=True) for p in child_p_tags)

        # Collect all text blocks
        all_texts.append((len(combined_text), combined_text, tag))

    # Sort the texts by their length (longest first)
    all_texts.sort(key=lambda x: x[0], reverse=True)

    if all_texts:
        # Return the longest text and its corresponding tag
        return all_texts[0][1], all_texts[0][2]
    return None, None


if __name__ == '__main__':
    URL = 'https://finance.yahoo.com/news/nio-inc-nio-stock-drops-214509624.html'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    response = requests.get(URL, headers=headers)
    if response.status_code != 200:
        logger.error("Error fetching the webpage.")
    else:
        soup = BeautifulSoup(response.content, 'html.parser')

        # ... after fetching the content and creating the soup object

        largest_text, tag = get_largest_text_block(soup)
        if largest_text:
            logger.info(largest_text)
        else:
            logger.error("Error extracting the content.")
