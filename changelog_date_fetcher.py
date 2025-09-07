from bs4 import BeautifulSoup
from urllib.request import urlopen
from datetime import datetime
import ssl
import re

ssl_context = ssl.create_default_context()

def extract_dates(text: str):
    # Regex for format: Mar 13, 2025
    pattern = r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{1,2}, \d{4}\b"
    return re.findall(pattern, text)


def sort_dates_newest_first(dates: list[str]):
    # Convert to datetime objects for proper comparison
    parsed = [datetime.strptime(date, "%b %d, %Y") for date in dates]
    # Sort descending
    parsed.sort(reverse=True)
    # Convert back to strings
    return [dt.strftime("%b %d, %Y") for dt in parsed]

def process(url):
    html = urlopen(url, context=ssl_context)

    soup = BeautifulSoup(html, features="html.parser")

    return sort_dates_newest_first(extract_dates(soup.get_text()))