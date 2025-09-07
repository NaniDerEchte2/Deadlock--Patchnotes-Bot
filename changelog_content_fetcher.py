from bs4 import BeautifulSoup
from urllib.request import urlopen
from datetime import datetime
import ssl
import re

ssl_context = ssl.create_default_context()

def process(url):
    html = urlopen(url, context=ssl_context)

    soup = BeautifulSoup(html, features="html.parser")

    div = soup.find("div", attrs="bbWrapper")
    if div:
        return div.text

if __name__ == "__main__":
    print(type(process("https://forums.playdeadlock.com/threads/07-29-2025-update.72760/")))