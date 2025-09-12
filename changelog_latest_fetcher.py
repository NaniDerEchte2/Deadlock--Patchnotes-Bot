import requests, time
from bs4 import BeautifulSoup

FORUM_URL = "https://forums.playdeadlock.com/forums/changelog.10/"


def get_timestamp(soup):
    "gets a timestamp from a time tag found in given soup"

    time_tag = soup.find('time', {'data-timestamp': True})
    timestamp = time_tag.get('data-timestamp')
    return timestamp


def check_latest():
    "Check the link of the latest changelog thread"

    response = requests.get(FORUM_URL, timeout=10)
    soup = BeautifulSoup(response.text, "html.parser")

    div_entries = soup.find_all("div", class_="structItemContainer-group js-threadList")
    thread_list = []

    for i, thread in enumerate(div_entries):
        thread_list.append(thread.find("div", class_ = "structItem-cell structItem-cell--main"))

    thread = thread_list[0]

    thread_display = thread.find('div', class_ = "structItem-title")
    thread_link = thread_display.find("a").get('href')

    return thread_link