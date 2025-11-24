import requests
from bs4 import BeautifulSoup

FORUM_URL = "https://forums.playdeadlock.com/forums/changelog.10/"
FORUM_BASE_URL = "https://forums.playdeadlock.com"


def _extract_post_id(article):
    if not article:
        return None
    raw_id = article.get("data-content") or article.get("id") or ""
    raw_id = str(raw_id).replace("js-post-", "").replace("post-", "")
    return raw_id or None


def check_latest():
    "Check the latest changelog thread and newest post inside it"

    response = requests.get(FORUM_URL, timeout=10)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    div_entries = soup.find_all("div", class_="structItemContainer-group js-threadList")
    thread_list = []

    for thread in div_entries:
        thread_list.append(thread.find("div", class_="structItem-cell structItem-cell--main"))

    thread = thread_list[0] if thread_list else None
    thread_display = thread.find('div', class_="structItem-title") if thread else None
    thread_link_tag = thread_display.find("a") if thread_display else None
    thread_link = thread_link_tag.get('href') if thread_link_tag else None
    thread_url = (
        thread_link
        if thread_link and thread_link.startswith("http")
        else f"{FORUM_BASE_URL}{thread_link}" if thread_link else None
    )

    latest_post_id = None
    latest_post_url = None
    if thread_url:
        thread_response = requests.get(thread_url, timeout=10)
        thread_response.raise_for_status()
        thread_soup = BeautifulSoup(thread_response.text, "html.parser")
        posts = thread_soup.select("article.message")
        latest_post = posts[-1] if posts else None
        latest_post_id = _extract_post_id(latest_post)
        latest_post_url = f"{FORUM_BASE_URL}/posts/{latest_post_id}/" if latest_post_id else thread_url
        post_urls = []
        for p in posts:
            pid = _extract_post_id(p)
            if pid:
                post_urls.append(f"{FORUM_BASE_URL}/posts/{pid}/")
    else:
        post_urls = []

    return {
        "thread_link": thread_link,
        "thread_url": thread_url,
        "latest_post_id": latest_post_id,
        "latest_post_url": latest_post_url,
        "post_urls": post_urls,
    }
