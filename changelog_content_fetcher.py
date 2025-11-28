from __future__ import annotations

from bs4 import BeautifulSoup
from urllib.request import urlopen
import ssl
import re
from typing import Dict, Optional

ssl_context = ssl.create_default_context()


def _extract_post_id(source: str | None) -> str | None:
    if not source:
        return None
    # Threads liefern absolute und relative Pfade in verschiedenen Formen; hier die Post-ID extrahieren
    match = re.search("/posts/(\\d+)/", source)
    if match:
        return match.group(1)
    match = re.search("/post-(\\d+)", source)
    if match:
        return match.group(1)
    match = re.search("#post-(\\d+)", source)
    if match:
        return match.group(1)
    return None


def _select_post(soup: BeautifulSoup, post_id: str | None):
    posts = soup.select("article.message")
    if not posts:
        return None
    if post_id:
        for post in posts:
            raw_id = post.get("data-content") or post.get("id") or ""
            if post_id in str(raw_id):
                return post
    return posts[0]


def process(url: str) -> Optional[Dict[str, str]]:
    with urlopen(url, context=ssl_context) as html:
        soup = BeautifulSoup(html, features="html.parser")

    post_id = _extract_post_id(url)
    target_post = _select_post(soup, post_id)

    content_div = target_post.find("div", class_="bbWrapper") if target_post else None
    if not content_div:
        content_div = soup.find("div", attrs="bbWrapper")
    content = content_div.get_text("\n") if content_div else None

    title_el = soup.select_one("h1.p-title-value")
    title = title_el.get_text(strip=True) if title_el else None

    time_el = target_post.select_one("time[data-timestamp]") if target_post else None
    if not time_el:
        time_el = soup.select_one("time[data-timestamp]")

    posted_at = None
    if time_el:
        posted_at = (
            time_el.get("datetime")
            or time_el.get("data-datestring")
            or time_el.get("data-time")
            or time_el.get("data-timestamp")
        )

    if not content:
        return None

    return {
        "url": url,
        "title": title,
        "posted_at": posted_at,
        "content": content,
    }


if __name__ == "__main__":
    sample = process("https://forums.playdeadlock.com/threads/07-29-2025-update.72760/")
    print(type(sample))
