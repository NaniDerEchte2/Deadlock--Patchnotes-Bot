from __future__ import annotations

from bs4 import BeautifulSoup
from urllib.request import urlopen
import ssl
from typing import Dict, Optional

ssl_context = ssl.create_default_context()


def process(url: str) -> Optional[Dict[str, str]]:
    html = urlopen(url, context=ssl_context)
    soup = BeautifulSoup(html, features="html.parser")

    content_div = soup.find("div", attrs="bbWrapper")
    content = content_div.get_text("\n") if content_div else None

    title_el = soup.select_one("h1.p-title-value")
    title = title_el.get_text(strip=True) if title_el else None

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
