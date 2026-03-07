from __future__ import annotations

import html
import json
import re
from typing import Dict, Optional

import requests
from bs4 import BeautifulSoup

STEAM_APP_ID = 1422450
STEAM_COMMUNITY_BASE_URL = "https://steamcommunity.com"
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
    )
}


def _extract_post_id(source: str | None) -> str | None:
    if not source:
        return None
    match = re.search(r"/posts/(\d+)/", source)
    if match:
        return match.group(1)
    match = re.search(r"/post-(\d+)", source)
    if match:
        return match.group(1)
    match = re.search(r"#post-(\d+)", source)
    if match:
        return match.group(1)
    return None


def _extract_steam_event_id(source: str | None) -> str | None:
    if not source:
        return None
    for pattern in (
        r"/announcements/detail/(\d+)/?$",
        r"/news/app/\d+/view/(\d+)/?$",
    ):
        match = re.search(pattern, str(source).strip())
        if match:
            return match.group(1)
    return None


def _build_steam_detail_url(announcement_gid: str | int | None) -> str | None:
    if announcement_gid in (None, ""):
        return None
    return f"{STEAM_COMMUNITY_BASE_URL}/games/{STEAM_APP_ID}/announcements/detail/{announcement_gid}"


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


def _parse_steam_event_store(soup: BeautifulSoup) -> list[dict]:
    store = soup.find(attrs={"data-partnereventstore": True})
    if not store:
        return []
    raw = store.get("data-partnereventstore")
    if not raw:
        return []
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return []


def _select_steam_event(events: list[dict], url: str | None) -> dict | None:
    if not events:
        return None

    event_id = _extract_steam_event_id(url)
    if event_id:
        for event in events:
            announcement_body = event.get("announcement_body") or {}
            if str(announcement_body.get("gid") or "") == event_id:
                return event
            if str(event.get("gid") or "") == event_id:
                return event

    for event in events:
        if isinstance(event.get("announcement_body"), dict):
            return event
    return events[0]


def _steam_bbcode_to_text(text: str | None) -> str:
    if not text:
        return ""

    cleaned = html.unescape(str(text))
    cleaned = cleaned.replace("\r", "\n").replace("\xa0", " ")
    cleaned = cleaned.replace("\\[", "[").replace("\\]", "]")

    replacements = [
        (r"\[img\].*?\[/img\]", "\n"),
        (r"\[url=(.*?)\](.*?)\[/url\]", r"\2 (\1)"),
        (r"\[url\](.*?)\[/url\]", r"\1"),
        (r"\[(?:\/)?p\]", "\n"),
        # Convert BBCode headers to recognizable markers for section-aware splitting
        (r"\[h[12]\](.*?)\[/h[12]\]", r"\n[ \1 ]\n"),
        (r"\[h[3-6]\](.*?)\[/h[3-6]\]", r"\n**\1**\n"),
        # Fallback for unmatched/orphaned h-tags
        (r"\[(?:\/)?h[1-6]\]", "\n"),
        (r"\[(?:\/)?list(?:=[^\]]+)?\]", "\n"),
        (r"\[\*\]", "\n- "),
        (r"\[(?:\/)?(?:b|i|u|strike|spoiler|quote|code|noparse)\]", ""),
    ]
    for pattern, replacement in replacements:
        cleaned = re.sub(pattern, replacement, cleaned, flags=re.IGNORECASE | re.DOTALL)

    cleaned = re.sub(r"(?m)(^- .+)\n{2,}(?=- )", r"\1\n", cleaned)
    cleaned = re.sub(r"\n[ \t]+", "\n", cleaned)
    cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def _process_forum_page(url: str, soup: BeautifulSoup) -> Optional[Dict[str, str]]:
    post_id = _extract_post_id(url)
    target_post = _select_post(soup, post_id)

    content_div = target_post.find("div", class_="bbWrapper") if target_post else None
    if not content_div:
        content_div = soup.find("div", class_="bbWrapper")
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


def _process_steam_page(url: str, soup: BeautifulSoup) -> Optional[Dict[str, str]]:
    event = _select_steam_event(_parse_steam_event_store(soup), url)
    if not event:
        return None

    announcement_body = event.get("announcement_body") or {}
    title = announcement_body.get("headline") or event.get("event_name")
    posted_at = announcement_body.get("posttime") or event.get("rtime32_start_time")
    content = _steam_bbcode_to_text(announcement_body.get("body"))
    canonical_url = _build_steam_detail_url(announcement_body.get("gid")) or url

    if not content:
        return None

    return {
        "url": canonical_url,
        "title": title,
        "posted_at": str(posted_at) if posted_at is not None else None,
        "content": content,
    }


def process(url: str) -> Optional[Dict[str, str]]:
    response = requests.get(url, headers=REQUEST_HEADERS, timeout=20)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, features="html.parser")

    steam_result = _process_steam_page(response.url, soup)
    if steam_result:
        return steam_result

    return _process_forum_page(response.url, soup)


if __name__ == "__main__":
    sample = process("https://steamcommunity.com/games/1422450/announcements/detail/519740319207522796")
    print(type(sample))
