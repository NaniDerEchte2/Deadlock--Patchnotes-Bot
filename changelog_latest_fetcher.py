from __future__ import annotations

import re

import requests
from bs4 import BeautifulSoup

FORUM_URL = "https://forums.playdeadlock.com/forums/changelog.10/"
FORUM_BASE_URL = "https://forums.playdeadlock.com"
STEAM_APP_ID = 1422450
STEAM_NEWS_API_URL = "https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/"
STEAM_NEWS_FEED = "steam_community_announcements"
STEAM_NEWS_FETCH_COUNT = 25
STEAM_PATCH_HISTORY_LIMIT = 6
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
    )
}
_STEAM_TITLE_PATCH_HINTS = ("update", "patch", "hotfix", "balance")
_STEAM_BODY_SECTION_HINTS = ("general", "items", "heroes", "hero", "map", "ui", "audio", "misc")


def _extract_post_id(article):
    if not article:
        return None
    raw_id = article.get("data-content") or article.get("id") or ""
    raw_id = str(raw_id).replace("js-post-", "").replace("post-", "")
    return raw_id or None


def _steam_patch_score(item: dict) -> int:
    title = str(item.get("title") or "").strip().lower()
    body = str(item.get("contents") or "")
    body_lower = body.lower()
    score = 0

    if any(hint in title for hint in _STEAM_TITLE_PATCH_HINTS):
        score += 5

    bullet_count = len(re.findall(r"\[p\]\s*-\s+", body, flags=re.IGNORECASE))
    if bullet_count >= 25:
        score += 5
    elif bullet_count >= 8:
        score += 4
    elif bullet_count >= 3:
        score += 2

    section_count = 0
    for hint in _STEAM_BODY_SECTION_HINTS:
        if f"[ {hint} ]" in body_lower or f"\\[ {hint} ]" in body_lower:
            section_count += 1
    if section_count >= 2:
        score += 2

    return score


def _resolve_redirect_url(url: str | None) -> str | None:
    if not url:
        return None
    response = requests.head(
        url,
        headers=REQUEST_HEADERS,
        timeout=10,
        allow_redirects=True,
    )
    response.raise_for_status()
    return response.url or url


def _check_latest_steam():
    response = requests.get(
        STEAM_NEWS_API_URL,
        params={
            "appid": STEAM_APP_ID,
            "count": STEAM_NEWS_FETCH_COUNT,
            "maxlength": 0,
            "feeds": STEAM_NEWS_FEED,
        },
        headers=REQUEST_HEADERS,
        timeout=10,
    )
    response.raise_for_status()
    newsitems = response.json().get("appnews", {}).get("newsitems", [])

    raw_patch_candidates = []
    for item in newsitems:
        if _steam_patch_score(item) < 5:
            continue
        raw_patch_candidates.append((int(item.get("date") or 0), item.get("url")))
        if len(raw_patch_candidates) >= STEAM_PATCH_HISTORY_LIMIT:
            break

    patch_candidates = []
    seen_urls = set()
    for published_at, raw_url in raw_patch_candidates:
        resolved_url = _resolve_redirect_url(raw_url) or raw_url
        if not resolved_url or resolved_url in seen_urls:
            continue
        seen_urls.add(resolved_url)
        patch_candidates.append((published_at, resolved_url))

    if not patch_candidates:
        return None

    patch_candidates.sort(key=lambda item: item[0])
    post_urls = [url for _, url in patch_candidates]
    latest_post_url = post_urls[-1]
    return {
        "source": "steam",
        "thread_url": latest_post_url,
        "latest_post_url": latest_post_url,
        "post_urls": post_urls,
    }


def _check_latest_forum():
    response = requests.get(FORUM_URL, headers=REQUEST_HEADERS, timeout=10)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    div_entries = soup.find_all("div", class_="structItemContainer-group js-threadList")
    thread_list = []

    for thread in div_entries:
        thread_list.append(thread.find("div", class_="structItem-cell structItem-cell--main"))

    thread = thread_list[0] if thread_list else None
    thread_display = thread.find("div", class_="structItem-title") if thread else None
    thread_link_tag = thread_display.find("a") if thread_display else None
    thread_link = thread_link_tag.get("href") if thread_link_tag else None
    thread_url = (
        thread_link
        if thread_link and thread_link.startswith("http")
        else f"{FORUM_BASE_URL}{thread_link}" if thread_link else None
    )

    latest_post_id = None
    latest_post_url = None
    if thread_url:
        thread_response = requests.get(thread_url, headers=REQUEST_HEADERS, timeout=10)
        thread_response.raise_for_status()
        thread_soup = BeautifulSoup(thread_response.text, "html.parser")
        posts = thread_soup.select("article.message")
        latest_post = posts[-1] if posts else None
        latest_post_id = _extract_post_id(latest_post)
        latest_post_url = f"{FORUM_BASE_URL}/posts/{latest_post_id}/" if latest_post_id else thread_url
        post_urls = []
        for post in posts:
            post_id = _extract_post_id(post)
            if post_id:
                post_urls.append(f"{FORUM_BASE_URL}/posts/{post_id}/")
    else:
        post_urls = []

    return {
        "source": "forum",
        "thread_link": thread_link,
        "thread_url": thread_url,
        "latest_post_id": latest_post_id,
        "latest_post_url": latest_post_url,
        "post_urls": post_urls,
    }


def check_latest():
    "Check the latest changelog source and newest patch entry."

    try:
        steam_result = _check_latest_steam()
        if steam_result:
            return steam_result
    except Exception:
        pass

    return _check_latest_forum()
