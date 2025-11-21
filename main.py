import dotenv
import os
import sys
from pathlib import Path
import discord
import asyncio

import changelog_content_fetcher
import changelog_latest_fetcher

import perplexity_requests

KV_NAMESPACE = "patchnotes_bot"
KV_LAST_FORUM_KEY = "last_forum_url"

DEADLOCK_ROOT = Path(os.getenv("DEADLOCK_HOME") or Path.home() / "Documents" / "Deadlock")
if str(DEADLOCK_ROOT) not in sys.path:
    sys.path.insert(0, str(DEADLOCK_ROOT))

from service import db as deadlock_db  # type: ignore

dotenv.load_dotenv()

channel_id = int(os.getenv("PATCH_CHANNEL_ID"))  # convert to int
token = os.getenv("BOT_TOKEN")
DEFAULT_CHECK_INTERVAL_SECONDS = 30
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", DEFAULT_CHECK_INTERVAL_SECONDS))
FORUM_BASE_URL = "https://forums.playdeadlock.com"

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
client = discord.Client(intents=intents)


def save_changelog_to_db(
    *,
    url: str,
    title: str | None,
    posted_at: str | None,
    raw_content: str,
    translated_content: str,
) -> None:
    if not url:
        raise ValueError("URL fehlt, kann Changelog nicht speichern.")

    # Sicherstellen, dass benötigte Tabellen/Spalten vorhanden sind
    deadlock_db.execute(
        """
        CREATE TABLE IF NOT EXISTS changelog_posts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          title TEXT NOT NULL,
          url TEXT NOT NULL,
          posted_at TEXT,
          raw_content TEXT
        )
        """
    )
    try:
        deadlock_db.execute("ALTER TABLE changelog_posts ADD COLUMN translated_content TEXT")
    except Exception as exc:
        if "duplicate column name" not in str(exc).lower():
            raise

    # Backfill in altem Legacy-Table (falls noch genutzt)
    deadlock_db.execute(
        """
        CREATE TABLE IF NOT EXISTS deadlock_changelogs(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          title TEXT,
          url TEXT,
          posted_at TEXT,
          content TEXT
        )
        """
    )

    existing = deadlock_db.query_one("SELECT id FROM changelog_posts WHERE url=?", (url,))
    if existing:
        deadlock_db.execute(
            """
            UPDATE changelog_posts
            SET title=?,
                posted_at=COALESCE(?, posted_at),
                raw_content=?,
                translated_content=?
            WHERE id=?
            """,
            (title or url, posted_at, raw_content, translated_content, existing[0]),
        )
    else:
        deadlock_db.execute(
            """
            INSERT INTO changelog_posts(title, url, posted_at, raw_content, translated_content)
            VALUES(?,?,?,?,?)
            """,
            (title or url, url, posted_at, raw_content, translated_content),
        )

    legacy = deadlock_db.query_one("SELECT id FROM deadlock_changelogs WHERE url=?", (url,))
    if legacy:
        deadlock_db.execute(
            """
            UPDATE deadlock_changelogs
            SET title=?,
                posted_at=COALESCE(?, posted_at),
                content=?
            WHERE id=?
            """,
            (title or url, posted_at, raw_content, legacy[0]),
        )
    else:
        deadlock_db.execute(
            """
            INSERT INTO deadlock_changelogs(title, url, posted_at, content)
            VALUES(?,?,?,?)
            """,
            (title or url, url, posted_at, raw_content),
        )

def _normalize_forum_link(link: str | None) -> str | None:
    if not link:
        return None
    if link.startswith("http://") or link.startswith("https://"):
        return link
    # forum liefert relative Links; hier vereinheitlichen
    if not link.startswith("/"):
        link = f"/{link}"
    return f"{FORUM_BASE_URL}{link}"


def load_last_forum_update() -> str | None:
    # 1) Prim�re Quelle: zentrale Deadlock-DB
    try:
        saved = deadlock_db.get_kv(KV_NAMESPACE, KV_LAST_FORUM_KEY)
        if saved:
            return _normalize_forum_link(saved)
    except Exception as exc:
        print(f"Konnte letzten Foren-Link nicht aus DB laden: {exc}")

    # 2) Fallback: neuester Eintrag aus changelog_posts
    try:
        row = deadlock_db.query_one("SELECT url FROM changelog_posts ORDER BY id DESC LIMIT 1")
        if row and row[0]:
            return _normalize_forum_link(str(row[0]))
    except Exception as exc:
        print(f"Konnte Backup-Link aus changelog_posts nicht laden: {exc}")
    return None


def save_last_forum_update(latest_link: str) -> None:
    normalized = _normalize_forum_link(latest_link)
    if not normalized:
        return

    try:
        deadlock_db.set_kv(KV_NAMESPACE, KV_LAST_FORUM_KEY, normalized)
    except Exception as exc:
        print(f"Konnte letzten Foren-Link nicht in DB speichern: {exc}")


def changelog_already_saved(url: str) -> bool:
    try:
        row = deadlock_db.query_one("SELECT 1 FROM changelog_posts WHERE url=?", (url,))
        return bool(row)
    except Exception as exc:
        print(f"DB-Check f�r vorhandene Patchnotes fehlgeschlagen: {exc}")
        return False


async def update_patch(url: str):
    channel = client.get_channel(channel_id)
    if channel is None:
        print(f"Konnte Channel {channel_id} nicht finden.")
        return

    patch_data = changelog_content_fetcher.process(url)
    if not patch_data or not patch_data.get("content"):
        print(f"Keine Patchnotes unter {url} gefunden.")
        return
    patch_content = patch_data["content"]

    try:
        api_response = perplexity_requests.fetch_answer(patch_content)
    except Exception as exc:
        print(f"Perplexity-Anfrage fehlgeschlagen: {exc}")
        return

    try:
        response = str(api_response["choices"][0]["message"]["content"])
    except Exception as exc:
        print(f"Antwortformat unerwartet: {exc} -> {api_response}")
        return

    try:
        save_changelog_to_db(
            url=url,
            title=patch_data.get("title"),
            posted_at=patch_data.get("posted_at"),
            raw_content=patch_content,
            translated_content=response,
        )
    except Exception as exc:
        print(f"Konnte Patch nicht in Deadlock-DB speichern: {exc}")

    await patch_response(channel, response)


async def patch_response(channel, response_content):
    for i in range(0, len(response_content), 1900):
        chunk = response_content[i:i+1900]
        await channel.send(chunk)


async def fetch_and_maybe_post(saved_last_forum, force: bool = False):
    try:
        latest_link = changelog_latest_fetcher.check_latest()
    except Exception as exc:
        print(f"Fehler beim Abrufen der neuesten Patchnotes: {exc}")
        return saved_last_forum

    latest_url = _normalize_forum_link(latest_link)
    if not latest_url:
        return saved_last_forum

    # Wenn nichts Neues und kein Force, still bleiben
    if not force and latest_url == saved_last_forum:
        return saved_last_forum

    if changelog_already_saved(latest_url):
        save_last_forum_update(latest_url)
        return latest_url

    if force or latest_url != saved_last_forum:
        print(f"Neuer Patch gefunden: {latest_url}")
        try:
            await update_patch(latest_url)
            save_last_forum_update(latest_url)
            return latest_url
        except Exception as exc:
            print(f"Fehler beim Posten der Patchnotes: {exc}")

    return saved_last_forum


@client.event
async def on_ready():
    print("Bot ist ready!")

    saved_last_forum = load_last_forum_update()

    # Sofort beim Start prüfen/posten
    saved_last_forum = await fetch_and_maybe_post(saved_last_forum, force=True)

    while True:
        saved_last_forum = await fetch_and_maybe_post(saved_last_forum)
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)

client.run(token)
