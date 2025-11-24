import dotenv
import os
import sys
from pathlib import Path
import discord
import asyncio
import re

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
PATCH_OUTPUT_DIR = os.getenv("PATCH_OUTPUT_DIR")  # wenn gesetzt, werden Ausgaben in Dateien geschrieben
_env_dry_run = os.getenv("BOT_DRY_RUN")
BOT_DRY_RUN = (
    _env_dry_run == "1"
    or (_env_dry_run is None and PATCH_OUTPUT_DIR)  # automatisch dry-run, wenn Datei-Ausgabe aktiv ist
)

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
client = discord.Client(intents=intents)


def _strip_code_fences(text: str) -> str:
    if not text:
        return text
    t = text.strip()
    if t.startswith("```"):
        t = t[3:]
        if "\n" in t:
            t = t.split("\n", 1)[1]
        else:
            t = ""
    if t.endswith("```"):
        t = t[:-3].rstrip()
    return t


def _get_db_raw_content(url: str | None) -> str | None:
    if not url:
        return None
    try:
        row = deadlock_db.query_one("SELECT raw_content FROM changelog_posts WHERE url=?", (url,))
        if row:
            return row[0]
    except Exception:
        return None
    return None


def _smart_chunks(text: str, limit: int = 1900) -> list[str]:
    chunks: list[str] = []
    remaining = text
    while len(remaining) > limit:
        window = remaining[:limit]
        split_idx = max(
            window.rfind("\n\n"),
            window.rfind(". "),
            window.rfind("\n"),
            window.rfind(" "),
        )
        if split_idx == -1 or split_idx < int(limit * 0.6):
            split_idx = limit
        chunk = remaining[:split_idx].rstrip()
        chunks.append(chunk)
        remaining = remaining[split_idx:].lstrip()
    if remaining:
        chunks.append(remaining)
    return chunks


def _write_patch_to_file(content: str, url: str | None):
    if not PATCH_OUTPUT_DIR:
        return False
    try:
        out_dir = Path(PATCH_OUTPUT_DIR)
        out_dir.mkdir(parents=True, exist_ok=True)
        base = "patch"
        if url:
            m = re.search(r"(\\d+)", url)
            if m:
                base = f"patch_{m.group(1)}"
        outfile = out_dir / f"{base}.txt"
        outfile.write_text(content, encoding="utf-8")
        print(f"[PATCH] Ergebnis in Datei geschrieben: {outfile}")
        return True
    except Exception as exc:
        print(f"[PATCH] Schreiben in Datei fehlgeschlagen: {exc}")
        return False


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
        print(f"DB-Check fuer vorhandene Patchnotes fehlgeschlagen: {exc}")
        return False

async def update_patch(url: str):
    channel = client.get_channel(channel_id)
    if channel is None and not PATCH_OUTPUT_DIR and not BOT_DRY_RUN:
        print(f"Konnte Channel {channel_id} nicht finden.")
        return

    patch_data = changelog_content_fetcher.process(url)
    if not patch_data or not patch_data.get("content"):
        print(f"Keine Patchnotes unter {url} gefunden.")
        return
    patch_content = patch_data["content"]

    try:
        api_response = await asyncio.to_thread(
            perplexity_requests.fetch_answer, patch_content
        )
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

    await patch_response(channel, response, url=url)


async def patch_response(channel, response_content, url: str | None = None):
    cleaned = _strip_code_fences(response_content)
    if _write_patch_to_file(cleaned, url):
        return
    if BOT_DRY_RUN:
        print("[PATCH] Dry-run aktiv, keine Discord-Nachricht gesendet.")
        return
    if channel is None:
        print("[PATCH] Kein Channel verfügbar und Datei-Write fehlgeschlagen.")
        return
    for chunk in _smart_chunks(cleaned, limit=1900):
        await channel.send(chunk)


async def fetch_and_maybe_post(saved_last_forum, force: bool = False):
    try:
        latest_info = changelog_latest_fetcher.check_latest()
    except Exception as exc:
        print(f"Fehler beim Abrufen der neuesten Patchnotes: {exc}")
        return saved_last_forum

    latest_thread_url = None
    latest_post_url = None
    if isinstance(latest_info, dict):
        latest_thread_url = _normalize_forum_link(
            latest_info.get("thread_url") or latest_info.get("thread_link")
        )
        latest_post_url = _normalize_forum_link(
            latest_info.get("latest_post_url")
            or latest_info.get("thread_url")
            or latest_info.get("thread_link")
        )
        post_urls = [
            _normalize_forum_link(p) for p in latest_info.get("post_urls", []) or []
        ]
    else:
        latest_post_url = _normalize_forum_link(latest_info)
        post_urls = []

    if not latest_post_url and latest_thread_url:
        latest_post_url = latest_thread_url

    if not latest_post_url:
        return saved_last_forum

    saved_norm = _normalize_forum_link(saved_last_forum)

    if post_urls:
        # Skip Haupt-Post, falls schon gespeichert (entweder Thread-URL oder erste Post-URL)
        if changelog_already_saved(post_urls[0]) or (
            latest_thread_url and changelog_already_saved(latest_thread_url)
        ):
            post_urls = post_urls[1:]
        # Fallback: entferne evtl. None
        post_urls = [p for p in post_urls if p]

    to_check = post_urls or [latest_post_url]
    main_raw = _get_db_raw_content(latest_thread_url or (post_urls[0] if post_urls else None))
    new_posts: list[str] = []
    for url in to_check:
        if not url:
            continue
        if not changelog_already_saved(url):
            new_posts.append(url)
            continue
        # Reprocess if the stored content looks identical zum Haupt-Patch (falsche Zuordnung)
        saved_raw = _get_db_raw_content(url)
        if saved_raw and main_raw and url != latest_thread_url:
            # gleiche/fast gleiche Laenge -> vermutlich Hauptpatch kopiert statt Kommentar
            if abs(len(saved_raw) - len(main_raw)) < 500 and len(saved_raw) > 2000:
                new_posts.append(url)

    print(f"Patch-Scan -> thread={latest_thread_url}, latest_post={latest_post_url}, candidates={to_check}, new={new_posts}")

    if not force and not new_posts:
        save_last_forum_update(latest_post_url)
        return latest_post_url

    last_processed = saved_norm
    for url in new_posts:
        print(f"Neuer Patch gefunden: {url}")
        try:
            await update_patch(url)
            save_last_forum_update(url)
            last_processed = url
        except Exception as exc:
            print(f"Fehler beim Posten der Patchnotes: {exc}")

    return last_processed or saved_last_forum


@client.event
async def on_ready():
    print("Bot ist ready!")

    saved_last_forum = load_last_forum_update()

    # Sofort beim Start prüfen/posten
    saved_last_forum = await fetch_and_maybe_post(saved_last_forum, force=True)

    while True:
        saved_last_forum = await fetch_and_maybe_post(saved_last_forum)
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__" and os.getenv("BOT_SKIP_RUN") != "1" and not BOT_DRY_RUN:
    client.run(token)
