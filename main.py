import dotenv
import os
import sys
from pathlib import Path
import aiohttp
import discord
import asyncio
import signal
import re
import socket
from datetime import datetime, timezone

import changelog_content_fetcher
import changelog_latest_fetcher

import perplexity_requests

KV_NAMESPACE = "patchnotes_bot"
KV_LAST_FORUM_KEY = "last_forum_url"

DEADLOCK_ROOT = Path(os.getenv("DEADLOCK_HOME") or Path.home() / "Documents" / "Deadlock")
# Load Deadlock env first so service.config picks up required tokens, then this repo's .env.
dotenv.load_dotenv(DEADLOCK_ROOT / ".env")
dotenv.load_dotenv()

if str(DEADLOCK_ROOT) not in sys.path:
    sys.path.insert(0, str(DEADLOCK_ROOT))

from service import db as deadlock_db  # type: ignore

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


def _build_http_connector(loop: asyncio.AbstractEventLoop) -> aiohttp.TCPConnector:
    return aiohttp.TCPConnector(
        # Force system getaddrinfo instead of aiodns/pycares to avoid DNS failures during login.
        resolver=aiohttp.resolver.ThreadedResolver(loop=loop),
        family=socket.AF_INET,
    )


class PatchnotesClient(discord.Client):
    def __init__(self, *, intents: discord.Intents, **options):
        super().__init__(intents=intents, **options)
        self._connector: aiohttp.TCPConnector | None = None

    async def _ensure_threaded_resolver(self) -> None:
        loop = asyncio.get_running_loop()
        if self._connector and not self._connector.closed:
            if self.http.connector is not self._connector:
                try:
                    if getattr(self.http, "connector", None):
                        await self.http.connector.close()
                except Exception:
                    pass
                self.http.connector = self._connector
                self.http._HTTPClient__session = None
            return

        connector = _build_http_connector(loop)
        try:
            if getattr(self.http, "_HTTPClient__session", None):
                await self.http._HTTPClient__session.close()
        except Exception:
            pass
        try:
            if getattr(self.http, "connector", None):
                await self.http.connector.close()
        except Exception:
            pass

        self.http.connector = connector
        # Force a new session so login uses the patched connector.
        self.http._HTTPClient__session = None
        self._connector = connector

    async def login(self, token: str) -> None:
        await self._ensure_threaded_resolver()
        await super().login(token)

    async def setup_hook(self) -> None:
        # Ensure reconnects keep using the custom connector.
        await self._ensure_threaded_resolver()
        await super().setup_hook()


client = PatchnotesClient(intents=intents)
stop_event = asyncio.Event()
_scan_task: asyncio.Task | None = None


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


def _get_role_ping() -> str | None:
    return getattr(perplexity_requests, "ROLE_PING", None)


def _strip_role_ping(text: str) -> str:
    role_ping = _get_role_ping()
    if not role_ping or not text:
        return text
    lines = [line for line in text.splitlines() if role_ping not in line]
    return "\n".join(lines).strip()


def _ensure_role_ping(text: str) -> str:
    role_ping = _get_role_ping()
    if not role_ping or not text:
        return text
    if role_ping in text:
        return text
    return text.rstrip() + "\n" + role_ping


def _format_patch_date(posted_at: str | None) -> str | None:
    if not posted_at:
        return None
    raw = str(posted_at).strip()
    if not raw:
        return None
    if raw.isdigit():
        try:
            ts = int(raw)
            if ts > 10_000_000_000:
                ts = ts / 1000
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            return dt.strftime("%d.%m.%Y")
        except Exception:
            return None

    iso = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso)
        return dt.strftime("%d.%m.%Y")
    except Exception:
        pass

    for fmt in (
        "%b %d, %Y",
        "%B %d, %Y",
        "%d %b %Y",
        "%d %B %Y",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%d.%m.%Y")
        except Exception:
            continue

    return None


def _inject_patch_heading(text: str, posted_at: str | None) -> str:
    if not text:
        return text
    if not posted_at:
        return text
    date_str = _format_patch_date(posted_at) or str(posted_at).strip()
    if not date_str:
        return text

    heading = f"### Deadlock Patch Notes ({date_str})"
    stripped = text.lstrip()
    lines = stripped.splitlines()
    if not lines:
        return heading
    first_line = lines[0].strip()
    if first_line.lower().startswith("### deadlock patch notes"):
        rest = "\n".join(lines[1:]).lstrip("\n")
        return heading + ("\n" + rest if rest else "")
    return heading + "\n" + stripped


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


def _get_retranslate_mode(content: str | None) -> bool | None:
    if not content:
        return None
    lowered = content.strip().lower()
    if not lowered:
        return None
    if lowered == "!tpatch":
        return False
    if lowered == "!ppatch":
        return True
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
            m = re.search("(\\d+)", url)
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

    patch_data = await asyncio.to_thread(changelog_content_fetcher.process, url)
    if not patch_data or not patch_data.get("content"):
        print(f"Keine Patchnotes unter {url} gefunden.")
        return
    patch_content = patch_data["content"]

    response = patch_content  # Fallback: sende Rohtext, falls KI nicht erreichbar ist
    try:
        api_response = await asyncio.to_thread(
            perplexity_requests.fetch_answer, patch_content
        )
        try:
            response = str(api_response["choices"][0]["message"]["content"])
        except Exception as exc:
            print(f"Antwortformat unerwartet: {exc} -> {api_response}")
            response = patch_content
    except Exception as exc:
        print(f"Perplexity-Anfrage fehlgeschlagen, verwende Rohtext: {exc}")

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

    await patch_response(channel, response, url=url, posted_at=patch_data.get("posted_at"))


async def patch_response(channel, response_content, url: str | None = None, posted_at: str | None = None):
    cleaned = _strip_code_fences(response_content)
    cleaned = _inject_patch_heading(cleaned, posted_at)
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


def _load_latest_patch_from_db() -> tuple[str | None, str | None, str | None, str | None]:
    try:
        row = deadlock_db.query_one(
            "SELECT url, title, posted_at, raw_content FROM changelog_posts ORDER BY id DESC LIMIT 1"
        )
    except Exception as exc:
        print(f"Konnte letzten Patch nicht aus DB laden: {exc}")
        return None, None, None, None
    if not row:
        return None, None, None, None
    return row["url"], row["title"], row["posted_at"], row["raw_content"]


async def retranslate_latest_patch(channel, *, include_ping: bool):
    url, title, posted_at, raw_content = _load_latest_patch_from_db()

    if not raw_content and url:
        try:
            patch_data = await asyncio.to_thread(changelog_content_fetcher.process, url)
        except Exception as exc:
            await channel.send(f"Letzten Patch gefunden ({url}), aber konnte Inhalt nicht laden: {exc}")
            return
        if not patch_data or not patch_data.get("content"):
            await channel.send(f"Konnte Patch-Inhalt nicht finden: {url}")
            return
        raw_content = patch_data.get("content")
        title = patch_data.get("title") or title
        posted_at = patch_data.get("posted_at") or posted_at

    if not raw_content:
        await channel.send("Keine gespeicherten Patchnotes gefunden.")
        return

    response = raw_content
    try:
        api_response = await asyncio.to_thread(
            perplexity_requests.fetch_answer,
            raw_content,
            include_ping,
        )
        try:
            response = str(api_response["choices"][0]["message"]["content"])
        except Exception as exc:
            print(f"Antwortformat unerwartet: {exc} -> {api_response}")
            response = raw_content
    except Exception as exc:
        print(f"Perplexity-Anfrage fehlgeschlagen, verwende Rohtext: {exc}")

    if include_ping:
        response = _ensure_role_ping(response)
    else:
        response = _strip_role_ping(response)

    if url:
        try:
            save_changelog_to_db(
                url=url,
                title=title,
                posted_at=posted_at,
                raw_content=raw_content,
                translated_content=response,
            )
        except Exception as exc:
            print(f"Konnte Patch nicht in Deadlock-DB speichern: {exc}")

    await patch_response(channel, response, url=url, posted_at=posted_at)


async def fetch_and_maybe_post(saved_last_forum, force: bool = False):
    try:
        latest_info = await asyncio.to_thread(changelog_latest_fetcher.check_latest)
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

    should_log_scan = force or bool(new_posts) or (saved_norm and saved_norm != latest_post_url)
    if should_log_scan:
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


async def _scan_loop():
    saved_last_forum = load_last_forum_update()

    try:
        saved_last_forum = await fetch_and_maybe_post(saved_last_forum, force=True)
        while not stop_event.is_set():
            saved_last_forum = await fetch_and_maybe_post(saved_last_forum)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=CHECK_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                continue
    except Exception as exc:
        print(f"Unerwarteter Fehler im Scan-Loop: {exc}")
        raise


@client.event
async def on_ready():
    global _scan_task
    print("Bot ist ready!")

    if _scan_task and not _scan_task.done():
        print("Scan-Task laeuft bereits, kein Neustart erforderlich.")
        return

    _scan_task = asyncio.create_task(_scan_loop())


@client.event
async def on_message(message):
    if message.author.bot:
        return
    mode = _get_retranslate_mode(message.content)
    if mode is None:
        return
    async with message.channel.typing():
        await retranslate_latest_patch(message.channel, include_ping=mode)


if __name__ == "__main__" and os.getenv("BOT_SKIP_RUN") != "1" and not BOT_DRY_RUN:
    client.run(token)
