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
from time import perf_counter

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
DEFAULT_CHECK_INTERVAL_SECONDS = 35
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", DEFAULT_CHECK_INTERVAL_SECONDS))
FORUM_BASE_URL = "https://forums.playdeadlock.com"
PATCH_OUTPUT_DIR = os.getenv("PATCH_OUTPUT_DIR")  # wenn gesetzt, werden Ausgaben in Dateien geschrieben
DEFAULT_MAX_CATCHUP_POSTS = 1
MAX_CATCHUP_POSTS = max(1, int(os.getenv("PATCH_MAX_CATCHUP_POSTS", str(DEFAULT_MAX_CATCHUP_POSTS))))
DISCORD_MESSAGE_HARD_LIMIT = 2000
DEFAULT_PATCH_CHUNK_LIMIT = 1950
PATCH_CHUNK_LIMIT = min(
    DISCORD_MESSAGE_HARD_LIMIT,
    max(1, int(os.getenv("PATCH_CHUNK_LIMIT", str(DEFAULT_PATCH_CHUNK_LIMIT)))),
)
_env_dry_run = os.getenv("BOT_DRY_RUN")
BOT_DRY_RUN = (
    _env_dry_run == "1"
    or (_env_dry_run is None and PATCH_OUTPUT_DIR)  # automatisch dry-run, wenn Datei-Ausgabe aktiv ist
)
PATCH_TIMING_LEVEL = (os.getenv("PATCH_TIMING_LEVEL", "minimal") or "minimal").strip().lower()
PATCH_SCAN_VERBOSE = (os.getenv("PATCH_SCAN_VERBOSE", "0") or "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

_TIMING_EVENTS_MINIMAL = {
    "new_patch_detected",
    "new_patch_processed",
    "new_patch_error",
}

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


def _parse_posted_at_datetime(posted_at: str | None) -> datetime | None:
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
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            return None

    iso = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
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
            dt = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue

    return None


def _fmt_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _fmt_local(dt: datetime) -> str:
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def _timing_log(event: str, **fields) -> None:
    if PATCH_TIMING_LEVEL == "off":
        return
    if PATCH_TIMING_LEVEL == "minimal" and event not in _TIMING_EVENTS_MINIMAL:
        return

    now = datetime.now(timezone.utc)
    parts = [
        f"[TIMING] {event}",
        f"utc={_fmt_utc(now)}",
        f"local={_fmt_local(now)}",
    ]
    for key, value in fields.items():
        if value is None:
            continue
        parts.append(f"{key}={value}")
    print(" | ".join(parts))


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


_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?\u2026])\s+")
_BULLET_PREFIX_RE = re.compile(r"^(\s*(?:[-*]|\u2022|\d+[.)])\s+)(.+)$")
_CITATION_RE = re.compile(r"\[(?:\d+(?:,\s*\d+)*)\]")
_BAD_TRANSLATION_MARKERS = (
    "ich kann diese anfrage nicht erfuellen",
    "ich kann diese anfrage nicht erfüllen",
    "keine patchnotes bereitgestellt",
    "sucherergebnisse",
    "discord-markdown-formatierung",
    "um ihnen zu helfen, benoetige ich",
    "um ihnen zu helfen, benötige ich",
    "bitte bestaetigen sie",
    "bitte bestätigen sie",
)


def _normalize_text(text: str | None) -> str:
    return " ".join((text or "").strip().lower().split())


def _looks_like_unusable_translation(text: str | None) -> bool:
    checker = getattr(perplexity_requests, "is_unusable_translation", None)
    if callable(checker):
        try:
            return bool(checker(text))
        except Exception:
            pass

    normalized = _normalize_text(text)
    if not normalized:
        return True
    return any(marker in normalized for marker in _BAD_TRANSLATION_MARKERS)


def _extract_model_response_text(api_response: dict | None) -> str:
    if not api_response:
        return ""

    extractor = getattr(perplexity_requests, "extract_answer_text", None)
    if callable(extractor):
        try:
            extracted = str(extractor(api_response) or "").strip()
            if extracted:
                return extracted
        except Exception:
            pass

    try:
        return str(api_response["choices"][0]["message"]["content"]).strip()
    except Exception:
        return ""


def _remove_inline_citations(text: str) -> str:
    if not text:
        return text
    cleaned = _CITATION_RE.sub("", text)
    cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


async def _translate_patch_content(
    patch_content: str,
    *,
    include_ping: bool,
    context_label: str,
) -> str:
    fallback = patch_content

    for strict_mode in (False, True):
        translate_start = perf_counter()
        try:
            try:
                api_response = await asyncio.to_thread(
                    perplexity_requests.fetch_answer,
                    patch_content,
                    include_ping,
                    strict_mode,
                )
            except TypeError:
                # Backward compatibility in case an older helper is still loaded.
                api_response = await asyncio.to_thread(
                    perplexity_requests.fetch_answer,
                    patch_content,
                    include_ping,
                )
        except Exception as exc:
            print(
                f"Perplexity-Anfrage fehlgeschlagen ({context_label}, strict={strict_mode}): {exc}"
            )
            _timing_log(
                "translate_request_error",
                context=context_label,
                strict=strict_mode,
                duration_s=f"{(perf_counter() - translate_start):.2f}",
                error=str(exc)[:180],
            )
            continue

        candidate = _extract_model_response_text(api_response)
        if not candidate:
            print(
                f"Perplexity lieferte leere Antwort ({context_label}, strict={strict_mode})."
            )
            _timing_log(
                "translate_empty",
                context=context_label,
                strict=strict_mode,
                duration_s=f"{(perf_counter() - translate_start):.2f}",
            )
            continue

        candidate = _remove_inline_citations(candidate)
        if _looks_like_unusable_translation(candidate):
            print(
                f"Perplexity lieferte unbrauchbare Antwort ({context_label}, strict={strict_mode}) -> retry."
            )
            _timing_log(
                "translate_unusable",
                context=context_label,
                strict=strict_mode,
                duration_s=f"{(perf_counter() - translate_start):.2f}",
                output_len=len(candidate),
            )
            continue

        _timing_log(
            "translate_ok",
            context=context_label,
            strict=strict_mode,
            duration_s=f"{(perf_counter() - translate_start):.2f}",
            output_len=len(candidate),
        )
        return candidate

    print(
        f"Perplexity lieferte keine brauchbare Antwort ({context_label}); verwende Rohtext."
    )
    _timing_log(
        "translate_fallback_raw",
        context=context_label,
        output_len=len(fallback),
    )
    return fallback


def _hard_wrap_words(text: str, limit: int) -> list[str]:
    stripped = text.strip()
    if not stripped:
        return []

    words = stripped.split()
    if not words:
        return [stripped[i : i + limit] for i in range(0, len(stripped), limit)]

    wrapped: list[str] = []
    current = ""
    for word in words:
        if not current:
            if len(word) <= limit:
                current = word
            else:
                wrapped.extend(word[i : i + limit] for i in range(0, len(word), limit))
            continue

        candidate = f"{current} {word}"
        if len(candidate) <= limit:
            current = candidate
            continue

        wrapped.append(current)
        if len(word) <= limit:
            current = word
        else:
            wrapped.extend(word[i : i + limit] for i in range(0, len(word), limit))
            current = ""

    if current:
        wrapped.append(current)
    return wrapped


def _split_line_units(line: str, limit: int) -> list[str]:
    if len(line) <= limit:
        return [line]

    match = _BULLET_PREFIX_RE.match(line)
    prefix = ""
    body = line.strip()
    if match:
        prefix = match.group(1)
        body = match.group(2).strip()

    sentences = [part.strip() for part in _SENTENCE_SPLIT_RE.split(body) if part.strip()]
    if len(sentences) <= 1:
        return _hard_wrap_words(line, limit)

    units: list[str] = []
    continuation_prefix = " " * len(prefix) if prefix else ""
    for idx, sentence in enumerate(sentences):
        line_part = f"{prefix if idx == 0 else continuation_prefix}{sentence}"
        if len(line_part) <= limit:
            units.append(line_part)
        else:
            units.extend(_hard_wrap_words(line_part, limit))
    return units


def _smart_chunks(text: str, limit: int = PATCH_CHUNK_LIMIT) -> list[str]:
    if not text:
        return []

    limit = min(max(int(limit), 1), DISCORD_MESSAGE_HARD_LIMIT)
    units: list[str] = []
    for raw_line in text.splitlines():
        if not raw_line.strip():
            units.append("")
            continue
        units.extend(_split_line_units(raw_line.rstrip(), limit))

    chunks: list[str] = []
    current_lines: list[str] = []
    current_len = 0

    def _flush() -> None:
        nonlocal current_lines, current_len
        while current_lines and not current_lines[-1].strip():
            current_lines.pop()
        if current_lines:
            chunks.append("\n".join(current_lines).rstrip())
        current_lines = []
        current_len = 0

    for unit in units:
        if not current_lines and unit == "":
            continue

        add_len = len(unit) if not current_lines else len(unit) + 1
        if current_lines and current_len + add_len > limit:
            _flush()
            if unit == "":
                continue
            add_len = len(unit)

        if len(unit) > limit:
            for piece in _hard_wrap_words(unit, limit):
                piece_add_len = len(piece) if not current_lines else len(piece) + 1
                if current_lines and current_len + piece_add_len > limit:
                    _flush()
                    piece_add_len = len(piece)
                current_lines.append(piece)
                current_len += piece_add_len
            continue

        current_lines.append(unit)
        current_len += add_len

    _flush()

    if not chunks and text.strip():
        return _hard_wrap_words(text, limit)
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


def _extract_post_id(url: str | None) -> int | None:
    if not url:
        return None
    match = re.search(r"/posts/(\d+)/?$", str(url).strip())
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _dedupe_urls(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for url in urls:
        if not url or url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def _select_candidate_urls(
    *,
    post_urls: list[str],
    latest_post_url: str,
    saved_norm: str | None,
) -> tuple[list[str], str]:
    """Select only unseen forum post URLs since the saved checkpoint."""
    if not latest_post_url:
        return [], "no_latest"

    normalized_posts = _dedupe_urls([_normalize_forum_link(url) for url in post_urls if url])
    latest_norm = _normalize_forum_link(latest_post_url)
    if latest_norm and latest_norm not in normalized_posts:
        normalized_posts.append(latest_norm)

    if not saved_norm:
        # Cold start: establish checkpoint first; avoid historical backfill spam.
        return [], "cold_start_checkpoint_only"

    candidates: list[str] = []
    if saved_norm in normalized_posts:
        saved_idx = normalized_posts.index(saved_norm)
        candidates = normalized_posts[saved_idx + 1 :]
    else:
        saved_id = _extract_post_id(saved_norm)
        if saved_id is not None:
            candidates = [
                url
                for url in normalized_posts
                if ((pid := _extract_post_id(url)) is not None and pid > saved_id)
            ]
        # Fallback for thread switches or mixed URL styles.
        if not candidates and latest_norm and latest_norm != saved_norm:
            latest_id = _extract_post_id(latest_norm)
            if saved_id is None or latest_id is None or latest_id > saved_id:
                candidates = [latest_norm]

    if len(candidates) > MAX_CATCHUP_POSTS:
        return candidates[-MAX_CATCHUP_POSTS:], "catchup_limited"
    return candidates, "ok"


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
    patch_start = perf_counter()
    channel = client.get_channel(channel_id)
    if channel is None and not PATCH_OUTPUT_DIR and not BOT_DRY_RUN:
        print(f"Konnte Channel {channel_id} nicht finden.")
        return

    patch_data = await asyncio.to_thread(changelog_content_fetcher.process, url)
    if not patch_data or not patch_data.get("content"):
        print(f"Keine Patchnotes unter {url} gefunden.")
        return
    patch_content = patch_data["content"]
    posted_raw = patch_data.get("posted_at")
    posted_dt = _parse_posted_at_datetime(posted_raw)
    now_utc = datetime.now(timezone.utc)
    lag_seconds = None
    posted_utc_label = None
    posted_local_label = None
    if posted_dt:
        lag_seconds = max(0.0, (now_utc - posted_dt).total_seconds())
        posted_utc_label = _fmt_utc(posted_dt)
        posted_local_label = _fmt_local(posted_dt)

    _timing_log(
        "patch_fetch",
        url=url,
        posted_at_raw=posted_raw,
        posted_at_utc=posted_utc_label,
        posted_at_local=posted_local_label,
        lag_s=f"{lag_seconds:.1f}" if lag_seconds is not None else None,
        raw_len=len(patch_content),
    )

    response = await _translate_patch_content(
        patch_content,
        include_ping=True,
        context_label=url,
    )
    response = _ensure_role_ping(response)

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
    _timing_log(
        "patch_pipeline_done",
        url=url,
        total_duration_s=f"{(perf_counter() - patch_start):.2f}",
    )


async def patch_response(channel, response_content, url: str | None = None, posted_at: str | None = None):
    send_start = perf_counter()
    cleaned = _strip_code_fences(response_content)
    cleaned = _remove_inline_citations(cleaned)
    cleaned = _inject_patch_heading(cleaned, posted_at)
    if _write_patch_to_file(cleaned, url):
        _timing_log(
            "patch_written_to_file",
            url=url,
            duration_s=f"{(perf_counter() - send_start):.2f}",
            chars=len(cleaned),
        )
        return
    if BOT_DRY_RUN:
        print("[PATCH] Dry-run aktiv, keine Discord-Nachricht gesendet.")
        _timing_log(
            "patch_dry_run",
            url=url,
            duration_s=f"{(perf_counter() - send_start):.2f}",
            chars=len(cleaned),
        )
        return
    if channel is None:
        print("[PATCH] Kein Channel verfügbar und Datei-Write fehlgeschlagen.")
        _timing_log(
            "patch_send_skipped_no_channel",
            url=url,
            duration_s=f"{(perf_counter() - send_start):.2f}",
            chars=len(cleaned),
        )
        return
    chunks = _smart_chunks(cleaned, limit=PATCH_CHUNK_LIMIT)
    _timing_log(
        "discord_send_start",
        url=url,
        chunks=len(chunks),
        chars=len(cleaned),
    )
    for chunk in chunks:
        await channel.send(chunk)
    _timing_log(
        "discord_send_done",
        url=url,
        chunks=len(chunks),
        duration_s=f"{(perf_counter() - send_start):.2f}",
    )


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

    response = await _translate_patch_content(
        raw_content,
        include_ping=include_ping,
        context_label=url or "retranslate_latest",
    )

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
    scan_start = perf_counter()
    _timing_log(
        "scan_start",
        force=force,
        saved_last_forum=_normalize_forum_link(saved_last_forum),
    )

    try:
        latest_info = await asyncio.to_thread(changelog_latest_fetcher.check_latest)
    except Exception as exc:
        print(f"Fehler beim Abrufen der neuesten Patchnotes: {exc}")
        _timing_log(
            "scan_latest_fetch_error",
            error=str(exc)[:200],
            duration_s=f"{(perf_counter() - scan_start):.2f}",
        )
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
        _timing_log(
            "scan_no_latest_post_url",
            duration_s=f"{(perf_counter() - scan_start):.2f}",
        )
        return saved_last_forum

    saved_norm = _normalize_forum_link(saved_last_forum)

    # Fallback: entferne evtl. None und normalisiere Reihenfolge.
    post_urls = [p for p in post_urls if p]
    to_check, candidate_mode = _select_candidate_urls(
        post_urls=post_urls,
        latest_post_url=latest_post_url,
        saved_norm=saved_norm,
    )
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

    should_log_scan = PATCH_SCAN_VERBOSE or bool(new_posts)
    if should_log_scan:
        print(
            f"Patch-Scan -> thread={latest_thread_url}, latest_post={latest_post_url}, "
            f"mode={candidate_mode}, candidates={to_check}, new={new_posts}"
        )
        _timing_log(
            "scan_result",
            thread=latest_thread_url,
            latest_post=latest_post_url,
            mode=candidate_mode,
            candidates=len(to_check),
            new_posts=len(new_posts),
            duration_s=f"{(perf_counter() - scan_start):.2f}",
        )

    if not new_posts:
        save_last_forum_update(latest_post_url)
        _timing_log(
            "scan_no_new_posts",
            latest_post=latest_post_url,
            mode=candidate_mode,
            duration_s=f"{(perf_counter() - scan_start):.2f}",
        )
        return latest_post_url

    last_processed = saved_norm
    for url in new_posts:
        print(f"Neuer Patch gefunden: {url}")
        post_start = perf_counter()
        _timing_log("new_patch_detected", url=url)
        try:
            await update_patch(url)
            save_last_forum_update(url)
            last_processed = url
            _timing_log(
                "new_patch_processed",
                url=url,
                duration_s=f"{(perf_counter() - post_start):.2f}",
            )
        except Exception as exc:
            print(f"Fehler beim Posten der Patchnotes: {exc}")
            _timing_log(
                "new_patch_error",
                url=url,
                duration_s=f"{(perf_counter() - post_start):.2f}",
                error=str(exc)[:200],
            )

    _timing_log(
        "scan_done",
        duration_s=f"{(perf_counter() - scan_start):.2f}",
        last_processed=last_processed,
    )
    return last_processed or saved_last_forum


async def _scan_loop():
    saved_last_forum = load_last_forum_update()
    _timing_log("scan_loop_start", saved_last_forum=saved_last_forum, interval_s=CHECK_INTERVAL_SECONDS)

    try:
        saved_last_forum = await fetch_and_maybe_post(saved_last_forum, force=True)
        while not stop_event.is_set():
            loop_tick_start = perf_counter()
            saved_last_forum = await fetch_and_maybe_post(saved_last_forum)
            _timing_log(
                "scan_loop_tick_done",
                duration_s=f"{(perf_counter() - loop_tick_start):.2f}",
                next_in_s=CHECK_INTERVAL_SECONDS,
            )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=CHECK_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                continue
    except Exception as exc:
        print(f"Unerwarteter Fehler im Scan-Loop: {exc}")
        _timing_log("scan_loop_error", error=str(exc)[:200])
        raise


@client.event
async def on_ready():
    global _scan_task
    print("Bot ist ready!")
    _timing_log("bot_ready")

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
