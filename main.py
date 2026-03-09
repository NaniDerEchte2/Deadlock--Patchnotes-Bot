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
KV_LAST_PATCH_KEY = "last_forum_url"
KV_LAST_TEST_POST_KEY = "last_test_post_url"

DEADLOCK_ROOT = Path(os.getenv("DEADLOCK_HOME") or Path.home() / "Documents" / "Deadlock")
# Load Deadlock env first so service.config picks up required tokens, then this repo's .env.
dotenv.load_dotenv(DEADLOCK_ROOT / ".env")
dotenv.load_dotenv()

if str(DEADLOCK_ROOT) not in sys.path:
    sys.path.insert(0, str(DEADLOCK_ROOT))

from service import db as deadlock_db  # type: ignore


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


channel_id = int(os.getenv("PATCH_CHANNEL_ID"))  # convert to int
token = os.getenv("BOT_TOKEN")
DEFAULT_CHECK_INTERVAL_SECONDS = 35
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", DEFAULT_CHECK_INTERVAL_SECONDS))
FORUM_BASE_URL = "https://forums.playdeadlock.com"
STEAM_COMMUNITY_BASE_URL = "https://steamcommunity.com"
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
PATCH_SCAN_VERBOSE = _env_flag("PATCH_SCAN_VERBOSE")
PATCH_AUTO_INCLUDE_PING = _env_flag("PATCH_AUTO_INCLUDE_PING", True)
PATCH_FORCE_POST_LATEST_ON_START = _env_flag("PATCH_FORCE_POST_LATEST_ON_START")
PATCH_TRANSLATE_SPLIT_THRESHOLD = max(2000, int(os.getenv("PATCH_TRANSLATE_SPLIT_THRESHOLD", "18000")))
PATCH_TRANSLATE_CHUNK_TARGET = max(2000, int(os.getenv("PATCH_TRANSLATE_CHUNK_TARGET", "9000")))

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


async def _resolve_patch_channel() -> discord.abc.Messageable | None:
    channel = client.get_channel(channel_id)
    if channel is not None:
        return channel
    try:
        return await client.fetch_channel(channel_id)
    except discord.Forbidden:
        print(f"Kein Zugriff auf Channel {channel_id} (Discord API: Missing Access).")
    except discord.NotFound:
        print(f"Channel {channel_id} wurde nicht gefunden.")
    except Exception as exc:
        print(f"Channel {channel_id} konnte nicht geladen werden: {exc}")
    return None


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


def _find_saved_changelog_row(url: str | None):
    normalized = _normalize_patch_link(url)
    if not normalized:
        return None
    try:
        row = deadlock_db.query_one(
            "SELECT id, url, raw_content FROM changelog_posts WHERE url=? ORDER BY id DESC LIMIT 1",
            (normalized,),
        )
        if row or not _is_forum_link(normalized):
            return row

        patch_id = _extract_patch_id(normalized)
        if patch_id is None:
            return None

        return deadlock_db.query_one(
            """
            SELECT id, url, raw_content
            FROM changelog_posts
            WHERE url LIKE ? OR url LIKE ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (f"%/posts/{patch_id}/%", f"%#post-{patch_id}%"),
        )
    except Exception:
        return None


def _get_db_raw_content(url: str | None) -> str | None:
    if not url:
        return None
    row = _find_saved_changelog_row(url)
    if row:
        return row["raw_content"]
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
_SECTION_HEADER_RE = re.compile(r"^\*\*[^*]+\*\*\s*$|^#{1,3}\s+\S|^\[\s*.+\s*\]\s*$")
# Matches "- HeroName: ..." bullets to group by hero name (e.g. "- Yamato: ..." → "Yamato")
_HERO_BULLET_RE = re.compile(r"^-\s+([A-ZÄÖÜ][a-zA-ZäöüÄÖÜß]*):\s+")
_CITATION_RE = re.compile(r"\[(?:\d+(?:,\s*\d+)*)\]")
_MASKED_LINK_RE = re.compile(r"\[([^\]\n]+)\]\((?:https?://[^)\s]+)\)")
_ANGLE_URL_RE = re.compile(r"<https?://[^>\s]+>")
_RAW_URL_RE = re.compile(r"(?<!\()https?://[^\s)>]+")
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


def _remove_links(text: str) -> str:
    if not text:
        return text
    cleaned = _MASKED_LINK_RE.sub(r"\1", text)
    cleaned = _ANGLE_URL_RE.sub("", cleaned)
    cleaned = _RAW_URL_RE.sub("", cleaned)
    cleaned = re.sub(r"\(\s*\)", "", cleaned)
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    cleaned = re.sub(r"[ \t]+\n", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def _repair_known_hero_sections(text: str) -> str:
    if not text:
        return text
    repairer = getattr(perplexity_requests, "repair_known_hero_sections", None)
    if not callable(repairer):
        return text
    try:
        repaired = str(repairer(text) or "").strip()
    except Exception:
        return text
    return repaired or text


def _cleanup_partial_translation(text: str) -> str:
    cleaned = _strip_code_fences(text)
    cleaned = _remove_inline_citations(cleaned)
    cleaned = _remove_links(cleaned)
    cleaned = _strip_role_ping(cleaned)
    cleaned = re.sub(r"(?im)^###\s*deadlock patch notes.*$", "", cleaned)
    cleaned = re.split(r"(?m)^_{3,}\s*$", cleaned, maxsplit=1)[0]
    cleaned = re.split(r"(?im)^\*\*kurzzusammenfassung\*\*.*$", cleaned, maxsplit=1)[0]
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return _repair_known_hero_sections(cleaned.strip())


def _split_text_for_translation(text: str, limit: int) -> list[str]:
    """Split text into chunks for translation, keeping hero/item sections together."""
    if not text:
        return []
    if len(text) <= limit:
        return [text.strip()]

    blocks = _parse_sections(text)

    # If no sections detected, fall back to simple line splitting
    if len(blocks) <= 1:
        return _split_text_for_translation_legacy(text, limit)

    def _block_to_str(block_lines: list[str]) -> str:
        return "\n".join(block_lines).strip()

    def _split_large_block(block_lines: list[str], lim: int) -> list[str]:
        """Split a block that alone exceeds limit at line boundaries."""
        result: list[str] = []
        current_lines: list[str] = []
        current_len = 0
        for line in block_lines:
            add_len = len(line) + (1 if current_lines else 0)
            if current_lines and current_len + add_len > lim:
                result.append("\n".join(current_lines).strip())
                current_lines = []
                current_len = 0
                add_len = len(line)
            current_lines.append(line)
            current_len += add_len
        if current_lines:
            result.append("\n".join(current_lines).strip())
        return [r for r in result if r]

    chunks: list[str] = []
    current_lines: list[str] = []
    current_len = 0

    def _flush() -> None:
        nonlocal current_lines, current_len
        if current_lines:
            chunks.append("\n".join(current_lines).strip())
        current_lines.clear()
        current_len = 0

    for block in blocks:
        block_str = _block_to_str(block)
        block_len = len(block_str)

        if block_len > limit:
            _flush()
            chunks.extend(_split_large_block(block, limit))
            continue

        sep_len = 2 if current_lines else 0
        if current_lines and current_len + sep_len + block_len > limit:
            _flush()

        if current_lines:
            current_lines.append("")
            current_len += 1
        current_lines.extend(block)
        current_len = len("\n".join(current_lines))

    _flush()
    return [c for c in chunks if c.strip()] or ([text.strip()] if text.strip() else [])


def _split_text_for_translation_legacy(text: str, limit: int) -> list[str]:
    """Legacy line-based splitting fallback when no sections detected."""
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
    return chunks or ([text.strip()] if text.strip() else [])


async def _request_patch_translation(
    patch_content: str,
    *,
    include_ping: bool,
    context_label: str,
    partial_mode: bool = False,
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
                    partial_mode,
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

        candidate = (
            _cleanup_partial_translation(candidate)
            if partial_mode
            else _repair_known_hero_sections(_remove_links(_remove_inline_citations(candidate)))
        )
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


async def _translate_patch_content(
    patch_content: str,
    *,
    include_ping: bool,
    context_label: str,
) -> str:
    if len(patch_content or "") <= PATCH_TRANSLATE_SPLIT_THRESHOLD:
        return await _request_patch_translation(
            patch_content,
            include_ping=include_ping,
            context_label=context_label,
        )

    parts = _split_text_for_translation(patch_content, PATCH_TRANSLATE_CHUNK_TARGET)
    if len(parts) <= 1:
        return await _request_patch_translation(
            patch_content,
            include_ping=include_ping,
            context_label=context_label,
        )

    print(
        f"Patch zu gross fuer Einzel-Translation ({context_label}); splitte in {len(parts)} Teile."
    )
    _timing_log(
        "translate_split_start",
        context=context_label,
        parts=len(parts),
        input_len=len(patch_content),
    )

    translated_parts: list[str] = []
    for idx, part in enumerate(parts, start=1):
        translated = await _request_patch_translation(
            part,
            include_ping=False,
            context_label=f"{context_label} part {idx}/{len(parts)}",
            partial_mode=True,
        )
        translated_parts.append(translated.strip())

    combined = _repair_known_hero_sections(
        "\n\n".join(part for part in translated_parts if part).strip()
    )
    _timing_log(
        "translate_split_done",
        context=context_label,
        parts=len(parts),
        output_len=len(combined),
    )
    return combined or patch_content


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


def _is_section_header(line: str) -> bool:
    """Returns True if line looks like a patchnote section header."""
    stripped = line.strip()
    if not stripped:
        return False
    return bool(_SECTION_HEADER_RE.match(stripped))


def _extract_hero_prefix(line: str) -> str | None:
    """Extract hero name from '- HeroName: ...' bullet, e.g. 'Yamato'."""
    m = _HERO_BULLET_RE.match(line.strip())
    return m.group(1) if m else None


def _parse_sections(text: str) -> list[list[str]]:
    """Split patchnote text into semantic blocks.

    Handles three section styles:
    - Explicit headers: **Bold Text**, [ Bracket ], ## Markdown
    - Implicit hero sections: consecutive '- HeroName: ...' bullets grouped by name
    - Plain lines without prefix stay in the current block
    """
    lines = text.splitlines()
    blocks: list[list[str]] = []
    current: list[str] = []
    current_hero: str | None = None

    def _flush() -> None:
        nonlocal current, current_hero
        while current and not current[-1].strip():
            current.pop()
        if current:
            blocks.append(current)
        current = []
        current_hero = None

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Explicit section header → always starts a new block
        if _is_section_header(line):
            _flush()
            current = [line]
            current_hero = None
            i += 1
            continue

        # Blank line: peek at next non-blank to decide if new section starts
        if not stripped:
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1
            if j < len(lines):
                next_line = lines[j]
                next_hero = _extract_hero_prefix(next_line)
                next_is_header = _is_section_header(next_line)
                # New section if: explicit header coming, or different hero prefix
                if next_is_header or (
                    next_hero is not None
                    and current_hero is not None
                    and next_hero != current_hero
                ):
                    _flush()
                    i += 1
                    continue
            # Same section → keep blank as visual spacer
            current.append(line)
            i += 1
            continue

        # Bullet with hero prefix
        hero = _extract_hero_prefix(line)
        if hero is not None and current_hero is not None and hero != current_hero:
            # Hero changed mid-block → split here
            _flush()

        if hero is not None and current_hero is None:
            current_hero = hero

        current.append(line)
        i += 1

    _flush()
    return blocks


def _section_aware_chunks(text: str, limit: int) -> list[str]:
    """Split patchnote text into Discord chunks, keeping sections together.

    Splits happen *between* sections. Only if a single section exceeds
    the limit is it split at bullet boundaries with a continuation marker.
    Falls back to _smart_chunks_legacy when no headers are detected.
    """
    blocks = _parse_sections(text)

    # Fallback: no sections detected → use legacy line-based splitting
    if len(blocks) <= 1:
        return None  # type: ignore[return-value]  # signals caller to use legacy

    def _block_to_str(block_lines: list[str]) -> str:
        return "\n".join(block_lines).strip()

    def _split_large_block(block_lines: list[str]) -> list[str]:
        """Split a block that alone exceeds limit, at bullet boundaries."""
        result: list[str] = []
        header = block_lines[0] if _is_section_header(block_lines[0]) else ""
        body_lines = block_lines[1:] if header else block_lines[:]

        current_lines: list[str] = [header] if header else []
        current_len = len(header) if header else 0

        for line in body_lines:
            add_len = len(line) + (1 if current_lines else 0)
            if current_lines and current_len + add_len > limit:
                result.append("\n".join(current_lines).strip())
                current_lines = []
                current_len = 0
                add_len = len(line)

            current_lines.append(line)
            current_len += add_len

        if current_lines:
            result.append("\n".join(current_lines).strip())
        return [r for r in result if r]

    chunks: list[str] = []
    current_lines: list[str] = []
    current_len = 0

    def _flush() -> None:
        nonlocal current_lines, current_len
        if current_lines:
            chunks.append("\n".join(current_lines).strip())
        current_lines.clear()
        current_len = 0

    for block in blocks:
        block_str = _block_to_str(block)
        block_len = len(block_str)

        if block_len > limit:
            # Block is too large on its own — flush current, split block
            _flush()
            chunks.extend(_split_large_block(block))
            continue

        # Would adding this block (with blank separator) exceed limit?
        sep_len = 2 if current_lines else 0  # "\n\n"
        if current_lines and current_len + sep_len + block_len > limit:
            _flush()

        if current_lines:
            current_lines.append("")  # blank separator between sections
            current_len += 1
        current_lines.extend(block)
        current_len = len("\n".join(current_lines))

    _flush()
    return [c for c in chunks if c.strip()]


def _smart_chunks(text: str, limit: int = PATCH_CHUNK_LIMIT) -> list[str]:
    if not text:
        return []

    limit = min(max(int(limit), 1), DISCORD_MESSAGE_HARD_LIMIT)

    # Try section-aware splitting first (returns None when no headers found)
    section_result = _section_aware_chunks(text, limit)
    if section_result is not None:
        return section_result
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
    url = _normalize_patch_link(url)
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

    existing = _find_saved_changelog_row(url)
    if existing:
        deadlock_db.execute(
            """
            UPDATE changelog_posts
            SET title=?,
                url=?,
                posted_at=COALESCE(?, posted_at),
                raw_content=?,
                translated_content=?
            WHERE id=?
            """,
            (title or url, url, posted_at, raw_content, translated_content, existing["id"]),
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
    if not legacy and _is_forum_link(url):
        patch_id = _extract_patch_id(url)
        if patch_id is not None:
            legacy = deadlock_db.query_one(
                """
                SELECT id
                FROM deadlock_changelogs
                WHERE url LIKE ? OR url LIKE ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (f"%/posts/{patch_id}/%", f"%#post-{patch_id}%"),
            )
    if legacy:
        deadlock_db.execute(
            """
            UPDATE deadlock_changelogs
            SET title=?,
                url=?,
                posted_at=COALESCE(?, posted_at),
                content=?
            WHERE id=?
            """,
            (title or url, url, posted_at, raw_content, legacy[0]),
        )
    else:
        deadlock_db.execute(
            """
            INSERT INTO deadlock_changelogs(title, url, posted_at, content)
            VALUES(?,?,?,?)
            """,
            (title or url, url, posted_at, raw_content),
        )

def _normalize_patch_link(link: str | None) -> str | None:
    if not link:
        return None
    link = str(link).strip()
    if link.startswith("http://") or link.startswith("https://"):
        normalized = link
    else:
        if not link.startswith("/"):
            link = f"/{link}"
        if link.startswith("/games/"):
            normalized = f"{STEAM_COMMUNITY_BASE_URL}{link}"
        else:
            normalized = f"{FORUM_BASE_URL}{link}"

    if _is_forum_link(normalized):
        patch_id = _extract_patch_id(normalized)
        if patch_id is not None and ("/posts/" in normalized or "#post-" in normalized or "post=" in normalized):
            return f"{FORUM_BASE_URL}/posts/{patch_id}/"

    return normalized


def _extract_patch_id(url: str | None) -> int | None:
    if not url:
        return None
    normalized = str(url).strip()
    for pattern in (
        r"/posts/(\d+)/?$",
        r"#post-(\d+)\b",
        r"[?&]post=(\d+)\b",
        r"/announcements/detail/(\d+)/?$",
        r"/news/app/\d+/view/(\d+)/?$",
    ):
        match = re.search(pattern, normalized)
        if not match:
            continue
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def _is_forum_link(url: str | None) -> bool:
    return bool(url and "forums.playdeadlock.com" in str(url))


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

    normalized_posts = _dedupe_urls([_normalize_patch_link(url) for url in post_urls if url])
    latest_norm = _normalize_patch_link(latest_post_url)
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
        saved_id = _extract_patch_id(saved_norm)
        if saved_id is not None:
            candidates = [
                url
                for url in normalized_posts
                if ((pid := _extract_patch_id(url)) is not None and pid > saved_id)
            ]
        # Fallback for thread switches or mixed URL styles.
        if not candidates and latest_norm and latest_norm != saved_norm:
            latest_id = _extract_patch_id(latest_norm)
            same_forum_source = _is_forum_link(saved_norm) and _is_forum_link(latest_norm)
            if not same_forum_source or saved_id is None or latest_id is None or latest_id > saved_id:
                candidates = [latest_norm]

    if len(candidates) > MAX_CATCHUP_POSTS:
        return candidates[-MAX_CATCHUP_POSTS:], "catchup_limited"
    return candidates, "ok"


def load_last_patch_update() -> str | None:
    # 1) Primäre Quelle: zentrale Deadlock-DB
    try:
        saved = deadlock_db.get_kv(KV_NAMESPACE, KV_LAST_PATCH_KEY)
        if saved:
            return _normalize_patch_link(saved)
    except Exception as exc:
        print(f"Konnte letzten Patch-Link nicht aus DB laden: {exc}")

    # 2) Fallback: neuester Eintrag aus changelog_posts
    try:
        row = deadlock_db.query_one("SELECT url FROM changelog_posts ORDER BY id DESC LIMIT 1")
        if row and row[0]:
            return _normalize_patch_link(str(row[0]))
    except Exception as exc:
        print(f"Konnte Backup-Link aus changelog_posts nicht laden: {exc}")
    return None


def save_last_patch_update(latest_link: str) -> None:
    normalized = _normalize_patch_link(latest_link)
    if not normalized:
        return

    try:
        deadlock_db.set_kv(KV_NAMESPACE, KV_LAST_PATCH_KEY, normalized)
    except Exception as exc:
        print(f"Konnte letzten Patch-Link nicht in DB speichern: {exc}")


def load_last_test_post() -> str | None:
    try:
        saved = deadlock_db.get_kv(KV_NAMESPACE, KV_LAST_TEST_POST_KEY)
        if saved:
            return _normalize_patch_link(saved)
    except Exception as exc:
        print(f"Konnte letzten Test-Post-Link nicht aus DB laden: {exc}")
    return None


def save_last_test_post(latest_link: str) -> None:
    normalized = _normalize_patch_link(latest_link)
    if not normalized:
        return
    try:
        deadlock_db.set_kv(KV_NAMESPACE, KV_LAST_TEST_POST_KEY, normalized)
    except Exception as exc:
        print(f"Konnte letzten Test-Post-Link nicht in DB speichern: {exc}")


def changelog_already_saved(url: str) -> bool:
    try:
        return bool(_find_saved_changelog_row(url))
    except Exception as exc:
        print(f"DB-Check fuer vorhandene Patchnotes fehlgeschlagen: {exc}")
        return False

async def update_patch(url: str) -> bool:
    patch_start = perf_counter()
    channel = await _resolve_patch_channel()
    if channel is None and not PATCH_OUTPUT_DIR and not BOT_DRY_RUN:
        print(f"Konnte Channel {channel_id} nicht finden.")
        return False

    patch_data = await asyncio.to_thread(changelog_content_fetcher.process, url)
    if not patch_data or not patch_data.get("content"):
        print(f"Keine Patchnotes unter {url} gefunden.")
        return False
    canonical_url = patch_data.get("url") or url
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
        url=canonical_url,
        posted_at_raw=posted_raw,
        posted_at_utc=posted_utc_label,
        posted_at_local=posted_local_label,
        lag_s=f"{lag_seconds:.1f}" if lag_seconds is not None else None,
        raw_len=len(patch_content),
    )

    response = await _translate_patch_content(
        patch_content,
        include_ping=PATCH_AUTO_INCLUDE_PING,
        context_label=canonical_url,
    )
    response = _strip_role_ping(response)

    try:
        save_changelog_to_db(
            url=canonical_url,
            title=patch_data.get("title"),
            posted_at=patch_data.get("posted_at"),
            raw_content=patch_content,
            translated_content=response,
        )
    except Exception as exc:
        print(f"Konnte Patch nicht in Deadlock-DB speichern: {exc}")

    await patch_response(
        channel,
        response,
        url=canonical_url,
        posted_at=patch_data.get("posted_at"),
        include_ping=PATCH_AUTO_INCLUDE_PING,
    )
    _timing_log(
        "patch_pipeline_done",
        url=canonical_url,
        total_duration_s=f"{(perf_counter() - patch_start):.2f}",
    )
    return True


async def patch_response(
    channel,
    response_content,
    url: str | None = None,
    posted_at: str | None = None,
    *,
    include_ping: bool = False,
):
    send_start = perf_counter()
    cleaned = _strip_code_fences(response_content)
    cleaned = _remove_inline_citations(cleaned)
    cleaned = _remove_links(cleaned)
    cleaned = _strip_role_ping(cleaned)
    cleaned = _repair_known_hero_sections(cleaned)
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
    role_ping = _get_role_ping() if include_ping and chunks else None
    if role_ping:
        await channel.send(role_ping)
    _timing_log(
        "discord_send_done",
        url=url,
        chunks=len(chunks) + (1 if role_ping else 0),
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
    return _normalize_patch_link(row["url"]), row["title"], row["posted_at"], row["raw_content"]


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

    await patch_response(
        channel,
        response,
        url=url,
        posted_at=posted_at,
        include_ping=include_ping,
    )


def _unpack_latest_info(latest_info) -> tuple[str | None, str | None, list[str]]:
    latest_thread_url = None
    latest_post_url = None
    if isinstance(latest_info, dict):
        latest_thread_url = _normalize_patch_link(
            latest_info.get("thread_url") or latest_info.get("thread_link")
        )
        latest_post_url = _normalize_patch_link(
            latest_info.get("latest_post_url")
            or latest_info.get("thread_url")
            or latest_info.get("thread_link")
        )
        post_urls = [
            _normalize_patch_link(p) for p in latest_info.get("post_urls", []) or []
        ]
    else:
        latest_post_url = _normalize_patch_link(latest_info)
        post_urls = []

    if not latest_post_url and latest_thread_url:
        latest_post_url = latest_thread_url

    return latest_thread_url, latest_post_url, [p for p in post_urls if p]


async def maybe_post_latest_patch_for_test(saved_last_patch):
    if not PATCH_FORCE_POST_LATEST_ON_START:
        return saved_last_patch

    try:
        latest_info = await asyncio.to_thread(changelog_latest_fetcher.check_latest)
    except Exception as exc:
        print(f"Fehler beim Abrufen des neuesten Patches fuer Test-Post: {exc}")
        return saved_last_patch

    _, latest_post_url, _ = _unpack_latest_info(latest_info)
    if not latest_post_url:
        return saved_last_patch

    last_test_post = load_last_test_post()
    saved_norm = _normalize_patch_link(saved_last_patch)
    if last_test_post == latest_post_url or saved_norm == latest_post_url or changelog_already_saved(latest_post_url):
        print(f"Testmodus uebersprungen, Patch bereits verarbeitet: {latest_post_url}")
        save_last_patch_update(latest_post_url)
        save_last_test_post(latest_post_url)
        return latest_post_url

    print(f"Testmodus aktiv: Poste neuesten Patch einmalig in Kanal {channel_id}: {latest_post_url}")
    try:
        posted = await update_patch(latest_post_url)
    except Exception as exc:
        print(f"Fehler beim Test-Post des neuesten Patches: {exc}")
        return saved_last_patch

    if not posted:
        return saved_last_patch

    save_last_patch_update(latest_post_url)
    save_last_test_post(latest_post_url)
    return latest_post_url


async def fetch_and_maybe_post(saved_last_patch, force: bool = False):
    scan_start = perf_counter()
    _timing_log(
        "scan_start",
        force=force,
        saved_last_patch=_normalize_patch_link(saved_last_patch),
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
        return saved_last_patch

    latest_thread_url, latest_post_url, post_urls = _unpack_latest_info(latest_info)

    if not latest_post_url:
        _timing_log(
            "scan_no_latest_post_url",
            duration_s=f"{(perf_counter() - scan_start):.2f}",
        )
        return saved_last_patch

    saved_norm = _normalize_patch_link(saved_last_patch)

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
        if (
            saved_raw
            and main_raw
            and url != latest_thread_url
            and _is_forum_link(url)
            and _is_forum_link(latest_thread_url)
        ):
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
        save_last_patch_update(latest_post_url)
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
            posted = await update_patch(url)
            if not posted:
                continue
            save_last_patch_update(url)
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
    return last_processed or saved_last_patch


async def _scan_loop():
    saved_last_patch = load_last_patch_update()
    _timing_log("scan_loop_start", saved_last_patch=saved_last_patch, interval_s=CHECK_INTERVAL_SECONDS)

    try:
        saved_last_patch = await maybe_post_latest_patch_for_test(saved_last_patch)
        saved_last_patch = await fetch_and_maybe_post(saved_last_patch, force=True)
        while not stop_event.is_set():
            loop_tick_start = perf_counter()
            saved_last_patch = await fetch_and_maybe_post(saved_last_patch)
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
