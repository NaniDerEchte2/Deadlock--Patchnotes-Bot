import os
import sys
import time
import json
import sqlite3
import logging
import asyncio
import io
from typing import Optional, Tuple, Dict, Any, List

import requests

# -------------------- .env LOADER (ohne Abhängigkeiten) -------------------- #
def _parse_env_line(line: str):
    s = line.strip()
    if not s or s.startswith("#"):
        return None
    if s.lower().startswith("export "):
        s = s[7:].lstrip()
    if "=" not in s:
        return None
    k, v = s.split("=", 1)
    k, v = k.strip(), v.strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
        quote = v[0]
        v = v[1:-1]
        if quote == '"':
            v = (v.replace("\\n", "\n")
                   .replace("\\r", "\r")
                   .replace("\\t", "\t")
                   .replace('\\"', '"')
                   .replace("\\\\", "\\"))
    return k, v

def _load_env_file(path: str) -> Dict[str, str]:
    out = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                kv = _parse_env_line(line)
                if kv:
                    out[kv[0]] = kv[1]
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"Warnung: .env konnte nicht gelesen werden ({path}): {e}", file=sys.stderr)
    return out

def load_dotenv_fallback() -> None:
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        script_dir = os.getcwd()
    cwd = os.getcwd()
    candidates = [
        os.path.join(script_dir, ".env.local"),
        os.path.join(script_dir, ".env"),
        os.path.join(cwd, ".env.local"),
        os.path.join(cwd, ".env"),
    ]
    merged: Dict[str, str] = {}
    for p in candidates:
        for k, v in _load_env_file(p).items():
            if k not in merged:
                merged[k] = v
    for k, v in merged.items():
        if k not in os.environ:
            os.environ[k] = v

load_dotenv_fallback()
# --------------------------------------------------------------------------- #

# ---------- Konfiguration ----------
DISCORD_WEBHOOK = (os.getenv("DISCORD_WEBHOOK") or "").strip()
WATCH_LIST = os.getenv("WATCH_LIST", "SteamDatabase/GameTracking-Deadlock@master")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SEC", "60"))
DB_PATH = os.getenv("DB_PATH", "./commit_watcher.sqlite3")
GITHUB_TOKEN = (os.getenv("GITHUB_TOKEN") or "").strip()

ATTACH_PATCH = (os.getenv("ATTACH_PATCH", "false").lower() == "true")
MAX_FILES_IN_EMBED = max(1, int(os.getenv("MAX_FILES_IN_EMBED", "6")))
MAX_DIFF_LINES_PER_FILE = max(3, int(os.getenv("MAX_DIFF_LINES_PER_FILE", "12")))

DISCORD_BOT_TOKEN = (os.getenv("DISCORD_BOT_TOKEN") or "").strip()
TEST_CHANNEL_ID = int(os.getenv("TEST_CHANNEL_ID", "0")) if os.getenv("TEST_CHANNEL_ID") else 0
TEST_COMMAND_NAME = os.getenv("TEST_COMMAND_NAME", "!tcom").strip() or "!tcom"

if not DISCORD_WEBHOOK:
    print("ERROR: DISCORD_WEBHOOK ist nicht gesetzt (ENV oder .env).", file=sys.stderr)
    sys.exit(2)
if not DISCORD_WEBHOOK.startswith("https://"):
    print("WARNUNG: DISCORD_WEBHOOK scheint keine valide HTTPS-URL zu sein.", file=sys.stderr)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("commit-watcher")

# ---------- DB Layer ----------
DDL = """
CREATE TABLE IF NOT EXISTS state (
  repo TEXT NOT NULL,
  branch TEXT NOT NULL,
  last_commit_sha TEXT,
  etag TEXT,
  PRIMARY KEY (repo, branch)
);
"""

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(DDL)
    return conn

def db_get_state(conn: sqlite3.Connection, repo: str, branch: str) -> Tuple[Optional[str], Optional[str]]:
    cur = conn.execute("SELECT last_commit_sha, etag FROM state WHERE repo=? AND branch=?", (repo, branch))
    row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)

def db_set_state(conn: sqlite3.Connection, repo: str, branch: str, sha: Optional[str], etag: Optional[str]) -> None:
    conn.execute(
        "INSERT INTO state(repo,branch,last_commit_sha,etag) VALUES(?,?,?,?) "
        "ON CONFLICT(repo,branch) DO UPDATE SET last_commit_sha=excluded.last_commit_sha, etag=excluded.etag",
        (repo, branch, sha, etag)
    )
    conn.commit()

# ---------- GitHub API ----------
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "deadlock-discord-commit-watcher/1.2"})
if GITHUB_TOKEN:
    SESSION.headers.update({"Authorization": f"Bearer {GITHUB_TOKEN}"})

def gh_list_commits(repo: str, branch: str, etag: Optional[str]):
    url = f"https://api.github.com/repos/{repo}/commits"
    params = {"sha": branch, "per_page": 10}
    headers = {"Accept": "application/vnd.github+json"}
    if etag:
        headers["If-None-Match"] = etag
    resp = SESSION.get(url, params=params, headers=headers, timeout=30)
    new_etag = resp.headers.get("ETag")
    if resp.status_code == 304:
        return 304, new_etag, None
    if resp.status_code != 200:
        log.warning("GitHub API %s -> %s %s", url, resp.status_code, resp.text[:200])
        return resp.status_code, new_etag, None
    try:
        data = resp.json()
        if not isinstance(data, list):
            raise ValueError("Unexpected JSON for list commits")
        return 200, new_etag, data
    except Exception as e:
        log.exception("JSON-Parse list commits: %s", e)
        return resp.status_code, new_etag, None

def gh_get_commit(repo: str, sha: str) -> Optional[Dict[str, Any]]:
    url = f"https://api.github.com/repos/{repo}/commits/{sha}"
    resp = SESSION.get(url, headers={"Accept": "application/vnd.github+json"}, timeout=30)
    if resp.status_code != 200:
        log.warning("GitHub API commit %s -> %s %s", url, resp.status_code, resp.text[:200])
        return None
    return resp.json()

def gh_get_patch_bytes(repo: str, sha: str) -> Optional[bytes]:
    # offizielle Patch-Ansicht
    url = f"https://github.com/{repo}/commit/{sha}.patch"
    resp = SESSION.get(url, timeout=30)
    if resp.status_code != 200:
        log.warning("Patch laden fehlgeschlagen %s -> %s", url, resp.status_code)
        return None
    return resp.content

# ---------- Discord Webhook ----------
def discord_post_embed(embed: Dict[str, Any]) -> bool:
    try:
        r = SESSION.post(DISCORD_WEBHOOK, json={"embeds": [embed]}, timeout=30)
        return r.status_code in (200, 204)
    except Exception:
        log.exception("Discord Webhook Fehler")
        return False

def discord_post_embed_with_file(embed: Dict[str, Any], filename: str, filebytes: bytes) -> bool:
    try:
        files = {"file": (filename, filebytes, "text/plain")}
        payload = {"payload_json": json.dumps({"embeds": [embed]})}
        r = SESSION.post(DISCORD_WEBHOOK, data=payload, files=files, timeout=60)
        return r.status_code in (200, 204)
    except Exception:
        log.exception("Discord Webhook Upload Fehler")
        return False

# ---------- Helpers ----------
def escape_md(s: str) -> str:
    for ch in ("*", "_", "`", "~"):
        s = s.replace(ch, f"\\{ch}")
    return s

def parse_watch_list(raw: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for item in [x.strip() for x in raw.split(",") if x.strip()]:
        if "@" in item:
            repo, branch = item.split("@", 1)
        else:
            repo, branch = item, "master"
        out.append((repo, branch))
    return out

def build_overview_embed(repo: str, branch: str, commits: List[Dict[str, Any]]) -> Dict[str, Any]:
    show = commits[:5]
    lines = []
    for c in show:
        sha = (c.get("sha") or "")[:7]
        msg = (c.get("commit", {}).get("message") or "").strip().splitlines()[0]
        author = (c.get("commit", {}).get("author", {}).get("name")) or (c.get("author") or {}).get("login") or "unknown"
        url = c.get("html_url")
        lines.append(f"- [`{sha}`]({url}) {escape_md(msg)} — {escape_md(author)}")
    desc = "\n".join(lines) if lines else "Keine Details verfügbar."
    repo_url = f"https://github.com/{repo}"
    compare_url = f"https://github.com/{repo}/commits/{branch}"
    title = f"Pushed {len(commits)} commit{'s' if len(commits)!=1 else ''} to {branch}"
    return {
        "title": title,
        "url": compare_url,
        "description": desc,
        "color": 0x7289DA,
        "author": {"name": repo, "url": repo_url},
        "footer": {"text": "GitHub → Discord"},
    }

def truncate_text(s: str, limit: int) -> str:
    if len(s) <= limit:
        return s
    return s[: max(0, limit - 1)].rstrip() + "…"

def build_single_commit_embed(repo: str, commit: Dict[str, Any]) -> Dict[str, Any]:
    sha_full = commit.get("sha") or ""
    sha7 = sha_full[:7]
    msg_full = (commit.get("commit", {}).get("message") or "").strip()
    title_line = msg_full.splitlines()[0] if msg_full else sha7
    author = (commit.get("commit", {}).get("author", {}).get("name")) or (commit.get("author") or {}).get("login") or "unknown"
    html_url = commit.get("html_url") or f"https://github.com/{repo}/commit/{sha_full}"

    files = commit.get("files") or []
    added = sum(1 for f in files if (f.get("status") == "added"))
    removed = sum(1 for f in files if (f.get("status") == "removed"))
    modified = sum(1 for f in files if (f.get("status") not in ("added","removed")))

    desc = f"**{escape_md(title_line)}**\n"
    desc += f"`{sha7}` by {escape_md(author)} • {added} ⊕  {removed} ⊖  {modified} ✎\n"
    desc += f"[Commit anzeigen]({html_url})\n"

    embed: Dict[str, Any] = {
        "title": f"Commit {sha7}",
        "url": html_url,
        "description": truncate_text(desc, 4096),
        "color": 0x43B581,
        "author": {"name": repo, "url": f"https://github.com/{repo}"},
        "footer": {"text": "GitHub → Discord (Diff preview)"},
        "fields": []
    }

    # Dateiliste + Diff-Snippets
    count = 0
    for f in files:
        if count >= MAX_FILES_IN_EMBED or len(embed["fields"]) >= 25:
            break
        fname = f.get("filename") or "(unbenannt)"
        patch = f.get("patch") or ""
        # nur +/- und Kontextzeilen, hart kürzen
        lines = patch.splitlines()
        # Nimm die ersten sinnvollen hunk-zeilen
        snippet_lines = []
        for ln in lines:
            if ln.startswith(("@@", "+", "-", " ")):
                snippet_lines.append(ln)
            if len(snippet_lines) >= MAX_DIFF_LINES_PER_FILE:
                break
        if not snippet_lines:
            snippet_lines = ["(kein Patch verfügbar)"]
        codeblock = "```diff\n" + "\n".join(snippet_lines) + "\n```"
        embed["fields"].append({
            "name": truncate_text(fname, 256),
            "value": truncate_text(codeblock, 1024),
            "inline": False
        })
        count += 1

    return embed

# ---------- Watcher ----------
def process_repo(conn: sqlite3.Connection, repo: str, branch: str) -> None:
    last_sha, etag = db_get_state(conn, repo, branch)
    status, new_etag, commits = gh_list_commits(repo, branch, etag)
    if status == 304:
        return
    if status != 200 or not commits:
        return

    new_items: List[Dict[str, Any]] = []
    if last_sha:
        for c in commits:
            if c.get("sha") == last_sha:
                break
            new_items.append(c)
    else:
        new_items = [commits[0]]  # Erstlauf: nur neuesten posten

    if not new_items:
        # nur ETag/State aktualisieren
        db_set_state(conn, repo, branch, last_sha or commits[0]["sha"], new_etag)
        return

    if len(new_items) == 1:
        # Hol Detaildaten für Single-Commit und rendere Diff
        sha = new_items[0].get("sha")
        detail = gh_get_commit(repo, sha)
        if detail:
            embed = build_single_commit_embed(repo, detail)
            if ATTACH_PATCH:
                patch_bytes = gh_get_patch_bytes(repo, sha) or b""
                ok = discord_post_embed_with_file(embed, f"{sha}.patch", patch_bytes)
            else:
                ok = discord_post_embed(embed)
            if ok:
                db_set_state(conn, repo, branch, sha, new_etag)
                log.info("[%s@%s] 1 Commit gemeldet (%s)", repo, branch, sha[:7])
                return

    # Fallback / mehrere Commits: Übersicht
    embed = build_overview_embed(repo, branch, new_items)
    if discord_post_embed(embed):
        newest_sha = new_items[0]["sha"]
        db_set_state(conn, repo, branch, newest_sha, new_etag)
        log.info("[%s@%s] %d Commits gemeldet (overview), last=%s", repo, branch, len(new_items), newest_sha[:7])

async def watcher_main():
    watch = parse_watch_list(WATCH_LIST)
    if not watch:
        log.error("WATCH_LIST leer – nichts zu tun.")
        sys.exit(1)
    conn = db_connect()
    log.info("Watcher gestartet. Interval=%ss, DB=%s, Repos=%s",
             POLL_INTERVAL, DB_PATH, ", ".join([f"{r}@{b}" for r,b in watch]))
    try:
        while True:
            for repo, branch in watch:
                try:
                    process_repo(conn, repo, branch)
                except Exception:
                    log.exception("Fehler bei %s@%s", repo, branch)
                await asyncio.sleep(1.5)
            await asyncio.sleep(POLL_INTERVAL)
    except asyncio.CancelledError:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ---------- Optional: Discord Bot (Test Command) ----------
_bot_started = False
def bot_enabled() -> bool:
    return bool(DISCORD_BOT_TOKEN)

async def run_bot_and_watcher():
    """Startet Bot (falls Token gesetzt) und den Watcher im selben Eventloop."""
    global _bot_started
    if not bot_enabled():
        # Nur Watcher
        await watcher_main()
        return

    import discord
    from discord.ext import commands

    intents = discord.Intents.none()
    intents.guilds = True
    intents.message_content = True  # nötig für Prefix-Command

    bot = commands.Bot(command_prefix=commands.when_mentioned_or(TEST_COMMAND_NAME.split()[0]),
                       intents=intents, help_command=None)

    @_safe_name
    def cmd_name():
        # erlaubt z.B. "!tcom"
        return TEST_COMMAND_NAME

    @bot.event
    async def on_ready():
        nonlocal_var = "ok"  # placeholder to keep function non-empty; no-op
        log.info("Bot eingeloggt als %s (id=%s)", bot.user, bot.user.id)
        _bot_started = True

    def channel_allowed(channel_id: int) -> bool:
        return (TEST_CHANNEL_ID == 0) or (channel_id == TEST_CHANNEL_ID)

    @bot.command(name=TEST_COMMAND_NAME.lstrip("!"))
    async def test_commit(ctx: commands.Context, commit_url: str = None):
        if not channel_allowed(ctx.channel.id):
            return
        await ctx.trigger_typing()

        try:
            # Ziel: erster Eintrag aus WATCH_LIST
            first_repo, first_branch = parse_watch_list(WATCH_LIST)[0]
        except Exception:
            await ctx.reply("WATCH_LIST ist nicht korrekt konfiguriert.", mention_author=False)
            return

        sha = None
        repo = first_repo

        # Wenn eine URL übergeben wurde, extrahiere SHA & ggf. Repo
        if commit_url:
            # Erwartete Form: https://github.com/OWNER/REPO/commit/<sha>
            try:
                parts = commit_url.split("/")
                idx = parts.index("github.com")
                owner = parts[idx+1]; repo_name = parts[idx+2]
                if parts[idx+3] == "commit":
                    sha = parts[idx+4].split(".")[0]
                    repo = f"{owner}/{repo_name}"
            except Exception:
                sha = None

        detail = None
        if sha:
            detail = gh_get_commit(repo, sha)

        if not detail:
            # hole neuesten Commit des Branches
            status, _etag, commits = gh_list_commits(repo, first_branch, etag=None)
            if status != 200 or not commits:
                await ctx.reply("Konnte Commits nicht laden.", mention_author=False)
                return
            sha = commits[0]["sha"]
            detail = gh_get_commit(repo, sha)
            if not detail:
                await ctx.reply("Konnte Commit-Details nicht laden.", mention_author=False)
                return

        embed_dict = build_single_commit_embed(repo, detail)
        # Kennzeichne Test
        embed_dict["title"] = "[TEST] " + embed_dict.get("title", f"Commit {sha[:7]}")
        embed_dict["color"] = 0xFAA61A

        # Senden als Bot (nicht per Webhook), optional .patch anhängen
        if ATTACH_PATCH:
            patch = gh_get_patch_bytes(repo, sha)
            if patch:
                file = discord.File(io.BytesIO(patch), filename=f"{sha}.patch")
                await ctx.send(embed=discord.Embed.from_dict(embed_dict), file=file)
                return

        await ctx.send(embed=discord.Embed.from_dict(embed_dict))

    # Watcher als Hintergrund-Task
    async def bg_watcher():
        try:
            await watcher_main()
        except Exception:
            log.exception("Watcher-Task abgestürzt")

    watcher_task = asyncio.create_task(bg_watcher())
    try:
        await bot.start(DISCORD_BOT_TOKEN)
    finally:
        watcher_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watcher_task

# kleine Deko um statische lints zu beruhigen (keine sicherheitsrelevante Wirkung)
def _safe_name(fn):
    return fn

# ---------- main ----------
def main():
    if bot_enabled():
        asyncio.run(run_bot_and_watcher())
    else:
        asyncio.run(watcher_main())

if __name__ == "__main__":
    main()
