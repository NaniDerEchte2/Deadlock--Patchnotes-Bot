import json
import os
import re
import time

import dotenv
import requests
from requests import exceptions as req_exc

dotenv.load_dotenv()

api_key = os.getenv("PERPLEXITY_API_KEY")
ROLE_PING = "<@&1330994309524357140>"
MODEL = os.getenv("PERPLEXITY_MODEL", "sonar-pro")
DEFAULT_MAX_TOKENS = int(os.getenv("PERPLEXITY_MAX_TOKENS", "4000"))

url = "https://api.perplexity.ai/chat/completions"

KNOWN_HERO_NAMES = (
    "Infernus",
    "Seven",
    "Vindicta",
    "Lady Geist",
    "Abrams",
    "Wraith",
    "McGinnis",
    "Paradox",
    "Dynamo",
    "Kelvin",
    "Haze",
    "Holliday",
    "Bebop",
    "Calico",
    "Grey Talon",
    "Mo & Krill",
    "Shiv",
    "Ivy",
    "Warden",
    "Yamato",
    "Lash",
    "Viscous",
    "Pocket",
    "Mirage",
    "Vyper",
    "Sinclair",
    "Mina",
    "Drifter",
    "Venator",
    "Victor",
    "Paige",
    "The Doorman",
    "Billy",
    "Graves",
    "Apollo",
    "Rem",
    "Silver",
    "Celeste",
)
KNOWN_HERO_NAMES_TEXT = ", ".join(KNOWN_HERO_NAMES)
_SECTION_HEADER_RE = re.compile(
    r"^(?:\*\*\[\s*(.+?)\s*\]\*\*|\[\s*(.+?)\s*\]|#{1,3}\s+(.+?)|\*\*(.+?)\*\*)\s*$"
)
_BOLD_SUBHEADER_RE = re.compile(r"^\*\*(.+?)\*\*\s*$")
_HERO_NAME_ALIASES = {
    "doorman": "the doorman",
}
DISCORD_MARKDOWN_FORMATTING = """
5. Discord-Textformatierung:
   - Nutze echte Discord-Markdown-Syntax, nicht nur allgemein "Discord-geeignete" Formatierung
   - Verwende '### ' fuer die Hauptrubrik
   - Verwende '## ' fuer Hauptabschnitte wie General, Items und Heroes
   - Verwende '**...**' fuer Themen-, Hero- und Item-Ueberschriften innerhalb eines Abschnitts
   - Verwende '-' fuer Listenpunkte
   - Verwende '__...__', '*...*', '***...***', '-# ' oder '> ' nur wenn es inhaltlich wirklich passt
   - Keine Code-Bloecke, keine Blockquotes und kein Durchstreichen fuer normale Patchnotes-Struktur verwenden
   - Keine Links, keine maskierten Links und keine URLs ausgeben
   - Keine Klammer-Header wie '[ Heroes ]' oder '**[ Heroes ]**' ausgeben
   - Keine ungueltigen Markdown-Mischformen oder dekorative Sonderformatierung erfinden
"""


def _normalize_hero_name(name: str) -> str:
    normalized = (name or "").casefold()
    normalized = normalized.replace("&", " and ")
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    normalized = " ".join(normalized.split())
    return _HERO_NAME_ALIASES.get(normalized, normalized)


_KNOWN_HERO_NAME_KEYS = {_normalize_hero_name(name) for name in KNOWN_HERO_NAMES}
_CANONICAL_HERO_NAME_BY_KEY = {
    _normalize_hero_name(name): name for name in KNOWN_HERO_NAMES
}


def is_known_hero_name(name: str) -> bool:
    return _normalize_hero_name(name) in _KNOWN_HERO_NAME_KEYS


def canonical_hero_name(name: str | None) -> str | None:
    return _CANONICAL_HERO_NAME_BY_KEY.get(_normalize_hero_name(name or ""))


def _format_section_header(section_kind: str) -> str:
    return f"## {section_kind.title()}"


def _extract_section_kind(line: str) -> str | None:
    match = _SECTION_HEADER_RE.match((line or "").strip())
    if not match:
        return None
    raw = next((group for group in match.groups() if group), "").strip().casefold()
    if raw == "heroes":
        return "heroes"
    if raw == "items":
        return "items"
    if raw == "general":
        return "general"
    return None


def _extract_bold_subheader(line: str) -> str | None:
    stripped = (line or "").strip()
    if not stripped:
        return None
    if _extract_section_kind(stripped):
        return None
    match = _BOLD_SUBHEADER_RE.match(stripped)
    if not match:
        return None
    return match.group(1).strip()


def _collect_section_block(lines: list[str], start: int) -> tuple[list[str], int]:
    first_line = lines[start]
    heading = _extract_bold_subheader(first_line)
    block = [first_line]
    index = start + 1

    if heading is None:
        return block, index

    while index < len(lines):
        line = lines[index]
        if _extract_section_kind(line) is not None or _extract_bold_subheader(line) is not None:
            break
        block.append(line)
        index += 1
    return block, index


def repair_known_hero_sections(text: str) -> str:
    if not text:
        return text

    lines = text.splitlines()
    if not lines:
        return text

    output: list[str] = []
    current_section_kind: str | None = None
    active_section_kind: str | None = None
    section_has_content = False
    last_section_header_index: int | None = None
    index = 0

    while index < len(lines):
        line = lines[index]
        section_kind = _extract_section_kind(line)
        if section_kind is not None:
            output.append(_format_section_header(section_kind))
            current_section_kind = section_kind
            active_section_kind = section_kind
            section_has_content = False
            last_section_header_index = len(output) - 1
            index += 1
            continue

        if current_section_kind not in {"heroes", "items"}:
            output.append(line)
            index += 1
            continue

        block, next_index = _collect_section_block(lines, index)
        heading = _extract_bold_subheader(block[0]) if block else None
        if heading is None:
            canonical_heading = None
            block_kind = active_section_kind or current_section_kind
        else:
            canonical_heading = canonical_hero_name(heading)
            block_kind = "heroes" if canonical_heading else "items"

        if canonical_heading and block:
            leading = block[0][: len(block[0]) - len(block[0].lstrip())]
            block[0] = f"{leading}**{canonical_heading}**"

        if block_kind != active_section_kind:
            replacement = _format_section_header(block_kind)
            if (
                not section_has_content
                and last_section_header_index is not None
                and last_section_header_index < len(output)
            ):
                output[last_section_header_index] = replacement
            else:
                output.append(replacement)
                last_section_header_index = len(output) - 1
                if block and block[0].strip():
                    output.append("")
            active_section_kind = block_kind

        output.extend(block)
        if any(line.strip() for line in block):
            section_has_content = True
        index = next_index

    return "\n".join(output).strip()

system_prompt_base = f"""Du bist ein Deadlock-Patchnotes-Uebersetzer fuer Discord.

Uebersetze die folgenden Deadlock Patchnotes ins Deutsche und formatiere sie fuer Discord:

1. Hauptstruktur:
   - Beginne mit '### Deadlock Patch Notes' als Hauptrubrik
   - Verwende echte Discord-Kopfzeilen fuer Hauptkategorien, z.B. '## General', '## Items', '## Heroes'
   - Wenn ein Block inhaltlich Items und Heroes mischt, trenne ihn in passende Hauptkategorien statt alles unter einer falschen Überschrift zu lassen

2. Unterstruktur (WICHTIG):
   - Innerhalb jedes Abschnitts: Gruppiere thematisch zusammengehoerige Aenderungen unter '**Thema**' als Unterueberschrift
   - Beispiele für General: **Shrine**, **Troopers**, **Walkers & Guardians**, **Mid Boss**, **Map**, **Allgemein** 
   - Beispiele für Items: Jedes Item mit mehreren Änderungen bekommt eine eigene '**Item-Name**' Überschrift
   - Beispiele für Heroes: Jeder Held bekommt eine '**Heldenname**' Ueberschrift, darunter seine Ability-Änderungen als '-' Bullets
   - Ability- oder Item-Namen duerfen nicht als Helden-Überschrift missverstanden werden
   - Unter Heroes  sollen nur echte Heldennamen als Unterueberschriften stehen
   - Als gueltige Heldennamen gelten nur: {KNOWN_HERO_NAMES_TEXT}
   - 'Doorman' ist als Kurzform fuer 'The Doorman' erlaubt
   - Einzelne Aenderungen ohne passende Gruppe kommen direkt als '-' Bullet ohne extra Ueberschrift
   - Verwende '-' fuer alle Aufzaehlungspunkte

3. Inhalt:
   - Uebersetze alle Texte ins Deutsche, AUSSER Eigennamen, Item-Bezeichnungen und Game-Mechaniken wie Melee Parry oder Souls
   - Verwende nur die gegebenen Informationen, keine externen Quellen
   - Ignoriere Bilder oder Links im Originaltext
   - Gib keine Links, keine maskierten Links und keine URLs aus
   - Behalte die Reihenfolge der Aenderungen innerhalb jeder Gruppe bei

4. Formatierung:
   - Halte dich an Discord-Formatierungsrichtlinien
   - Keine **Kurzzusammenfassung** ausgeben

{DISCORD_MARKDOWN_FORMATTING}
"""

strict_system_prompt = (
    system_prompt_base
    + """

WICHTIG:
- Auch wenn der Input sehr kurz ist (z.B. nur 1 Bullet), trotzdem normal uebersetzen und formatieren.
- Keine Meta-Texte wie "Ich kann diese Anfrage nicht erfuellen", "keine Patchnotes bereitgestellt", "Sucherergebnisse ...".
- Keine Rueckfragen an den User.
- Keine **Kurzzusammenfassung** ausgeben.
- Gib ausschliesslich die finale formatierte Antwort aus.
"""
)

partial_system_prompt = f"""Du bist ein Deadlock-Patchnotes-Uebersetzer fuer Discord.

Uebersetze den folgenden Ausschnitt eines groesseren Deadlock-Patchnotes-Posts ins Deutsche und formatiere ihn fuer Discord:

1. Struktur:
   - Uebersetze nur den gegebenen Ausschnitt
   - Behalte die Reihenfolge der Zeilen exakt bei
   - Verwende echte Discord-Kopfzeilen fuer Hauptkategorien falls vorhanden, z.B. '## General', '## Items', '## Heroes'
   - Wenn der Ausschnitt inhaltlich Items und Heroes mischt, trenne sie in passende Hauptkategorien

2. Unterstruktur (WICHTIG):
   - Innerhalb jedes Abschnitts: Gruppiere thematisch zusammengehoerige Aenderungen unter '**Thema**' als Unterueberschrift
   - Beispiele fuer [ General ]: **Shrine**, **Troopers**, **Walkers & Guardians**, **Mid Boss**, **Map**, **Allgemein**
   - Beispiele fuer [ Items ]: Jedes Item mit mehreren Aenderungen bekommt eine eigene '**Item-Name**' Ueberschrift
   - Beispiele fuer [ Heroes ]: Jeder Held bekommt eine '**Heldenname**' Ueberschrift, darunter seine Ability-Aenderungen als '-' Bullets
   - Ability- oder Item-Namen duerfen nicht als Helden-Ueberschrift missverstanden werden
   - Unter **[ Heroes ]** sollen nur echte Heldennamen als Unterueberschriften stehen
   - Als gueltige Heldennamen gelten nur: {KNOWN_HERO_NAMES_TEXT}
   - 'Doorman' ist als Kurzform fuer 'The Doorman' erlaubt
   - Einzelne Aenderungen ohne passende Gruppe kommen direkt als '-' Bullet ohne extra Ueberschrift
   - Verwende '-' fuer alle Aufzaehlungspunkte

3. Inhalt:
   - Uebersetze alle Texte ins Deutsche, AUSSER Eigennamen, Item-Bezeichnungen und Game-Mechaniken wie Melee Parry oder Souls
   - Verwende nur die gegebenen Informationen, keine externen Quellen
   - Ignoriere Bilder oder Links im Originaltext
   - Gib keine Links, keine maskierten Links und keine URLs aus

4. Discord-Textformatierung:
   - Nutze echte Discord-Markdown-Syntax
   - Verwende '## ' fuer Hauptabschnitte wie General, Items und Heroes
   - Verwende '**...**' fuer Themen-, Hero- und Item-Ueberschriften innerhalb eines Abschnitts
   - Verwende '-' fuer Listenpunkte
   - Keine Code-Bloecke, keine Blockquotes und kein Durchstreichen fuer normale Patchnotes-Struktur verwenden
   - Keine Klammer-Header wie '[ Heroes ]' oder '**[ Heroes ]**' ausgeben
   - Keine ungueltigen Markdown-Mischformen oder dekorative Sonderformatierung erfinden

WICHTIG:
- Dies ist nur ein Teil eines groesseren Patchnotes-Posts.
- Kein '### Deadlock Patch Notes' Titel ausgeben.
- Keine Einleitung, keine Erklaerung, keine Rueckfragen.
- Keine **Kurzzusammenfassung** ausgeben.
- Gib ausschliesslich den uebersetzten Ausschnitt aus.
"""

partial_strict_system_prompt = (
    partial_system_prompt
    + """

WICHTIG:
- Auch bei sehr kurzen Ausschnitten normal uebersetzen.
- Keine Meta-Texte wie "Ich kann diese Anfrage nicht erfuellen", "keine Patchnotes bereitgestellt", "Sucherergebnisse ...".
- Gib ausschliesslich die finale formatierte Antwort aus.
"""
)

_BAD_RESPONSE_MARKERS = (
    "ich kann diese anfrage nicht erfuellen",
    "ich kann diese anfrage nicht erfüllen",
    "keine patchnotes bereitgestellt",
    "keine patchnotes zum uebersetzen bereitgestellt",
    "keine patchnotes zum übersetzen bereitgestellt",
    "sucherergebnisse",
    "discord-markdown-formatierung",
    "um ihnen zu helfen, benoetige ich",
    "um ihnen zu helfen, benötige ich",
    "bitte bestaetigen sie",
    "bitte bestätigen sie",
)


def _normalize_text(text: str) -> str:
    return " ".join((text or "").strip().lower().split())


def is_unusable_translation(text: str | None) -> bool:
    normalized = _normalize_text(text or "")
    if not normalized:
        return True
    return any(marker in normalized for marker in _BAD_RESPONSE_MARKERS)


def extract_answer_text(api_response: dict) -> str:
    try:
        return str(api_response["choices"][0]["message"]["content"]).strip()
    except Exception:
        return ""


def _build_messages(
    content: str,
    include_ping: bool,
    strict_mode: bool,
    partial_mode: bool = False,
) -> list[dict]:
    if partial_mode:
        system_prompt = partial_strict_system_prompt if strict_mode else partial_system_prompt
    else:
        system_prompt = strict_system_prompt if strict_mode else system_prompt_base
    user_prompt = (
        "Hier sind die Patchnotes. Nutze nur den folgenden Block:\n"
        "<PATCHNOTES>\n"
        f"{content}\n"
        "</PATCHNOTES>"
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def fetch_answer(
    content,
    include_ping: bool = True,
    strict_mode: bool = False,
    partial_mode: bool = False,
):
    if not api_key:
        raise RuntimeError("PERPLEXITY_API_KEY fehlt in der Umgebung.")

    max_attempts = 3
    backoff_seconds = 2
    last_error = None

    payload = {
        "model": MODEL,
        "messages": _build_messages(str(content or ""), include_ping, strict_mode, partial_mode),
        "temperature": 0.0 if strict_mode else 0.2,
        "max_tokens": DEFAULT_MAX_TOKENS,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=(10, 90),  # etwas mehr Zeit fuer grosse Patchnotes
            )
            break
        except (req_exc.Timeout, req_exc.ConnectionError) as exc:
            last_error = exc
            if attempt >= max_attempts:
                raise RuntimeError(
                    f"Perplexity API nicht erreichbar nach {max_attempts} Versuchen: {exc}"
                ) from exc
            time.sleep(backoff_seconds * attempt)
    else:  # pragma: no cover - defensive
        raise RuntimeError(f"Perplexity API Fehler: {last_error}")

    if response.status_code != 200:
        raise RuntimeError(f"Perplexity API Fehler {response.status_code}: {response.text}")

    try:
        return response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Perplexity Antwort konnte nicht geparst werden: {exc} / Raw: {response.text[:500]}"
        )
