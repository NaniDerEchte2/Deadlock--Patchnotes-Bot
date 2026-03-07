import json
import os
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

system_prompt_base = """Du bist ein Deadlock-Patchnotes-Uebersetzer fuer Discord.

Uebersetze die folgenden Deadlock Patchnotes ins Deutsche und formatiere sie fuer Discord:

1. Hauptstruktur:
   - Beginne mit '### Deadlock Patch Notes' als Hauptrubrik
   - Verwende '**[ Abschnitt ]**' fuer Hauptkategorien (z.B. '**[ General ]**', '**[ Items ]**', '**[ Heroes ]**')

2. Unterstruktur (WICHTIG):
   - Innerhalb jedes Abschnitts: Gruppiere thematisch zusammengehoerige Aenderungen unter '**Thema**' als Unterueberschrift
   - Beispiele fuer [ General ]: **Shrine**, **Troopers**, **Walkers & Guardians**, **Mid Boss**, **Map**, **Allgemein**
   - Beispiele fuer [ Items ]: Jedes Item mit mehreren Aenderungen bekommt eine eigene '**Item-Name**' Ueberschrift
   - Beispiele fuer [ Heroes ]: Jeder Held bekommt eine '**Heldenname**' Ueberschrift, darunter seine Ability-Aenderungen als '-' Bullets
   - Einzelne Aenderungen ohne passende Gruppe kommen direkt als '-' Bullet ohne extra Ueberschrift
   - Verwende '-' fuer alle Aufzaehlungspunkte

3. Inhalt:
   - Uebersetze alle Texte ins Deutsche, AUSSER Eigennamen, Item-Bezeichnungen und Game-Mechaniken wie Melee Parry oder Souls
   - Verwende nur die gegebenen Informationen, keine externen Quellen
   - Ignoriere Bilder oder Links im Originaltext
   - Behalte die Reihenfolge der Aenderungen innerhalb jeder Gruppe bei

4. Formatierung:
   - Halte dich an Discord-Formatierungsrichtlinien
   - Fuege am Ende eine **Kurzzusammenfassung** hinzu, getrennt durch eine _____ Linie
"""

strict_system_prompt = (
    system_prompt_base
    + """

WICHTIG:
- Auch wenn der Input sehr kurz ist (z.B. nur 1 Bullet), trotzdem normal uebersetzen und formatieren.
- Keine Meta-Texte wie "Ich kann diese Anfrage nicht erfuellen", "keine Patchnotes bereitgestellt", "Sucherergebnisse ...".
- Keine Rueckfragen an den User.
- Gib ausschliesslich die finale formatierte Antwort aus.
"""
)

partial_system_prompt = """Du bist ein Deadlock-Patchnotes-Uebersetzer fuer Discord.

Uebersetze den folgenden Ausschnitt eines groesseren Deadlock-Patchnotes-Posts ins Deutsche und formatiere ihn fuer Discord:

1. Struktur:
   - Uebersetze nur den gegebenen Ausschnitt
   - Behalte die Reihenfolge der Zeilen exakt bei
   - Verwende '**[ Abschnitt ]**' fuer Hauptkategorien falls vorhanden (z.B. '**[ General ]**', '**[ Items ]**', '**[ Heroes ]**')

2. Unterstruktur (WICHTIG):
   - Innerhalb jedes Abschnitts: Gruppiere thematisch zusammengehoerige Aenderungen unter '**Thema**' als Unterueberschrift
   - Beispiele fuer [ General ]: **Shrine**, **Troopers**, **Walkers & Guardians**, **Mid Boss**, **Map**, **Allgemein**
   - Beispiele fuer [ Items ]: Jedes Item mit mehreren Aenderungen bekommt eine eigene '**Item-Name**' Ueberschrift
   - Beispiele fuer [ Heroes ]: Jeder Held bekommt eine '**Heldenname**' Ueberschrift, darunter seine Ability-Aenderungen als '-' Bullets
   - Einzelne Aenderungen ohne passende Gruppe kommen direkt als '-' Bullet ohne extra Ueberschrift
   - Verwende '-' fuer alle Aufzaehlungspunkte

3. Inhalt:
   - Uebersetze alle Texte ins Deutsche, AUSSER Eigennamen, Item-Bezeichnungen und Game-Mechaniken wie Melee Parry oder Souls
   - Verwende nur die gegebenen Informationen, keine externen Quellen
   - Ignoriere Bilder oder Links im Originaltext

WICHTIG:
- Dies ist nur ein Teil eines groesseren Patchnotes-Posts.
- Kein '### Deadlock Patch Notes' Titel ausgeben.
- Keine Einleitung, keine Erklaerung, keine Rueckfragen.
- Keine **Kurzzusammenfassung** ausgeben.
- Keinen Role-Ping ausgeben.
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
        ping_instruction = "Dies ist ein Teilstueck. Keinen Role-Ping ausgeben."
    else:
        system_prompt = strict_system_prompt if strict_mode else system_prompt_base
        ping_instruction = (
            f"Beende die Nachricht zwingend mit {ROLE_PING} in einer eigenen Zeile."
            if include_ping
            else "Kein Role-Ping am Ende ausgeben."
        )
    user_prompt = (
        f"{ping_instruction}\n\n"
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
