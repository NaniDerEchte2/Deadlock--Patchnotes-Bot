import requests
import dotenv
import os
import json
import time
from requests import exceptions as req_exc

dotenv.load_dotenv()

api_key = os.getenv("PERPLEXITY_API_KEY")
ROLE_PING = "<@&1330994309524357140>"

# Prompt fuer Perplexity: einfache Discord-Formatierung
prompt_base = """Uebersetze die folgenden Deadlock Patchnotes ins Deutsche und formatiere sie fuer Discord:

1. Struktur:
   - Beginne mit '### Deadlock Patch Notes' als Hauptrubrik
   - Verwende '##' fuer Kategorien/Abschnitte
   - Verwende '**Ueberschrift**' fuer Unterabschnitte
   - Verwende '-' fuer Aufzaehlungspunkte

2. Inhalt:
   - Behalte die exakte Reihenfolge der Aenderungen bei
   - Uebersetze alle Texte ins Deutsche, AUSSER Eigennamen und Item-Bezeichnungen oder game Mechaniken wie Melee Parry oder Souls sowas einfach übernehmen
   - Verwende nur die gegebenen Informationen, keine externen Quellen
   - Ignoriere Bilder oder Links im Originaltext

3. Formatierung:
   - Halte dich an Discord-Formatierungsrichtlinien
   - Fuege am Ende eine **Kurzzusammenfassung** hinzu, getrennt durch eine _____ Linie, hier soll nur die Wichtigsten Paar Patchnotes Punkte stehen die den Größten Impact haben wie Große Gameplay änderungen.
"""

prompt_with_ping = (
    prompt_base
    + f"   - Beende die Nachricht zwingend mit {ROLE_PING}\n\n"
    + "Hier sind die Patchnotes: "
)
prompt_no_ping = prompt_base + "\nHier sind die Patchnotes: "


url = "https://api.perplexity.ai/chat/completions"


def fetch_answer(content, include_ping: bool = True):
    if not api_key:
        raise RuntimeError("PERPLEXITY_API_KEY fehlt in der Umgebung.")

    max_attempts = 3
    backoff_seconds = 2
    last_error = None

    prompt = prompt_with_ping if include_ping else prompt_no_ping
    payload = {
        "model": "sonar-pro",
        "messages": [
            {"role": "user", "content": prompt + content}
        ]
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
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
        raise RuntimeError(f"Perplexity Antwort konnte nicht geparst werden: {exc} / Raw: {response.text[:500]}")
