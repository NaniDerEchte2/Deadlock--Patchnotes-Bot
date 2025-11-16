import requests
import dotenv
import os
import json

dotenv.load_dotenv()

api_key = os.getenv("PERPLEXITY_API_KEY")

# - Verwende **# Überschriften** für Hauptbereiche (# Deadlock Patch Notes)
# - Verwende **## Unterüberschriften** für Charaktere, Items, etc.
# - Verwende **### Kleine Überschriften** für Unterkategorien
# - Verwende **fetten Text** für wichtige Änderungen
# - Verwende *kursiven Text* für Erklärungen
# - Verwende **- Listen** für übersichtliche Aufzählung von Änderungen


prompt = """Übersetze die folgenden Deadlock Patchnotes ins Deutsche und formatiere sie für Discord:

1. Struktur:
   - Beginne mit '### Deadlock Patch Notes' als Hauptüberschrift
   - Verwende '##' für Kategorien/Abschnitte
   - Verwende '**Überschrift**' für Unterabschnitte
   - Verwende '-' für Aufzählungspunkte

2. Inhalt:
   - Behalte die exakte Reihenfolge der Änderungen bei
   - Übersetze alle Texte ins Deutsche, AUSSER Eigennamen und Item-Bezeichnungen
   - Verwende nur die gegebenen Informationen, keine externen Quellen
   - Ignoriere Bilder oder Links im Originaltext

3. Formatierung:
   - Halte dich an Discord-Formatierungsrichtlinien
   - Füge am Ende eine **Kurzzusammenfassung** hinzu, getrennt durch eine _____ Linie
   - Beende die Nachricht zwingend mit <@&1330994309524357140>

Hier sind die Patchnotes: """


url = "https://api.perplexity.ai/chat/completions"

def fetch_answer(content):
    if not api_key:
        raise RuntimeError("PERPLEXITY_API_KEY fehlt in der Umgebung.")

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

    response = requests.post(url, json=payload, headers=headers, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(f"Perplexity API Fehler {response.status_code}: {response.text}")

    try:
        return response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Perplexity Antwort konnte nicht geparst werden: {exc} / Raw: {response.text[:500]}")
