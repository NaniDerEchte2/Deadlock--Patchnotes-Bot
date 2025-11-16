import dotenv
import os
import discord
import json
import asyncio

import changelog_content_fetcher
import changelog_latest_fetcher

import perplexity_requests

dotenv.load_dotenv()

channel_id = int(os.getenv("PATCH_CHANNEL_ID"))  # convert to int
token = os.getenv("BOT_TOKEN")
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))
FORUM_BASE_URL = "https://forums.playdeadlock.com"

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
client = discord.Client(intents=intents)

def load_last_forum_update():
    try:
        with open("last_forum_update.json", "r") as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def save_last_forum_update(latest_link: str):
    with open("last_forum_update.json", "w") as file:
        json.dump(latest_link, file)


async def update_patch(url: str):
    channel = client.get_channel(channel_id)
    if channel is None:
        print(f"Konnte Channel {channel_id} nicht finden.")
        return

    patch_content = changelog_content_fetcher.process(url)
    if not patch_content:
        print(f"Keine Patchnotes unter {url} gefunden.")
        return

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

    if latest_link and (force or latest_link != saved_last_forum):
        print(f"Neuer Patch gefunden: {latest_link}")
        try:
            await update_patch(f"{FORUM_BASE_URL}{latest_link}")
            save_last_forum_update(latest_link)
            return latest_link
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
