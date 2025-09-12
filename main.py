import dotenv
import os
import discord
import json
import asyncio

import changelog_date_fetcher
import changelog_content_fetcher
import changelog_latest_fetcher

import perplexity_requests

dotenv.load_dotenv()

channel_id = int(os.getenv("PATCH_CHANNEL_ID"))  # convert to int
token = os.getenv("BOT_TOKEN")

intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
client = discord.Client(intents=intents)

url_date = "https://forums.playdeadlock.com/forums/changelog.10/"
url_content = "https://forums.playdeadlock.com/threads/09-04-2025-update.80693/"

last_forum_update = changelog_latest_fetcher.check_latest()

async def update_patch(url):
    patch_content = changelog_content_fetcher.process(url)
    response = str(perplexity_requests.fetch_answer(patch_content)["choices"][0]["message"]["content"])
    await patch_response(client.get_channel(channel_id), response)


async def patch_response(channel, response_content):
    for i in range(0, len(response_content), 1900):
        chunk = response_content[i:i+1900]
        await channel.send(chunk)

@client.event
async def on_ready():
    print("Bot ist ready!")

    while True:
        try:
            with open("last_forum_update.json", "r") as file:
                saved_last_forum = json.load(file)
        except FileNotFoundError:
            saved_last_forum = None

        # Compare with new value
        if saved_last_forum != last_forum_update:
            with open("last_forum_update.json", "w") as file:
                json.dump(last_forum_update, file)
                await update_patch("https://forums.playdeadlock.com" + last_forum_update)
        await asyncio.sleep(1)

client.run(token)