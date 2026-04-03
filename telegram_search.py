from telethon import TelegramClient
from telethon.tl.functions.messages import SearchRequest
from telethon.tl.types import InputMessagesFilterEmpty
import asyncio

from credentials import telegram_api_id, telegram_api_hash

API_ID   = int(telegram_api_id)   # Telethon requires an integer
API_HASH = telegram_api_hash

async def search_telegram(query, channel_username):
    async with TelegramClient("session", API_ID, API_HASH, connection_retries=1, timeout=10) as client:
        # Get messages from a specific public channel
        channel = await client.get_entity(channel_username)
        messages = []
        async for message in client.iter_messages(channel, search=query, limit=100):
            if message.text:
                messages.append({
                    "id":    message.id,
                    "date":  message.date,
                    "text":  message.text,
                    "views": message.views,
                })
        return messages

QUERIES  = ["carburant prix", "gaz la monte", "gaz", "carburant", "pri eneji"]
CHANNELS = ["machannzen", "tripotayPaysDhaiti"]

# Use (channel, message_id) as a unique key to deduplicate across queries
seen = set()
results = []

for query in QUERIES:
    for channel in CHANNELS:
        for msg in asyncio.run(search_telegram(query, channel)):
            key = (channel, msg["id"])
            if key not in seen:
                seen.add(key)
                results.append(msg)

print(f"Total unique messages retrieved: {len(results)}")

# Quick preview
for msg in results:
    print(msg["date"], "|", msg["views"], "views |", msg["text"][:100])
