# каждые 5 минут import requests
import asyncio
import requests

URL = "https://e345b822-f985-4aad-b5e5-4c488fa48e5a-00-v9dvfd8b0rkm.worf.replit.dev/"

async def self_ping():
    while True:
        try:
            response = requests.get(URL)
            print(f"🔁 Самопинг: {response.status_code}")
        except Exception as e:
            print(f"❌ Ошибка самопинга: {e}")
        await asyncio.sleep(300)  # каждые 5 минут