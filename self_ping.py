# self_ping.py
import asyncio, aiohttp, os

URL = os.getenv("SELF_PING_URL", "https://laputabot.onrender.com/health")

async def self_ping():
    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                async with session.get(URL) as r:
                    print(f"🔁 Самопинг: {r.status}")
            except Exception as e:
                print(f"❌ Ошибка самопинга: {e}")
            await asyncio.sleep(300)
