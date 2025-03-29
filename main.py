import asyncio
import os
import time
from parser import TelegramParser
from config import *


async def run_parser():
    parser = TelegramParser()
    
    if os.path.exists(OUTPUT_CSV):
        os.replace(OUTPUT_CSV, BACKUP_CSV)

    # Определяем начальный ID
    last_id = START_FROM_ID if START_FROM_ID > 0 else parser.get_last_message_id()
    print(f"🔍 Начинаем парсинг с ID: {last_id}")

    client = await parser.auth_client()
    success = await parser.parse_messages(last_id)
    await client.disconnect()

    if not success and os.path.exists(BACKUP_CSV):
        print("🔙 Восстанавливаем резервную копию...")
        os.replace(BACKUP_CSV, OUTPUT_CSV)


if __name__ == '__main__':
    while True:
        asyncio.run(run_parser())
        print("♻️ Перезапуск парсера через 60 секунд...")
        time.sleep(60)
