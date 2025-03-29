from telethon.sync import TelegramClient
from telethon.tl.types import PeerChannel
import csv
import time
from datetime import datetime
import os
from config import *


class TelegramParser:
    def __init__(self):
        self.client = None

    async def auth_client(self):
        """Авторизация в Telegram"""
        self.client = TelegramClient('tg_session', API_ID, API_HASH)
        await self.client.start(PHONE)
        return self.client

    @staticmethod
    def get_last_message_id():
        """Получаем последнее сохранённое ID сообщения из CSV"""
        try:
            with open(OUTPUT_CSV, 'r', encoding='utf-8') as f:
                last_line = list(csv.reader(f))[-1]
                return int(last_line[0]) if last_line[0].isdigit() else 0
        except:
            return 0

    async def parse_messages(self, start_id=0):
        """Основная функция парсинга"""
        offset_id = start_id
        retry_count = 0

        while retry_count < MAX_RETRIES:
            try:
                # Получаем информацию о группе
                if isinstance(GROUP_IDENTIFIER, int) or (
                        isinstance(GROUP_IDENTIFIER, str) and GROUP_IDENTIFIER.lstrip('-').isdigit()):
                    group = await self.client.get_entity(PeerChannel(int(GROUP_IDENTIFIER)))
                else:
                    group = await self.client.get_entity(GROUP_IDENTIFIER)

                print(f"🔄 Парсинг с ID: {offset_id or 'начала'}")

                with open(OUTPUT_CSV, 'a' if offset_id > 0 else 'w', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    if offset_id == 0:
                        writer.writerow(['ID', 'Год', 'Месяц', 'Текст'])

                    while True:
                        messages = await self.client.get_messages(
                            group,
                            limit=CHUNK_SIZE,
                            offset_id=offset_id
                        )

                        if not messages:
                            print("✅ Парсинг завершён!")
                            return True

                        for msg in messages:
                            if msg.text:
                                writer.writerow([
                                    msg.id,
                                    msg.date.year,
                                    msg.date.month,
                                    msg.text.replace('\n', ' ')
                                ])
                                offset_id = msg.id

                        print(f"⏳ Обработано до ID: {offset_id} | {datetime.now().strftime('%H:%M:%S')}")
                        file.flush()
                        time.sleep(REQUEST_DELAY)

            except Exception as e:
                print(f"⚠️ Ошибка: {str(e)}")
                retry_count += 1
                if retry_count < MAX_RETRIES:
                    wait_time = 10 * retry_count
                    print(f"🕒 Повторная попытка через {wait_time} сек...")
                    time.sleep(wait_time)
                    await self.client.disconnect()
                    self.client = await self.auth_client()
                else:
                    print("❌ Превышено количество попыток")
                    return False
