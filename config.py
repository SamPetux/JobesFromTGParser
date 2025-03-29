# Telegram API credentials
API_ID = 1234567
API_HASH = 'bdjdfkd33021'
PHONE = '+77777777777'

# Параметры парсера
GROUP_IDENTIFIER = 'joboneC'  # Имя пользователя или ID канала
OUTPUT_CSV = 'telegram_messages.csv'
BACKUP_CSV = 'telegram_messages_backup.csv'
START_FROM_ID = 0  # 0 - с последнего сообщения, или укажите конкретный ID

# Настройки производительности (оптимальные для минимизации вылетов парсера) 
CHUNK_SIZE = 100
REQUEST_DELAY = 0.5
MAX_RETRIES = 3
