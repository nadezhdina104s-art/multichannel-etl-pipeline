import os
from dotenv import load_dotenv
from pathlib import Path
BASE_DIR = Path(__file__).parent.parent
load_dotenv(BASE_DIR / '.env')
START_DATE = os.getenv('START_DATE', '2025-01-01')      # Начало периода по умолчанию
END_DATE   = os.getenv('END_DATE', None)                 # Конец периода (если None – сегодня)
INCREMENTAL = os.getenv('INCREMENTAL', 'false').lower() == 'true'  # Режим инкремента
# Wildberries
WB_API_KEY = os.getenv('WB_API_KEY')
if not WB_API_KEY:
    raise ValueError("WB_API_KEY не найден!")
# Ozon
OZON_CLIENT_ID = os.getenv('OZON_CLIENT_ID')
OZON_API_KEY = os.getenv('OZON_API_KEY')
# PostgreSQL
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'analytics'),
    'user': os.getenv('POSTGRES_USER', 'airflow'),
    'password': os.getenv('POSTGRES_PASSWORD', 'airflow')
}
# Пути
RAW_DATA_PATH = BASE_DIR / 'data/raw'
PROCESSED_DATA_PATH = BASE_DIR / 'data/processed'
MOCK_DATA_PATH = BASE_DIR / 'data/mock'
for path in [RAW_DATA_PATH, PROCESSED_DATA_PATH, MOCK_DATA_PATH]:
    path.mkdir(parents=True, exist_ok=True)
DAYS_BACK = int(os.getenv('DAYS_BACK', 7))
API_LIMIT = int(os.getenv('API_LIMIT', 1000))
def get_db_url():
    return f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
print("✅ Конфигурация загружена")
