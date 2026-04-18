import os
import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv('POSTGRES_DB', 'analytics')
DB_USER = os.getenv('POSTGRES_USER', 'postgres')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_PORT = os.getenv('POSTGRES_PORT', '5432')

def init_database():
    if not DB_PASSWORD:
        print("❌ Переменная окружения POSTGRES_PASSWORD не задана!")
        return False

    # Подключаемся к стандартной БД postgres (существует всегда)
    admin_config = {
        'host': DB_HOST,
        'port': DB_PORT,
        'user': DB_USER,
        'password': DB_PASSWORD,
        'database': 'postgres'
    }
    try:
        conn = psycopg2.connect(**admin_config)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Создаём БД analytics, если не существует
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
        if not cur.fetchone():
            cur.execute(f"CREATE DATABASE {DB_NAME}")
            print(f"✅ База данных '{DB_NAME}' создана")
        else:
            print(f"ℹ️ База данных '{DB_NAME}' уже существует")

        cur.close()
        conn.close()

        # Подключаемся к БД analytics и создаём таблицы (если их нет)
        user_config = {
            'host': DB_HOST,
            'port': DB_PORT,
            'user': DB_USER,
            'password': DB_PASSWORD,
            'database': DB_NAME
        }
        conn = psycopg2.connect(**user_config)
        conn.autocommit = True
        cur = conn.cursor()

        sql_path = os.path.join(os.path.dirname(__file__), '..', 'sql', 'create_tables.sql')
        if not os.path.exists(sql_path):
            print(f"❌ Файл {sql_path} не найден. Таблицы не созданы.")
            return False

        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()

        # Выполняем SQL, игнорируя ошибки duplicate table / index
        try:
            cur.execute(sql_script)
            print(f"✅ Все таблицы успешно созданы/проверены в БД '{DB_NAME}'")
        except psycopg2.errors.DuplicateTable:
            print(f"ℹ️ Таблицы уже существуют в БД '{DB_NAME}', пропускаем создание")
        except Exception as e:
            # Если ошибка не связана с дубликатами, выводим, но не прерываем выполнение
            if 'already exists' in str(e):
                print(f"ℹ️ Некоторые объекты уже существуют, пропускаем: {e}")
            else:
                print(f"⚠️ Ошибка при создании таблиц: {e}")
                return False

        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Ошибка при инициализации: {e}")
        return False

if __name__ == "__main__":
    if init_database():
        print("Инициализация завершена успешно.")
    else:
        print("Инициализация завершилась с ошибкой.")
        sys.exit(1)
        