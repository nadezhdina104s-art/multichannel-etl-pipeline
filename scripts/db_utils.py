"""
РАБОТА С POSTGRESQL
=============================================
Умеет:
1. Подключаться к базе данных
2. Сохранять DataFrame в таблицу
3. Выполнять SQL запросы
4. Читать данные из таблиц
"""
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional
class PostgresDB:
    """Класс для работы с PostgreSQL"""
    def __init__(self, db_url: str):
        """
        Инициализация подключения к БД
        Args:
            db_url: строка подключения вида 
                   postgresql://user:password@host:port/database
        """
        self.db_url = db_url
        self.engine = create_engine(db_url)
        print(f"✅ Подключение к PostgreSQL создано")
    def test_connection(self) -> bool:
        """Проверка подключения к базе данных"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                print("✅ Соединение с БД работает")
                return True
        except Exception as e:
            print(f"❌ Ошибка подключения: {e}")
            return False
    def create_table_from_sql(self, sql_file_path: str):
        """
        Создать таблицы из SQL файла
        Args:
            sql_file_path: путь к файлу .sql
        """
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        with self.engine.connect() as conn:
            # Разделяем скрипт на отдельные команды
            for statement in sql_script.split(';'):
                if statement.strip():
                    conn.execute(text(statement))
            conn.commit()
        print(f"✅ Таблицы созданы из {sql_file_path}")
    def save_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append'):
        """
        СОХРАНИТЬ DATAFRAME В ТАБЛИЦУ
        Args:
            df: данные для сохранения
            table_name: имя таблицы
            if_exists: что делать если таблица существует
                       - 'append': добавить строки
                       - 'replace': удалить и создать заново
                       - 'fail': ничего не делать
        """
        if df.empty:
            print(f"⚠️ DataFrame пуст, ничего не сохранено в {table_name}")
            return
        try:
            df.to_sql(
                table_name,
                self.engine,
                if_exists=if_exists,
                index=False,
                method='multi'  # Быстрая вставка множества строк
            )
            print(f"✅ Сохранено {len(df):,} строк в {table_name}")
        except Exception as e:
            print(f"❌ Ошибка сохранения в {table_name}: {e}")
    def read_table(self, table_name: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        ПРОЧИТАТЬ ТАБЛИЦУ
        Args:
            table_name: имя таблицы
            limit: ограничение на количество строк
        """
        query = f"SELECT * FROM {table_name}"
        if limit:
            query += f" LIMIT {limit}"
        try:
            df = pd.read_sql(query, self.engine)
            print(f"✅ Прочитано {len(df):,} строк из {table_name}")
            return df
        except Exception as e:
            print(f"❌ Ошибка чтения {table_name}: {e}")
            return pd.DataFrame()
    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """
        ВЫПОЛНИТЬ SQL ЗАПРОС
        Args:
            query: SQL запрос (SELECT, INSERT, UPDATE, DELETE)
        Returns:
            Для SELECT - DataFrame с результатами
            Для других - None
        """
        try:
            # Проверяем тип запроса
            if query.strip().upper().startswith('SELECT'):
                df = pd.read_sql(query, self.engine)
                print(f"✅ Выполнен SELECT: {len(df):,} строк")
                return df
            else:
                with self.engine.connect() as conn:
                    conn.execute(text(query))
                    conn.commit()
                print(f"✅ Запрос выполнен успешно")
                return None
        except Exception as e:
            print(f"❌ Ошибка выполнения запроса: {e}")
            return None
if __name__ == "__main__":
    # Тест подключения
    import sys
    sys.path.append('.')
    from config import get_db_url
    db = PostgresDB(get_db_url())
    db.test_connection()
