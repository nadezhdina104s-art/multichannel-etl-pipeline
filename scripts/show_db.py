import sys
import pandas as pd
from sqlalchemy import create_engine, text
from config import get_db_url
def show_tables():
    engine = create_engine(get_db_url())
    tables = pd.read_sql("SELECT table_name FROM information_schema.tables WHERE table_schema='public'", engine)
    print("Таблицы в БД:")
    for t in tables['table_name']:
        cnt = pd.read_sql(f"SELECT COUNT(*) as c FROM {t}", engine)['c'].iloc[0]
        print(f"  {t} ({cnt} записей)")
def show_mart():
    engine = create_engine(get_db_url())
    try:
        df = pd.read_sql("SELECT * FROM mart_unified_sales LIMIT 5", engine)
        print("\nВитрина mart_unified_sales (первые 5):")
        print(df.to_string())
    except:
        print("Витрина не найдена")
if __name__ == "__main__":
    show_tables()
    show_mart()
