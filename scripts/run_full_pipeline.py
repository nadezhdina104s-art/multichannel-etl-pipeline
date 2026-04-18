"""
ПОЛНЫЙ ETL ПАЙПЛАЙН (только реальные данные WB + Ozon)
1. Выгружает артикулы из WB и Ozon
2. Автоматически сопоставляет их
3. Выгружает заказы за указанный период (с удалением старых записей за период)
4. Сохраняет всё в БД и CSV
5. Создаёт единую витрину и обновляет в ней продажи
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import json
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from config import (
    get_db_url, START_DATE, END_DATE, INCREMENTAL,
    WB_API_KEY, OZON_CLIENT_ID, OZON_API_KEY
)
from wb_api import WildberriesAPI
from ozon_api import OzonAPI


def save_to_db(df, table_name, if_exists='replace'):
    """Сохраняет DataFrame в PostgreSQL"""
    if df.empty:
        print(f"⚠️ DataFrame пуст, {table_name} не сохранена")
        return
    try:
        engine = create_engine(get_db_url())
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        print(f"   💾 БД: {len(df)} записей → {table_name}")
    except Exception as e:
        print(f"   ⚠️ Ошибка сохранения в {table_name}: {e}")


def run_full_pipeline():
    print("=" * 70)
    print("🚀 ЗАПУСК ETL ПАЙПЛАЙНА (ТОЛЬКО РЕАЛЬНЫЕ ДАННЫЕ)")
    print("=" * 70)
    start_time = datetime.now()

    mapping_df = None

    # ---------- ПЕРИОД ----------
    if INCREMENTAL:
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        print(f"📅 Инкрементальный режим: {start_date} - {end_date}")
    else:
        start_date = START_DATE
        end_date = END_DATE or datetime.now().strftime('%Y-%m-%d')
        print(f"📅 Полная загрузка: {start_date} - {end_date}")

    # ---------- ШАГ 1: Выгрузка артикулов ----------
    print("\n" + "=" * 70)
    print("ШАГ 1: ВЫГРУЗКА АРТИКУЛОВ ИЗ API")
    print("=" * 70)
    try:
        from extract_articles import extract_all_articles
        articles_df = extract_all_articles()
        if articles_df.empty:
            print("❌ Нет артикулов от WB или Ozon. Остановка.")
            sys.exit(1)
        print("✅ ШАГ 1 выполнен")
    except Exception as e:
        print(f"❌ Ошибка в ШАГЕ 1: {e}")
        sys.exit(1)

    save_to_db(articles_df, 'stg_articles_raw', 'replace')

    # ---------- ШАГ 2: Сопоставление артикулов ----------
    print("\n" + "=" * 70)
    print("ШАГ 2: АВТОМАТИЧЕСКОЕ СОПОСТАВЛЕНИЕ АРТИКУЛОВ")
    print("=" * 70)
    try:
        from match_articles import create_full_mapping, save_article_mapping
        wb_articles = articles_df[articles_df['source'] == 'WB'].copy()
        ozon_articles = articles_df[articles_df['source'] == 'OZON'].copy()
        mapping_df = create_full_mapping(wb_articles, ozon_articles)
        if mapping_df.empty:
            print("❌ Не удалось создать маппинг артикулов. Остановка.")
            sys.exit(1)
        save_article_mapping(mapping_df)
        print("✅ ШАГ 2 выполнен")
    except Exception as e:
        print(f"❌ Ошибка в ШАГЕ 2: {e}")
        sys.exit(1)

    save_to_db(mapping_df, 'dim_article_mapping', 'replace')

    # ---------- ШАГ 3: Выгрузка заказов ----------
    print("\n" + "=" * 70)
    print("ШАГ 3: ВЫГРУЗКА ЗАКАЗОВ С WB И OZON")
    print("=" * 70)

    engine = create_engine(get_db_url())

    try:
        wb = WildberriesAPI(WB_API_KEY)
        if INCREMENTAL:
            wb_orders = wb.get_orders(date_from=start_date)
            if not wb_orders.empty:
                if 'date' in wb_orders.columns:
                    wb_orders['date'] = pd.to_datetime(wb_orders['date'], errors='coerce')
                    end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
                    wb_orders = wb_orders[wb_orders['date'].dt.date <= end_dt]
                
                wb_orders.to_csv(f'data/raw/wb_orders_{start_date}_{end_date}.csv', index=False, encoding='utf-8-sig')
                
                wb_orders['gNumber'] = wb_orders['gNumber'].fillna('').astype(str)
                wb_orders['nmId'] = wb_orders['nmId'].fillna('').astype(str)
                wb_orders['supplierArticle'] = wb_orders['supplierArticle'].fillna('').astype(str)
                # Удаляем суффиксы .1, .2 и т.д. (вот эту строку добавляете)
                wb_orders['supplierArticle'] = wb_orders['supplierArticle'].str.replace(r'\.\d+$', '', regex=True)
                
                wb_orders = wb_orders.drop_duplicates(subset=['gNumber', 'nmId'])
                
                with engine.begin() as conn:
                    for _, row in wb_orders.iterrows():
                        g = row['gNumber']
                        nm = row['nmId']
                        if g and nm:
                            conn.execute(
                                text("DELETE FROM stg_wb_orders WHERE \"gNumber\" = :g AND \"nmId\" = :nm"),
                                {"g": g, "nm": nm}
                            )
                
                save_to_db(wb_orders, 'stg_wb_orders', 'append')
                print(f"✅ Заказы WB: {len(wb_orders)} записей")
            else:
                print("⚠️ Заказов WB за период нет")
        else:
            wb_finance = wb.get_orders_history(date_from=start_date, date_to=end_date)
            if not wb_finance.empty:
                wb_finance.to_csv(f'data/raw/wb_finance_{start_date}_{end_date}.csv', index=False, encoding='utf-8-sig')
                save_to_db(wb_finance, 'stg_wb_finance', 'replace')
                print(f"✅ Финансовый отчёт WB: {len(wb_finance)} записей")
            else:
                print("⚠️ Финансовый отчёт WB пуст")
    except Exception as e:
        print(f"❌ Ошибка выгрузки данных WB: {e}")
        
    # Ozon
        # Ozon
    try:
        ozon = OzonAPI(OZON_CLIENT_ID, OZON_API_KEY)
        ozon_orders = ozon.get_orders_fbo(start_date, end_date)
        if not ozon_orders.empty:
            # Сериализация вложенных структур (если они есть в DataFrame)
            complex_cols = ['products', 'analytics_data', 'financial_data', 'additional_data', 'legal_info']
            for col in complex_cols:
                if col in ozon_orders.columns:
                    ozon_orders[col] = ozon_orders[col].apply(
                        lambda x: json.dumps(x, ensure_ascii=False) if x is not None else None
                    )
            ozon_orders.to_csv(f'data/raw/ozon_orders_{start_date}_{end_date}.csv', index=False, encoding='utf-8-sig')
            save_to_db(ozon_orders, 'stg_ozon_orders', 'replace')
            print(f"✅ Заказы Ozon: {len(ozon_orders)} записей")
        else:
            print("⚠️ Заказов Ozon FBO за период нет")
    except Exception as e:
        print(f"❌ Ошибка выгрузки заказов Ozon: {e}")
        
        
        

        # ---------- ШАГ 5: Создание витрины и обновление продаж ----------
    print("\n" + "=" * 70)
    print("ШАГ 5: СОЗДАНИЕ ЕДИНОЙ ВИТРИНЫ ДАННЫХ")
    print("=" * 70)
    try:
        mapping = pd.read_csv('data/processed/article_mapping.csv')
        summary = []
        for _, row in mapping.iterrows():
            summary.append({
                'internal_code': row.get('internal_code', ''),
                'product_name': row.get('product_name', ''),
                'wb_article': row.get('wb_article', ''),
                'ozon_article': row.get('ozon_article', ''),
                'onec_article': row.get('onec_article', ''),
                'wb_sales_count': 0,
                'ozon_sales_count': 0,
                'crm_orders_count': 0,
                'total_revenue': 0,
                'updated_at': datetime.now()
            })
        summary_df = pd.DataFrame(summary)
        summary_df.to_csv('data/processed/unified_mart.csv', index=False, encoding='utf-8-sig')
        save_to_db(summary_df, 'mart_unified_sales', 'replace')
        print(f"✅ Базовая витрина: {len(summary_df)} товаров")

        engine = create_engine(get_db_url())
        with engine.begin() as conn:
                    # WB: используем nmId (числовой) из stg_wb_orders
                    conn.execute(text("""
                        UPDATE mart_unified_sales 
                        SET wb_sales_count = COALESCE((
                            SELECT COUNT(*) FROM stg_wb_orders 
                            WHERE "supplierArticle" = wb_article::text
                        ), 0)
                    """))
                    # Ozon: используем offer_id (строковый) из stg_ozon_orders
                    conn.execute(text("""
                        UPDATE mart_unified_sales 
                        SET ozon_sales_count = COALESCE((
                            SELECT COUNT(*) FROM stg_ozon_orders 
                            WHERE offer_id = ozon_article::text
                        ), 0)
                    """))
                    # Общая выручка
                    conn.execute(text("""
                        UPDATE mart_unified_sales 
                        SET total_revenue = COALESCE((
                            SELECT SUM("finishedPrice"::numeric) FROM stg_wb_orders 
                            WHERE "supplierArticle" = wb_article::text
                        ), 0) + COALESCE((
                            SELECT SUM(price::numeric) FROM stg_ozon_orders 
                            WHERE offer_id = ozon_article::text
                        ), 0)
                    """))            
        print("✅ Витрина обновлена: добавлены реальные продажи")
    except Exception as e:
        print(f"❌ Ошибка в ШАГЕ 5: {e}")


       
    # ---------- ФИНАЛ ----------
    duration = (datetime.now() - start_time).total_seconds()
    print("\n" + "=" * 70)
    print("✅ ETL ПАЙПЛАЙН ЗАВЕРШЕН")
    print("=" * 70)
    print(f"⏱️ Время выполнения: {duration:.2f} секунд")
    print(f"📁 Результаты сохранены в data/processed/ и базе данных")
    if mapping_df is not None:
        print("\n📋 Пример таблицы соответствий:")
        print(mapping_df.head(10).to_string())


if __name__ == "__main__":
    try:
        engine = create_engine(get_db_url())
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Подключение к PostgreSQL успешно\n")
    except Exception as e:
        print(f"⚠️ Нет подключения к БД: {e}\n")
    run_full_pipeline()
    