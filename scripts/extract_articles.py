"""
ВЫГРУЗКА АРТИКУЛОВ ИЗ API
Собирает все артикулы товаров из Wildberries и Ozon
"""
import sys
sys.path.append('.')
import pandas as pd
from config import WB_API_KEY, OZON_CLIENT_ID, OZON_API_KEY
from wb_api import WildberriesAPI
from ozon_api import OzonAPI
def extract_wb_articles():
    wb = WildberriesAPI(WB_API_KEY)
    cards = wb.get_cards()
    if not cards.empty and 'nmID' in cards.columns:
        wb_articles = cards[['nmID', 'title', 'vendorCode', 'brand']].copy()
        wb_articles = wb_articles.rename(columns={
            'nmID': 'article',
            'vendorCode': 'internal_code',
            'title': 'product_name'
        })
        wb_articles['source'] = 'WB'
        return wb_articles[['article', 'internal_code', 'product_name', 'source']]
    else:
        return pd.DataFrame(columns=['article', 'internal_code', 'product_name', 'source'])
def extract_ozon_articles():
    ozon = OzonAPI(OZON_CLIENT_ID, OZON_API_KEY)
    products = ozon.get_products()
    if products.empty:
        return pd.DataFrame(columns=['article', 'internal_code', 'product_name', 'source'])
    # Получаем названия по offer_id
    offer_ids = products['offer_id'].tolist()
    print(f"   📖 Получение названий для {len(offer_ids)} товаров Ozon...")
    names = ozon.get_product_names(offer_ids)
    ozon_articles = products[['product_id', 'offer_id']].copy()
    ozon_articles = ozon_articles.rename(columns={
        'product_id': 'article',
        'offer_id': 'internal_code'
    })
    ozon_articles['product_name'] = ozon_articles['internal_code'].map(names).fillna('')
    #ozon_articles['internal_code'] = ozon_articles['article'].map(item_code_by_sku).fillna(ozon_articles['internal_code'])
    ozon_articles['source'] = 'OZON'
    return ozon_articles[['article', 'internal_code', 'product_name', 'source']]
def extract_all_articles():
    print("=" * 60)
    print("ВЫГРУЗКА АРТИКУЛОВ ИЗ ВСЕХ ИСТОЧНИКОВ")
    print("=" * 60)
    wb_df = extract_wb_articles()
    ozon_df = extract_ozon_articles()
    all_articles = pd.concat([wb_df, ozon_df], ignore_index=True)
    all_articles.to_csv('data/raw/all_articles_raw.csv', index=False, encoding='utf-8-sig')
    print(f"\n💾 Сохранено: data/raw/all_articles_raw.csv ({len(all_articles)} артикулов)")
    return all_articles
if __name__ == "__main__":
    articles = extract_all_articles()
    print("\n📋 Пример выгруженных артикулов:")
    print(articles.head(10))
