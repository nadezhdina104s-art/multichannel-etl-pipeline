"""
АВТОМАТИЧЕСКОЕ СОПОСТАВЛЕНИЕ АРТИКУЛОВ ПО internal_code (vendorCode / item_code)
"""

import pandas as pd
import sys
sys.path.append('.')

def create_full_mapping(wb_articles, ozon_articles):
    """
    Создаёт таблицу соответствий на основе internal_code.
    wb_articles и ozon_articles должны содержать колонки: internal_code, internal_code, product_name, source
    """
    wb_dict = {}
    for _, row in wb_articles.iterrows():
        code = row['internal_code']
        if code and pd.notna(code):
            wb_dict[code] = {
                'wb_article': row['internal_code'],
                'product_name': row['product_name']
            }
    
    ozon_dict = {}
    for _, row in ozon_articles.iterrows():
        code = row['internal_code']
        if code and pd.notna(code):
            ozon_dict[code] = {
                'ozon_article': row['internal_code'],
                'product_name': row['product_name']
            }
    
    all_codes = set(wb_dict.keys()) | set(ozon_dict.keys())
    
    mapping = []
    for code in all_codes:
        wb_info = wb_dict.get(code, {})
        ozon_info = ozon_dict.get(code, {})
        product_name = wb_info.get('product_name') or ozon_info.get('product_name') or ''
        mapping.append({
            'internal_code': code,
            'wb_article': wb_info.get('wb_article', ''),
            'ozon_article': ozon_info.get('ozon_article', ''),
            'product_name': product_name,
            'match_method': 'direct' if (code in wb_dict and code in ozon_dict) else 'single_source'
        })
    
    mapping_df = pd.DataFrame(mapping)
    # Генерируем артикулы для 1С
    mapping_df['onec_article'] = mapping_df.apply(lambda x: f"1C-{x.name:05d}", axis=1)
    return mapping_df

def save_article_mapping(mapping_df):
    mapping_df.to_csv('data/processed/article_mapping.csv', index=False, encoding='utf-8-sig')
    print(f"💾 Сохранено: data/processed/article_mapping.csv ({len(mapping_df)} записей)")
    return mapping_df

if __name__ == "__main__":
    wb_articles = pd.read_csv('data/raw/all_articles_raw.csv')
    wb_only = wb_articles[wb_articles['source'] == 'WB'].copy()
    ozon_only = wb_articles[wb_articles['source'] == 'OZON'].copy()
    
    print(f"📊 Загружено артикулов: WB: {len(wb_only)}, Ozon: {len(ozon_only)}")
    
    mapping_df = create_full_mapping(wb_only, ozon_only)
    save_article_mapping(mapping_df)
    
    matched = mapping_df[(mapping_df['wb_article'] != '') & (mapping_df['ozon_article'] != '')]
    only_wb = mapping_df[(mapping_df['wb_article'] != '') & (mapping_df['ozon_article'] == '')]
    only_ozon = mapping_df[(mapping_df['wb_article'] == '') & (mapping_df['ozon_article'] != '')]
    print(f"\n✅ Сопоставлено: {len(matched)}")
    print(f"⚠️ Только WB: {len(only_wb)}")
    print(f"⚠️ Только Ozon: {len(only_ozon)}")
    print("\n📋 Пример сопоставлений (первые 10):")
    print(mapping_df.head(10).to_string())