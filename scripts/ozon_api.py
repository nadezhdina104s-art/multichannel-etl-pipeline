"""
КЛИЕНТ ДЛЯ OZON API (РАБОЧАЯ ВЕРСИЯ)
- Товары: /v3/product/list
- Заказы FBO: /v2/posting/fbo/list
- Остатки: /v2/analytics/stock_on_warehouses
- Названия: /v3/product/info/list по offer_id
"""
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
class OzonAPI:
    def __init__(self, client_id: str, api_key: str):
        self.client_id = client_id
        self.api_key = api_key
        self.headers = {
            "Client-Id": client_id,
            "Api-Key": api_key,
            "Content-Type": "application/json"
        }
        self.base_url = "https://api-seller.ozon.ru"
        print(f"✅ Ozon API клиент создан")
    def ping(self) -> bool:
        url = f"{self.base_url}/v2/ping"
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            return resp.status_code == 200
        except:
            return False
    def get_products(self) -> pd.DataFrame:
        print(f"📋 Загрузка товаров Ozon...")
        url = f"{self.base_url}/v3/product/list"
        body = {"filter": {"visibility": "ALL"}, "limit": 1000}
        response = requests.post(url, headers=self.headers, json=body)
        if response.status_code == 200:
            data = response.json()
            items = data.get("result", {}).get("items", [])
            df = pd.DataFrame(items)
            print(f"   ✅ Загружено {len(df):,} товаров")
            return df
        else:
            print(f"   ❌ Ошибка загрузки товаров: {response.status_code}")
            return pd.DataFrame()
    def get_product_names(self, offer_ids):
        """Получает названия товаров по списку offer_id (используя v3/product/info/list)"""
        print(f"   📖 Получение названий для {len(offer_ids)} товаров Ozon...")
        url = f"{self.base_url}/v3/product/info/list"
        names = {}
        for i in range(0, len(offer_ids), 100):
            batch = offer_ids[i:i+100]
            payload = {"offer_id": batch}
            response = requests.post(url, headers=self.headers, json=payload)
            if response.status_code == 200:
                data = response.json()
                for item in data.get('items', []):
                    offer_id = item.get('offer_id')
                    name = item.get('name', '')
                    names[offer_id] = name
            else:
                print(f"   ⚠️ Ошибка получения названий для пакета: {response.status_code}")
            time.sleep(0.2)
        print(f"   ✅ Получены названия для {len(names)} товаров")
        return names
    
    def get_stocks(self) -> pd.DataFrame:
        print("📦 Загрузка остатков Ozon...")
        url = f"{self.base_url}/v2/analytics/stock_on_warehouses"
        payload = {"limit": 1000, "offset": 0}
        response = requests.post(url, headers=self.headers, json=payload)
        if response.status_code == 200:
            data = response.json()
            rows = data.get("result", {}).get("rows", [])
            df = pd.DataFrame(rows)
            print(f"   ✅ Загружено {len(df)} записей остатков")
            return df
        else:
            print(f"   ❌ Ошибка загрузки остатков: {response.status_code}")
            return pd.DataFrame()
    def get_orders_fbo(self, date_from: str, date_to: str) -> pd.DataFrame:
        print(f"📦 Загрузка заказов Ozon FBO: {date_from} → {date_to}")
        url = f"{self.base_url}/v2/posting/fbo/list"
        all_postings = []
        offset = 0
        limit = 100
        while True:
            body = {
                "dir": "ASC",
                "filter": {
                    "since": f"{date_from}T00:00:00Z",
                    "to": f"{date_to}T23:59:59Z"
                },
                "limit": limit,
                "offset": offset
            }
            response = requests.post(url, headers=self.headers, json=body)
            if response.status_code == 200:
                data = response.json()
                postings = data.get("result", [])
                for posting in postings:
                    # Извлекаем offer_id из первого товара (если есть)
                    products = posting.get('products', [])
                    if products:
                        posting['offer_id'] = products[0].get('offer_id')
                    else:
                        posting['offer_id'] = None
                    # Добавляем также sku, name, quantity, price, если нужны
                    # (они могут пригодиться для витрины)
                    if products:
                        posting['sku'] = products[0].get('sku')
                        posting['product_name'] = products[0].get('name')
                        posting['quantity'] = products[0].get('quantity')
                        posting['price'] = float(products[0].get('price', 0))
                    else:
                        posting['sku'] = None
                        posting['product_name'] = None
                        posting['quantity'] = 0
                        posting['price'] = 0.0
                    all_postings.append(posting)
                if len(postings) < limit:
                    break
                offset += limit
            else:
                print(f"   ❌ Ошибка API FBO: {response.status_code}")
                break
        df = pd.DataFrame(all_postings)
        print(f"   ✅ Загружено {len(df):,} заказов FBO")
        return df
    
    
    
if __name__ == "__main__":
    import sys
    sys.path.append('.')
    from config import OZON_CLIENT_ID, OZON_API_KEY
    print("=" * 60)
    print("ТЕСТИРОВАНИЕ OZON API")
    print("=" * 60)
    ozon = OzonAPI(OZON_CLIENT_ID, OZON_API_KEY)
    if ozon.ping():
        print("✅ API доступен, ключ рабочий")
    else:
        print("⚠️ Пинг не прошёл")
    products = ozon.get_products()
    print(f"Товаров: {len(products)}")
    if not products.empty:
        offer_ids = products['offer_id'].tolist()
        names = ozon.get_product_names(offer_ids)
        print(f"Пример названий: {dict(list(names.items())[:3])}")
    stocks = ozon.get_stocks()
    print(f"Остатков: {len(stocks)}")
    date_to = datetime.now().strftime('%Y-%m-%d')
    date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    fbo_orders = ozon.get_orders_fbo(date_from, date_to)
    print(f"Заказов FBO: {len(fbo_orders)}")
