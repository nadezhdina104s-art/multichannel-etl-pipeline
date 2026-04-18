"""
Клиент для Wildberries API (FBO) - только нужные методы для ETL
- Карточки товаров (nmID, title, vendorCode, brand)
- Цены и скидки (извлекаем базовую цену из sizes)
- Остатки на складах WB через аналитический отчёт
- Заказы за период
"""
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import time
from tqdm import tqdm
from typing import Optional, List
class WildberriesAPI:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        self.content_url = "https://content-api.wildberries.ru"
        self.prices_url = "https://discounts-prices-api.wildberries.ru"
        self.statistics_url = "https://statistics-api.wildberries.ru"
        self.analytics_url = "https://seller-analytics-api.wildberries.ru"
    def _request(self, method: str, url: str, **kwargs):
        time.sleep(0.2)
        resp = requests.request(method, url, headers=self.headers, **kwargs)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 429:
            print("⚠️ Rate limit, ждём 5 сек...")
            time.sleep(5)
            return self._request(method, url, **kwargs)
        else:
            print(f"❌ Ошибка {resp.status_code}: {resp.text[:200]}")
            return None
    def get_cards(self) -> pd.DataFrame:
        print("📋 Загрузка карточек товаров...")
        url = f"{self.content_url}/content/v2/get/cards/list"
        body = {
            "settings": {
                "sort": {"ascending": True},
                "cursor": {"limit": 100},
                "filter": {"withPhoto": -1}
            }
        }
        all_cards = []
        while True:
            data = self._request("POST", url, json=body)
            if not data:
                break
            cards = data.get("cards", [])
            all_cards.extend(cards)
            cursor = data.get("cursor", {})
            if not cursor.get("updatedAt") or not cursor.get("nmID") or len(cards) < 100:
                break
            body["settings"]["cursor"]["updatedAt"] = cursor["updatedAt"]
            body["settings"]["cursor"]["nmID"] = cursor["nmID"]
        df = pd.DataFrame(all_cards)
        if not df.empty:
            df['nmID'] = df['nmID'].astype(str)
            print(f"   ✅ Загружено {len(df)} карточек")
        return df
    def get_prices(self, nm_ids: Optional[List[int]] = None) -> pd.DataFrame:
        print("💰 Загрузка цен...")
        url = f"{self.prices_url}/api/v2/list/goods/filter"
        if nm_ids:
            data = self._request("POST", url, json={"nmList": nm_ids[:1000]})
            goods = data.get("data", {}).get("listGoods", []) if data else []
        else:
            all_goods = []
            offset = 0
            limit = 1000
            while True:
                resp = self._request("GET", url, params={"limit": limit, "offset": offset})
                if not resp:
                    break
                goods = resp.get("data", {}).get("listGoods", [])
                all_goods.extend(goods)
                if len(goods) < limit:
                    break
                offset += limit
            goods = all_goods
        if not goods:
            return pd.DataFrame()
        rows = []
        for g in goods:
            row = {"nmID": str(g.get("nmID"))}
            sizes = g.get("sizes", [])
            if sizes and "price" in sizes[0]:
                row["price"] = sizes[0]["price"]
            else:
                row["price"] = None
            row["discount"] = g.get("discount")
            row["clubDiscount"] = g.get("clubDiscount")
            rows.append(row)
        df = pd.DataFrame(rows)
        print(f"   ✅ Загружено {len(df)} записей о ценах")
        return df
    def get_remains(self) -> pd.DataFrame:
        print("📦 Загрузка остатков...")
        create_url = f"{self.analytics_url}/api/v1/warehouse_remains"
        params = {
            "groupByBrand": "true",
            "groupBySubject": "true",
            "groupBySa": "true",
            "groupByNm": "true",
            "groupByBarcode": "true",
            "groupBySize": "true"
        }
        resp = self._request("GET", create_url, params=params)
        if not resp:
            return pd.DataFrame()
        task_id = resp.get("data", {}).get("taskId")
        if not task_id:
            print("   ❌ Не удалось создать задание")
            return pd.DataFrame()
        status_url = f"{self.analytics_url}/api/v1/warehouse_remains/tasks/{task_id}/status"
        for _ in range(24):
            time.sleep(5)
            st = self._request("GET", status_url)
            if st and st.get("data", {}).get("status") == "done":
                break
        else:
            print("   ⚠️ Таймаут ожидания отчёта")
            return pd.DataFrame()
        download_url = f"{self.analytics_url}/api/v1/warehouse_remains/tasks/{task_id}/download"
        data = self._request("GET", download_url)
        if not data:
            return pd.DataFrame()
        rows = []
        for item in data:
            nm_id = str(item.get("nmId"))
            for wh in item.get("warehouses", []):
                rows.append({
                    "nmId": nm_id,
                    "warehouseName": wh.get("warehouseName"),
                    "quantity": wh.get("quantity", 0),
                    "inWayToClient": wh.get("inWayToClient", 0),
                    "inWayFromClient": wh.get("inWayFromClient", 0)
                })
        df = pd.DataFrame(rows)
        print(f"   ✅ Загружено {len(df)} записей об остатках")
        return df
    def get_orders(self, date_from: str) -> pd.DataFrame:
        print(f"📦 Загрузка заказов с {date_from}...")
        url = f"{self.statistics_url}/api/v1/supplier/orders"
        params = {"dateFrom": date_from, "flag": 0}
        all_orders = []
        while True:
            data = self._request("GET", url, params=params)
            if not data:
                break
            if isinstance(data, list):
                all_orders.extend(data)
                if len(data) == 0:
                    break
                last = data[-1].get("lastChangeDate")
                if last:
                    params["dateFrom"] = last
                else:
                    break
            else:
                break
        df = pd.DataFrame(all_orders)
        if not df.empty:
            df['nmId'] = df['nmId'].astype(str)
            print(f"   ✅ Загружено {len(df)} заказов")
        return df

    def get_orders_history(self, date_from: str, date_to: str = None) -> pd.DataFrame:
        """Получает исторические данные о заказах через финансовый отчёт WB."""
        if date_to is None:
            date_to = datetime.now().strftime("%Y-%m-%d")
        print(f"📦 Загрузка исторических заказов WB (отчёт) с {date_from} по {date_to}...")
        url = f"{self.statistics_url}/api/v5/supplier/reportDetailByPeriod"
        params = {
            'dateFrom': date_from,
            'dateTo': date_to,
            'limit': 100000,
            'rrdid': 0
        }
        
        all_rows = []
        with tqdm(desc="Загрузка отчёта WB", unit=" стр.") as pbar:
            data = self._request("GET", url, params=params)
            
            # Проверяем, что data — словарь
            if isinstance(data, dict):
                rows = data.get('data', [])
            elif isinstance(data, list):
                rows = data  # Если ответ уже список
            else:
                rows = []
                
            all_rows.extend(rows)
            pbar.update(len(rows))
        
        df = pd.DataFrame(all_rows)
        if not df.empty:
            date_cols = ['date', 'rr_dt']
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
            # Приводим nm_id к строке для совместимости с маппингом
            if 'nm_id' in df.columns:
                df['nmId'] = df['nm_id'].astype(str)
            print(f"   ✅ Загружено {len(df)} записей отчёта")
        else:
            print("   ⚠️ Отчёт пуст")
        return df
        
        
  
  

