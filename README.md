#  ETL-пайплайн для мультиканальной аналитики

> **Airflow + PostgreSQL + API Wildberries/Ozon**  
> Автоматический сбор данных из 2 источников в единую витрину

[![Airflow](https://img.shields.io/badge/Airflow-2.8+-blue.svg)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-green.svg)](https://www.postgresql.org/)
[![Python](https://img.shields.io/badge/Python-3.10+-yellow.svg)](https://www.python.org/)

---

## 📋 Оглавление

- [Описание проекта](#-описание-проекта)
- [Архитектура решения](#-архитектура-решения)
- [Структура проекта](#-структура-проекта)
- [Установка и запуск](#-установка-и-запуск)
- [Результаты](#-результаты)
- [Ключевые навыки](#-ключевые-навыки)

---

## 🎯 Описание проекта

Компания продаёт товары для дома через:

- **Wildberries** (основной канал, ~200 SKU)
- **Ozon** (дополнительный канал, ~50 SKU)

**Проблема:** Данные разрознены. Чтобы посмотреть общую картину, нужно заходить в 2 разные системы и вручную сводить отчёты.

**Цель:** Построить ETL-пайплайн, который автоматически собирает данные из API Wildberries и Ozon в единую базу данных, сопоставляет товары по единому артикулу поставщика и предоставляет дашборд.

---

## 🏗️ Архитектура решения

```
┌─────────────────────────────────────────────────┐
│              ИСТОЧНИКИ ДАННЫХ (2)               │
├─────────────────────┬───────────────────────────┤
│    Wildberries      │          Ozon             │
│   (API реальный)    │      (API реальный)       │
└──────────┬──────────┴──────────────┬────────────┘
           │                         │
           └────────────┬────────────┘
                        ▼
            ┌───────────────────────┐
            │     APACHE AIRFLOW     │
            │   (оркестрация ETL)    │
            │ • DAG каждую неделю    │
            │ • Параллельная загрузка│
            └───────────┬───────────┘
                        ▼
            ┌───────────────────────┐
            │      POSTGRESQL        │
            │  stg_* (сырые данные)  │
            │  dim_* (справочники)   │
            │  mart_* (витрины)      │
            └───────────┬───────────┘
                        ▼
            ┌───────────────────────┐
            │   STREAMLIT DASHBOARD  │
            │  (выручка, топ товаров)│
            └───────────────────────┘
```
Архитектура модульная: при необходимости легко подключить новые источники (1С, CRM, Google Analytics) через отдельные модули извлечения.
---

## 📁 Структура проекта

```
project_7_etl_pipeline/
├── .env                         # переменные окружения (API ключи, БД)
├── .env.example
├── .gitignore
├── requirements.txt
├── Dockerfile                   # сборка образа Airflow с зависимостями
├── docker-compose.yml           # PostgreSQL + Airflow
├── README.md
├── dags/
│   └── etl_pipeline.py          # Airflow DAG (еженедельный запуск)
├── scripts/
│   ├── config.py                # чтение переменных окружения
│   ├── wb_api.py                # клиент Wildberries API
│   ├── ozon_api.py              # клиент Ozon API
│   ├── extract_articles.py      # выгрузка артикулов
│   ├── match_articles.py        # сопоставление артикулов по internal_code
│   ├── run_full_pipeline.py     # основной ETL-скрипт
│   ├── init_database.py         # создание таблиц
│   └── show_db.py               # просмотр данных в БД
├── sql/
│   └── create_tables.sql        # схема БД
├── data/
│   ├── raw/                     # выгруженные CSV
│   └── processed/               # article_mapping.csv, unified_mart.csv
└── dashboard/
    └── dashboard.py             # Streamlit дашборд
```

---

## 🚀 Установка и запуск

### 1. Клонирование репозитория

```bash
git clone https://github.com/username/project_7_etl_pipeline.git
cd project_7_etl_pipeline
```

### 2. Настройка переменных окружения

Скопируйте `.env.example` в `.env` и заполните реальными API-ключами:

```bash
WB_API_KEY=ваш_ключ_wb
OZON_CLIENT_ID=ваш_client_id_ozon
OZON_API_KEY=ваш_api_key_ozon
POSTGRES_PASSWORD=postgres          # или свой пароль
AIRFLOW_SECRET_KEY=сгенерируйте_строку
AIRFLOW_FERNET_KEY=сгенерируйте_фернет_ключ
```

### 3. Запуск контейнеров (PostgreSQL + Airflow)

```bash
docker-compose up -d
```

После первого запуска дождитесь инициализации (около 2 минут).  
Проверить логи инициализации:

```bash
docker logs project_7_etl_pipeline-airflow-init-1
```

### 4. Создание таблиц аналитики

Таблицы создаются автоматически при инициализации, но при необходимости можно запустить вручную:

```bash
python scripts/init_database.py
```

### 5. Запуск ETL-пайплайна

**Через Airflow (рекомендуется):**

- Откройте `http://localhost:8080` (логин `airflow`, пароль `airflow`)
- Найдите DAG `etl_multichannel_pipeline`
- Нажмите кнопку запуска (▶️)

**Или напрямую (без Airflow):**

```bash
docker exec project_7_etl_pipeline-airflow-scheduler-1 python /opt/airflow/scripts/run_full_pipeline.py
```

### 6. Запуск дашборда

```bash
streamlit run dashboard/dashboard.py
```

Дашборд откроется по адресу `http://localhost:8501`

### 7. Просмотр данных в БД

```bash
docker exec project_7_postgres psql -U postgres -d analytics -c "SELECT * FROM mart_unified_sales LIMIT 10;"
```

---

## 📊 Результаты

- **Полная автоматизация** сбора данных из API WB и Ozon.
- **Единая витрина** `mart_unified_sales`, где каждый товар имеет внутренний код (`internal_code`), связывающий оба канала.
- **Интерактивный дашборд** на Streamlit: общая выручка, заказы по каналам, топ товаров.
- **Инкрементальная загрузка** – каждый запуск ETL загружает только данные за последние 7 дней (настраивается через `.env`).

**Пример данных в витрине (после запуска ETL):**

| product_name                              | wb_sales_count | ozon_sales_count | total_revenue |
|-------------------------------------------|----------------|------------------|---------------|
| Искусственные лианы эвкалипта для декора 3 шт | 7              | 0                | 4772          |
| Искусственные лианы для декора с розами 3 шт | 0              | 2                | 2030          |
| ...                                       | ...            | ...              | ...           |

---

## 🎓 Ключевые навыки

✅ ETL-пайплайны (Extract, Transform, Load)  
✅ Apache Airflow (DAG, операторы, расписания)  
✅ Работа с реальными API (Wildberries, Ozon)  
✅ PostgreSQL (создание таблиц, витрин, индексов)  
✅ Docker / Docker Compose  
✅ Streamlit (интерактивный дашборд)  
✅ Обработка дубликатов и конфликтов уникальности  

---

## 📌 Примечания

- Для корректной работы дашборда убедитесь, что на вашем компьютере не запущен локальный PostgreSQL, использующий порт 5432. В противном случае измените порт в `docker-compose.yml` (например, `5433:5432`) и укажите его в `dashboard.py`.
- Пароль PostgreSQL по умолчанию `postgres`. Если вы его меняли, укажите правильный в `.env` и в `dashboard.py`.

---

## 💡 Почему этот проект важен для портфолио

- **Реальные API** – работа с боевыми системами Wildberries и Ozon.
- **Автоматизация** – Airflow + Docker = production‑ready решение.
- **Data Engineering** – выход за рамки «просто аналитика».
- **Масштабируемость** – архитектура готова к добавлению новых источников: 1С (складские остатки), CRM (заказы с сайта), Google Analytics (трафик) и других. Достаточно реализовать соответствующий модуль извлечения и расширить витрину.

---

## 📫 Контакты

- **GitHub:** [github.com/nadezhdina104s-art](https://github.com/nadezhdina104s-art)
- **Telegram:** [@sweet100](https://t.me/sweet100)
- **Email:** [nadezhdina_s@mail.ru](mailto:nadezhdina_s@mail.ru)


```

## Лицензия

MIT
