import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Загружаем переменные из .env (файл в корне проекта)
load_dotenv()

st.set_page_config(layout="wide")
st.title("📊 ETL Dashboard - Wildberries & Ozon")

# Параметры подключения из переменных окружения
DB_HOST = "localhost"
DB_PORT = 5433
DB_NAME = "analytics"
DB_USER = "postgres"
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")  # берём из .env, если нет — postgres

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

@st.cache_data(ttl=60)
def load_data():
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text("SELECT * FROM mart_unified_sales"), conn)
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки данных: {e}")
        return pd.DataFrame()

df = load_data()

if not df.empty:
    total_revenue = df['total_revenue'].sum()
    total_wb_orders = df['wb_sales_count'].sum()
    total_ozon_orders = df['ozon_sales_count'].sum()
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Общая выручка", f"{total_revenue:,.0f} руб.")
    col2.metric("Заказы Wildberries", int(total_wb_orders))
    col3.metric("Заказы Ozon", int(total_ozon_orders))
    
    st.subheader("Топ-10 товаров по выручке")
    top_revenue = df.sort_values('total_revenue', ascending=False).head(10)
    fig1 = px.bar(top_revenue, x='internal_code', y='total_revenue', 
                  title='Выручка по товарам (руб)', color='total_revenue')
    st.plotly_chart(fig1, width='stretch')
    
    st.subheader("Топ-10 товаров по заказам Wildberries")
    top_wb = df.sort_values('wb_sales_count', ascending=False).head(10)
    fig2 = px.bar(top_wb, x='internal_code', y='wb_sales_count', 
                  title='Заказы Wildberries', color='wb_sales_count')
    st.plotly_chart(fig2, width='stretch')
    
    st.subheader("Топ-10 товаров по заказам Ozon")
    top_ozon = df.sort_values('ozon_sales_count', ascending=False).head(10)
    fig3 = px.bar(top_ozon, x='internal_code', y='ozon_sales_count', 
                  title='Заказы Ozon', color='ozon_sales_count')
    st.plotly_chart(fig3, width='stretch')
else:
    st.info("Нет данных. Запустите ETL-пайплайн.")