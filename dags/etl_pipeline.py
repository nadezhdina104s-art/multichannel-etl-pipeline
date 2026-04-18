"""
AIRFLOW DAG - МУЛЬТИКАНАЛЬНЫЙ ETL ПАЙПЛАЙН
===========================================
Запускает полный цикл загрузки из:
- Wildberries (заказы)
- Ozon (заказы FBO)
- Генерация моков 1С, CRM, GA (только при полной загрузке)
- Обновление витрины mart_unified_sales

Расписание: каждый понедельник в 8:00 (инкрементальная загрузка за неделю)
Для первоначальной полной загрузки запускается вручную с conf={"INCREMENTAL": "false"}
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Добавляем путь к скриптам проекта
sys.path.append('/opt/airflow')

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_multichannel_pipeline',
    default_args=default_args,
    description='ETL из WB, Ozon, 1C, CRM, GA с инкрементальной загрузкой',
    schedule_interval='0 8 * * 1',      # каждый понедельник в 8:00
    catchup=False,
    tags=['etl', 'wildberries', 'ozon', 'multichannel'],
)

def run_etl_with_params(**context):
    """
    Запускает основной ETL скрипт с параметрами из конфигурации.
    При инкрементальном запуске (по расписанию) используется последняя неделя.
    Для ручного запуска полной загрузки можно передать conf:
    {"INCREMENTAL": "false", "START_DATE": "2025-01-01", "END_DATE": "2026-04-11"}
    """
    # Устанавливаем переменные окружения на основе конфигурации DAG run
    dag_run = context.get('dag_run')
    if dag_run and dag_run.conf:
        # Ручной запуск с параметрами
        if 'INCREMENTAL' in dag_run.conf:
            os.environ['INCREMENTAL'] = str(dag_run.conf['INCREMENTAL']).lower()
        if 'START_DATE' in dag_run.conf:
            os.environ['START_DATE'] = dag_run.conf['START_DATE']
        if 'END_DATE' in dag_run.conf:
            os.environ['END_DATE'] = dag_run.conf['END_DATE']
    else:
        # Запуск по расписанию: инкрементальный режим (последние 7 дней)
        os.environ['INCREMENTAL'] = 'true'
        # START_DATE и END_DATE не нужны, т.к. run_full_pipeline сам вычислит

    # Импортируем и запускаем основной пайплайн
    from scripts.run_full_pipeline import run_full_pipeline
    run_full_pipeline()

run_etl_task = PythonOperator(
    task_id='run_full_etl_pipeline',
    python_callable=run_etl_with_params,
    dag=dag,
)

# Можно добавить задачу проверки качества, если нужно
# quality_check = PythonOperator(...)

run_etl_task