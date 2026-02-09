import sys
import os
sys.path.append('/opt/airflow')

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.scraper.news_scraper import scrape_bist_news

with DAG(
    dag_id='bist100_v1',
    start_date=datetime(2026, 2, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    scrape_task = PythonOperator(
        task_id='scrape_news',
        python_callable=scrape_bist_news
    )