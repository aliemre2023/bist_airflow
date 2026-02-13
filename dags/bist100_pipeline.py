import sys
import os
sys.path.append(os.environ.get('AIRFLOW_HOME', '/opt/airflow'))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.scraper.news_scraper import fetch_news_page, scrape_bist_news


def full_pipeline():
    """Önce sayfayı çek, sonra haberleri scrape et"""
    fetch_news_page()
    scrape_bist_news()


with DAG(
    dag_id='bist100_v1',
    start_date=datetime(2026, 2, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    pipeline_task = PythonOperator(
        task_id='fetch_and_scrape',
        python_callable=full_pipeline
    )