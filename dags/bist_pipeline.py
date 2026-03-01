import sys
import os
sys.path.append(os.environ.get('AIRFLOW_HOME', '/opt/airflow'))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from src.scraper.news_scraper_v3 import scrape_bist_news
from src.matcher.matcher import match_content
from src.sentimenter.analyzer import prepare_analyzer
from src.sentimenter.sentimenter import sentimenter
from src.stockbroker.stockbroker import Wallet, stockbroker


def load_df():
    """Şirket listesini yükler"""
    base_dir = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
    xlsx_path = os.path.join(base_dir, 'src/matcher/data/sirketler.xlsx')
    xlsx_local_path = os.path.join("/Users/aliemre2023/Desktop/apache_project/bist100_airflow", 'src/matcher/data/sirketler.xlsx')
    try:
        df = pd.read_excel(xlsx_path, header=2)
    except Exception as e:
        print(f"[ERROR] DF read: {e}")
        df = pd.read_excel(xlsx_local_path, header=2)
    df = df.dropna(subset=["Şirket Ünvanı"])
    ruining_codes = ["HEDEF"]
    df = df[~df["Kod"].isin(ruining_codes)]
    return df

# Modül seviyesinde bir kez yüklenir (Airflow worker process'i ayakta kaldığı sürece cache'de kalır)
_analyzer_cache = None

def get_analyzer():
    global _analyzer_cache
    if _analyzer_cache is None:
        _analyzer_cache = prepare_analyzer()
    return _analyzer_cache


def full_pipeline():
    """Haberleri çek, şirketlerle eşleştir, sentiment analizi yap, al/sat"""
    df = load_df()             # Her gün (liste değişmiş olabilir)
    analyzer = get_analyzer()  # İlk çalışmada yükle, sonra cache'den al
    wallet = Wallet("default")
    today = datetime.now().date()

    result = scrape_bist_news()

    if result["status"] != "success":
        return

    news = result["extracted_news"]

    decisions = {}  # {kod: sentiment_result}

    for new in news:
        new_date = datetime.strptime(new["date"], "%Y.%m.%d").date()
        new_content = new["content"]

        # 2 günden eski haberleri atla
        if new_date < today - timedelta(days=2):
            continue

        matched = match_content(df, new_content)
        matched_kods = matched["matched_kods"]

        if len(matched_kods) == 0:
            continue

        sentiment_result, sentiment_ratio = sentimenter(analyzer, new_content)

        print(f"  📰 Haber → {matched_kods} | sentiment: {sentiment_result} ({sentiment_ratio:.2f})")

        for kod in matched_kods:
            # Aynı şirket için en kötü/iyi sentiment'i tut (en negatif kazanır)
            if kod not in decisions or sentiment_result < decisions[kod]:
                decisions[kod] = sentiment_result

    # Önce negatifler: sat
    for kod, sentiment in decisions.items():
        if sentiment < 0:
            stockbroker(wallet, [kod], sentiment)

    # Sonra pozitifler: al
    for kod, sentiment in decisions.items():
        if sentiment > 0:
            stockbroker(wallet, [kod], sentiment)

    wallet.show()

            


        





with DAG(
    dag_id='bist_v1',
    start_date=datetime(2026, 2, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    pipeline_task = PythonOperator(
        task_id='fetch_and_scrape',
        python_callable=full_pipeline
    )