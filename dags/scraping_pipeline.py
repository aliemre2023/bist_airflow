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
from src.db.init_db import init_db, upsert_sirket, insert_news, link_news_sirket


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


def step_scrape_news(**kwargs):
    """Haberleri çeker ve XCom üzerinden bir sonraki göreve aktarır."""
    result = scrape_bist_news()
    if result["status"] != "success":
        print("  ⚠️ Haber çekme başarısız veya yeni haber yok.")
        return []

    return result["extracted_news"]


def step_process_news(**kwargs):
    """Çekilen haberleri alır, şirketlerle eşleştirir, sentiment analizi yapar ve DB'ye kaydeder."""
    ti = kwargs["ti"]
    news = ti.xcom_pull(task_ids="scrape_news")

    if not news:
        print("  ⏭️ İşlenecek haber bulunamadı.")
        return

    init_db()
    df = load_df()             # Her gün (liste değişmiş olabilir)
    analyzer = get_analyzer()  # İlk çalışmada yükle, sonra cache'den al
    wallet = Wallet("default")
    today = datetime.now().date()

    for new in news:
        new_date = datetime.strptime(new["date"], "%Y.%m.%d").date()
        new_content = new["content"]
        new_url      = new.get("url", "")
        new_provider = new.get("provider", "")

        # 10 günden eski haberleri atla
        if new_date < today - timedelta(days=10):
            continue

        matched = match_content(df, new_content)
        matched_kods = matched["matched_kods"]
        kod_to_name  = matched["kod_to_name"]

        if len(matched_kods) == 0:
            continue

        sentiment_result, sentiment_ratio = sentimenter(analyzer, new_content)

        print(f"  📰 Haber → {matched_kods} | sentiment: {sentiment_result} ({sentiment_ratio:.2f})")

        # --- DB: haberi kaydet ---
        news_id = insert_news(
            news_url=new_url,
            news_date=new["date"],
            news_provider=new_provider,
            news_content=new_content,
            news_type=sentiment_result,
            news_ratio=sentiment_ratio,
        )

        for kod in matched_kods:
            # --- DB: şirketi upsert et ve junction kaydı ekle ---
            sirket_name = kod_to_name.get(kod, kod)
            sirket_id = upsert_sirket(kod, sirket_name)
            if news_id:
                link_news_sirket(news_id, sirket_id)

    wallet.show()

            


        




# Runs every 30 minutes
schedule_interval=timedelta(minutes=30)

with DAG(
    dag_id='scraper_v1',
    start_date=datetime(2026, 2, 1),
    schedule_interval=schedule_interval,
    catchup=False
) as dag:

    t_scrape = PythonOperator(
        task_id='scrape_news',
        python_callable=step_scrape_news,
    )

    t_process = PythonOperator(
        task_id='process_news',
        python_callable=step_process_news,
        # provide_context is True by default in Airflow 2+, but we can leave it implicit. 
        # The kwargs in the function signature will receive 'ti'.
    )

    # Pipeline flow
    t_scrape >> t_process