"""
bist_nn_training_pipeline.py — Airflow DAG for Training NN-based BIST100 models
===================================================================

Schedule: Fridays at 19:00 Istanbul time (16:00 UTC)
          → After Borsa Istanbul market close for the week

Pipeline:
    init_db → fetch_macro_data → train_models

Tasks:
    1. init_db          — Ensure all DB tables exist (sirket, news, prediction)
    2. fetch_macro_data — Fetch EVDS (TCMB) and FRED macro data → cache to pickle
    3. train_models     — For each BIST100 company:
                            a. Fetch yfinance OHLCV
                            b. Engineer 60+ features
                            c. Train model (forced execution)
"""

import sys
import os
sys.path.append(os.environ.get('AIRFLOW_HOME', '/opt/airflow'))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

#from src.estimater.estimatier1 import (
#    fetch_macro_data,
#    train_all_models,
#    init_prediction_table,
#)
#from src.db.init_db import init_db


# ─── Task callables ──────────────────────────────────────────────────────────

def step_init():
    """Initialize all DB tables."""
    from src.db.init_db import init_db
    from src.estimater.estimatier1 import (
        init_prediction_table,
    )
    init_db()
    init_prediction_table()


def step_fetch_macro():
    """Fetch and cache EVDS + FRED macro data."""
    from src.estimater.estimatier1 import (
        fetch_macro_data,
    )
    fetch_macro_data()


def step_train():
    """Train NN models for all BIST100 companies."""
    # Force retrain so every Friday we get a fresh model
    from src.estimater.estimatier1 import (
        train_all_models,
    )
    train_all_models(force_retrain=True)


# ─── DAG definition ──────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="training_v1",
    default_args=default_args,
    description="NN-based BIST100 stock prediction - WEEKLY TRAINING pipeline",
    start_date=datetime(2026, 3, 1),
    # Fridays 16:00 UTC = 19:00 Istanbul (UTC+3)
    schedule_interval="0 16 * * 5",
    catchup=False,
    tags=["bist100", "nn", "training", "prediction"],
    max_active_runs=1,
) as dag:

    t_init = PythonOperator(
        task_id="init_db",
        python_callable=step_init,
    )

    t_fetch = PythonOperator(
        task_id="fetch_macro_data",
        python_callable=step_fetch_macro,
        execution_timeout=timedelta(minutes=10),
    )

    t_train = PythonOperator(
        task_id="train_models",
        python_callable=step_train,
        # NN training for all companies can take a long time
        execution_timeout=timedelta(hours=5),
    )

    # Pipeline flow
    t_init >> t_fetch >> t_train
