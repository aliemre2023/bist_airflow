"""
bist_nn_pipeline.py — Airflow DAG for NN-based BIST100 predictions
===================================================================

Schedule: Weekdays at 18:30 Istanbul time (15:30 UTC)
          → After Borsa Istanbul market close (18:00)

Pipeline:
    init_db → fetch_macro_data → run_predictions → execute_trades

Tasks:
    1. init_db          — Ensure all DB tables exist (sirket, news, prediction)
    2. fetch_macro_data — Fetch EVDS (TCMB) and FRED macro data → cache to pickle
    3. run_predictions  — For each BIST100 company:
                            a. Fetch yfinance OHLCV
                            b. Engineer 60+ features
                            c. Load pre-trained model
                            d. Predict next-day direction
                            e. Save prediction to DB
    4. execute_trades   — Convert predictions to signals, execute via Wallet
"""

import sys
import os
sys.path.append(os.environ.get('AIRFLOW_HOME', '/opt/airflow'))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from src.estimater.estimatier1 import (
    fetch_macro_data,
    run_predictions,
    execute_trades,
    init_prediction_table,
)
from src.db.init_db import init_db


# ─── Task callables ──────────────────────────────────────────────────────────

def step_init():
    """Initialize all DB tables."""
    init_db()
    init_prediction_table()


def step_fetch_macro():
    """Fetch and cache EVDS + FRED macro data."""
    fetch_macro_data()


def step_predict():
    """Run NN predictions for all BIST100 companies."""
    run_predictions()


def step_trade():
    """Execute trades based on today's predictions."""
    execute_trades()


# ─── DAG definition ──────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bist_nn_v1",
    default_args=default_args,
    description="NN-based BIST100 stock prediction & trading pipeline",
    start_date=datetime(2026, 3, 1),
    # Weekdays 15:30 UTC = 18:30 Istanbul (UTC+3)
    schedule_interval="30 15 * * 1-5",
    catchup=False,
    tags=["bist100", "nn", "prediction", "trading"],
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

    t_predict = PythonOperator(
        task_id="run_predictions",
        python_callable=step_predict,
        # Only loading models and generating predictions
        execution_timeout=timedelta(minutes=30),
    )

    t_trade = PythonOperator(
        task_id="execute_trades",
        python_callable=step_trade,
        execution_timeout=timedelta(minutes=30),
    )

    # Pipeline flow
    t_init >> t_fetch >> t_predict >> t_trade
