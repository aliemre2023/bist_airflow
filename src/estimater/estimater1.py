"""
estimatier1.py — NN-Based BIST100 Stock Prediction Module
==========================================================

Production pipeline for predicting stock direction using:
- Technical indicators (60+ features)
- TCMB/EVDS macro data (FX rates)
- FRED US macro data (VIX, oil, yields)
- News sentiment from SQLite DB

Usage (standalone):
    python -m src.estimater.estimatier1

Usage (from DAG):
    from src.estimater.estimatier1 import fetch_macro_data, run_predictions, execute_trades

    fetch_macro_data()      # Step 1: Cache EVDS + FRED
    run_predictions()       # Step 2: Predict all companies
    execute_trades()        # Step 3: Trade based on predictions
"""

import os

# ── M1 Mac segfault prevention — MUST be before torch import ──
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn import metrics
import yfinance as yf
import requests
from datetime import datetime, timedelta
from typing import Optional
import json
import gc
import pickle
import sqlite3
import warnings

warnings.filterwarnings("ignore")

# Threading safety (M1 Mac)
torch.set_num_threads(1)
torch.set_num_interop_threads(1)


# ═══════════════════════════════════════════════════════════════════════════════
# PATHS & CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

BASE_DIR = os.environ.get(
    "AIRFLOW_HOME",
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
DB_PATH = os.path.join(BASE_DIR, "src", "db", "bist.db")
MODELS_DIR = os.path.join(BASE_DIR, "src", "workspace", "models")
CACHE_DIR = os.path.join(BASE_DIR, "src", "workspace", "cache")

# Training defaults
DEFAULT_EPOCHS = 150
DEFAULT_BATCH_SIZE = 128
DEFAULT_LR = 1e-3
DEFAULT_PATIENCE = 50
DEFAULT_DROPOUT = 0.3
RETRAIN_AGE_DAYS = 7
MIN_DATA_ROWS = 300          # minimum rows required for training
YFINANCE_PERIOD = "2000d"    # how much history to fetch

# Signal thresholds
STRONG_BUY_THRESHOLD = 0.65
BUY_THRESHOLD = 0.55
SELL_THRESHOLD = 0.45
STRONG_SELL_THRESHOLD = 0.35


def _load_env():
    """Load .env file if environment variables not set."""
    env_path = os.path.join(BASE_DIR, ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    os.environ.setdefault(key.strip(), val.strip())


_load_env()
os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)


def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


# ═══════════════════════════════════════════════════════════════════════════════
# EVDS (TCMB) MACRO DATA COLLECTOR
# ═══════════════════════════════════════════════════════════════════════════════

EVDS_URL_TPL = (
    "https://evds3.tcmb.gov.tr/igmevdsms-dis/series={series}"
    "&startDate={start}&endDate={end}&type=json"
    "&aggregationTypes={agg}&formulas={formula}&frequency={freq}"
)

# Daily FX series to fetch
EVDS_DAILY_SERIES = [
    "TP.DK.USD.A.YTL", "TP.DK.USD.S.YTL",
    "TP.DK.EUR.A.YTL", "TP.DK.EUR.S.YTL",
    "TP.DK.GBP.A.YTL", "TP.DK.CHF.A.YTL",
    "TP.DK.JPY.A.YTL",
]

# Expected column names after API → DataFrame (API uses underscores)
EVDS_EXPECTED_COLS = [
    "TP_DK_USD_A_YTL", "TP_DK_USD_S_YTL",
    "TP_DK_EUR_A_YTL", "TP_DK_EUR_S_YTL",
    "TP_DK_GBP_A_YTL", "TP_DK_CHF_A_YTL",
    "TP_DK_JPY_A_YTL",
]


class EVDSCollector:
    """Minimal TCMB EVDS API client for FX rate data."""

    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers["key"] = api_key

    def _fetch(self, codes: list[str], start: str, end: str, freq: int = 1) -> pd.DataFrame:
        """Fetch EVDS series and return DataFrame with date index."""
        n = len(codes)
        url = EVDS_URL_TPL.format(
            series="-".join(codes), start=start, end=end,
            agg="-".join(["avg"] * n),
            formula="-".join(["0"] * n),
            freq=freq,
        )
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        items = resp.json().get("items", [])
        if not items:
            return pd.DataFrame()

        df = pd.DataFrame(items)
        if "Tarih" not in df.columns:
            return pd.DataFrame()
        df["date"] = pd.to_datetime(df["Tarih"], dayfirst=True, errors="coerce")
        df = df.drop(columns=["Tarih"], errors="ignore").set_index("date").sort_index()
        # Remove non-numeric metadata columns
        for col in list(df.columns):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(axis=1, how="all")
        return df

    def get_all(self, start: str = "01-01-2018", end: str = "31-12-2026") -> pd.DataFrame:
        """Fetch daily FX rates. Column names will be TP_DK_* format from API."""
        return self._fetch(EVDS_DAILY_SERIES, start, end, freq=1)


# ═══════════════════════════════════════════════════════════════════════════════
# FRED US MACRO DATA
# ═══════════════════════════════════════════════════════════════════════════════

# Map: FRED code → Turkish column name (matching notebook FEATURE_GROUPS)
FRED_SERIES = {
    "FEDFUNDS":    "Fed Faiz Oranı",
    "DGS10":       "10Y Tahvil Faizi",
    "DGS2":        "2Y Tahvil Faizi",
    "T10Y2Y":      "Yield Curve (10Y-2Y spread)",
    "VIXCLS":      "VIX (volatilite endeksi)",
    "DCOILWTICO":  "Ham Petrol Fiyatı",
    "DEXUSEU":     "EUR/USD",
    "DEXCHUS":     "USD/CNY",
    "UMCSENT":     "Tüketici Güven Endeksi",
    "TEDRATE":     "TED Spread (kredi riski)",
}


def fetch_fred_data(api_key: str, start: str = "2018-01-01") -> pd.DataFrame:
    """Fetch US macro data from FRED API. Columns use Turkish names."""
    try:
        from fredapi import Fred
        fred = Fred(api_key=api_key)
        df = pd.DataFrame()
        for code, name in FRED_SERIES.items():
            try:
                df[name] = fred.get_series(code, observation_start=start)
            except Exception as e:
                print(f"  [FRED] {name} error: {e}", flush=True)
        return df.sort_index().ffill()
    except ImportError:
        print("  [FRED] fredapi not installed, skipping", flush=True)
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════════
# FEATURE ENGINEERING
# ═══════════════════════════════════════════════════════════════════════════════

class FeatureEngineer:
    """Builds all features for a company's OHLCV DataFrame.

    Pipeline:
        yfinance OHLCV → technical indicators → EVDS join → FRED join
        → sentiment join → target → clean inf/NaN → ready DataFrame
    """

    # ─── Target ───────────────────────────────────────────────────────────

    @staticmethod
    def add_target(df: pd.DataFrame) -> pd.DataFrame:
        """Next-day direction: 1 if close goes up, 0 otherwise."""
        df["target"] = np.where(df["Close"].shift(-1) > df["Close"], 1, 0)
        return df

    # ─── Core Technical (19 features) ────────────────────────────────────

    @staticmethod
    def add_technical(df: pd.DataFrame) -> pd.DataFrame:
        df["open-close"] = df["Open"] - df["Close"]
        df["low-high"] = df["Low"] - df["High"]
        df["is_quarter_end"] = df.index.to_series().dt.is_quarter_end.astype(int)

        # SMA
        df["SMA_7"] = df["Close"].rolling(7).mean()
        df["SMA_21"] = df["Close"].rolling(21).mean()
        df["SMA_50"] = df["Close"].rolling(50).mean()

        # EMA
        df["EMA_12"] = df["Close"].ewm(span=12, adjust=False).mean()
        df["EMA_26"] = df["Close"].ewm(span=26, adjust=False).mean()

        # MACD
        df["MACD"] = df["EMA_12"] - df["EMA_26"]
        df["MACD_signal"] = df["MACD"].ewm(span=9, adjust=False).mean()
        df["MACD_hist"] = df["MACD"] - df["MACD_signal"]

        # RSI 14
        delta = df["Close"].diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        df["RSI_14"] = 100 - (100 / (1 + gain / loss))

        # Bollinger Bands
        df["BB_mid"] = df["Close"].rolling(20).mean()
        df["BB_std"] = df["Close"].rolling(20).std()
        df["BB_upper"] = df["BB_mid"] + 2 * df["BB_std"]
        df["BB_lower"] = df["BB_mid"] - 2 * df["BB_std"]
        df["BB_width"] = df["BB_upper"] - df["BB_lower"]

        # ATR 14
        high_low = df["High"] - df["Low"]
        high_close = (df["High"] - df["Close"].shift(1)).abs()
        low_close = (df["Low"] - df["Close"].shift(1)).abs()
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df["ATR_14"] = true_range.rolling(14).mean()

        # Stochastic Oscillator
        low_14 = df["Low"].rolling(14).min()
        high_14 = df["High"].rolling(14).max()
        df["Stoch_K"] = 100 * (df["Close"] - low_14) / (high_14 - low_14)
        df["Stoch_D"] = df["Stoch_K"].rolling(3).mean()

        # Others
        df["OBV"] = (np.sign(df["Close"].diff()) * df["Volume"]).fillna(0).cumsum()
        df["Momentum_10"] = df["Close"] - df["Close"].shift(10)
        df["VWAP"] = (
            (df["Volume"] * (df["High"] + df["Low"] + df["Close"]) / 3).cumsum()
            / df["Volume"].cumsum()
        )
        return df

    # ─── Extended Technical (39 features) ─────────────────────────────────

    @staticmethod
    def add_technical_extra(df: pd.DataFrame) -> pd.DataFrame:
        # ── Trend ──
        df["EMA_12_26_cross"] = df["EMA_12"] - df["EMA_26"]
        df["SMA_7_21_cross"] = df["SMA_7"] - df["SMA_21"]

        # ADX
        plus_dm = df["High"].diff()
        minus_dm = -df["Low"].diff()
        plus_dm = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0), 0.0)
        minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)
        plus_di = 100 * (plus_dm.rolling(14).mean() / (df["ATR_14"] + 1e-10))
        minus_di = 100 * (minus_dm.rolling(14).mean() / (df["ATR_14"] + 1e-10))
        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10)
        df["ADX_14"] = dx.rolling(14).mean()
        df["Plus_DI"] = plus_di
        df["Minus_DI"] = minus_di
        df["close_above_SMA50"] = (df["Close"] > df["SMA_50"]).astype(int)
        df["close_above_SMA21"] = (df["Close"] > df["SMA_21"]).astype(int)

        # ── Volatility ──
        df["KC_mid"] = df["Close"].ewm(span=20, adjust=False).mean()
        df["KC_upper"] = df["KC_mid"] + 1.5 * df["ATR_14"]
        df["KC_lower"] = df["KC_mid"] - 1.5 * df["ATR_14"]
        df["KC_width"] = df["KC_upper"] - df["KC_lower"]
        df["BB_pctB"] = (df["Close"] - df["BB_lower"]) / (df["BB_upper"] - df["BB_lower"] + 1e-10)
        df["HV_10"] = df["Close"].pct_change().rolling(10).std() * np.sqrt(252) * 100
        df["HV_30"] = df["Close"].pct_change().rolling(30).std() * np.sqrt(252) * 100
        df["ATR_pct"] = df["ATR_14"] / df["Close"] * 100

        # ── Volume ──
        df["Vol_SMA_20"] = df["Volume"].rolling(20).mean()
        df["Vol_ratio"] = df["Volume"] / (df["Vol_SMA_20"] + 1e-10)

        typical_price = (df["High"] + df["Low"] + df["Close"]) / 3
        raw_money_flow = typical_price * df["Volume"]
        mf_sign = np.sign(typical_price.diff())
        pos_mf = (raw_money_flow * (mf_sign > 0)).rolling(14).sum()
        neg_mf = (raw_money_flow * (mf_sign < 0)).abs().rolling(14).sum()
        df["MFI_14"] = 100 - (100 / (1 + pos_mf / (neg_mf + 1e-10)))

        clv = ((df["Close"] - df["Low"]) - (df["High"] - df["Close"])) / (df["High"] - df["Low"] + 1e-10)
        df["CMF_20"] = (clv * df["Volume"]).rolling(20).sum() / (df["Volume"].rolling(20).sum() + 1e-10)
        df["Force_13"] = (df["Close"].diff() * df["Volume"]).ewm(span=13, adjust=False).mean()

        # ── Momentum / Oscillator ──
        low_14 = df["Low"].rolling(14).min()
        high_14 = df["High"].rolling(14).max()
        df["Williams_R"] = -100 * (high_14 - df["Close"]) / (high_14 - low_14 + 1e-10)

        tp = (df["High"] + df["Low"] + df["Close"]) / 3
        df["CCI_20"] = (tp - tp.rolling(20).mean()) / (0.015 * tp.rolling(20).std() + 1e-10)

        df["ROC_5"] = df["Close"].pct_change(5) * 100
        df["ROC_10"] = df["Close"].pct_change(10) * 100
        df["ROC_20"] = df["Close"].pct_change(20) * 100

        pc = df["Close"].diff()
        double_smooth_pc = pc.ewm(span=25).mean().ewm(span=13).mean()
        double_smooth_abs = pc.abs().ewm(span=25).mean().ewm(span=13).mean()
        df["TSI"] = 100 * double_smooth_pc / (double_smooth_abs + 1e-10)

        # ── Price Structure ──
        df["return_1d"] = df["Close"].pct_change(1)
        df["return_5d"] = df["Close"].pct_change(5)
        df["return_20d"] = df["Close"].pct_change(20)
        df["close_position"] = (df["Close"] - df["Low"]) / (df["High"] - df["Low"] + 1e-10)
        df["gap"] = (df["Open"] - df["Close"].shift(1)) / (df["Close"].shift(1) + 1e-10)
        df["body_ratio"] = (df["Close"] - df["Open"]).abs() / (df["High"] - df["Low"] + 1e-10)
        df["upper_shadow"] = (df["High"] - df[["Open", "Close"]].max(axis=1)) / (df["High"] - df["Low"] + 1e-10)
        df["lower_shadow"] = (df[["Open", "Close"]].min(axis=1) - df["Low"]) / (df["High"] - df["Low"] + 1e-10)
        df["pct_from_52w_high"] = df["Close"] / df["High"].rolling(252).max() - 1
        df["pct_from_52w_low"] = df["Close"] / df["Low"].rolling(252).min() - 1
        df["dist_from_SMA50"] = (df["Close"] - df["SMA_50"]) / (df["SMA_50"] + 1e-10) * 100

        # ── Lag Features ──
        for lag in [1, 2, 3, 5]:
            df[f"return_lag_{lag}"] = df["return_1d"].shift(lag)
            df[f"volume_chg_lag_{lag}"] = df["Volume"].pct_change().shift(lag)

        return df

    # ─── Macro FX Features ────────────────────────────────────────────────

    @staticmethod
    def add_macro_features(df: pd.DataFrame, df_evds: Optional[pd.DataFrame]) -> pd.DataFrame:
        """Join EVDS FX data and derive macro features."""
        if df_evds is None or df_evds.empty:
            return df

        # Select available EVDS columns
        evds_cols = [c for c in EVDS_EXPECTED_COLS if c in df_evds.columns]
        if not evds_cols:
            return df

        df = df.join(df_evds[evds_cols], how="left")

        # Forward fill weekends / holidays
        for col in evds_cols:
            if col in df.columns:
                df[col] = df[col].ffill()

        # Derived FX features
        if "TP_DK_USD_S_YTL" in df.columns and "TP_DK_USD_A_YTL" in df.columns:
            df["USD_spread"] = df["TP_DK_USD_S_YTL"] - df["TP_DK_USD_A_YTL"]
        if "TP_DK_USD_A_YTL" in df.columns:
            df["USD_change"] = df["TP_DK_USD_A_YTL"].pct_change()
            df["USD_change_5d"] = df["TP_DK_USD_A_YTL"].pct_change(5)
            df["USD_sma_7"] = df["TP_DK_USD_A_YTL"].rolling(7).mean()
            df["USD_volatility"] = df["TP_DK_USD_A_YTL"].pct_change().rolling(14).std()
        if "TP_DK_EUR_A_YTL" in df.columns and "TP_DK_USD_A_YTL" in df.columns:
            df["EUR_USD_ratio"] = df["TP_DK_EUR_A_YTL"] / df["TP_DK_USD_A_YTL"]
        if "TP_DK_USD_A_YTL" in df.columns:
            df["close_usd"] = df["Close"] / df["TP_DK_USD_A_YTL"]
            df["close_usd_change"] = df["close_usd"].pct_change()

        return df

    # ─── FRED Features ────────────────────────────────────────────────────

    @staticmethod
    def add_fred_features(df: pd.DataFrame, df_fred: Optional[pd.DataFrame]) -> pd.DataFrame:
        """Join FRED US macro data (already using Turkish column names)."""
        if df_fred is None or df_fred.empty:
            return df

        fred_cols = [c for c in df_fred.columns if c in df_fred.columns]
        df = df.join(df_fred[fred_cols], how="left")

        for col in fred_cols:
            if col in df.columns:
                df[col] = df[col].ffill()

        return df

    # ─── Sentiment Features ───────────────────────────────────────────────

    @staticmethod
    def add_sentiment(df: pd.DataFrame, sirket_code: str, conn: sqlite3.Connection) -> pd.DataFrame:
        """Join sentiment data from SQLite news tables."""
        query = """
        SELECT n.news_date AS date,
               AVG(n.news_ratio) AS avg_sentiment,
               COUNT(n.news_id)  AS news_count
        FROM   news n
        JOIN   news_sirket ns ON ns.news_id  = n.news_id
        JOIN   sirket      s  ON s.sirket_id = ns.sirket_id
        WHERE  s.sirket_code = ?
        GROUP BY n.news_date
        """
        try:
            sdf = pd.read_sql_query(query, conn, params=(sirket_code,))
            sdf["date"] = pd.to_datetime(sdf["date"])
            sdf.set_index("date", inplace=True)
            df = df.join(sdf, how="left")
        except Exception as e:
            print(f"  [Sentiment] {sirket_code}: {e}", flush=True)

        # Ensure sentiment columns exist (fill with 0 if no news data)
        for col in ["avg_sentiment", "news_count"]:
            if col not in df.columns:
                df[col] = 0
            else:
                df[col] = df[col].fillna(0)

        df["sentiment_7d"] = df["avg_sentiment"].rolling(7).mean().fillna(0)
        df["sentiment_14d"] = df["avg_sentiment"].rolling(14).mean().fillna(0)
        df["sentiment_30d"] = df["avg_sentiment"].rolling(30).mean().fillna(0)
        df["sentiment_50d"] = df["avg_sentiment"].rolling(50).mean().fillna(0)

        return df

    # ─── Full Pipeline ────────────────────────────────────────────────────

    @classmethod
    def build_features(
        cls,
        sirket_code: str,
        df_evds: Optional[pd.DataFrame] = None,
        df_fred: Optional[pd.DataFrame] = None,
        conn: Optional[sqlite3.Connection] = None,
        period: str = YFINANCE_PERIOD,
    ) -> Optional[pd.DataFrame]:
        """Full feature engineering pipeline for one company.

        Returns ready-to-train DataFrame with target + all features, or None.
        """
        print(f"  📊 {sirket_code}: Fetching yfinance...", flush=True)
        try:
            df = yf.Ticker(f"{sirket_code}.IS").history(period=period)
        except Exception as e:
            print(f"  ❌ {sirket_code}: yfinance error: {e}", flush=True)
            return None

        if df.empty or len(df) < MIN_DATA_ROWS:
            print(f"  ⚠️  {sirket_code}: Insufficient data ({len(df)} rows)", flush=True)
            return None

        # Remove timezone
        if hasattr(df.index, "tz") and df.index.tz is not None:
            df.index = df.index.tz_localize(None)

        # Build features in order
        df = cls.add_target(df)
        df = cls.add_technical(df)
        df = cls.add_technical_extra(df)
        df = cls.add_macro_features(df, df_evds)
        df = cls.add_fred_features(df, df_fred)
        if conn is not None:
            df = cls.add_sentiment(df, sirket_code, conn)

        # Clean inf → NaN, then drop NaN rows
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df.dropna(inplace=True)

        print(f"  ✅ {sirket_code}: {len(df)} rows × {len(df.columns)} cols", flush=True)
        return df


# ═══════════════════════════════════════════════════════════════════════════════
# FEATURE GROUPS
# ═══════════════════════════════════════════════════════════════════════════════

FEATURE_GROUPS = {
    "technical": [
        "open-close", "low-high", "is_quarter_end",
        "SMA_7", "SMA_21", "SMA_50",
        "MACD", "MACD_signal", "MACD_hist",
        "RSI_14", "BB_width", "BB_upper", "BB_lower",
        "ATR_14", "Stoch_K", "Stoch_D",
        "OBV", "Momentum_10", "VWAP",
    ],
    "technical_extra": [
        "EMA_12_26_cross", "SMA_7_21_cross",
        "ADX_14", "Plus_DI", "Minus_DI",
        "close_above_SMA50", "close_above_SMA21",
        "KC_width", "BB_pctB", "HV_10", "HV_30", "ATR_pct",
        "Vol_ratio", "MFI_14", "CMF_20", "Force_13",
        "Williams_R", "CCI_20", "ROC_5", "ROC_10", "ROC_20", "TSI",
        "return_1d", "return_5d", "return_20d",
        "close_position", "gap", "body_ratio",
        "upper_shadow", "lower_shadow",
        "pct_from_52w_high", "pct_from_52w_low", "dist_from_SMA50",
        "return_lag_1", "return_lag_2", "return_lag_3", "return_lag_5",
        "volume_chg_lag_1", "volume_chg_lag_2", "volume_chg_lag_3", "volume_chg_lag_5",
    ],
    "sentiment": [
        "avg_sentiment", "news_count", "sentiment_7d",
        "sentiment_14d", "sentiment_30d",
    ],
    "macro_fx": [
        "USD_change", "USD_change_5d", "USD_sma_7",
        "close_usd", "close_usd_change",
        "TP_DK_USD_S_YTL", "TP_DK_USD_A_YTL",
        "USD_spread", "USD_volatility", "EUR_USD_ratio",
    ],
    "fred": [
        "Fed Faiz Oranı", "10Y Tahvil Faizi", "2Y Tahvil Faizi",
        "Yield Curve (10Y-2Y spread)", "VIX (volatilite endeksi)",
        "Ham Petrol Fiyatı", "USD/CNY", "Tüketici Güven Endeksi",
        "TED Spread (kredi riski)",
    ],
}

# Default: use all groups
DEFAULT_FEATURE_GROUPS_old = ["technical", "technical_extra", "sentiment", "macro_fx", "fred"]
DEFAULT_FEATURE_GROUPS = ["technical", "technical_extra", "sentiment"]


def get_feature_columns(groups: Optional[list[str]] = None) -> list[str]:
    """Flat list of feature column names for the given groups."""
    if groups is None:
        groups = DEFAULT_FEATURE_GROUPS
    cols = []
    for g in groups:
        cols.extend(FEATURE_GROUPS.get(g, []))
    return cols


# ═══════════════════════════════════════════════════════════════════════════════
# NEURAL NETWORK MODEL
# ═══════════════════════════════════════════════════════════════════════════════

class TradingNN(nn.Module):
    """
    Residual Block NN for tabular stock prediction.

    Architecture:
        Input → Proj(256) → Block1(256→256) → Block2(256→128) → Block3(128→64)
        → Head(64→32→1)

    Each block: Linear → BatchNorm → GELU → Dropout + residual shortcut
    """

    def __init__(self, input_dim: int, dropout: float = DEFAULT_DROPOUT):
        super().__init__()

        self.input_proj = nn.Sequential(
            nn.Linear(input_dim, 256),
            nn.BatchNorm1d(256),
            nn.GELU(),
            nn.Dropout(dropout),
        )

        self.block1 = self._res_block(256, 256, dropout)
        self.block2 = self._res_block(256, 128, dropout)
        self.block3 = self._res_block(128, 64, dropout)

        self.head = nn.Sequential(
            nn.Linear(64, 32),
            nn.GELU(),
            nn.Linear(32, 1),
        )

    @staticmethod
    def _res_block(in_dim: int, out_dim: int, dropout: float) -> nn.ModuleList:
        block = nn.Sequential(
            nn.Linear(in_dim, out_dim),
            nn.BatchNorm1d(out_dim),
            nn.GELU(),
            nn.Dropout(dropout),
        )
        shortcut = nn.Linear(in_dim, out_dim) if in_dim != out_dim else nn.Identity()
        return nn.ModuleList([block, shortcut])

    def _forward_res(self, x: torch.Tensor, res_block: nn.ModuleList) -> torch.Tensor:
        block, shortcut = res_block
        return block(x) + shortcut(x)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = self.input_proj(x)
        x = self._forward_res(x, self.block1)
        x = self._forward_res(x, self.block2)
        x = self._forward_res(x, self.block3)
        return self.head(x).squeeze(1)


# ═══════════════════════════════════════════════════════════════════════════════
# MODEL MANAGER — Train / Save / Load / Predict
# ═══════════════════════════════════════════════════════════════════════════════

class ModelManager:
    """Manages per-company TradingNN lifecycle."""

    def __init__(self, sirket_code: str, feature_groups: Optional[list[str]] = None):
        self.sirket_code = sirket_code
        self.feature_groups = feature_groups or DEFAULT_FEATURE_GROUPS
        self.model: Optional[TradingNN] = None
        self.scaler = StandardScaler()
        self.feature_cols: list[str] = []
        self.metadata: dict = {}

    @property
    def model_path(self) -> str:
        return os.path.join(MODELS_DIR, f"{self.sirket_code}_nn.pt")

    @property
    def scaler_path(self) -> str:
        return os.path.join(MODELS_DIR, f"{self.sirket_code}_scaler.pkl")

    @property
    def meta_path(self) -> str:
        return os.path.join(MODELS_DIR, f"{self.sirket_code}_meta.json")

    # ─── Training ─────────────────────────────────────────────────────────

    def train(
        self,
        df: pd.DataFrame,
        epochs: int = DEFAULT_EPOCHS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        lr: float = DEFAULT_LR,
        dropout: float = DEFAULT_DROPOUT,
        patience: int = DEFAULT_PATIENCE,
    ) -> Optional[float]:
        """Train model on a company's feature DataFrame. Returns best val AUC or None."""

        # Select available feature columns
        all_feature_cols = get_feature_columns(self.feature_groups)
        available_cols = [c for c in all_feature_cols if c in df.columns]

        if len(available_cols) < 5:
            print(f"  ❌ {self.sirket_code}: Only {len(available_cols)} features available", flush=True)
            return None

        self.feature_cols = available_cols

        X = df[available_cols].values.astype(np.float64)
        y = df["target"].values

        # Clean any remaining NaN/inf
        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)

        # Scale
        X = self.scaler.fit_transform(X)

        # Train / validation split
        X_train, X_valid, y_train, y_valid = train_test_split(
            X, y, test_size=0.1, random_state=42, stratify=y
        )

        # Convert to tensors
        X_tr = torch.from_numpy(np.ascontiguousarray(X_train, dtype=np.float32))
        Y_tr = torch.from_numpy(np.asarray(y_train, dtype=np.float32))
        X_va = torch.from_numpy(np.ascontiguousarray(X_valid, dtype=np.float32))
        Y_va = torch.from_numpy(np.asarray(y_valid, dtype=np.float32))

        loader = DataLoader(
            TensorDataset(X_tr, Y_tr),
            batch_size=batch_size,
            shuffle=True,
            drop_last=True,
            num_workers=0,
        )

        model = TradingNN(input_dim=len(available_cols), dropout=dropout)
        optimizer = optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-4)
        scheduler = optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=epochs)

        # Class imbalance handling
        pos_weight = torch.tensor(
            float((y_train == 0).sum()) / max(float((y_train == 1).sum()), 1.0)
        )
        criterion = nn.BCEWithLogitsLoss(pos_weight=pos_weight)

        best_auc = 0.0
        best_state = None
        no_improve = 0
        final_epoch = 0

        for epoch in range(1, epochs + 1):
            model.train()
            for xb, yb in loader:
                optimizer.zero_grad()
                loss = criterion(model(xb), yb)
                loss.backward()
                nn.utils.clip_grad_norm_(model.parameters(), 1.0)
                optimizer.step()
            scheduler.step()

            # Evaluate
            model.eval()
            with torch.no_grad():
                val_prob = torch.sigmoid(model(X_va)).numpy()

            try:
                val_auc = metrics.roc_auc_score(y_valid, val_prob)
            except ValueError:
                val_auc = 0.5

            if val_auc > best_auc:
                best_auc = val_auc
                best_state = {k: v.clone() for k, v in model.state_dict().items()}
                no_improve = 0
            else:
                no_improve += 1
                if no_improve >= patience:
                    final_epoch = epoch
                    break

            final_epoch = epoch

        if best_state is not None:
            model.load_state_dict(best_state)

        self.model = model
        gc.collect()

        # Persist
        self._save(best_auc, final_epoch)

        print(
            f"  🧠 {self.sirket_code}: Val AUC={best_auc:.4f} "
            f"({len(available_cols)} features, {final_epoch} epochs)",
            flush=True,
        )
        return best_auc

    # ─── Persistence ──────────────────────────────────────────────────────

    def _save(self, val_auc: Optional[float] = None, epochs_trained: int = 0):
        """Save model weights, scaler, and metadata to disk."""
        os.makedirs(MODELS_DIR, exist_ok=True)

        torch.save(self.model.state_dict(), self.model_path)

        with open(self.scaler_path, "wb") as f:
            pickle.dump(self.scaler, f)

        self.metadata = {
            "sirket_code": self.sirket_code,
            "feature_cols": self.feature_cols,
            "feature_groups": self.feature_groups,
            "input_dim": len(self.feature_cols),
            "val_auc": val_auc,
            "epochs_trained": epochs_trained,
            "trained_at": datetime.now().isoformat(),
        }
        with open(self.meta_path, "w") as f:
            json.dump(self.metadata, f, indent=2, ensure_ascii=False)

    def load(self) -> bool:
        """Load model from disk. Returns True if successful."""
        if not all(os.path.exists(p) for p in [self.model_path, self.scaler_path, self.meta_path]):
            return False

        try:
            with open(self.meta_path) as f:
                self.metadata = json.load(f)

            self.feature_cols = self.metadata["feature_cols"]
            input_dim = self.metadata["input_dim"]

            with open(self.scaler_path, "rb") as f:
                self.scaler = pickle.load(f)

            self.model = TradingNN(input_dim=input_dim)
            self.model.load_state_dict(
                torch.load(self.model_path, map_location="cpu", weights_only=True)
            )
            self.model.eval()
            return True
        except Exception as e:
            print(f"  ⚠️  {self.sirket_code}: Model load failed: {e}", flush=True)
            return False

    def needs_retrain(self, max_age_days: int = RETRAIN_AGE_DAYS) -> bool:
        """Check if model is missing or stale."""
        if not self.load():
            return True
        trained_at = self.metadata.get("trained_at", "")
        if not trained_at:
            return True
        try:
            age = (datetime.now() - datetime.fromisoformat(trained_at)).days
            return age >= max_age_days
        except Exception:
            return True

    # ─── Prediction ───────────────────────────────────────────────────────

    def predict(self, df: pd.DataFrame) -> Optional[dict]:
        """Predict using the last row of the DataFrame.

        Returns dict with sirket_code, probability, signal, prediction_date.
        """
        if self.model is None and not self.load():
            return None

        # Handle missing columns: fill with 0
        for col in self.feature_cols:
            if col not in df.columns:
                df[col] = 0

        # Last row
        X = df[self.feature_cols].iloc[-1:].values.astype(np.float64)
        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
        X = self.scaler.transform(X)

        self.model.eval()
        Xt = torch.from_numpy(np.ascontiguousarray(X, dtype=np.float32))
        with torch.no_grad():
            prob = torch.sigmoid(self.model(Xt)).item()

        # Map probability → trading signal
        if prob >= STRONG_BUY_THRESHOLD:
            signal = 2
        elif prob >= BUY_THRESHOLD:
            signal = 1
        elif prob <= STRONG_SELL_THRESHOLD:
            signal = -2
        elif prob <= SELL_THRESHOLD:
            signal = -1
        else:
            signal = 0

        return {
            "sirket_code": self.sirket_code,
            "probability": round(prob, 4),
            "signal": signal,
            "prediction_date": datetime.now().strftime("%Y-%m-%d"),
            "model_auc": self.metadata.get("val_auc"),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# DB OPERATIONS — prediction table
# ═══════════════════════════════════════════════════════════════════════════════

def init_prediction_table():
    """Create prediction table if it doesn't exist."""
    conn = get_db_connection()
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS prediction (
                prediction_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                sirket_id       INTEGER NOT NULL REFERENCES sirket(sirket_id),
                prediction_date TEXT    NOT NULL,
                signal          INTEGER,
                probability     REAL,
                model_version   TEXT    DEFAULT 'nn_v1',
                model_auc       REAL,
                created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
                UNIQUE(sirket_id, prediction_date, model_version)
            )
        """)
    conn.close()
    print("  ✅ prediction table ready", flush=True)


def save_prediction(sirket_code: str, prediction: dict):
    """Upsert a prediction into the DB."""
    conn = get_db_connection()
    try:
        row = conn.execute(
            "SELECT sirket_id FROM sirket WHERE sirket_code = ?", (sirket_code,)
        ).fetchone()
        if not row:
            print(f"  ⚠️  {sirket_code}: not in DB, skipping save", flush=True)
            return

        sirket_id = row["sirket_id"]
        with conn:
            conn.execute(
                """
                INSERT INTO prediction
                    (sirket_id, prediction_date, signal, probability, model_version, model_auc)
                VALUES (?, ?, ?, ?, 'nn_v1', ?)
                ON CONFLICT(sirket_id, prediction_date, model_version)
                DO UPDATE SET
                    signal      = excluded.signal,
                    probability = excluded.probability,
                    model_auc   = excluded.model_auc,
                    created_at  = datetime('now')
                """,
                (
                    sirket_id,
                    prediction["prediction_date"],
                    prediction["signal"],
                    prediction["probability"],
                    prediction.get("model_auc"),
                ),
            )
    finally:
        conn.close()


def get_today_predictions() -> list[dict]:
    """Retrieve all predictions for today."""
    today = datetime.now().strftime("%Y-%m-%d")
    conn = get_db_connection()
    rows = conn.execute(
        """
        SELECT s.sirket_code, p.signal, p.probability, p.model_auc
        FROM   prediction p
        JOIN   sirket s ON s.sirket_id = p.sirket_id
        WHERE  p.prediction_date = ? AND p.model_version = 'nn_v1'
        ORDER  BY p.probability DESC
        """,
        (today,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE STEPS (called by DAG or standalone)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_macro_data():
    """Step 1: Fetch EVDS + FRED macro data and cache to pickle files."""
    print("\n" + "=" * 60, flush=True)
    print("  STEP 1 — Fetch Macro Data", flush=True)
    print("=" * 60, flush=True)

    # ── EVDS ──
    evds_cache = os.path.join(CACHE_DIR, "evds_latest.pkl")
    evds_key = os.environ.get("EVDS_API_KEY", "")
    if evds_key:
        try:
            collector = EVDSCollector(evds_key)
            chunks = []
            for year in range(2018, datetime.now().year + 1):
                chunk = collector.get_all(f"01-01-{year}", f"31-12-{year}")
                if not chunk.empty:
                    chunks.append(chunk)
                    print(f"    EVDS {year}: {len(chunk)} rows", flush=True)
            if chunks:
                df_evds = pd.concat(chunks).sort_index()
                df_evds = df_evds[~df_evds.index.duplicated(keep="last")]
                df_evds.to_pickle(evds_cache)
                print(f"  ✅ EVDS cached: {df_evds.shape}", flush=True)
        except Exception as e:
            print(f"  ❌ EVDS error: {e}", flush=True)
    else:
        print("  ⚠️  EVDS_API_KEY not set — skipping", flush=True)

    # ── FRED ──
    fred_cache = os.path.join(CACHE_DIR, "fred_latest.pkl")
    fred_key = os.environ.get("FRED_API_KEY", "")
    if fred_key:
        try:
            df_fred = fetch_fred_data(fred_key)
            if not df_fred.empty:
                df_fred.to_pickle(fred_cache)
                print(f"  ✅ FRED cached: {df_fred.shape}", flush=True)
        except Exception as e:
            print(f"  ❌ FRED error: {e}", flush=True)
    else:
        print("  ⚠️  FRED_API_KEY not set — skipping", flush=True)


def _load_cached_macro():
    """Load cached EVDS + FRED DataFrames (or None)."""
    evds_path = os.path.join(CACHE_DIR, "evds_latest.pkl")
    fred_path = os.path.join(CACHE_DIR, "fred_latest.pkl")
    df_evds = pd.read_pickle(evds_path) if os.path.exists(evds_path) else None
    df_fred = pd.read_pickle(fred_path) if os.path.exists(fred_path) else None
    return df_evds, df_fred


def train_all_models(force_retrain: bool = False, max_age_days: int = RETRAIN_AGE_DAYS):
    """Step 1.5 (Optional): Train NN models for all companies.
    
    If force_retrain is True, it trains regardless of age.
    Otherwise, it trains only if the model is missing or >max_age_days old.
    """
    print("\n" + "=" * 60, flush=True)
    print("  STEP 1.5 — Train NN Models", flush=True)
    print("=" * 60, flush=True)

    init_prediction_table()

    # Load macro caches
    df_evds, df_fred = _load_cached_macro()

    # All companies
    conn = get_db_connection()
    companies = conn.execute("SELECT sirket_code FROM sirket").fetchall()
    company_codes = [r["sirket_code"] for r in companies]
    conn.close()

    print(f"\n  📋 {len(company_codes)} companies to check for training\n", flush=True)

    stats = {"trained": 0, "failed": 0, "skipped": 0, "not_needed": 0}

    for i, code in enumerate(company_codes, 1):
        try:
            manager = ModelManager(code)
            
            if not force_retrain and not manager.needs_retrain(max_age_days=max_age_days):
                print(f"  ⏭️  [{i}/{len(company_codes)}] {code}: Model is fresh, skipping training.", flush=True)
                stats["not_needed"] += 1
                continue

            print(f"  🔄 [{i}/{len(company_codes)}] {code}: Training...", flush=True)
            
            conn = get_db_connection()
            df = FeatureEngineer.build_features(
                code, df_evds=df_evds, df_fred=df_fred, conn=conn
            )
            conn.close()

            if df is None or len(df) < MIN_DATA_ROWS:
                print(f"  ⚠️  [{i}/{len(company_codes)}] {code}: Insufficient data for training.", flush=True)
                stats["skipped"] += 1
                continue

            auc = manager.train(df)
            if auc is None:
                stats["failed"] += 1
            else:
                stats["trained"] += 1

        except Exception as e:
            print(f"  ❌ [{i}/{len(company_codes)}] {code}: Training Error: {e}", flush=True)
            stats["failed"] += 1

    print(f"\n{'=' * 60}", flush=True)
    print(
        f"  📊 Training Done: {stats['trained']} trained | "
        f"{stats['not_needed']} up-to-date | "
        f"{stats['failed']} failed | {stats['skipped']} skipped",
        flush=True,
    )
    print("=" * 60, flush=True)
    return stats


def check_models_exist(min_models: int = 1):
    """Pre-check: Verify that at least `min_models` trained models exist.

    Raises AirflowFailException if no models found, so the DAG fails
    clearly instead of silently skipping all predictions.
    """
    from airflow.exceptions import AirflowFailException

    if not os.path.isdir(MODELS_DIR):
        raise AirflowFailException(
            f"Models directory does not exist: {MODELS_DIR}\n"
            "Training pipeline (bist_nn_training_v1) must run at least once before trading."
        )

    model_files = [f for f in os.listdir(MODELS_DIR) if f.endswith(".pt")]
    count = len(model_files)
    print(f"  🔍 Model check: found {count} trained model(s) in {MODELS_DIR}", flush=True)

    if count < min_models:
        raise AirflowFailException(
            f"Only {count} model(s) found (minimum required: {min_models}).\n"
            "Training pipeline (bist_nn_training_v1) must run at least once before trading.\n"
            "Trigger it manually:  airflow dags trigger bist_nn_training_v1"
        )

    print(f"  ✅ Model check passed ({count} models available)", flush=True)
    return count


def run_predictions():
    """Step 2: Run NN predictions for all companies in the DB.

    For each company:
        1. Build feature DataFrame
        2. Load model (skip if missing)
        3. Predict next-day direction
        4. Save prediction to DB
    """
    print("\n" + "=" * 60, flush=True)
    print("  STEP 2 — Run NN Predictions", flush=True)
    print("=" * 60, flush=True)

    init_prediction_table()

    # Load macro caches
    df_evds, df_fred = _load_cached_macro()
    if df_evds is not None:
        print(f"  EVDS cache loaded: {df_evds.shape}", flush=True)
    if df_fred is not None:
        print(f"  FRED cache loaded: {df_fred.shape}", flush=True)

    # All companies
    conn = get_db_connection()
    companies = conn.execute("SELECT sirket_code FROM sirket").fetchall()
    company_codes = [r["sirket_code"] for r in companies]
    conn.close()

    print(f"\n  📋 {len(company_codes)} companies to process\n", flush=True)

    stats = {"success": 0, "failed": 0, "skipped": 0, "no_model": 0}

    for i, code in enumerate(company_codes, 1):
        try:
            manager = ModelManager(code)
            
            if not manager.load():
                print(f"  ⚠️  [{i}/{len(company_codes)}] {code}: No model found, skipping prediction.", flush=True)
                stats["no_model"] += 1
                continue

            conn = get_db_connection()

            # 1. Build features
            df = FeatureEngineer.build_features(
                code, df_evds=df_evds, df_fred=df_fred, conn=conn
            )
            conn.close()

            if df is None or len(df) < MIN_DATA_ROWS:
                stats["skipped"] += 1
                continue

            # 2. Predict
            prediction = manager.predict(df)
            if prediction is None:
                stats["failed"] += 1
                continue

            # 3. Save
            save_prediction(code, prediction)

            signal_map = {2: "🟢🟢", 1: "🟢", 0: "⚪", -1: "🔴", -2: "🔴🔴"}
            emoji = signal_map.get(prediction["signal"], "?")
            print(
                f"  {emoji} [{i}/{len(company_codes)}] {code}: "
                f"P={prediction['probability']:.3f}  signal={prediction['signal']}",
                flush=True,
            )
            stats["success"] += 1

        except Exception as e:
            print(f"  ❌ [{i}/{len(company_codes)}] {code}: Prediction Error: {e}", flush=True)
            stats["failed"] += 1

    print(f"\n{'=' * 60}", flush=True)
    print(
        f"  📊 Prediction Done: {stats['success']} predicted | "
        f"{stats['no_model']} no model | "
        f"{stats['failed']} failed | {stats['skipped']} skipped",
        flush=True,
    )
    print("=" * 60, flush=True)
    return stats


def execute_trades():
    """Step 3: Execute trades based on today's NN predictions.

    Sells executed first, then buys (strongest signal first).
    Uses a dedicated 'nn_trader' wallet.
    """
    print("\n" + "=" * 60, flush=True)
    print("  STEP 3 — Execute Trades", flush=True)
    print("=" * 60, flush=True)

    from src.stockbroker.stockbroker import Wallet, stockbroker

    predictions = get_today_predictions()
    if not predictions:
        print("  ⚠️  No predictions for today", flush=True)
        return

    wallet = Wallet("nn_trader")

    sells = [p for p in predictions if p["signal"] < 0]
    buys = [p for p in predictions if p["signal"] > 0]
    holds = [p for p in predictions if p["signal"] == 0]

    print(f"  📈 Buy: {len(buys)} | 📉 Sell: {len(sells)} | ⏸️  Hold: {len(holds)}", flush=True)

    # Sells first (worst first)
    for p in sorted(sells, key=lambda x: x["signal"]):
        stockbroker(wallet, [p["sirket_code"]], p["signal"])

    # Then buys (strongest first)
    for p in sorted(buys, key=lambda x: x["signal"], reverse=True):
        stockbroker(wallet, [p["sirket_code"]], p["signal"])

    wallet.show()


# ═══════════════════════════════════════════════════════════════════════════════
# STANDALONE ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60, flush=True)
    print("  BIST100 NN Prediction Pipeline — Standalone Run", flush=True)
    print("=" * 60, flush=True)

    fetch_macro_data()
    # Explicitly call train_all_models so standalone continues to work exactly like before.
    train_all_models(force_retrain=False)
    run_predictions()
    execute_trades()
