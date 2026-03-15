# BIST Airflow Trading Pipeline

An experimental, fully automated daily trading pipeline for BIST stocks.  
It scrapes financial news, matches them to companies, runs Turkish sentiment analysis, and executes buy/sell decisions — all orchestrated by Apache Airflow.

---

## Pipelines & DAGs

**Scraping Pipeline:**
Fetches the latest financial news from TradingView, matches each article to BIST companies, analyzes sentiment, and saves the results to the database. Runs every 30 minutes to keep news and sentiment data up to date.

**Training Pipeline:**
Every Friday, collects macroeconomic data and retrains neural network models for all BIST companies using the latest technical, macro, and sentiment features. Ensures models are always up to date for accurate predictions.

**Trading Pipeline:**
Every weekday after market close, loads the latest news and macro data, generates daily predictions for all BIST companies using the trained models, and automatically executes buy/sell/hold decisions via the virtual wallet. This pipeline keeps the trading logic fully automated and up to date with the latest data.

---

## Quickstart

### Local Run
```bash
pip install -r requirements.txt
python local_run.py
```

### Docker / Airflow
```bash
# 1. Build the image
docker compose build --no-cache

# 2. Initialize the database (run once)
docker compose up airflow-init

# 3. Start services
docker compose up airflow-webserver airflow-scheduler airflow-worker
```

Open [http://localhost:8080](http://localhost:8080)  
Username: `airflow` | Password: `airflow`

Enable the `bist_v1` DAG and trigger it manually or let it run on its daily schedule.

---

## Components

### Scraper
Fetches news from the TradingView headlines API (`news-headlines.tradingview.com/v2/headlines`).

| Version | Method |
|---|---|
| v1 | Selenium |
| v2 | requests + BeautifulSoup |
| **v3** | **TradingView JSON API (current)** |

### Matcher
Matches article content to BIST company tickers.
- Company list sourced from [KAP](https://www.kap.org.tr/tr/bist-sirketler)
- Exact ticker regex match + Jaro-Winkler based fuzzy name matching

### Sentiment Analyzer
Fine-tuned Turkish BERT model for financial sentiment classification.

| Label | Meaning |
|---|---|
| -2 | Strong Sell |
| -1 | Sell |
|  0 | Hold |
| +1 | Buy |
| +2 | Strong Buy |

- Base dataset: [`mltrev23/financial-sentiment-analysis`](https://huggingface.co/datasets/mltrev23/financial-sentiment-analysis)
- Translated to Turkish using [`facebook/nllb-200-distilled-600M`](https://huggingface.co/facebook/nllb-200-distilled-600M)
- Fine-tuned on [`dbmdz/bert-base-turkish-cased`](https://huggingface.co/dbmdz/bert-base-turkish-cased)
- Published model: [aliemre2023/berturk-financial-sentiment-analysis](https://huggingface.co/aliemre2023/berturk-financial-sentiment-analysis)

> Model is automatically downloaded from HuggingFace on first run if not present locally.

### Stockbroker
Virtual wallet with JSON persistence (`src/workspace/wallet.json`).

- `buy_v1` — unit-based fallback (no price cap)
- `buy_v2` — `TRADE_PRICE = 10,000 TL` budget cap + unit fallback (default)
- Sentiment `2` → aggressive buy (sells other positions if needed)
- Sentiment `1` → normal buy
- Sentiment `0` → hold
- Sentiment `-1` / `-2` → sell all


### Estimater (NN-Based Stock Prediction)
`src/estimater/estimater1.py` — Neural network-based direction prediction for BIST stocks using technical, macroeconomic, and news sentiment data.

**Pipeline:**
1. **Macro Data Collection:**
      - Automatically fetches and caches TCMB/EVDS FX rates and FRED US macroeconomic data.
2. **Feature Engineering:**
      - Combines technical indicators (60+ features), macroeconomic signals, and news sentiment scores.
3. **Model Training:**
      - Trains a separate residual block neural network for each company and saves models to disk.
4. **Prediction:**
      - Uses trained models to predict next-day direction; results are saved to the SQLite database.
5. **Trade:**
      - Executes buy/sell operations automatically via the virtual wallet based on predictions.
---

## Project Structure

```
dags/               # Airflow DAG definition
src/
  scraper/          # News scraper (v1, v2, v3)
  matcher/          # Company matcher + BIST data
  sentimenter/      # Sentiment analyzer + fine-tune notebook
  estimater/        # Next day value estimater via Resudial NN
  stockbroker/      # Wallet + trade logic
  models/           # Local model cache (auto-downloaded if missing)
  workspace/        # wallet.json
```

---

## Requirements

- Python 3.10
- Docker Desktop (for Airflow)

---

Feel free to contribute!
