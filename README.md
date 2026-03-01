# BIST100 Airflow Trading Bot

An experimental, fully automated daily trading pipeline for BIST100 stocks.  
It scrapes financial news, matches them to companies, runs Turkish sentiment analysis, and executes buy/sell decisions — all orchestrated by Apache Airflow.

---

## How It Works

```
TradingView News API
        ↓
   News Scraper (v3)
        ↓
  Company Matcher  ←── BIST100 company list (KAP)
        ↓
Sentiment Analyzer  ←── Fine-tuned BERTurk model
        ↓
  Stockbroker / Wallet
        ↓
  buy / sell / hold
```

Every day at midnight, the Airflow DAG:
1. Fetches latest Turkish financial news from TradingView
2. Matches each article to BIST100 tickers using exact + Jaro-Winkler fuzzy matching
3. Scores sentiment per article (`-2` strong sell → `+2` strong buy)
4. Executes trades via a virtual wallet (persisted in `wallet.json`)

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
Matches article content to BIST100 company tickers.
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

---

## Project Structure

```
dags/               # Airflow DAG definition
src/
  scraper/          # News scraper (v1, v2, v3)
  matcher/          # Company matcher + BIST100 data
  sentimenter/      # Sentiment analyzer + fine-tune notebook
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