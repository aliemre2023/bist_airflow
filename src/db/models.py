CREATE_SIRKET = """
CREATE TABLE IF NOT EXISTS sirket (
    sirket_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    sirket_code TEXT    NOT NULL UNIQUE,
    sirket_name TEXT    NOT NULL
);
"""

CREATE_NEWS = """
CREATE TABLE IF NOT EXISTS news (
    news_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    news_url      TEXT    NOT NULL UNIQUE,
    news_date     TEXT    NOT NULL,
    news_website  TEXT    NOT NULL DEFAULT 'tradingview',
    news_provider TEXT,
    news_content  TEXT,
    news_type     INTEGER,          -- 2, 1, 0, -1, -2
    news_ratio    REAL,             -- ham skor (örn: 0.13)
    created_at    TEXT    NOT NULL DEFAULT (datetime('now'))
);
"""

CREATE_NEWS_SIRKET = """
CREATE TABLE IF NOT EXISTS news_sirket (
    news_id    INTEGER NOT NULL REFERENCES news(news_id)    ON DELETE CASCADE,
    sirket_id  INTEGER NOT NULL REFERENCES sirket(sirket_id) ON DELETE CASCADE,
    PRIMARY KEY (news_id, sirket_id)
);
"""

CREATE_PREDICTION = """
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
);
"""

ALL_TABLES = [CREATE_SIRKET, CREATE_NEWS, CREATE_NEWS_SIRKET, CREATE_PREDICTION]
