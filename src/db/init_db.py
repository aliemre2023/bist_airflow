"""
DB initialization + temel CRUD yardımcıları
Kullanım:
    python -m src.db.init_db          # tabloları oluştur
"""
from src.db.db import get_connection
from src.db.models import ALL_TABLES


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------

def init_db():
    """Tabloları oluşturur (yoksa). Varsa dokunmaz."""
    conn = get_connection()
    with conn:
        for ddl in ALL_TABLES:
            conn.execute(ddl)
    conn.close()
    print(f"  ✅ DB initialized")


# ---------------------------------------------------------------------------
# sirket
# ---------------------------------------------------------------------------

def upsert_sirket(sirket_code: str, sirket_name: str) -> int:
    """Şirketi ekler ya da varsa günceller. sirket_id döndürür."""
    conn = get_connection()
    with conn:
        conn.execute(
            "INSERT INTO sirket (sirket_code, sirket_name) VALUES (?, ?)"
            " ON CONFLICT(sirket_code) DO UPDATE SET sirket_name=excluded.sirket_name",
            (sirket_code, sirket_name)
        )
        row = conn.execute(
            "SELECT sirket_id FROM sirket WHERE sirket_code = ?", (sirket_code,)
        ).fetchone()
    conn.close()
    return row["sirket_id"]


def get_sirket_id(sirket_code: str) -> int | None:
    conn = get_connection()
    row = conn.execute(
        "SELECT sirket_id FROM sirket WHERE sirket_code = ?", (sirket_code,)
    ).fetchone()
    conn.close()
    return row["sirket_id"] if row else None


# ---------------------------------------------------------------------------
# news
# ---------------------------------------------------------------------------

def insert_news(
    news_url: str,
    news_date: str,
    news_provider: str,
    news_content: str,
    news_type: int,
    news_ratio: float,
    news_website: str = "tradingview",
) -> int | None:
    """
    Haberi ekler. Aynı URL daha önce eklendiyse None döner (duplicate).
    Başarılı olursa news_id döner.
    """
    conn = get_connection()
    try:
        with conn:
            cur = conn.execute(
                """INSERT INTO news
                   (news_url, news_date, news_website, news_provider, news_content, news_type, news_ratio)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (news_url, news_date, news_website, news_provider, news_content, news_type, news_ratio)
            )
            return cur.lastrowid
    except Exception:
        return None  # UNIQUE constraint → duplicate
    finally:
        conn.close()


def link_news_sirket(news_id: int, sirket_id: int):
    """news_sirket junction tablosuna kayıt ekler."""
    conn = get_connection()
    with conn:
        conn.execute(
            "INSERT OR IGNORE INTO news_sirket (news_id, sirket_id) VALUES (?, ?)",
            (news_id, sirket_id)
        )
    conn.close()


# ---------------------------------------------------------------------------
# prediction
# ---------------------------------------------------------------------------

def upsert_prediction(
    sirket_code: str,
    prediction_date: str,
    signal: int,
    probability: float,
    model_version: str = "nn_v1",
    model_auc: float | None = None,
) -> int | None:
    """Insert or update a prediction. Returns prediction_id."""
    sirket_id = get_sirket_id(sirket_code)
    if sirket_id is None:
        return None
    conn = get_connection()
    try:
        with conn:
            cur = conn.execute(
                """INSERT INTO prediction
                   (sirket_id, prediction_date, signal, probability, model_version, model_auc)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(sirket_id, prediction_date, model_version)
                   DO UPDATE SET signal=excluded.signal, probability=excluded.probability,
                                 model_auc=excluded.model_auc, created_at=datetime('now')""",
                (sirket_id, prediction_date, signal, probability, model_version, model_auc),
            )
            return cur.lastrowid
    finally:
        conn.close()


def get_predictions_by_date(prediction_date: str, model_version: str = "nn_v1") -> list[dict]:
    """Get all predictions for a specific date."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT s.sirket_code, p.signal, p.probability, p.model_auc
           FROM prediction p
           JOIN sirket s ON s.sirket_id = p.sirket_id
           WHERE p.prediction_date = ? AND p.model_version = ?
           ORDER BY p.probability DESC""",
        (prediction_date, model_version),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    init_db()
