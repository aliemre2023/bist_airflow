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
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    init_db()
