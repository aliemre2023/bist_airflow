import os
import sqlite3

DB_DIR = os.path.join(
    os.environ.get(
        'AIRFLOW_HOME',
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    ),
    'src', 'db'
)
DB_PATH = os.path.join(DB_DIR, 'bist.db')


def get_connection() -> sqlite3.Connection:
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn
