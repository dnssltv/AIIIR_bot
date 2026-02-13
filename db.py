import sqlite3
import time
from contextlib import contextmanager

DB_NAME = "air.db"


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_NAME)
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        c = conn.cursor()

        # Пользователи
        c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            badge TEXT NOT NULL
        )
        """)

        # Активные AIR-сессии
        c.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            user_id INTEGER PRIMARY KEY,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            created_at REAL NOT NULL,
            expires_at REAL NOT NULL
        )
        """)

        # Кулдауны
        c.execute("""
        CREATE TABLE IF NOT EXISTS cooldowns (
            user_id INTEGER PRIMARY KEY,
            last_used_at REAL NOT NULL
        )
        """)

        conn.commit()


def cleanup_sessions():
    now = time.time()
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM sessions WHERE expires_at < ?", (now,))
        conn.commit()
