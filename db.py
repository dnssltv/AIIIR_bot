import os
import json
import asyncpg
from datetime import datetime, timezone
from typing import Optional, Any, Dict, List, Tuple


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        if self.pool:
            return
        self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=5)
        await self.migrate()

    async def close(self):
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def migrate(self):
        assert self.pool
        async with self.pool.acquire() as con:
            # Users table
            await con.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                name TEXT,
                marker TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                premium_until TIMESTAMPTZ,
                ref_code TEXT,
                referred_by BIGINT
            );
            """)

            # Sessions (last known location + activity window)
            await con.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                user_id BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
                lat DOUBLE PRECISION NOT NULL,
                lon DOUBLE PRECISION NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                active_until TIMESTAMPTZ
            );
            """)

            # Analytics events
            await con.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT,
                event_type TEXT NOT NULL,
                meta JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)

            # Helpful indexes
            await con.execute("CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);")
            await con.execute("CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);")

    # -------- users --------
    async def upsert_user(self, user_id: int):
        assert self.pool
        async with self.pool.acquire() as con:
            await con.execute("""
            INSERT INTO users(user_id)
            VALUES($1)
            ON CONFLICT (user_id) DO NOTHING;
            """, user_id)

    async def set_profile(self, user_id: int, name: str, marker: str):
        assert self.pool
        async with self.pool.acquire() as con:
            await con.execute("""
            INSERT INTO users(user_id, name, marker)
            VALUES($1, $2, $3)
            ON CONFLICT (user_id)
            DO UPDATE SET name=EXCLUDED.name, marker=EXCLUDED.marker;
            """, user_id, name, marker)

    async def set_marker(self, user_id: int, marker: str):
        assert self.pool
        async with self.pool.acquire() as con:
            await con.execute("""
            UPDATE users SET marker=$2
            WHERE user_id=$1;
            """, user_id, marker)

    async def get_user(self, user_id: int) -> Optional[dict]:
        assert self.pool
        async with self.pool.acquire() as con:
            row = await con.fetchrow("SELECT * FROM users WHERE user_id=$1;", user_id)
            return dict(row) if row else None

    async def count_users(self) -> int:
        assert self.pool
        async with self.pool.acquire() as con:
            return await con.fetchval("SELECT COUNT(*) FROM users;")

    # -------- sessions --------
    async def upsert_session(self, user_id: int, lat: float, lon: float, active_until: Optional[datetime]):
        assert self.pool
        async with self.pool.acquire() as con:
            await con.execute("""
            INSERT INTO sessions(user_id, lat, lon, updated_at, active_until)
            VALUES($1, $2, $3, NOW(), $4)
            ON CONFLICT (user_id)
            DO UPDATE SET lat=EXCLUDED.lat, lon=EXCLUDED.lon, updated_at=NOW(), active_until=EXCLUDED.active_until;
            """, user_id, lat, lon, active_until)

    async def set_session_inactive(self, user_id: int):
        assert self.pool
        async with self.pool.acquire() as con:
            await con.execute("DELETE FROM sessions WHERE user_id=$1;", user_id)

    async def get_active_sessions(self, now: datetime) -> List[dict]:
        assert self.pool
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
            SELECT s.user_id, s.lat, s.lon, s.updated_at, s.active_until,
                   u.name, u.marker, u.premium_until
            FROM sessions s
            JOIN users u ON u.user_id = s.user_id
            WHERE s.active_until IS NULL OR s.active_until > $1;
            """, now)
            return [dict(r) for r in rows]

    # -------- analytics --------
    async def log_event(self, user_id: Optional[int], event_type: str, meta: Optional[Dict[str, Any]] = None):
        assert self.pool
        async with self.pool.acquire() as con:
            await con.execute("""
            INSERT INTO events(user_id, event_type, meta)
            VALUES($1, $2, $3);
            """, user_id, event_type, json.dumps(meta or {}))

    async def export_events_csv(self, limit: int = 5000) -> str:
        """
        Returns CSV text for quick export.
        """
        assert self.pool
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
            SELECT id, user_id, event_type, meta::text as meta, created_at
            FROM events
            ORDER BY created_at DESC
            LIMIT $1;
            """, limit)

        # simple csv (no external deps)
        def esc(v: Any) -> str:
            s = "" if v is None else str(v)
            s = s.replace('"', '""')
            return f'"{s}"'

        lines = ["id,user_id,event_type,meta,created_at"]
        for r in rows:
            lines.append(",".join([
                esc(r["id"]),
                esc(r["user_id"]),
                esc(r["event_type"]),
                esc(r["meta"]),
                esc(r["created_at"]),
            ]))
        return "\n".join(lines)

    async def export_users_csv(self, limit: int = 20000) -> str:
        assert self.pool
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
            SELECT user_id, name, marker, created_at, premium_until, ref_code, referred_by
            FROM users
            ORDER BY created_at DESC
            LIMIT $1;
            """, limit)

        def esc(v: Any) -> str:
            s = "" if v is None else str(v)
            s = s.replace('"', '""')
            return f'"{s}"'

        lines = ["user_id,name,marker,created_at,premium_until,ref_code,referred_by"]
        for r in rows:
            lines.append(",".join([
                esc(r["user_id"]),
                esc(r["name"]),
                esc(r["marker"]),
                esc(r["created_at"]),
                esc(r["premium_until"]),
                esc(r["ref_code"]),
                esc(r["referred_by"]),
            ]))
        return "\n".join(lines)
