import asyncio
import os
import math
import csv
import io
from datetime import datetime, timedelta, timezone

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)
from aiogram.enums import ParseMode
from aiogram.types.input_file import BufferedInputFile


# =========================
# ENV
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is empty")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is empty (Railway PostgreSQL)")

ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}

RADIUS_METERS = int(os.getenv("RADIUS_METERS", "300"))
NOTIFY_COOLDOWN_SEC = int(os.getenv("NOTIFY_COOLDOWN_SEC", "180"))  # анти-спам: 3 мин
MUTE_MINUTES = int(os.getenv("MUTE_MINUTES", "60"))  # на сколько выключать уведомления
# TTL сессии (чтобы старые точки не висели): 0 = не чистим автоматически (как ты просил для теста)
SESSION_TTL_MINUTES = int(os.getenv("SESSION_TTL_MINUTES", "0"))


# =========================
# BOT
# =========================
bot = Bot(BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()


# =========================
# HELPERS
# =========================
def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def haversine_m(lat1, lon1, lat2, lon2) -> float:
    """Distance in meters between 2 lat/lon points."""
    R = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# =========================
# DATABASE
# =========================
class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: asyncpg.Pool | None = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(self.dsn)

        async with self.pool.acquire() as con:
            # users
            await con.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    name TEXT,
                    marker TEXT,
                    muted_until TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            # active sessions (location)
            await con.execute("""
                CREATE TABLE IF NOT EXISTS air_sessions (
                    user_id BIGINT PRIMARY KEY,
                    lat DOUBLE PRECISION NOT NULL,
                    lon DOUBLE PRECISION NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)

            # events for analytics
            await con.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    event TEXT NOT NULL,
                    meta JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            # anti-spam notifications log
            await con.execute("""
                CREATE TABLE IF NOT EXISTS notification_log (
                    to_user_id BIGINT NOT NULL,
                    about_user_id BIGINT NOT NULL,
                    last_sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (to_user_id, about_user_id)
                );
            """)

    async def log_event(self, user_id: int, event: str, meta: dict | None = None):
        async with self.pool.acquire() as con:
            await con.execute(
                "INSERT INTO events (user_id, event, meta) VALUES ($1, $2, $3::jsonb)",
                user_id, event, (meta or {})
            )

    async def upsert_user(self, user_id: int, name: str | None = None, marker: str | None = None):
        async with self.pool.acquire() as con:
            await con.execute("""
                INSERT INTO users (user_id, name, marker)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id)
                DO UPDATE SET
                    name = COALESCE($2, users.name),
                    marker = COALESCE($3, users.marker)
            """, user_id, name, marker)

    async def set_mute(self, user_id: int, until_dt: datetime):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE users SET muted_until=$2 WHERE user_id=$1",
                user_id, until_dt
            )

    async def get_user(self, user_id: int):
        async with self.pool.acquire() as con:
            return await con.fetchrow("SELECT * FROM users WHERE user_id=$1", user_id)

    async def upsert_session(self, user_id: int, lat: float, lon: float):
        async with self.pool.acquire() as con:
            await con.execute("""
                INSERT INTO air_sessions (user_id, lat, lon, is_active, updated_at)
                VALUES ($1, $2, $3, TRUE, NOW())
                ON CONFLICT (user_id)
                DO UPDATE SET
                    lat=EXCLUDED.lat,
                    lon=EXCLUDED.lon,
                    is_active=TRUE,
                    updated_at=NOW()
            """, user_id, lat, lon)

    async def deactivate_session(self, user_id: int):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE air_sessions SET is_active=FALSE, updated_at=NOW() WHERE user_id=$1",
                user_id
            )

    async def cleanup_sessions(self):
        if SESSION_TTL_MINUTES <= 0:
            return 0
        async with self.pool.acquire() as con:
            res = await con.execute("""
                UPDATE air_sessions
                SET is_active=FALSE
                WHERE is_active=TRUE
                AND updated_at < (NOW() - ($1::int * INTERVAL '1 minute'))
            """, SESSION_TTL_MINUTES)
            # res like "UPDATE 12"
            return int(res.split()[-1])

    async def get_nearby_sessions(self, lat: float, lon: float, radius_m: int, exclude_user_id: int):
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                SELECT s.user_id, s.lat, s.lon, s.updated_at, u.name, u.marker, u.muted_until
                FROM air_sessions s
                JOIN users u ON u.user_id = s.user_id
                WHERE s.is_active=TRUE
                  AND s.user_id <> $1
            """, exclude_user_id)

        # distance filter in python (простое и надежное MVP)
        out = []
        for r in rows:
            d = haversine_m(lat, lon, r["lat"], r["lon"])
            if d <= radius_m:
                out.append((d, r))
        out.sort(key=lambda x: x[0])
        return out

    async def can_notify(self, to_user_id: int, about_user_id: int) -> bool:
        async with self.pool.acquire() as con:
            row = await con.fetchrow("""
                SELECT last_sent_at
                FROM notification_log
                WHERE to_user_id=$1 AND about_user_id=$2
            """, to_user_id, about_user_id)

            now = utcnow()
            if not row:
                await con.execute("""
                    INSERT INTO notification_log (to_user_id, about_user_id, last_sent_at)
                    VALUES ($1, $2, NOW())
                """, to_user_id, about_user_id)
                return True

            last = row["last_sent_at"]
            if (now - last).total_seconds() >= NOTIFY_COOLDOWN_SEC:
                await con.execute("""
                    UPDATE notification_log SET last_sent_at=NOW()
                    WHERE to_user_id=$1 AND about_user_id=$2
                """, to_user_id, about_user_id)
                return True

            return False

    async def stats(self):
        async with self.pool.acquire() as con:
            users = await con.fetchval("SELECT COUNT(*) FROM users")
            active = await con.fetchval("SELECT COUNT(*) FROM air_sessions WHERE is_active=TRUE")
            events = await con.fetchval("SELECT COUNT(*) FROM events")
        return users, active, events

    async def export_events_csv(self, days: int = 7) -> bytes:
        """CSV: day, event, count"""
        async with self.pool.acquire() as con:
            rows = await con.fetch("""
                SELECT
                    date_trunc('day', created_at) AS day,
                    event,
                    COUNT(*) AS cnt
                FROM events
                WHERE created_at >= (NOW() - ($1::int * INTERVAL '1 day'))
                GROUP BY 1, 2
                ORDER BY 1 DESC, 2 ASC
            """, days)

        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["day_utc", "event", "count"])
        for r in rows:
            w.writerow([r["day"].isoformat(), r["event"], r["cnt"]])
        return buf.getvalue().encode("utf-8")


db = Database(DATABASE_URL)


# =========================
# KEYBOARDS
# =========================
def main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📍 Выйти на AIR", request_location=True)],
            [KeyboardButton(text="👀 Кто рядом")],
            [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="✏️ Сменить признак")],
            [KeyboardButton(text="🛑 Уйти с AIR")]
        ],
        resize_keyboard=True
    )


def inline_nearby_kb(about_user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📍 Показать точку", callback_data=f"show:{about_user_id}"),
            InlineKeyboardButton(text="🔕 Не уведомлять 1ч", callback_data="mute:60"),
        ]
    ])


# =========================
# STATES (простые наборы)
# =========================
WAITING_NAME = set()
WAITING_MARKER = set()


# =========================
# HANDLERS
# =========================
@dp.message(CommandStart())
async def cmd_start(message: Message):
    await db.upsert_user(message.from_user.id)
    WAITING_NAME.add(message.from_user.id)
    await db.log_event(message.from_user.id, "start")

    await message.answer(
        "AIR — короткие паузы рядом.\n\n"
        "Сначала настроим профиль.\n"
        "Как тебя назвать? (2–20 символов)"
    )


@dp.message(lambda m: m.from_user.id in WAITING_NAME)
async def set_name(message: Message):
    name = (message.text or "").strip()
    if not (2 <= len(name) <= 20):
        await message.answer("Имя должно быть от 2 до 20 символов.")
        return

    WAITING_NAME.remove(message.from_user.id)
    WAITING_MARKER.add(message.from_user.id)

    await db.upsert_user(message.from_user.id, name=name)
    await db.log_event(message.from_user.id, "set_name")

    await message.answer(
        "Отлично 👍\n\n"
        "Теперь напиши <b>любой опознавательный признак</b>, чтобы тебя было проще узнать.\n\n"
        "Примеры:\n"
        "• в чёрной куртке\n"
        "• с ноутбуком\n"
        "• возле окна"
    )


@dp.message(lambda m: m.from_user.id in WAITING_MARKER)
async def set_marker(message: Message):
    marker = (message.text or "").strip()
    if len(marker) < 2:
        await message.answer("Слишком коротко, попробуй ещё раз.")
        return

    WAITING_MARKER.remove(message.from_user.id)

    await db.upsert_user(message.from_user.id, marker=marker)
    await db.log_event(message.from_user.id, "set_marker")

    user = await db.get_user(message.from_user.id)
    await message.answer(
        f"✅ Готово!\n\n"
        f"<b>Твой профиль:</b>\n"
        f"Имя: {user['name']}\n"
        f"Признак: {user['marker']}\n\n"
        f"Теперь жми «📍 Выйти на AIR» и отправляй геолокацию.",
        reply_markup=main_kb()
    )


@dp.message(F.text == "👤 Профиль")
async def profile(message: Message):
    user = await db.get_user(message.from_user.id)
    if not user:
        return
    await db.log_event(message.from_user.id, "open_profile")

    await message.answer(
        f"<b>Твой профиль:</b>\n\n"
        f"Имя: {user['name'] or '—'}\n"
        f"Признак: {user['marker'] or '—'}",
        reply_markup=main_kb()
    )


@dp.message(F.text == "✏️ Сменить признак")
async def change_marker(message: Message):
    WAITING_MARKER.add(message.from_user.id)
    await db.log_event(message.from_user.id, "change_marker")
    await message.answer("Напиши новый опознавательный признак 👇")


@dp.message(F.text == "🛑 Уйти с AIR")
async def leave_air(message: Message):
    await db.deactivate_session(message.from_user.id)
    await db.log_event(message.from_user.id, "leave_air")
    await message.answer("Ок, ты больше не на AIR ✅", reply_markup=main_kb())


@dp.message(F.text == "👀 Кто рядом")
async def who_near(message: Message):
    await db.cleanup_sessions()
    await db.log_event(message.from_user.id, "who_near")

    # если нет сохраненной позиции — попросим гео
    # (MVP: считаем только от последней отправленной геолокации)
    # поэтому говорим: отправь гео через кнопку
    await message.answer(
        "Чтобы показать людей рядом — нажми «📍 Выйти на AIR» и отправь геолокацию.\n"
        "После этого я покажу список рядом."
    )


@dp.message(F.location)
async def location_received(message: Message):
    await db.cleanup_sessions()

    user_id = message.from_user.id
    lat = message.location.latitude
    lon = message.location.longitude

    await db.upsert_user(user_id)  # гарантируем наличие
    await db.upsert_session(user_id, lat, lon)
    await db.log_event(user_id, "send_location", {"lat": lat, "lon": lon})

    # найдём людей рядом
    nearby = await db.get_nearby_sessions(lat, lon, RADIUS_METERS, user_id)

    # 1) Ответ пользователю — список
    if not nearby:
        await message.answer(
            f"📡 Ты на AIR.\n\n"
            f"Пока рядом никого нет в радиусе {RADIUS_METERS} м.",
            reply_markup=main_kb()
        )
    else:
        lines = [f"📡 Ты на AIR. Рядом <b>{len(nearby)}</b> чел. (≤ {RADIUS_METERS} м):\n"]
        for i, (dist, r) in enumerate(nearby[:10], start=1):
            name = r["name"] or "Без имени"
            marker = r["marker"] or "без признака"
            lines.append(f"{i}) <b>{name}</b> — {marker} (~{int(dist)} м)")
        lines.append("\nХочешь точку конкретного человека — нажми «📍 Показать точку» в уведомлении или попроси ещё раз гео.")
        await message.answer("\n".join(lines), reply_markup=main_kb())

    # 2) Уведомим соседей о новом человеке (анти-спам + mute)
    me = await db.get_user(user_id)
    my_name = me["name"] or "Кто-то"
    my_marker = me["marker"] or "без признака"

    for dist, r in nearby:
        to_id = int(r["user_id"])

        # если у получателя mute активен — пропускаем
        muted_until = r["muted_until"]
        if muted_until and muted_until > utcnow():
            continue

        # кулдаун уведомлений
        if not await db.can_notify(to_id, user_id):
            continue

        try:
            await bot.send_message(
                to_id,
                f"👀 Рядом вышел(а) <b>{my_name}</b> — {my_marker}\n"
                f"Расстояние ~{int(dist)} м",
                reply_markup=inline_nearby_kb(user_id)
            )
            await db.log_event(user_id, "notify_sent", {"to": to_id, "dist_m": int(dist)})
        except Exception:
            # если человек запретил боту писать — просто молчим
            pass


# =========================
# CALLBACKS
# =========================
@dp.callback_query(F.data.startswith("show:"))
async def cb_show_point(call: CallbackQuery):
    await call.answer()
    about_id = int(call.data.split(":")[1])

    # достанем координаты about_id
    async with db.pool.acquire() as con:
        row = await con.fetchrow("""
            SELECT s.lat, s.lon, u.name, u.marker
            FROM air_sessions s
            JOIN users u ON u.user_id = s.user_id
            WHERE s.user_id=$1 AND s.is_active=TRUE
        """, about_id)

    if not row:
        await call.message.answer("Этот человек уже не на AIR или точка устарела.")
        return

    name = row["name"] or "Пользователь"
    marker = row["marker"] or "без признака"

    await db.log_event(call.from_user.id, "show_point", {"about": about_id})
    await call.message.answer(f"📍 Точка: <b>{name}</b> — {marker}")
    await call.message.answer_location(latitude=row["lat"], longitude=row["lon"])


@dp.callback_query(F.data.startswith("mute:"))
async def cb_mute(call: CallbackQuery):
    await call.answer("Ок, отключил уведомления на 1 час ✅", show_alert=False)
    until_dt = utcnow() + timedelta(minutes=MUTE_MINUTES)
    await db.set_mute(call.from_user.id, until_dt)
    await db.log_event(call.from_user.id, "mute", {"minutes": MUTE_MINUTES})


# =========================
# ADMIN
# =========================
@dp.message(F.text == "/stats")
async def admin_stats(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    users, active, events = await db.stats()
    await message.answer(
        f"<b>Статистика:</b>\n\n"
        f"👤 Пользователей: {users}\n"
        f"📡 Активных на AIR: {active}\n"
        f"📊 Событий: {events}\n\n"
        f"RADIUS={RADIUS_METERS}m, cooldown={NOTIFY_COOLDOWN_SEC}s, TTL={SESSION_TTL_MINUTES}min"
    )


@dp.message(F.text.startswith("/export"))
async def admin_export(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    parts = (message.text or "").split()
    days = 7
    if len(parts) > 1 and parts[1].isdigit():
        days = int(parts[1])

    data = await db.export_events_csv(days=days)
    file = BufferedInputFile(data, filename=f"air_events_{days}d.csv")
    await message.answer_document(file, caption=f"CSV событий за {days} дней (UTC)")


@dp.message(F.text == "/cleanup")
async def admin_cleanup(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    n = await db.cleanup_sessions()
    await message.answer(f"✅ Cleanup: деактивировано {n} старых сессий.")


# =========================
# MAIN
# =========================
async def main():
    await db.connect()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
