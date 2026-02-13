# bot.py
# AIR — короткие паузы рядом.
#
# Требования (requirements.txt примерно):
# aiogram==3.4.1
# asyncpg==0.29.0
#
# Railway env vars:
# BOT_TOKEN=...
# DATABASE_URL=postgresql://...
# (опционально)
# RADIUS_METERS=300
# ACTIVE_TTL_MINUTES=1440        # на тестах можно поставить большое число, либо 0 = без TTL
# NOTIFY_COOLDOWN_SECONDS=300    # анти-спам уведомлений
# ADMIN_IDS=123,456              # если захочешь админ-команды позже

import asyncio
import json
import os
import re
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any, Tuple

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder


# ------------------------- CONFIG -------------------------

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

RADIUS_METERS = int(os.getenv("RADIUS_METERS", "300"))
# 0 = без TTL (активность бессрочная, пока не нажмёт /stop)
ACTIVE_TTL_MINUTES = int(os.getenv("ACTIVE_TTL_MINUTES", "0"))
NOTIFY_COOLDOWN_SECONDS = int(os.getenv("NOTIFY_COOLDOWN_SECONDS", "300"))

ADMIN_IDS = set()
_admin_raw = os.getenv("ADMIN_IDS", "").strip()
if _admin_raw:
    for x in _admin_raw.split(","):
        x = x.strip()
        if x.isdigit():
            ADMIN_IDS.add(int(x))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is empty")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is empty (Railway Postgres)")

UTC = timezone.utc


# ------------------------- DB -------------------------

class DB:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(self.dsn, min_size=1, max_size=10)
        await self._ensure_schema()

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def _ensure_schema(self):
        # ВАЖНО: meta = JSONB, чтобы можно было класть dict.
        # Если таблицы пустые/новые — будет ок. Если старые — проще удалить старую events и users/locations,
        # либо привести типы руками.
        ddl = """
        CREATE TABLE IF NOT EXISTS users (
            user_id     BIGINT PRIMARY KEY,
            name        TEXT,
            marker      TEXT,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS locations (
            user_id     BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
            lat         DOUBLE PRECISION NOT NULL,
            lon         DOUBLE PRECISION NOT NULL,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            is_active   BOOLEAN NOT NULL DEFAULT TRUE
        );

        CREATE TABLE IF NOT EXISTS events (
            id          BIGSERIAL PRIMARY KEY,
            user_id     BIGINT,
            event_type  TEXT NOT NULL,
            meta        JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS proximity_notifications (
            from_user   BIGINT NOT NULL,
            to_user     BIGINT NOT NULL,
            last_sent   TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (from_user, to_user)
        );
        """
        async with self.pool.acquire() as con:
            await con.execute(ddl)

    async def upsert_user(self, user_id: int, name: Optional[str] = None, marker: Optional[str] = None):
        async with self.pool.acquire() as con:
            # Обновляем только то, что не None
            row = await con.fetchrow("SELECT user_id, name, marker FROM users WHERE user_id=$1", user_id)
            if row is None:
                await con.execute(
                    "INSERT INTO users(user_id, name, marker) VALUES ($1,$2,$3)",
                    user_id, name, marker
                )
            else:
                new_name = name if name is not None else row["name"]
                new_marker = marker if marker is not None else row["marker"]
                await con.execute(
                    "UPDATE users SET name=$2, marker=$3, updated_at=NOW() WHERE user_id=$1",
                    user_id, new_name, new_marker
                )

    async def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        async with self.pool.acquire() as con:
            row = await con.fetchrow("SELECT user_id, name, marker FROM users WHERE user_id=$1", user_id)
            return dict(row) if row else None

    async def set_location(self, user_id: int, lat: float, lon: float, is_active: bool = True):
        async with self.pool.acquire() as con:
            await con.execute(
                """
                INSERT INTO locations(user_id, lat, lon, updated_at, is_active)
                VALUES ($1,$2,$3,NOW(),$4)
                ON CONFLICT (user_id)
                DO UPDATE SET lat=EXCLUDED.lat, lon=EXCLUDED.lon, updated_at=NOW(), is_active=EXCLUDED.is_active
                """,
                user_id, lat, lon, is_active
            )

    async def set_active(self, user_id: int, is_active: bool):
        async with self.pool.acquire() as con:
            await con.execute(
                "UPDATE locations SET is_active=$2, updated_at=NOW() WHERE user_id=$1",
                user_id, is_active
            )

    async def get_location(self, user_id: int) -> Optional[Dict[str, Any]]:
        async with self.pool.acquire() as con:
            row = await con.fetchrow(
                "SELECT user_id, lat, lon, updated_at, is_active FROM locations WHERE user_id=$1",
                user_id
            )
            return dict(row) if row else None

    async def get_nearby(self, lat: float, lon: float, radius_m: int, exclude_user: int) -> List[Dict[str, Any]]:
        # Берём всех активных, фильтруем по TTL (если включен), потом по расстоянию.
        now = datetime.now(tz=UTC)
        ttl_cutoff = None
        if ACTIVE_TTL_MINUTES > 0:
            ttl_cutoff = now - timedelta(minutes=ACTIVE_TTL_MINUTES)

        query = """
        SELECT u.user_id, u.name, u.marker, l.lat, l.lon, l.updated_at, l.is_active
        FROM users u
        JOIN locations l ON l.user_id = u.user_id
        WHERE l.is_active = TRUE
          AND u.user_id <> $1
        """
        params = [exclude_user]
        if ttl_cutoff is not None:
            query += " AND l.updated_at >= $2"
            params.append(ttl_cutoff)

        async with self.pool.acquire() as con:
            rows = await con.fetch(query, *params)

        res = []
        for r in rows:
            dist = haversine_m(lat, lon, float(r["lat"]), float(r["lon"]))
            if dist <= radius_m:
                d = dict(r)
                d["distance_m"] = int(dist)
                res.append(d)

        res.sort(key=lambda x: x["distance_m"])
        return res

    async def log_event(self, user_id: int, event_type: str, meta: Optional[Dict[str, Any]] = None):
        meta_json = json.dumps(meta or {}, ensure_ascii=False)
        async with self.pool.acquire() as con:
            await con.execute(
                "INSERT INTO events (user_id, event_type, meta) VALUES ($1, $2, $3::jsonb)",
                user_id, event_type, meta_json
            )

    async def can_notify(self, from_user: int, to_user: int) -> bool:
        # анти-спам: не чаще, чем NOTIFY_COOLDOWN_SECONDS на пару (from->to)
        now = datetime.now(tz=UTC)
        async with self.pool.acquire() as con:
            row = await con.fetchrow(
                "SELECT last_sent FROM proximity_notifications WHERE from_user=$1 AND to_user=$2",
                from_user, to_user
            )
            if not row:
                await con.execute(
                    "INSERT INTO proximity_notifications(from_user,to_user,last_sent) VALUES ($1,$2,$3)",
                    from_user, to_user, now
                )
                return True

            last_sent: datetime = row["last_sent"]
            if (now - last_sent).total_seconds() >= NOTIFY_COOLDOWN_SECONDS:
                await con.execute(
                    "UPDATE proximity_notifications SET last_sent=$3 WHERE from_user=$1 AND to_user=$2",
                    from_user, to_user, now
                )
                return True

            return False


db = DB(DATABASE_URL)


# ------------------------- GEO -------------------------

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    # meters
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def maps_link(lat: float, lon: float) -> str:
    # Безопасно и просто
    return f"https://www.google.com/maps?q={lat},{lon}"

def clamp_text(s: str, max_len: int) -> str:
    s = (s or "").strip()
    return s[:max_len]

NAME_RE = re.compile(r"^[^\n\r\t]{2,20}$")
MARKER_RE = re.compile(r"^[^\n\r\t]{2,40}$")


# ------------------------- FSM -------------------------

class ProfileSetup(StatesGroup):
    waiting_name = State()
    waiting_marker = State()
    changing_marker = State()
    changing_name = State()


# ------------------------- UI (KEYBOARDS) -------------------------

def kb_main() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📍 Выйти на AIR", request_location=True)],
            [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="✏️ Сменить признак")],
            [KeyboardButton(text="🔄 Сменить имя"), KeyboardButton(text="⛔️ Выйти с AIR")],
        ],
        resize_keyboard=True
    )

def ikb_nearby_list(items: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for it in items[:10]:
        uid = it["user_id"]
        dist = it["distance_m"]
        name = it.get("name") or "Без имени"
        marker = it.get("marker") or "без признака"
        b.button(text=f"🗺️ {name} ({dist}м) — {marker}", callback_data=f"map:{uid}")
    b.adjust(1)
    return b.as_markup()

def ikb_profile_actions() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="✏️ Сменить признак", callback_data="chg_marker")
    b.button(text="🔄 Сменить имя", callback_data="chg_name")
    b.adjust(2)
    return b.as_markup()


# ------------------------- BOT -------------------------

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()


# ------------------------- HELPERS -------------------------

async def ensure_profile(user_id: int) -> Tuple[bool, Optional[Dict[str, Any]]]:
    u = await db.get_user(user_id)
    if not u:
        await db.upsert_user(user_id)
        return False, await db.get_user(user_id)

    if not u.get("name") or not u.get("marker"):
        return False, u
    return True, u

async def show_profile(message: Message, user_id: int):
    u = await db.get_user(user_id)
    if not u:
        await message.answer("Профиль не найден. Нажми /start")
        return
    name = u.get("name") or "—"
    marker = u.get("marker") or "—"
    await message.answer(
        f"👤 <b>Твой профиль</b>\n"
        f"Имя/ник: <b>{escape(name)}</b>\n"
        f"Признак: <b>{escape(marker)}</b>\n\n"
        f"Нажми «📍 Выйти на AIR» и отправь геолокацию.",
        reply_markup=ikb_profile_actions()
    )

def escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
    )


# ------------------------- HANDLERS -------------------------

@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await db.upsert_user(message.from_user.id)
    await db.log_event(message.from_user.id, "start")

    ok, u = await ensure_profile(message.from_user.id)
    if ok:
        await message.answer(
            "AIR — короткие паузы рядом.\n\n"
            "Нажми «📍 Выйти на AIR» и отправь геолокацию.",
            reply_markup=kb_main()
        )
        return

    await state.clear()
    await state.set_state(ProfileSetup.waiting_name)
    await message.answer(
        "AIR — короткие паузы рядом.\n\n"
        "Сначала настроим профиль.\n"
        "Как тебя назвать? (имя/ник 2–20 символов)",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Отмена")]],
            resize_keyboard=True
        )
    )

@dp.message(F.text == "Отмена")
async def cancel_any(message: Message, state: FSMContext):
    await state.clear()
    await db.log_event(message.from_user.id, "cancel")
    await message.answer("Ок. Возвращаю в меню.", reply_markup=kb_main())

@dp.message(ProfileSetup.waiting_name, F.text)
async def set_name(message: Message, state: FSMContext):
    text = clamp_text(message.text, 50)
    if not NAME_RE.match(text):
        await message.answer("Имя/ник должно быть 2–20 символов. Попробуй ещё раз.")
        return

    await db.upsert_user(message.from_user.id, name=text)
    await db.log_event(message.from_user.id, "set_name", {"name": text})
    await state.set_state(ProfileSetup.waiting_marker)
    await message.answer(
        "Теперь напиши свой <b>признак</b>, чтобы тебя было проще узнать.\n"
        "Примеры: «в чёрной куртке», «с рюкзаком», «в красной шапке»\n"
        "(2–40 символов)",
    )

@dp.message(ProfileSetup.waiting_marker, F.text)
async def set_marker(message: Message, state: FSMContext):
    text = clamp_text(message.text, 80)
    if not MARKER_RE.match(text):
        await message.answer("Признак должен быть 2–40 символов. Попробуй ещё раз.")
        return

    await db.upsert_user(message.from_user.id, marker=text)
    await db.log_event(message.from_user.id, "set_marker", {"marker": text})
    await state.clear()

    u = await db.get_user(message.from_user.id)
    await message.answer(
        f"Готово ✅\n"
        f"Твой профиль: <b>{escape(u.get('name') or '')}</b>, <b>{escape(u.get('marker') or '')}</b>\n\n"
        f"Теперь жми «📍 Выйти на AIR» и отправляй геолокацию.",
        reply_markup=kb_main()
    )

@dp.message(F.text == "👤 Профиль")
async def profile_btn(message: Message):
    await db.log_event(message.from_user.id, "profile_open")
    await show_profile(message, message.from_user.id)

@dp.message(F.text == "✏️ Сменить признак")
async def change_marker_btn(message: Message, state: FSMContext):
    await db.log_event(message.from_user.id, "change_marker_click")
    await state.set_state(ProfileSetup.changing_marker)
    await message.answer("Ок, напиши новый признак (2–40 символов).", reply_markup=ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Отмена")]],
        resize_keyboard=True
    ))

@dp.message(F.text == "🔄 Сменить имя")
async def change_name_btn(message: Message, state: FSMContext):
    await db.log_event(message.from_user.id, "change_name_click")
    await state.set_state(ProfileSetup.changing_name)
    await message.answer("Ок, напиши новое имя/ник (2–20 символов).", reply_markup=ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Отмена")]],
        resize_keyboard=True
    ))

@dp.callback_query(F.data == "chg_marker")
async def change_marker_cb(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await db.log_event(call.from_user.id, "change_marker_click")
    await state.set_state(ProfileSetup.changing_marker)
    await call.message.answer("Ок, напиши новый признак (2–40 символов).", reply_markup=ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Отмена")]],
        resize_keyboard=True
    ))

@dp.callback_query(F.data == "chg_name")
async def change_name_cb(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await db.log_event(call.from_user.id, "change_name_click")
    await state.set_state(ProfileSetup.changing_name)
    await call.message.answer("Ок, напиши новое имя/ник (2–20 символов).", reply_markup=ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Отмена")]],
        resize_keyboard=True
    ))

@dp.message(ProfileSetup.changing_marker, F.text)
async def changing_marker(message: Message, state: FSMContext):
    text = clamp_text(message.text, 80)
    if not MARKER_RE.match(text):
        await message.answer("Признак должен быть 2–40 символов. Попробуй ещё раз.")
        return
    await db.upsert_user(message.from_user.id, marker=text)
    await db.log_event(message.from_user.id, "marker_changed", {"marker": text})
    await state.clear()
    await message.answer("Готово ✅ Признак обновлён.", reply_markup=kb_main())

@dp.message(ProfileSetup.changing_name, F.text)
async def changing_name(message: Message, state: FSMContext):
    text = clamp_text(message.text, 50)
    if not NAME_RE.match(text):
        await message.answer("Имя/ник должно быть 2–20 символов. Попробуй ещё раз.")
        return
    await db.upsert_user(message.from_user.id, name=text)
    await db.log_event(message.from_user.id, "name_changed", {"name": text})
    await state.clear()
    await message.answer("Готово ✅ Имя обновлено.", reply_markup=kb_main())

@dp.message(Command("stop"))
@dp.message(F.text == "⛔️ Выйти с AIR")
async def stop_air(message: Message):
    await db.log_event(message.from_user.id, "stop_air")
    await db.set_active(message.from_user.id, False)
    await message.answer("Ок, ты вышел с AIR. Чтобы снова появиться — «📍 Выйти на AIR».", reply_markup=kb_main())

@dp.message(F.location)
async def on_location(message: Message):
    # Проверим профиль
    ok, u = await ensure_profile(message.from_user.id)
    if not ok:
        await message.answer("Сначала заполним профиль: /start")
        return

    lat = float(message.location.latitude)
    lon = float(message.location.longitude)

    await db.set_location(message.from_user.id, lat, lon, is_active=True)
    await db.log_event(message.from_user.id, "location_sent", {"lat": lat, "lon": lon})

    # Найти всех в радиусе
    nearby = await db.get_nearby(lat, lon, RADIUS_METERS, exclude_user=message.from_user.id)
    await db.log_event(message.from_user.id, "nearby_list_shown", {"count": len(nearby), "radius_m": RADIUS_METERS})

    # Сообщение пользователю
    if not nearby:
        await message.answer(
            f"Ты на AIR ✅\n"
            f"В радиусе <b>{RADIUS_METERS}м</b> пока никого не видно.\n\n"
            f"Твоя точка: {maps_link(lat, lon)}",
            reply_markup=kb_main()
        )
    else:
        lines = []
        for it in nearby[:10]:
            name = it.get("name") or "Без имени"
            marker = it.get("marker") or "без признака"
            dist = it["distance_m"]
            lines.append(f"• <b>{escape(name)}</b> — {escape(marker)} (<b>{dist}м</b>)")
        await message.answer(
            "Ты на AIR ✅\n\n"
            f"Рядом ({len(nearby)}):\n" + "\n".join(lines) + "\n\n"
            "Нажми на человека ниже — покажу точку на карте.",
            reply_markup=ikb_nearby_list(nearby)
        )

    # Уведомить соседей, что ты вышел рядом (анти-спам по парам)
    me_name = u.get("name") or "Кто-то"
    me_marker = u.get("marker") or "без признака"
    for it in nearby:
        to_uid = int(it["user_id"])
        dist = int(it["distance_m"])
        if await db.can_notify(from_user=message.from_user.id, to_user=to_uid):
            try:
                await bot.send_message(
                    to_uid,
                    f"📣 Рядом появился человек:\n"
                    f"<b>{escape(me_name)}</b> — {escape(me_marker)}\n"
                    f"Дистанция: <b>{dist}м</b>\n"
                    f"Точка: {maps_link(lat, lon)}"
                )
                await db.log_event(to_uid, "nearby_notification_received", {"from": message.from_user.id, "distance_m": dist})
            except Exception:
                # не ломаем поток
                pass

@dp.callback_query(F.data.startswith("map:"))
async def show_map(call: CallbackQuery):
    await call.answer()
    try:
        uid = int(call.data.split(":", 1)[1])
    except Exception:
        return

    loc = await db.get_location(uid)
    u = await db.get_user(uid)

    await db.log_event(call.from_user.id, "map_open", {"target_user_id": uid})

    if not loc or not loc.get("is_active"):
        await call.message.answer("Эта точка сейчас недоступна (человек вышел с AIR).")
        return

    lat = float(loc["lat"])
    lon = float(loc["lon"])
    name = (u or {}).get("name") or "Без имени"
    marker = (u or {}).get("marker") or "без признака"

    # Telegram-локация + ссылка на карты
    await bot.send_location(call.from_user.id, latitude=lat, longitude=lon)
    await call.message.answer(
        f"🧭 <b>{escape(name)}</b> — {escape(marker)}\n"
        f"{maps_link(lat, lon)}"
    )


# ------------------------- OPTIONAL: TTL CLEANUP -------------------------

async def ttl_watcher():
    # Если ACTIVE_TTL_MINUTES > 0 — периодически выключаем тех, кто давно не обновлял точку.
    if ACTIVE_TTL_MINUTES <= 0:
        return
    while True:
        try:
            cutoff = datetime.now(tz=UTC) - timedelta(minutes=ACTIVE_TTL_MINUTES)
            async with db.pool.acquire() as con:
                await con.execute(
                    "UPDATE locations SET is_active=FALSE WHERE is_active=TRUE AND updated_at < $1",
                    cutoff
                )
        except Exception:
            pass
        await asyncio.sleep(60)


# ------------------------- MAIN -------------------------

async def main():
    await db.connect()

    # Фоновая задачка TTL (если включена)
    asyncio.create_task(ttl_watcher())

    # Важно: если ловишь TelegramConflictError — значит где-то ещё запущен polling этого же бота.
    # Останови локальный запуск, оставь только Railway.
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])

if __name__ == "__main__":
    asyncio.run(main())
