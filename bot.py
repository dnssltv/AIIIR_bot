import asyncio
import math
import time

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)

from config import BOT_TOKEN
from db import init_db, get_conn, cleanup_sessions

dp = Dispatcher()

# ===== НАСТРОЙКИ =====
RADIUS_M = 150                 # радиус поиска
SESSION_TTL_SEC = 10 * 60      # 10 минут
COOLDOWN_SEC = 30 * 60         # 30 минут

# ===== UI =====
kb_main = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🌬 Выйти на AIR", request_location=True)],
        [KeyboardButton(text="👤 Профиль")],
    ],
    resize_keyboard=True
)

def badge_kb() -> InlineKeyboardMarkup:
    badges = [
        "в очках",
        "с рюкзаком",
        "в чёрной куртке",
        "в светлом худи",
        "в шапке",
        "другое",
    ]
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=b, callback_data=f"badge:{b}")]
            for b in badges
        ]
    )

def notify_kb(owner_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗺 Показать точку", callback_data=f"map:{owner_id}")],
            [InlineKeyboardButton(text="👍 Я иду", callback_data="join")],
            [InlineKeyboardButton(text="✖ Не сейчас", callback_data="noop")],
        ]
    )

# ===== УТИЛИТЫ =====
def distance_m(lat1, lon1, lat2, lon2) -> float:
    """Haversine distance in meters."""
    R = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# ===== DB HELPERS =====
def get_user_profile(user_id: int):
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT name, badge FROM users WHERE user_id=?", (user_id,))
        return c.fetchone()  # (name, badge) or None

def set_user_profile(user_id: int, name: str, badge: str):
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            "REPLACE INTO users (user_id, name, badge) VALUES (?, ?, ?)",
            (user_id, name, badge),
        )
        conn.commit()

def is_on_cooldown(user_id: int):
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT last_used_at FROM cooldowns WHERE user_id=?", (user_id,))
        row = c.fetchone()
        if not row:
            return False, 0
        last = row[0]
        delta = time.time() - last
        if delta < COOLDOWN_SEC:
            return True, int(COOLDOWN_SEC - delta)
        return False, 0

def set_cooldown(user_id: int):
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            "REPLACE INTO cooldowns (user_id, last_used_at) VALUES (?, ?)",
            (user_id, time.time()),
        )
        conn.commit()

def upsert_session(user_id: int, lat: float, lon: float):
    now = time.time()
    with get_conn() as conn:
        c = conn.cursor()
        c.execute(
            """
            REPLACE INTO sessions (user_id, lat, lon, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (user_id, lat, lon, now, now + SESSION_TTL_SEC),
        )
        conn.commit()

def delete_session(user_id: int):
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM sessions WHERE user_id=?", (user_id,))
        conn.commit()

def get_active_sessions():
    cleanup_sessions()
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT user_id, lat, lon, expires_at FROM sessions")
        return c.fetchall()

def get_session(owner_id: int):
    cleanup_sessions()
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT user_id, lat, lon, expires_at FROM sessions WHERE user_id=?", (owner_id,))
        return c.fetchone()

# ===== ПРОСТОЕ СОСТОЯНИЕ: ЖДЁМ ИМЯ =====
WAITING_NAME: set[int] = set()

# ===== HANDLERS =====
@dp.message(CommandStart())
async def start(message: Message):
    init_db()

    prof = get_user_profile(message.from_user.id)
    if prof is None:
        WAITING_NAME.add(message.from_user.id)
        await message.answer(
            "AIR — короткие паузы рядом.\n\n"
            "Сначала настроим профиль.\n"
            "Как тебя назвать? (имя/ник 2–20 символов)",
            reply_markup=kb_main
        )
        return

    name, badge = prof
    await message.answer(
        f"Привет, {name} 👋\n"
        f"Твой признак: {badge}\n\n"
        "Нажми «🌬 Выйти на AIR» и отправь геолокацию.\n"
        "Другие увидят *точку встречи* на карте, без слежки за людьми.",
        reply_markup=kb_main
    )

@dp.message(F.text == "👤 Профиль")
async def profile(message: Message):
    prof = get_user_profile(message.from_user.id)
    if prof is None:
        WAITING_NAME.add(message.from_user.id)
        await message.answer("Как тебя назвать? (имя/ник 2–20 символов)")
        return

    name, badge = prof
    WAITING_NAME.add(message.from_user.id)
    await message.answer(
        f"Твой профиль:\n"
        f"• Имя: {name}\n"
        f"• Признак: {badge}\n\n"
        f"Чтобы изменить имя — просто напиши новое имя (2–20 символов)."
    )

@dp.message(F.text)
async def handle_text(message: Message):
    # Если мы ждём имя
    if message.from_user.id not in WAITING_NAME:
        return

    name = (message.text or "").strip()
    if len(name) < 2 or len(name) > 20:
        await message.answer("Имя должно быть 2–20 символов. Попробуй ещё раз.")
        return

    # Сохраняем имя, признак выберем кнопками
    WAITING_NAME.remove(message.from_user.id)

    old = get_user_profile(message.from_user.id)
    old_badge = old[1] if old else "не выбрано"

    set_user_profile(message.from_user.id, name, old_badge if old_badge != "не выбрано" else "не выбрано")
    await message.answer("Выбери признак, чтобы тебя было проще узнать:", reply_markup=badge_kb())

@dp.callback_query(F.data.startswith("badge:"))
async def set_badge(callback: CallbackQuery):
    badge = callback.data.split(":", 1)[1]
    prof = get_user_profile(callback.from_user.id)
    if not prof:
        # если вдруг без имени
        WAITING_NAME.add(callback.from_user.id)
        await callback.message.answer("Напиши имя/ник (2–20 символов):")
        await callback.answer()
        return

    name = prof[0]
    set_user_profile(callback.from_user.id, name, badge)

    await callback.message.answer(
        f"Готово ✅\nТвой профиль: {name}, {badge}\n\n"
        f"Теперь жми «🌬 Выйти на AIR» и отправляй геолокацию.",
        reply_markup=kb_main
    )
    await callback.answer()

@dp.message(F.location)
async def on_location(message: Message):
    init_db()
    cleanup_sessions()

    user_id = message.from_user.id
    lat = message.location.latitude
    lon = message.location.longitude

    prof = get_user_profile(user_id)
    if prof is None or prof[1] == "не выбрано":
        await message.answer("Сначала настроим профиль: нажми /start")
        return

    # Кулдаун
    on_cd, remain = is_on_cooldown(user_id)
    if on_cd:
        await message.answer(f"⏳ Можно выйти на AIR через {remain//60} мин {remain%60} сек.")
        return

    # Создать/обновить сессию
    upsert_session(user_id, lat, lon)
    set_cooldown(user_id)

    name, badge = prof

    # Найти рядом и уведомить
    sessions = get_active_sessions()
    nearby = []
    for uid, slat, slon, exp in sessions:
        if uid == user_id:
            continue
        if distance_m(lat, lon, slat, slon) <= RADIUS_M:
            nearby.append(uid)

    await message.answer(
        f"Ты на AIR 🌿\n"
        f"Профиль: {name}, {badge}\n"
        f"Радиус: ~{RADIUS_M} м\n"
        f"Время: 10 минут\n"
        f"Рядом активных: {len(nearby)}\n\n"
        f"Чтобы завершить раньше: /stop"
    )

    bot: Bot = message.bot
    for uid in nearby:
        try:
            await bot.send_message(
                uid,
                f"🌬 AIR рядом (~{RADIUS_M}м)\n"
                f"Кто: {name}, {badge}\n"
                f"Хочешь присоединиться?",
                reply_markup=notify_kb(owner_id=user_id)
            )
        except Exception:
            pass

@dp.message(Command("stop"))
async def stop_air(message: Message):
    delete_session(message.from_user.id)
    await message.answer("AIR завершён. Ты снова невидим 👋")

@dp.callback_query(F.data.startswith("map:"))
async def show_map(callback: CallbackQuery):
    owner_id = int(callback.data.split(":", 1)[1])
    sess = get_session(owner_id)
    if not sess:
        await callback.message.answer("AIR уже закончился 😕")
        await callback.answer()
        return

    _, lat, lon, exp = sess
    await callback.message.answer_location(latitude=lat, longitude=lon)
    await callback.answer()

@dp.callback_query(F.data.in_({"join", "noop"}))
async def join_or_noop(callback: CallbackQuery):
    if callback.data == "join":
        await callback.message.answer(
            "Ок! Нажми «🌬 Выйти на AIR» и отправь геолокацию.\n"
            "После этого открой уведомление и нажми «🗺 Показать точку» — увидишь место встречи."
        )
    await callback.answer()

async def main():
    init_db()
    bot = Bot(token=BOT_TOKEN)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
