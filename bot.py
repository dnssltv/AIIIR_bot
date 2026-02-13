import os
import math
import asyncio
from io import BytesIO
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple, Dict, Any

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    BufferedInputFile,
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from db import Database


# ---------------- config ----------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# Радиус поиска (метры)
AIR_RADIUS_METERS = int(os.getenv("AIR_RADIUS_METERS", "300").strip() or "300")

# Время активности (сек). 0 = без авто-истечения (для тестов)
AIR_SESSION_TTL_SECONDS = int(os.getenv("AIR_SESSION_TTL_SECONDS", "0").strip() or "0")

# Админы для экспорта (через запятую)
ADMIN_IDS = set()
_raw_admins = os.getenv("ADMIN_IDS", "").strip()
if _raw_admins:
    for x in _raw_admins.split(","):
        x = x.strip()
        if x.isdigit():
            ADMIN_IDS.add(int(x))


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------- utils ----------------
def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    # distance in meters
    R = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🫧 Выйти на AIR"), KeyboardButton(text="👤 Профиль")],
            [KeyboardButton(text="✏️ Сменить признак"), KeyboardButton(text="🧾 Премиум")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Выбери действие…",
    )


def location_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📍 Отправить геолокацию", request_location=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


# ---------------- FSM ----------------
class ProfileFlow(StatesGroup):
    waiting_name = State()
    waiting_marker = State()
    waiting_new_marker = State()


# ---------------- app ----------------
dp = Dispatcher()


@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, db: Database):
    await db.upsert_user(message.from_user.id)
    await db.log_event(message.from_user.id, "start", {"username": message.from_user.username})

    user = await db.get_user(message.from_user.id)
    if not user or not user.get("name") or not user.get("marker"):
        await message.answer(
            "AIR — короткие паузы рядом.\n\n"
            "Сначала настроим профиль.\n"
            "Как тебя назвать? (имя/ник 2–20 символов)",
            reply_markup=ReplyKeyboardRemove(),
        )
        await state.set_state(ProfileFlow.waiting_name)
        return

    await message.answer(
        f"С возвращением, {user['name']}!\n"
        f"Твой признак: {user['marker']}\n\n"
        f"Нажми «🫧 Выйти на AIR» и отправь геолокацию — покажу людей рядом.",
        reply_markup=main_menu_kb(),
    )


@dp.message(ProfileFlow.waiting_name, F.text)
async def profile_name(message: Message, state: FSMContext, db: Database):
    name = (message.text or "").strip()
    if not (2 <= len(name) <= 20):
        await message.answer("Имя/ник должно быть 2–20 символов. Попробуй ещё раз.")
        return

    await state.update_data(name=name)
    await db.log_event(message.from_user.id, "profile_name_set", {"len": len(name)})

    await message.answer(
        "Теперь напиши свой признак (2–40 символов).\n"
        "Например: «в чёрной куртке», «с ноутбуком», «в красной шапке».",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.set_state(ProfileFlow.waiting_marker)


@dp.message(ProfileFlow.waiting_marker, F.text)
async def profile_marker(message: Message, state: FSMContext, db: Database):
    marker = (message.text or "").strip()
    if not (2 <= len(marker) <= 40):
        await message.answer("Признак должен быть 2–40 символов. Напиши ещё раз.")
        return

    data = await state.get_data()
    name = data.get("name", "User")

    await db.set_profile(message.from_user.id, name=name, marker=marker)
    await db.log_event(message.from_user.id, "profile_marker_set", {"len": len(marker)})

    await state.clear()
    await message.answer(
        f"Готово ✅\n\nТвой профиль:\n— {name}\n— {marker}\n\n"
        "Теперь жми «🫧 Выйти на AIR» и отправляй геолокацию.",
        reply_markup=main_menu_kb(),
    )


@dp.message(F.text == "👤 Профиль")
async def show_profile(message: Message, db: Database):
    await db.upsert_user(message.from_user.id)
    user = await db.get_user(message.from_user.id)

    if not user or not user.get("name") or not user.get("marker"):
        await message.answer(
            "Профиль ещё не заполнен. Напиши /start и пройди настройку.",
            reply_markup=main_menu_kb(),
        )
        return

    premium_until = user.get("premium_until")
    premium_str = "нет"
    if premium_until and isinstance(premium_until, datetime) and premium_until > utcnow():
        premium_str = f"до {premium_until.strftime('%Y-%m-%d %H:%M')} (UTC)"

    await db.log_event(message.from_user.id, "profile_view")
    await message.answer(
        f"👤 Профиль\n\n"
        f"Имя: {user['name']}\n"
        f"Признак: {user['marker']}\n"
        f"Премиум: {premium_str}\n\n"
        f"Радиус поиска: {AIR_RADIUS_METERS} м",
        reply_markup=main_menu_kb(),
    )


@dp.message(F.text == "✏️ Сменить признак")
async def change_marker_start(message: Message, state: FSMContext, db: Database):
    await db.upsert_user(message.from_user.id)
    user = await db.get_user(message.from_user.id)
    if not user or not user.get("name"):
        await message.answer("Сначала заполни профиль через /start.")
        return

    await db.log_event(message.from_user.id, "marker_change_start")
    await message.answer(
        "Ок. Напиши новый признак (2–40 символов).",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.set_state(ProfileFlow.waiting_new_marker)


@dp.message(ProfileFlow.waiting_new_marker, F.text)
async def change_marker_save(message: Message, state: FSMContext, db: Database):
    marker = (message.text or "").strip()
    if not (2 <= len(marker) <= 40):
        await message.answer("Признак должен быть 2–40 символов. Напиши ещё раз.")
        return

    await db.set_marker(message.from_user.id, marker)
    await db.log_event(message.from_user.id, "marker_changed", {"len": len(marker)})

    await state.clear()
    await message.answer(f"Готово ✅ Новый признак: {marker}", reply_markup=main_menu_kb())


@dp.message(F.text == "🫧 Выйти на AIR")
async def go_air(message: Message, db: Database):
    await db.upsert_user(message.from_user.id)
    user = await db.get_user(message.from_user.id)
    if not user or not user.get("name") or not user.get("marker"):
        await message.answer("Сначала заполни профиль через /start.")
        return

    await db.log_event(message.from_user.id, "air_button")
    await message.answer(
        "Отправь геолокацию — покажу людей рядом и отмечу их на карте.",
        reply_markup=location_kb(),
    )


@dp.message(F.location)
async def on_location(message: Message, db: Database):
    await db.upsert_user(message.from_user.id)
    user = await db.get_user(message.from_user.id)
    if not user or not user.get("name") or not user.get("marker"):
        await message.answer("Сначала заполни профиль через /start.", reply_markup=main_menu_kb())
        return

    lat = float(message.location.latitude)
    lon = float(message.location.longitude)

    # TTL: 0 = no expiry
    active_until = None
    if AIR_SESSION_TTL_SECONDS > 0:
        active_until = utcnow() + timedelta(seconds=AIR_SESSION_TTL_SECONDS)

    await db.upsert_session(message.from_user.id, lat, lon, active_until)
    await db.log_event(message.from_user.id, "location_sent", {"lat": lat, "lon": lon})

    # Get active sessions and filter by radius
    sessions = await db.get_active_sessions(utcnow())
    others = []
    for s in sessions:
        if int(s["user_id"]) == int(message.from_user.id):
            continue
        d = haversine_m(lat, lon, float(s["lat"]), float(s["lon"]))
        if d <= AIR_RADIUS_METERS:
            others.append((d, s))

    others.sort(key=lambda x: x[0])

    if not others:
        await message.answer(
            f"Пока никого рядом (в радиусе {AIR_RADIUS_METERS} м) не видно.\n"
            f"Я сохранил твою точку — когда кто-то появится рядом, можно снова нажать «🫧 Выйти на AIR».",
            reply_markup=main_menu_kb(),
        )
        return

    # Text summary
    lines = [f"Нашёл рядом (≤ {AIR_RADIUS_METERS} м): {len(others)}"]
    for i, (d, s) in enumerate(others[:10], start=1):
        name = s.get("name") or "Кто-то"
        marker = s.get("marker") or "без признака"
        lines.append(f"{i}) {name} — {marker} (~{int(d)} м)")

    await message.answer("\n".join(lines), reply_markup=main_menu_kb())

    # Send their locations so user can see on map inside Telegram
    # (Telegram shows these messages as map points)
    for d, s in others[:10]:
        title = (s.get("name") or "AIR")[:32]
        desc = (s.get("marker") or "")[:128]
        await message.answer_location(latitude=float(s["lat"]), longitude=float(s["lon"]))
        await asyncio.sleep(0.2)

    await db.log_event(message.from_user.id, "nearby_list_shown", {"count": len(others)})


@dp.message(F.text == "🧾 Премиум")
async def premium_info(message: Message, db: Database):
    """
    Задел под монетизацию (пока без оплаты):
    - расширенный радиус
    - VIP бейдж
    - видимость дольше
    - фильтры (например "только мой БЦ")
    """
    await db.upsert_user(message.from_user.id)
    await db.log_event(message.from_user.id, "premium_view")

    await message.answer(
        "🧾 Премиум (идея для монетизации)\n\n"
        "Можно сделать платные фичи:\n"
        "• Радиус поиска 1–2 км\n"
        "• Видимость дольше (например 2 часа)\n"
        "• VIP-значок\n"
        "• Фильтры: «только мой БЦ/район»\n"
        "• «Супер-пауза»: показывать тебя выше в списке\n\n"
        "Пока это заглушка — добавим оплату позже (Telegram Payments/Stripe).",
        reply_markup=main_menu_kb(),
    )


# ---------------- admin: analytics export ----------------
@dp.message(Command("stats"))
async def cmd_stats(message: Message, db: Database):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Команда только для админа.")
        return

    users_count = await db.count_users()
    await db.log_event(message.from_user.id, "admin_stats")

    await message.answer(
        f"📊 Stats\n\n"
        f"Users: {users_count}\n"
        f"Radius: {AIR_RADIUS_METERS} m\n"
        f"TTL: {AIR_SESSION_TTL_SECONDS} sec (0 = no expiry)"
    )


@dp.message(Command("export_events"))
async def cmd_export_events(message: Message, db: Database):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Команда только для админа.")
        return

    csv_text = await db.export_events_csv(limit=5000)
    buf = BytesIO(csv_text.encode("utf-8"))
    file = BufferedInputFile(buf.getvalue(), filename="air_events.csv")

    await db.log_event(message.from_user.id, "admin_export_events", {"rows": 5000})
    await message.answer_document(file, caption="CSV: events (последние 5000)")


@dp.message(Command("export_users"))
async def cmd_export_users(message: Message, db: Database):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Команда только для админа.")
        return

    csv_text = await db.export_users_csv(limit=20000)
    buf = BytesIO(csv_text.encode("utf-8"))
    file = BufferedInputFile(buf.getvalue(), filename="air_users.csv")

    await db.log_event(message.from_user.id, "admin_export_users", {"rows": 20000})
    await message.answer_document(file, caption="CSV: users")


# ---------------- entrypoint ----------------
async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is empty")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is empty (Railway PostgreSQL)")

    bot = Bot(token=BOT_TOKEN)
    db = Database(DATABASE_URL)
    await db.connect()

    # inject db into handlers
    dp["db"] = db

    try:
        # IMPORTANT: only one instance must run (no local + railway together)
        await db.log_event(None, "bot_started", {"ts": utcnow().isoformat()})
        await dp.start_polling(bot, db=db)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
