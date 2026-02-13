import os
import csv
import math
import sqlite3
from io import StringIO, BytesIO
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple, Set, Dict, Any

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BufferedInputFile,
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

from dotenv import load_dotenv

load_dotenv()

# =========================
# ENV / SETTINGS
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set (Railway Variables)")

ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()
ADMIN_IDS: Set[int] = set()
if ADMIN_IDS_RAW:
    for part in ADMIN_IDS_RAW.replace(" ", "").split(","):
        if part.isdigit():
            ADMIN_IDS.add(int(part))

DB_PATH = os.getenv("DB_PATH", "air.db").strip()
ENV_NAME = os.getenv("ENV_NAME", "local").strip()

AIR_RADIUS_M_DEFAULT = int(os.getenv("AIR_RADIUS_M", "200").strip())
AIR_TTL_MINUTES = int(os.getenv("AIR_TTL_MINUTES", "0").strip())  # 0 = no TTL (testing)

TZ = timezone.utc

# Monetization defaults (potential)
PREMIUM_RADIUS_M = int(os.getenv("PREMIUM_RADIUS_M", "800").strip())  # for AIR+
PREMIUM_DAILY_PINGS = int(os.getenv("PREMIUM_DAILY_PINGS", "30").strip())
FREE_DAILY_PINGS = int(os.getenv("FREE_DAILY_PINGS", "5").strip())

# =========================
# UTILS
# =========================
def now_utc() -> datetime:
    return datetime.now(TZ)

def iso(dt: datetime) -> str:
    return dt.astimezone(TZ).isoformat()

def clamp_text(s: str) -> str:
    return " ".join((s or "").strip().split())

def valid_name(s: str) -> bool:
    s = clamp_text(s)
    return 2 <= len(s) <= 20

def valid_marker(s: str) -> bool:
    s = clamp_text(s)
    return 2 <= len(s) <= 40

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def maps_link(lat: float, lon: float) -> str:
    return f"https://maps.google.com/?q={lat:.6f},{lon:.6f}"

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS if ADMIN_IDS else False

def today_ymd(dt: Optional[datetime] = None) -> str:
    dt = dt or now_utc()
    return dt.strftime("%Y-%m-%d")

# =========================
# KEYBOARDS
# =========================
def menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚬 Выйти на AIR")],
            [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="✏️ Сменить признак")],
            [KeyboardButton(text="⭐ AIR+")],
        ],
        resize_keyboard=True,
        selective=True,
    )

def location_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📍 Отправить геолокацию", request_location=True)],
            [KeyboardButton(text="⬅️ Назад")],
        ],
        resize_keyboard=True,
        selective=True,
    )

def cancel_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⬅️ Отмена")]],
        resize_keyboard=True,
        selective=True,
    )

# =========================
# DB
# =========================
class DB:
    def __init__(self, path: str):
        self.path = path
        self._init()

    def _conn(self):
        # check_same_thread False helps in some hosting environments
        return sqlite3.connect(self.path, check_same_thread=False)

    def _init(self):
        with self._conn() as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    name TEXT,
                    marker TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS air_sessions (
                    user_id INTEGER PRIMARY KEY,
                    lat REAL,
                    lon REAL,
                    updated_at TEXT
                )
            """)
            # Monetization (potential): plans + subscriptions
            con.execute("""
                CREATE TABLE IF NOT EXISTS plans (
                    plan_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    radius_m INTEGER NOT NULL,
                    daily_pings INTEGER NOT NULL
                )
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS subscriptions (
                    user_id INTEGER PRIMARY KEY,
                    plan_id TEXT NOT NULL,
                    until_at TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            """)
            # Analytics: counters + events + daily limits
            con.execute("""
                CREATE TABLE IF NOT EXISTS counters (
                    key TEXT PRIMARY KEY,
                    value INTEGER NOT NULL
                )
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    user_id INTEGER,
                    event TEXT NOT NULL,
                    meta TEXT
                )
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS daily_limits (
                    ymd TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    key TEXT NOT NULL,
                    value INTEGER NOT NULL,
                    PRIMARY KEY (ymd, user_id, key)
                )
            """)

            # seed counters
            for k in [
                "users_total",
                "profiles_completed",
                "press_air",
                "press_profile",
                "press_change_marker",
                "press_airplus",
                "locations_received",
                "lists_shown",
                "notifications_sent",
            ]:
                con.execute("INSERT OR IGNORE INTO counters(key, value) VALUES (?, 0)", (k,))

            # seed plans
            con.execute(
                "INSERT OR IGNORE INTO plans(plan_id, title, radius_m, daily_pings) VALUES (?,?,?,?)",
                ("free", "FREE", AIR_RADIUS_M_DEFAULT, FREE_DAILY_PINGS)
            )
            con.execute(
                "INSERT OR IGNORE INTO plans(plan_id, title, radius_m, daily_pings) VALUES (?,?,?,?)",
                ("air_plus", "AIR+", PREMIUM_RADIUS_M, PREMIUM_DAILY_PINGS)
            )

            con.commit()

    # --- counters / events ---
    def inc(self, key: str, by: int = 1):
        with self._conn() as con:
            con.execute("INSERT OR IGNORE INTO counters(key, value) VALUES (?, 0)", (key,))
            con.execute("UPDATE counters SET value = value + ? WHERE key = ?", (by, key))
            con.commit()

    def event(self, user_id: Optional[int], event: str, meta: Optional[str] = None):
        with self._conn() as con:
            con.execute(
                "INSERT INTO events(ts, user_id, event, meta) VALUES (?,?,?,?)",
                (iso(now_utc()), user_id, event, meta),
            )
            con.commit()

    def get_counters(self) -> List[Tuple[str, int]]:
        with self._conn() as con:
            rows = con.execute("SELECT key, value FROM counters ORDER BY key").fetchall()
        return [(r[0], int(r[1])) for r in rows]

    def export_csv(self, query: str, headers: List[str], params: Tuple[Any, ...] = ()) -> bytes:
        with self._conn() as con:
            rows = con.execute(query, params).fetchall()
        buf = StringIO()
        w = csv.writer(buf)
        w.writerow(headers)
        for r in rows:
            w.writerow(list(r))
        return buf.getvalue().encode("utf-8")

    # --- users / profiles ---
    def get_user(self, user_id: int) -> Optional[Tuple[int, Optional[str], Optional[str]]]:
        with self._conn() as con:
            row = con.execute(
                "SELECT user_id, name, marker FROM users WHERE user_id = ?",
                (user_id,),
            ).fetchone()
        return row

    def upsert_user(self, user_id: int, name: Optional[str] = None, marker: Optional[str] = None):
        ts = iso(now_utc())
        existing = self.get_user(user_id)
        with self._conn() as con:
            if not existing:
                con.execute(
                    "INSERT INTO users(user_id, name, marker, created_at, updated_at) VALUES(?,?,?,?,?)",
                    (user_id, name, marker, ts, ts),
                )
                con.commit()
                self.inc("users_total", 1)
                self.event(user_id, "user_created", None)
            else:
                cur_name = existing[1]
                cur_marker = existing[2]
                new_name = name if name is not None else cur_name
                new_marker = marker if marker is not None else cur_marker
                con.execute(
                    "UPDATE users SET name = ?, marker = ?, updated_at = ? WHERE user_id = ?",
                    (new_name, new_marker, ts, user_id),
                )
                con.commit()

    def profile_complete(self, user_id: int) -> bool:
        u = self.get_user(user_id)
        return bool(u and u[1] and u[2])

    # --- monetization (potential) ---
    def get_plan_for_user(self, user_id: int) -> Tuple[str, str, int, int]:
        """
        Returns (plan_id, title, radius_m, daily_pings)
        If subscription active -> that plan, else free plan.
        """
        with self._conn() as con:
            sub = con.execute(
                "SELECT plan_id, until_at FROM subscriptions WHERE user_id = ?",
                (user_id,),
            ).fetchone()

            plan_id = "free"
            if sub:
                pid, until_at = sub
                if until_at is None:
                    plan_id = pid
                else:
                    try:
                        until_dt = datetime.fromisoformat(until_at)
                        if until_dt >= now_utc():
                            plan_id = pid
                    except Exception:
                        plan_id = pid

            plan = con.execute(
                "SELECT plan_id, title, radius_m, daily_pings FROM plans WHERE plan_id = ?",
                (plan_id,),
            ).fetchone()

        if not plan:
            return ("free", "FREE", AIR_RADIUS_M_DEFAULT, FREE_DAILY_PINGS)
        return (plan[0], plan[1], int(plan[2]), int(plan[3]))

    def set_subscription(self, user_id: int, plan_id: str, days: Optional[int] = None):
        # admin helper: grant subscription
        now = now_utc()
        until_at = None
        if days is not None:
            until_at = iso(now + timedelta(days=days))
        ts = iso(now)
        with self._conn() as con:
            con.execute(
                """INSERT INTO subscriptions(user_id, plan_id, until_at, created_at, updated_at)
                   VALUES(?,?,?,?,?)
                   ON CONFLICT(user_id) DO UPDATE SET
                      plan_id=excluded.plan_id,
                      until_at=excluded.until_at,
                      updated_at=excluded.updated_at
                """,
                (user_id, plan_id, until_at, ts, ts),
            )
            con.commit()

    # --- AIR sessions ---
    def upsert_air_location(self, user_id: int, lat: float, lon: float):
        ts = iso(now_utc())
        with self._conn() as con:
            con.execute(
                """INSERT INTO air_sessions(user_id, lat, lon, updated_at)
                   VALUES(?,?,?,?)
                   ON CONFLICT(user_id) DO UPDATE SET
                     lat=excluded.lat, lon=excluded.lon, updated_at=excluded.updated_at
                """,
                (user_id, lat, lon, ts),
            )
            con.commit()

    def get_active_sessions(self) -> List[Tuple[int, float, float, str]]:
        with self._conn() as con:
            rows = con.execute("SELECT user_id, lat, lon, updated_at FROM air_sessions").fetchall()

        if AIR_TTL_MINUTES <= 0:
            return [(int(r[0]), float(r[1]), float(r[2]), str(r[3])) for r in rows]

        cutoff = now_utc() - timedelta(minutes=AIR_TTL_MINUTES)
        out = []
        for r in rows:
            try:
                ts = datetime.fromisoformat(r[3])
                if ts >= cutoff:
                    out.append((int(r[0]), float(r[1]), float(r[2]), str(r[3])))
            except Exception:
                out.append((int(r[0]), float(r[1]), float(r[2]), str(r[3])))
        return out

    # --- daily limits (for monetization potential) ---
    def get_daily(self, ymd: str, user_id: int, key: str) -> int:
        with self._conn() as con:
            row = con.execute(
                "SELECT value FROM daily_limits WHERE ymd=? AND user_id=? AND key=?",
                (ymd, user_id, key),
            ).fetchone()
        return int(row[0]) if row else 0

    def inc_daily(self, ymd: str, user_id: int, key: str, by: int = 1):
        with self._conn() as con:
            con.execute(
                """INSERT INTO daily_limits(ymd, user_id, key, value)
                   VALUES(?,?,?,?)
                   ON CONFLICT(ymd, user_id, key) DO UPDATE SET
                      value = value + excluded.value
                """,
                (ymd, user_id, key, by),
            )
            con.commit()

db = DB(DB_PATH)

# =========================
# FSM
# =========================
class ProfileState(StatesGroup):
    waiting_name = State()
    waiting_marker = State()
    changing_marker = State()

# =========================
# BOT SETUP
# =========================
bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# =========================
# TEXTS
# =========================
INTRO = "AIR — короткие паузы рядом."
HELP_AIR = (
    "Нажми «🚬 Выйти на AIR» → отправь геолокацию.\n"
    "Я покажу людей рядом и дам ссылки, чтобы открыть их точку на карте."
)

def profile_text(user_id: int) -> str:
    u = db.get_user(user_id)
    name = u[1] if u else None
    marker = u[2] if u else None
    name = name or "—"
    marker = marker or "—"
    plan_id, title, radius_m, daily_pings = db.get_plan_for_user(user_id)
    return (
        f"Твой профиль:\n"
        f"• Имя: {name}\n"
        f"• Признак: {marker}\n\n"
        f"План: {title}\n"
        f"• Радиус: {radius_m} м\n"
        f"• Дневной лимит «пингов»: {daily_pings}\n"
    )

def airplus_text() -> str:
    return (
        "⭐ AIR+ (идея монетизации)\n\n"
        "Что можно дать в AIR+:\n"
        "• Больший радиус поиска\n"
        "• Больше «пингов» в день\n"
        "• Приоритет в списке\n"
        "• Режим «тихий» (видишь других, но тебя видят меньше)\n\n"
        "Платежи сейчас не подключены — это задел.\n"
        "Хочешь — следующим шагом подключим оплату через Telegram Payments."
    )

# =========================
# COMMANDS
# =========================
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    db.upsert_user(message.from_user.id)  # ensures user exists
    db.event(message.from_user.id, "start", None)
    await state.clear()
    await message.answer(
        f"{INTRO}\n\nСначала настроим профиль.\nКак тебя назвать? (имя/ник 2–20 символов)",
        reply_markup=cancel_kb(),
    )
    await state.set_state(ProfileState.waiting_name)

@dp.message(Command("help"))
async def cmd_help(message: Message):
    db.event(message.from_user.id, "help", None)
    await message.answer(HELP_AIR, reply_markup=menu_kb())

@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if not is_admin(message.from_user.id):
        return
    counters = db.get_counters()
    lines = [f"ENV: {ENV_NAME}", "Counters:"]
    for k, v in counters:
        lines.append(f"- {k}: {v}")
    await message.answer("\n".join(lines))

@dp.message(Command("export_users"))
async def cmd_export_users(message: Message):
    if not is_admin(message.from_user.id):
        return
    data = db.export_csv(
        "SELECT user_id, name, marker, created_at, updated_at FROM users ORDER BY created_at DESC",
        ["user_id", "name", "marker", "created_at", "updated_at"],
    )
    file = BufferedInputFile(data, filename="users.csv")
    await message.answer_document(file, caption="users.csv")

@dp.message(Command("export_events"))
async def cmd_export_events(message: Message):
    if not is_admin(message.from_user.id):
        return
    data = db.export_csv(
        "SELECT id, ts, user_id, event, meta FROM events ORDER BY id DESC LIMIT 5000",
        ["id", "ts", "user_id", "event", "meta"],
    )
    file = BufferedInputFile(data, filename="events.csv")
    await message.answer_document(file, caption="events.csv (last 5000)")

@dp.message(Command("export_counters"))
async def cmd_export_counters(message: Message):
    if not is_admin(message.from_user.id):
        return
    data = db.export_csv(
        "SELECT key, value FROM counters ORDER BY key",
        ["key", "value"],
    )
    file = BufferedInputFile(data, filename="counters.csv")
    await message.answer_document(file, caption="counters.csv")

@dp.message(Command("grant_airplus"))
async def cmd_grant_airplus(message: Message):
    """
    Admin helper:
    /grant_airplus <user_id> <days>
    example: /grant_airplus 123456789 30
    """
    if not is_admin(message.from_user.id):
        return
    parts = (message.text or "").split()
    if len(parts) < 3 or not parts[1].isdigit() or not parts[2].isdigit():
        await message.answer("Usage: /grant_airplus <user_id> <days>")
        return
    uid = int(parts[1])
    days = int(parts[2])
    db.set_subscription(uid, "air_plus", days=days)
    db.event(message.from_user.id, "grant_airplus", f"{uid}:{days}")
    await message.answer(f"OK: AIR+ granted to {uid} for {days} days")

# =========================
# PROFILE FLOW
# =========================
@dp.message(ProfileState.waiting_name)
async def profile_name(message: Message, state: FSMContext):
    txt = clamp_text(message.text)
    if txt == "⬅️ Отмена":
        await state.clear()
        await message.answer("Ок. Меню.", reply_markup=menu_kb())
        return

    if not valid_name(txt):
        await message.answer("Имя должно быть 2–20 символов. Попробуй ещё раз.", reply_markup=cancel_kb())
        return

    db.upsert_user(message.from_user.id, name=txt)
    db.event(message.from_user.id, "profile_set_name", txt)
    await state.set_state(ProfileState.waiting_marker)
    await message.answer(
        "Теперь напиши опознавательный признак (2–40 символов).\n\n"
        "Например:\n"
        "• в чёрной куртке\n"
        "• с зелёным рюкзаком\n"
        "• в красной шапке",
        reply_markup=cancel_kb(),
    )

@dp.message(ProfileState.waiting_marker)
async def profile_marker(message: Message, state: FSMContext):
    txt = clamp_text(message.text)
    if txt == "⬅️ Отмена":
        await state.clear()
        await message.answer("Ок. Меню.", reply_markup=menu_kb())
        return

    if not valid_marker(txt):
        await message.answer("Признак должен быть 2–40 символов. Попробуй ещё раз.", reply_markup=cancel_kb())
        return

    db.upsert_user(message.from_user.id, marker=txt)
    db.inc("profiles_completed", 1)
    db.event(message.from_user.id, "profile_set_marker", txt)
    await state.clear()

    await message.answer("Готово ✅\n\n" + profile_text(message.from_user.id), reply_markup=menu_kb())

# =========================
# MENU BUTTONS
# =========================
@dp.message(F.text == "👤 Профиль")
async def btn_profile(message: Message):
    db.inc("press_profile", 1)
    db.event(message.from_user.id, "press_profile", None)

    if not db.profile_complete(message.from_user.id):
        await message.answer("Сначала настроим профиль. Как тебя назвать? (2–20)", reply_markup=cancel_kb())
        return

    await message.answer(profile_text(message.from_user.id), reply_markup=menu_kb())

@dp.message(F.text == "✏️ Сменить признак")
async def btn_change_marker(message: Message, state: FSMContext):
    db.inc("press_change_marker", 1)
    db.event(message.from_user.id, "press_change_marker", None)

    if not db.profile_complete(message.from_user.id):
        await message.answer("Сначала настроим профиль. Как тебя назвать? (2–20)", reply_markup=cancel_kb())
        await state.set_state(ProfileState.waiting_name)
        return

    await state.set_state(ProfileState.changing_marker)
    await message.answer(
        "Напиши новый опознавательный признак (2–40 символов).",
        reply_markup=cancel_kb(),
    )

@dp.message(ProfileState.changing_marker)
async def change_marker_save(message: Message, state: FSMContext):
    txt = clamp_text(message.text)
    if txt == "⬅️ Отмена":
        await state.clear()
        await message.answer("Отменено.", reply_markup=menu_kb())
        return

    if not valid_marker(txt):
        await message.answer("Признак должен быть 2–40 символов. Попробуй ещё раз.", reply_markup=cancel_kb())
        return

    db.upsert_user(message.from_user.id, marker=txt)
    db.event(message.from_user.id, "profile_change_marker", txt)
    await state.clear()
    await message.answer("✅ Признак обновлён!\n\n" + profile_text(message.from_user.id), reply_markup=menu_kb())

@dp.message(F.text == "⭐ AIR+")
async def btn_airplus(message: Message):
    db.inc("press_airplus", 1)
    db.event(message.from_user.id, "press_airplus", None)
    await message.answer(airplus_text(), reply_markup=menu_kb())

@dp.message(F.text == "🚬 Выйти на AIR")
async def btn_air(message: Message):
    db.inc("press_air", 1)
    db.event(message.from_user.id, "press_air", None)

    if not db.profile_complete(message.from_user.id):
        await message.answer("Сначала настроим профиль. Как тебя назвать? (2–20)", reply_markup=cancel_kb())
        return

    await message.answer(
        "Отправь геолокацию 👇",
        reply_markup=location_kb(),
    )

@dp.message(F.text == "⬅️ Назад")
async def btn_back(message: Message):
    db.event(message.from_user.id, "back", None)
    await message.answer("Меню.", reply_markup=menu_kb())

# =========================
# AIR LOGIC
# =========================
def build_user_card_line(name: str, marker: str, dist_m: int) -> str:
    return f"• {name} — {marker} ({dist_m} м)"

def user_inline_map_kb(lat: float, lon: float, title: str = "Открыть на карте") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=title, url=maps_link(lat, lon))]
        ]
    )

async def enforce_daily_ping_limit(user_id: int) -> Tuple[bool, str]:
    """
    "Ping" is: when user sends location, we notify others nearby.
    Limits are plan-based.
    """
    plan_id, title, radius_m, daily_pings = db.get_plan_for_user(user_id)
    ymd = today_ymd()
    used = db.get_daily(ymd, user_id, "pings")
    if daily_pings <= 0:
        return True, ""
    if used >= daily_pings:
        return False, (
            f"Лимит на сегодня исчерпан ({used}/{daily_pings}).\n"
            f"Хочешь больше — это будет частью ⭐ AIR+."
        )
    db.inc_daily(ymd, user_id, "pings", 1)
    return True, ""

@dp.message(F.location)
async def on_location(message: Message):
    user_id = message.from_user.id
    loc = message.location

    if not db.profile_complete(user_id):
        await message.answer("Сначала настрой профиль: /start", reply_markup=menu_kb())
        return

    ok, reason = await enforce_daily_ping_limit(user_id)
    if not ok:
        db.event(user_id, "limit_ping_block", reason)
        await message.answer(reason, reply_markup=menu_kb())
        return

    lat = float(loc.latitude)
    lon = float(loc.longitude)

    db.upsert_air_location(user_id, lat, lon)
    db.inc("locations_received", 1)
    db.event(user_id, "location_received", f"{lat:.6f},{lon:.6f}")

    # find nearby users
    plan_id, title, radius_m, daily_pings = db.get_plan_for_user(user_id)

    sessions = db.get_active_sessions()
    nearby: List[Tuple[int, int, float, float]] = []  # (uid, dist_m, lat, lon)
    for uid, olat, olon, updated_at in sessions:
        if uid == user_id:
            continue
        dist = int(haversine_m(lat, lon, olat, olon))
        if dist <= radius_m:
            nearby.append((uid, dist, olat, olon))

    nearby.sort(key=lambda x: x[1])

    # show list to sender
    me = db.get_user(user_id)
    me_name = me[1] or "Кто-то"
    me_marker = me[2] or "—"

    if not nearby:
        await message.answer(
            f"Пока никого рядом в радиусе {radius_m} м.\n"
            f"Ты на AIR. Если кто-то появится рядом — увидишь его после своей следующей геолокации.",
            reply_markup=menu_kb(),
        )
        db.inc("lists_shown", 1)
        db.event(user_id, "nearby_list_empty", str(radius_m))
        return

    lines = [f"Рядом с тобой (радиус {radius_m} м):"]
    inline_blocks: List[Tuple[str, InlineKeyboardMarkup]] = []
    for uid, dist_m, ulat, ulon in nearby[:10]:
        u = db.get_user(uid)
        if not u or not u[1] or not u[2]:
            continue
        lines.append(build_user_card_line(u[1], u[2], dist_m))
        inline_blocks.append((f"{u[1]} ({dist_m} м)", user_inline_map_kb(ulat, ulon)))

    await message.answer("\n".join(lines), reply_markup=menu_kb())
    db.inc("lists_shown", 1)
    db.event(user_id, "nearby_list_shown", f"count={len(nearby)};radius={radius_m}")

    # also send map links as separate messages (clean UX)
    for title_line, kb in inline_blocks:
        await message.answer(title_line, reply_markup=kb)

    # notify others nearby (simple ping)
    notified = 0
    for uid, dist_m, _, _ in nearby[:25]:
        # do not spam too hard
        u = db.get_user(uid)
        if not u:
            continue
        try:
            await bot.send_message(
                uid,
                f"🚬 AIR рядом!\n"
                f"{me_name} — {me_marker}\n"
                f"Дистанция: ~{dist_m} м\n\n"
                f"Чтобы увидеть точку и других — нажми «🚬 Выйти на AIR» и отправь геолокацию.",
                reply_markup=menu_kb(),
            )
            notified += 1
        except Exception:
            pass

    if notified > 0:
        db.inc("notifications_sent", notified)
        db.event(user_id, "notifications_sent", f"{notified}")

# =========================
# FALLBACK
# =========================
@dp.message()
async def fallback(message: Message):
    txt = clamp_text(message.text)
    # keep minimal
    if txt.lower() in {"меню", "menu"}:
        await message.answer("Меню.", reply_markup=menu_kb())
        return
    await message.answer("Не понял 🙂 Нажми кнопки в меню или /help", reply_markup=menu_kb())

# =========================
# MAIN
# =========================
async def main():
    # log event
    db.event(None, "bot_boot", f"env={ENV_NAME}")
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
