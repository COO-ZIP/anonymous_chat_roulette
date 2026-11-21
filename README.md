import asyncio
import logging
import random
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from aiogram import Bot, Dispatcher, F, types
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "chat_roulette_data"
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "database.db"
LOG_FILE = DATA_DIR / "bot.log"

BOT_TOKEN = "ВСТАВЬ_ТОКЕН_БОТА"
ADMIN_ID = 123456789

if not BOT_TOKEN or BOT_TOKEN == "ВСТАВЬ_ТОКЕН_БОТА":
    raise RuntimeError("Вставь токен бота в BOT_TOKEN")

MIN_AGE = 16
REPORT_LIMIT = 3
REPORT_WINDOW_HOURS = 24
MUTE_MINUTES = 60
MAX_MESSAGES_PER_MINUTE = 20
MAX_MESSAGES_PER_10_SECONDS = 8

BLOCKED_WORDS = {
    "примерзапрещенногослова",
}
LINK_RE = re.compile(r"(https?://|www\.|t\.me/|telegram\.me/)", re.IGNORECASE)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()],
)
log = logging.getLogger("anonymous_chat_roulette")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA foreign_keys = ON")
conn.execute("PRAGMA journal_mode = WAL")

queue_lock = asyncio.Lock()
search_queue: list[int] = []
active_chats: dict[int, int] = {}
chat_last_activity: dict[int, datetime] = {}
chat_started_at: dict[int, datetime] = {}
message_times: dict[int, list[datetime]] = {}


class AdminState(StatesGroup):
    waiting_broadcast = State()


class ReportState(StatesGroup):
    choosing_category = State()


def init_db() -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            referrer_id INTEGER,
            referrals_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS agreements (
            user_id INTEGER PRIMARY KEY,
            accepted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            age_confirmed INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(user_id) REFERENCES users(user_id)
        );

        CREATE TABLE IF NOT EXISTS user_stats (
            user_id INTEGER PRIMARY KEY,
            completed_chats INTEGER NOT NULL DEFAULT 0,
            total_seconds INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(user_id) REFERENCES users(user_id)
        );

        CREATE TABLE IF NOT EXISTS blocked_users (
            user_id INTEGER NOT NULL,
            blocked_id INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, blocked_id)
        );

        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reporter_id INTEGER NOT NULL,
            reported_id INTEGER NOT NULL,
            category TEXT NOT NULL DEFAULT 'не указана',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(reporter_id, reported_id, created_at)
        );

        CREATE TABLE IF NOT EXISTS bans (
            user_id INTEGER PRIMARY KEY,
            banned_until TEXT NOT NULL,
            reason TEXT NOT NULL DEFAULT 'административная блокировка',
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS moderation (
            user_id INTEGER PRIMARY KEY,
            muted_until TEXT,
            reason TEXT,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(reports)").fetchall()
    }
    if "category" not in columns:
        conn.execute("ALTER TABLE reports ADD COLUMN category TEXT NOT NULL DEFAULT 'не указана'")
    if "reported_by" not in columns:
        conn.execute("ALTER TABLE reports ADD COLUMN reported_by INTEGER")
        conn.execute("UPDATE reports SET reported_by = reporter_id WHERE reported_by IS NULL")
    if "timestamp" not in columns:
        conn.execute("ALTER TABLE reports ADD COLUMN timestamp TEXT")
        conn.execute("UPDATE reports SET timestamp = created_at WHERE timestamp IS NULL")

    conn.commit()


def add_user(user_id: int, referrer_id: int | None = None) -> bool:
    exists = conn.execute(
        "SELECT 1 FROM users WHERE user_id = ?", (user_id,)
    ).fetchone()
    if exists:
        return False

    if referrer_id == user_id:
        referrer_id = None
    if referrer_id is not None:
        ref_exists = conn.execute(
            "SELECT 1 FROM users WHERE user_id = ?", (referrer_id,)
        ).fetchone()
        if not ref_exists:
            referrer_id = None

    conn.execute(
        "INSERT INTO users (user_id, referrer_id) VALUES (?, ?)",
        (user_id, referrer_id),
    )
    if referrer_id is not None:
        conn.execute(
            "UPDATE users SET referrals_count = referrals_count + 1 WHERE user_id = ?",
            (referrer_id,),
        )
    conn.commit()
    return True


def has_agreement(user_id: int) -> bool:
    return conn.execute(
        "SELECT 1 FROM agreements WHERE user_id = ? AND age_confirmed = 1",
        (user_id,),
    ).fetchone() is not None


def save_agreement(user_id: int) -> None:
    conn.execute(
        """
        INSERT INTO agreements (user_id, age_confirmed)
        VALUES (?, 1)
        ON CONFLICT(user_id) DO UPDATE SET
            accepted_at = CURRENT_TIMESTAMP,
            age_confirmed = 1
        """,
        (user_id,),
    )
    conn.commit()


def is_blocked(user_id: int, other_id: int) -> bool:
    return conn.execute(
        "SELECT 1 FROM blocked_users WHERE user_id = ? AND blocked_id = ?",
        (user_id, other_id),
    ).fetchone() is not None


def block_user(user_id: int, other_id: int) -> None:
    if user_id != other_id:
        conn.execute(
            "INSERT OR IGNORE INTO blocked_users (user_id, blocked_id) VALUES (?, ?)",
            (user_id, other_id),
        )
        conn.commit()


def add_report(reporter_id: int, reported_id: int, category: str) -> int:
    existing = conn.execute(
        """
        SELECT 1 FROM reports
        WHERE reporter_id = ? AND reported_id = ?
          AND datetime(created_at) >= datetime('now', ?)
        """,
        (reporter_id, reported_id, f"-{REPORT_WINDOW_HOURS} hours"),
    ).fetchone()
    if not existing:
        conn.execute(
            """
            INSERT INTO reports (reporter_id, reported_id, category, reported_by, timestamp)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (reporter_id, reported_id, category, reporter_id),
        )
        conn.commit()

    row = conn.execute(
        """
        SELECT COUNT(DISTINCT reporter_id) AS total
        FROM reports
        WHERE reported_id = ?
          AND datetime(created_at) >= datetime('now', ?)
        """,
        (reported_id, f"-{REPORT_WINDOW_HOURS} hours"),
    ).fetchone()
    return int(row["total"])


def parse_duration(value: str) -> timedelta | None:
    """Поддерживает минуты (m), часы (h), дни (d), недели (w)."""
    match = re.fullmatch(r"([1-9][0-9]{0,4})(m|h|d|w)", value.lower())
    if not match:
        return None
    amount = int(match.group(1))
    unit = match.group(2)
    return {
        "m": timedelta(minutes=amount),
        "h": timedelta(hours=amount),
        "d": timedelta(days=amount),
        "w": timedelta(weeks=amount),
    }[unit]


def format_duration(value: str) -> str:
    return {"m": "минут", "h": "часов", "d": "дней", "w": "недель"}[value[-1]]


def is_banned(user_id: int) -> bool:
    row = conn.execute("SELECT banned_until FROM bans WHERE user_id = ?", (user_id,)).fetchone()
    if not row:
        return False
    try:
        until = datetime.fromisoformat(row["banned_until"])
    except ValueError:
        return False
    if until.tzinfo is None:
        until = until.replace(tzinfo=timezone.utc)
    if datetime.now(timezone.utc) >= until:
        conn.execute("DELETE FROM bans WHERE user_id = ?", (user_id,))
        conn.commit()
        return False
    return True


def ban_user(user_id: int, duration: timedelta, reason: str) -> datetime:
    until = datetime.now(timezone.utc) + duration
    conn.execute(
        """INSERT INTO bans (user_id, banned_until, reason, updated_at)
           VALUES (?, ?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(user_id) DO UPDATE SET banned_until=excluded.banned_until,
           reason=excluded.reason, updated_at=CURRENT_TIMESTAMP""",
        (user_id, until.isoformat(), reason),
    )
    conn.commit()
    return until


def is_muted(user_id: int) -> bool:
    row = conn.execute(
        "SELECT muted_until FROM moderation WHERE user_id = ?", (user_id,)
    ).fetchone()
    if not row or not row["muted_until"]:
        return False
    try:
        until = datetime.fromisoformat(row["muted_until"])
    except ValueError:
        return False
    if until.tzinfo is None:
        until = until.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) < until


def mute_user(user_id: int, minutes: int, reason: str) -> None:
    until = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    conn.execute(
        """
        INSERT INTO moderation (user_id, muted_until, reason, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(user_id) DO UPDATE SET
            muted_until = excluded.muted_until,
            reason = excluded.reason,
            updated_at = CURRENT_TIMESTAMP
        """,
        (user_id, until.isoformat(), reason),
    )
    conn.commit()


def main_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="🔍 Найти собеседника")],
        [
            KeyboardButton(text="👤 Профиль"),
            KeyboardButton(text="🔗 Пригласить друга"),
        ],
        [KeyboardButton(text="📜 Правила"), KeyboardButton(text="🔐 Приватность")],
    ]
    if user_id == ADMIN_ID:
        rows.append([KeyboardButton(text="⚙️ Админ-панель")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def search_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🚫 Отменить поиск")]],
        resize_keyboard=True,
    )


def chat_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➡️ Следующий")],
            [KeyboardButton(text="🎲 Тема для разговора")],
            [KeyboardButton(text="❌ Завершить диалог")],
            [
                KeyboardButton(text="🚩 Пожаловаться"),
                KeyboardButton(text="⛔ Заблокировать"),
            ],
        ],
        resize_keyboard=True,
    )


def report_keyboard() -> InlineKeyboardMarkup:
    categories = [
        ("report_sexual", "🔞 Сексуальные сообщения"),
        ("report_insult", "🤬 Оскорбления"),
        ("report_spam", "📨 Спам / реклама"),
        ("report_threat", "⚠️ Угрозы / другое"),
    ]
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=label, callback_data=data)] for data, label in categories
    ])


def agreement_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="✅ Мне есть 16 лет, принимаю правила",
                callback_data="accept_agreement",
            )]
        ]
    )


AGREEMENT_TEXT = (
    "📜 **Правила Анонимная чат-рулетка**\n\n"
    "Продолжая, вы подтверждаете, что вам исполнилось 16 лет.\n\n"
    "1. Запрещены угрозы, травля, оскорбления и преследование.\n"
    "2. Запрещены NSFW/сексуальные материалы, спам, реклама и подозрительные ссылки.\n"
    "3. Не отправляйте пароли, адрес, телефон, документы, геолокацию и другие личные данные.\n"
    "4. Не просите у собеседника личные данные и не пытайтесь установить его личность.\n"
    "5. Используйте «Пожаловаться» и «Заблокировать», если собеседник нарушает правила.\n"
    "6. За нарушения возможны временное ограничение доступа или блокировка.\n\n"
    "**Данные и работа сервиса**\n"
    "Для работы чата и модерации обрабатывается ваш Telegram ID. "
    "Текст переписки бот не сохраняет намеренно, однако сообщения передаются через Telegram. "
    "Жалобы и технические сведения могут храниться для безопасности сервиса.\n\n"
    "Сервис предоставляется «как есть». Собеседники — другие пользователи; "
    "администрация не может гарантировать их поведение или достоверность их слов. "
    "При этом администрация может рассматривать жалобы и применять меры модерации.\n\n"
    "Нажимая кнопку, вы подтверждаете возраст 16+ и согласие с правилами."
)


async def remove_from_queue(user_id: int) -> None:
    async with queue_lock:
        while user_id in search_queue:
            search_queue.remove(user_id)


async def finish_chat(user_id: int, notify: bool = True) -> bool:
    async with queue_lock:
        partner_id = active_chats.pop(user_id, None)
        if partner_id is None:
            return False
        active_chats.pop(partner_id, None)
        chat_last_activity.pop(user_id, None)
        chat_last_activity.pop(partner_id, None)
        started = chat_started_at.pop(user_id, None) or chat_started_at.pop(partner_id, None)
        chat_started_at.pop(partner_id, None)

    if started:
        elapsed = max(0, int((datetime.now(timezone.utc) - started).total_seconds()))
        for participant_id in (user_id, partner_id):
            conn.execute(
                "INSERT INTO user_stats (user_id, completed_chats, total_seconds) VALUES (?, 1, ?) "
                "ON CONFLICT(user_id) DO UPDATE SET completed_chats = completed_chats + 1, "
                "total_seconds = total_seconds + excluded.total_seconds",
                (participant_id, elapsed),
            )
        conn.commit()

    if notify:
        try:
            await bot.send_message(
                partner_id,
                "🛑 Собеседник завершил диалог.",
                reply_markup=main_keyboard(partner_id),
            )
        except TelegramAPIError:
            pass
    return True


async def send_pair_started(user_id: int, partner_id: int) -> None:
    now = datetime.now(timezone.utc)
    chat_last_activity[user_id] = now
    chat_last_activity[partner_id] = now
    chat_started_at[user_id] = now
    chat_started_at[partner_id] = now
    try:
        await bot.send_message(
            user_id, "✅ Собеседник найден! Общайтесь уважительно и не передавайте личные данные.",
            reply_markup=chat_keyboard(),
        )
        await bot.send_message(
            partner_id, "✅ Собеседник найден! Общайтесь уважительно и не передавайте личные данные.",
            reply_markup=chat_keyboard(),
        )
    except TelegramAPIError:
        await finish_chat(user_id, notify=False)


def check_flood(user_id: int) -> bool:
    now = datetime.now(timezone.utc)
    recent = [stamp for stamp in message_times.get(user_id, []) if now - stamp < timedelta(minutes=1)]
    last_10_seconds = [stamp for stamp in recent if now - stamp < timedelta(seconds=10)]
    if len(recent) >= MAX_MESSAGES_PER_MINUTE or len(last_10_seconds) >= MAX_MESSAGES_PER_10_SECONDS:
        message_times[user_id] = recent
        return False
    recent.append(now)
    message_times[user_id] = recent
    return True


def moderation_reason(message: types.Message) -> str | None:
    text = (message.text or message.caption or "").lower()
    if LINK_RE.search(text):
        return "ссылки запрещены"
    if any(word and word.lower() in text for word in BLOCKED_WORDS):
        return "сообщение содержит запрещённое слово"
    return None


async def ensure_agreement(message: types.Message) -> bool:
    user_id = message.from_user.id
    if has_agreement(user_id):
        return True
    await message.answer(
        "🔒 Чтобы пользоваться Анонимная чат-рулетка, подтвердите возраст 16+ "
        "и примите правила:",
        reply_markup=agreement_keyboard(),
    )
    return False


@dp.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext) -> None:
    await state.clear()
    user_id = message.from_user.id
    parts = (message.text or "").split(maxsplit=1)
    referrer_id = int(parts[1]) if len(parts) == 2 and parts[1].isdigit() else None

    is_new = add_user(user_id, referrer_id)
    if is_new and referrer_id and referrer_id != user_id:
        try:
            await bot.send_message(referrer_id, "🎉 По вашей ссылке зарегистрировался новый пользователь!")
        except TelegramAPIError:
            pass

    if not has_agreement(user_id):
        await message.answer(
            "👋 Добро пожаловать в Анонимная чат-рулетка!\n\n"
            "Сервис предназначен только для пользователей от 16 лет. "
            "Перед началом прочитайте правила и подтвердите возраст:",
            reply_markup=agreement_keyboard(),
        )
        await message.answer(AGREEMENT_TEXT, parse_mode="Markdown")
        return

    await message.answer(
        "👋 Добро пожаловать обратно в Анонимная чат-рулетка!\n"
        "Нажмите «Найти собеседника», чтобы начать.",
        reply_markup=main_keyboard(user_id),
    )


@dp.callback_query(F.data == "accept_agreement")
async def accept_agreement(callback: types.CallbackQuery) -> None:
    user_id = callback.from_user.id
    add_user(user_id)
    save_agreement(user_id)
    await callback.message.answer(
        "✅ Согласие сохранено. Добро пожаловать!",
        reply_markup=main_keyboard(user_id),
    )
    await callback.answer("Правила приняты")


@dp.message(Command("privacy"))
@dp.message(F.text == "🔐 Приватность")
async def privacy_info(message: types.Message) -> None:
    await message.answer(
        "🔐 Telegram ID обрабатывается для работы чата, жалоб и модерации. "
        "Текст переписки бот намеренно не записывает в базу данных. "
        "Не отправляйте личные данные. Подробнее — команда /rules."
    )


@dp.message(Command("rules"))
@dp.message(F.text == "📜 Правила")
async def rules_info(message: types.Message) -> None:
    await message.answer(AGREEMENT_TEXT, parse_mode="Markdown")


@dp.message(F.text == "🔍 Найти собеседника")
async def search_partner(message: types.Message) -> None:
    if not await ensure_agreement(message):
        return
    user_id = message.from_user.id
    if is_banned(user_id):
        await message.answer("⛔ Ваш аккаунт временно заблокирован в чат-рулетке.")
        return
    if is_muted(user_id):
        await message.answer("⏳ Для вашего аккаунта временно ограничен доступ к чату.")
        return

    add_user(user_id)
    async with queue_lock:
        if user_id in active_chats:
            await message.answer("Вы уже находитесь в диалоге.", reply_markup=chat_keyboard())
            return
        if user_id in search_queue:
            await message.answer("Вы уже в очереди. Ожидайте...", reply_markup=search_keyboard())
            return

        partner_id = None
        for candidate in list(search_queue):
            if candidate == user_id or candidate in active_chats:
                continue
            if is_muted(candidate):
                search_queue.remove(candidate)
                continue
            if is_blocked(user_id, candidate) or is_blocked(candidate, user_id):
                continue
            partner_id = candidate
            search_queue.remove(candidate)
            break

        if partner_id is None:
            search_queue.append(user_id)
        else:
            active_chats[user_id] = partner_id
            active_chats[partner_id] = user_id

    if partner_id is None:
        await message.answer("🔎 Ищем свободного собеседника...", reply_markup=search_keyboard())
    else:
        await send_pair_started(user_id, partner_id)


@dp.message(F.text == "🚫 Отменить поиск")
async def cancel_search(message: types.Message) -> None:
    await remove_from_queue(message.from_user.id)
    await message.answer("Поиск остановлен.", reply_markup=main_keyboard(message.from_user.id))


@dp.message(F.text == "➡️ Следующий")
async def next_partner(message: types.Message) -> None:
    user_id = message.from_user.id
    if active_chats.get(user_id) is None:
        await message.answer("У вас нет активного диалога.", reply_markup=main_keyboard(user_id))
        return
    await finish_chat(user_id)
    await message.answer("🔎 Ищем нового собеседника...")
    await search_partner(message)


@dp.message(F.text == "❌ Завершить диалог")
async def stop_chat(message: types.Message) -> None:
    user_id = message.from_user.id
    ended = await finish_chat(user_id)
    await message.answer(
        "🛑 Диалог завершён." if ended else "У вас нет активного диалога.",
        reply_markup=main_keyboard(user_id),
    )


@dp.message(F.text == "⛔ Заблокировать")
async def block_partner(message: types.Message) -> None:
    user_id = message.from_user.id
    partner_id = active_chats.get(user_id)
    if partner_id is None:
        await message.answer("Сейчас у вас нет собеседника.", reply_markup=main_keyboard(user_id))
        return
    block_user(user_id, partner_id)
    await finish_chat(user_id)
    await message.answer("Пользователь заблокирован и больше не будет подобран вам.", reply_markup=main_keyboard(user_id))


@dp.message(F.text == "🚩 Пожаловаться")
async def report_partner(message: types.Message, state: FSMContext) -> None:
    user_id = message.from_user.id
    partner_id = active_chats.get(user_id)
    if partner_id is None:
        await message.answer("Сейчас у вас нет собеседника.", reply_markup=main_keyboard(user_id))
        return
    await state.update_data(reported_id=partner_id)
    await state.set_state(ReportState.choosing_category)
    await message.answer("Выберите причину жалобы:", reply_markup=report_keyboard())


@dp.callback_query(ReportState.choosing_category, F.data.startswith("report_"))
async def submit_report(callback: types.CallbackQuery, state: FSMContext) -> None:
    user_id = callback.from_user.id
    data = await state.get_data()
    partner_id = data.get("reported_id")
    if not partner_id or active_chats.get(user_id) != partner_id:
        await state.clear()
        await callback.answer("Диалог уже завершён", show_alert=True)
        return

    category_map = {
        "report_sexual": "сексуальные сообщения",
        "report_insult": "оскорбления",
        "report_spam": "спам / реклама",
        "report_threat": "угрозы / другое",
    }
    category = category_map.get(callback.data)
    if not category:
        await callback.answer("Неизвестная категория", show_alert=True)
        return

    total = add_report(user_id, partner_id, category)
    log.warning("Report: reporter=%s reported=%s category=%s total=%s", user_id, partner_id, category, total)
    try:
        await bot.send_message(
            ADMIN_ID,
            f"🚩 Новая жалоба\nКатегория: {category}\nНа пользователя: {partner_id}\n"
            f"От пользователя: {user_id}\nЖалоб за {REPORT_WINDOW_HOURS} ч.: {total}",
        )
    except TelegramAPIError:
        log.exception("Could not notify admin about report")

    if total >= REPORT_LIMIT:
        mute_user(partner_id, MUTE_MINUTES, "порог независимых жалоб")
        await remove_from_queue(partner_id)
        await finish_chat(partner_id, notify=True)
        try:
            await bot.send_message(
                partner_id,
                f"⏳ Доступ к чату временно ограничен на {MUTE_MINUTES} минут из-за нескольких жалоб. "
                "Администрация проверит ситуацию.",
                reply_markup=main_keyboard(partner_id),
            )
        except TelegramAPIError:
            pass

    await state.clear()
    await finish_chat(user_id)
    await callback.message.answer("✅ Жалоба отправлена администрации. Диалог завершён.", reply_markup=main_keyboard(user_id))
    await callback.answer("Жалоба отправлена")


@dp.message(F.text == "🔗 Пригласить друга")
async def referral_link(message: types.Message) -> None:
    me = await bot.get_me()
    await message.answer(f"🔗 Ваша ссылка:\nhttps://t.me/{me.username}?start={message.from_user.id}")


@dp.message(F.text == "👤 Профиль")
async def user_profile(message: types.Message) -> None:
    user_id = message.from_user.id
    row = conn.execute(
        "SELECT referrals_count, created_at FROM users WHERE user_id = ?", (user_id,)
    ).fetchone()
    refs = row["referrals_count"] if row else 0
    created = row["created_at"] if row else "неизвестно"
    stats = conn.execute(
        "SELECT completed_chats, total_seconds FROM user_stats WHERE user_id = ?", (user_id,)
    ).fetchone()
    completed_chats = stats["completed_chats"] if stats else 0
    total_seconds = stats["total_seconds"] if stats else 0
    total_minutes = total_seconds // 60
    hours, minutes = divmod(total_minutes, 60)
    duration_text = f"{hours} ч. {minutes} мин." if hours else f"{minutes} мин."
    await message.answer(
        f"👤 Профиль\n\n🆔 ID: {user_id}\n👥 Приглашено: {refs}\n"
        f"💬 Завершённых диалогов: {completed_chats}\n⏱️ Время общения: {duration_text}\n"
        f"📅 В боте с: {created}"
    )


@dp.message(Command("ban"))
async def admin_ban(message: types.Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    parts = (message.text or "").split()
    if len(parts) < 3 or not parts[2].isdigit():
        await message.answer("Формат: /ban 2d 123456789 причина (m — минуты, h — часы, d — дни, w — недели)")
        return
    duration = parse_duration(parts[1])
    target_id = int(parts[2])
    if duration is None or target_id == ADMIN_ID:
        await message.answer("Проверьте время и ID. Пример: /ban 2d 123456789")
        return
    reason = " ".join(parts[3:]) or "нарушение правил"
    until = ban_user(target_id, duration, reason)
    await remove_from_queue(target_id)
    await finish_chat(target_id)
    try:
        await bot.send_message(target_id, f"⛔ Вы заблокированы в чат-рулетке на {parts[1]} ({format_duration(parts[1])}).\nПричина: {reason}")
    except TelegramAPIError:
        pass
    await message.answer(f"✅ Пользователь {target_id} заблокирован до {until.strftime('%Y-%m-%d %H:%M UTC')}.")


@dp.message(Command("mute"))
async def admin_mute(message: types.Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    parts = (message.text or "").split()
    if len(parts) < 3 or not parts[2].isdigit():
        await message.answer("Формат: /mute 30m 123456789 причина (m — минуты, h — часы, d — дни, w — недели)")
        return
    duration = parse_duration(parts[1])
    target_id = int(parts[2])
    if duration is None or target_id == ADMIN_ID:
        await message.answer("Проверьте время и ID. Пример: /mute 30m 123456789")
        return
    reason = " ".join(parts[3:]) or "нарушение правил"
    mute_user(target_id, int(duration.total_seconds() // 60), reason)
    await remove_from_queue(target_id)
    await finish_chat(target_id)
    try:
        await bot.send_message(target_id, f"🔇 Вам выдан мут в чат-рулетке на {parts[1]} ({format_duration(parts[1])}).\nПричина: {reason}")
    except TelegramAPIError:
        pass
    await message.answer(f"✅ Пользователь {target_id} замьючен на {parts[1]}.")


@dp.message(F.text == "⚙️ Админ-панель")
async def admin_panel(message: types.Message) -> None:
    if message.from_user.id != ADMIN_ID:
        return
    total = conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]
    reports = conn.execute("SELECT COUNT(*) AS n FROM reports").fetchone()["n"]
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
            [InlineKeyboardButton(text="📄 Скачать техлог", callback_data="admin_logs")],
        ]
    )
    await message.answer(
        f"⚙️ Админ-панель\n\n👥 Пользователей: {total}\n"
        f"🔎 В очереди: {len(search_queue)}\n"
        f"💬 Активных диалогов: {len(active_chats) // 2}\n"
        f"🚩 Жалоб: {reports}",
        reply_markup=keyboard,
    )


@dp.callback_query(F.data == "admin_logs")
async def admin_send_logs(callback: types.CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет доступа", show_alert=True)
        return
    if LOG_FILE.exists() and LOG_FILE.stat().st_size:
        await callback.message.answer_document(types.FSInputFile(LOG_FILE))
    else:
        await callback.message.answer("Технический лог пока пуст.")
    await callback.answer()


@dp.callback_query(F.data == "admin_broadcast")
async def admin_start_broadcast(callback: types.CallbackQuery, state: FSMContext) -> None:
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(AdminState.waiting_broadcast)
    await callback.message.answer("Отправьте сообщение для рассылки. Для отмены: /cancel")
    await callback.answer()


@dp.message(AdminState.waiting_broadcast, F.text == "/cancel")
async def admin_cancel_broadcast(message: types.Message, state: FSMContext) -> None:
    if message.from_user.id == ADMIN_ID:
        await state.clear()
        await message.answer("Рассылка отменена.", reply_markup=main_keyboard(ADMIN_ID))


@dp.message(AdminState.waiting_broadcast)
async def admin_process_broadcast(message: types.Message, state: FSMContext) -> None:
    if message.from_user.id != ADMIN_ID:
        await state.clear()
        return

    await state.clear()
    rows = conn.execute("SELECT user_id FROM users").fetchall()
    status = await message.answer(f"⏳ Начинаю рассылку: {len(rows)} получателей...")
    sent = blocked = failed = 0
    for row in rows:
        try:
            await message.copy_to(chat_id=row["user_id"])
            sent += 1
            await asyncio.sleep(0.08)
        except TelegramRetryAfter as exc:
            await asyncio.sleep(exc.retry_after + 1)
            try:
                await message.copy_to(chat_id=row["user_id"])
                sent += 1
            except TelegramForbiddenError:
                blocked += 1
            except TelegramAPIError:
                failed += 1
        except TelegramForbiddenError:
            blocked += 1
        except TelegramAPIError:
            failed += 1

    await status.edit_text(
        f"✅ Рассылка завершена\nДоставлено: {sent}\nБот заблокирован: {blocked}\nОшибок: {failed}"
    )


CONVERSATION_TOPICS = [
    "Какую игру ты можешь рекомендовать?",
    "Какую музыку чаще всего слушаешь?",
    "Если бы можно было посетить любой город, какой бы выбрал?",
    "Какое новое хобби хотелось бы попробовать?",
    "Какой фильм или сериал тебе запомнился?",
    "Что интересного ты узнал недавно?",
    "Как выглядит твой идеальный выходной?",
    "Какую суперспособность выбрал бы — и почему?",
    "Какую необычную еду хотелось бы попробовать?",
    "Что тебе больше нравится: море, горы или большой город?",
]


@dp.message(F.text == "🎲 Тема для разговора")
async def conversation_topic(message: types.Message) -> None:
    user_id = message.from_user.id
    partner_id = active_chats.get(user_id)
    if partner_id is None:
        await message.answer("Эта кнопка работает во время диалога.", reply_markup=main_keyboard(user_id))
        return
    topic = random.choice(CONVERSATION_TOPICS)
    now = datetime.now(timezone.utc)
    chat_last_activity[user_id] = now
    chat_last_activity[partner_id] = now
    prompt = f"🎲 Тема для разговора:\n{topic}"
    try:
        await bot.send_message(user_id, prompt, reply_markup=chat_keyboard())
        await bot.send_message(partner_id, prompt, reply_markup=chat_keyboard())
    except TelegramAPIError:
        await finish_chat(user_id, notify=False)
        await message.answer("Собеседник недоступен. Диалог завершён.", reply_markup=main_keyboard(user_id))


@dp.message()
async def relay_messages(message: types.Message) -> None:
    if message.from_user is None:
        return
    user_id = message.from_user.id
    if is_banned(user_id):
        await message.answer("⛔ Ваш аккаунт временно заблокирован в чат-рулетке.")
        return
    if not await ensure_agreement(message):
        return
    if is_muted(user_id):
        await message.answer("⏳ Вам временно ограничена отправка сообщений.")
        return

    partner_id = active_chats.get(user_id)
    if partner_id is None:
        if user_id in search_queue:
            await message.answer("Вы пока в поиске. Отмените поиск или дождитесь собеседника.")
        else:
            await message.answer(
                "Нажмите «🔍 Найти собеседника», чтобы начать общение.",
                reply_markup=main_keyboard(user_id),
            )
        return

    reason = moderation_reason(message)
    if reason:
        await message.answer(f"⚠️ Сообщение не отправлено: {reason}.")
        return

    if not check_flood(user_id):
        await message.answer("🐢 Слишком часто. Подождите минуту и продолжайте спокойнее.")
        return

    try:
        now = datetime.now(timezone.utc)
        chat_last_activity[user_id] = now
        chat_last_activity[partner_id] = now
        await message.copy_to(chat_id=partner_id)
    except TelegramForbiddenError:
        await finish_chat(user_id, notify=False)
        await message.answer("Собеседник недоступен. Диалог завершён.", reply_markup=main_keyboard(user_id))
    except TelegramAPIError:
        await message.answer("⚠️ Не удалось отправить сообщение. Попробуйте другой тип сообщения.")


async def idle_chat_watcher() -> None:
    while True:
        await asyncio.sleep(10)
        now = datetime.now(timezone.utc)
        checked_pairs: set[tuple[int, int]] = set()
        for user_id, partner_id in list(active_chats.items()):
            pair = tuple(sorted((user_id, partner_id)))
            if pair in checked_pairs:
                continue
            checked_pairs.add(pair)
            last_activity = chat_last_activity.get(user_id) or chat_last_activity.get(partner_id)
            if last_activity and now - last_activity >= timedelta(minutes=3):
                ended = await finish_chat(user_id, notify=False)
                if ended:
                    for participant_id in pair:
                        try:
                            await bot.send_message(
                                participant_id,
                                "⌛ Диалог автоматически завершён: в течение 3 минут не было сообщений.",
                                reply_markup=main_keyboard(participant_id),
                            )
                        except TelegramAPIError:
                            pass


async def main() -> None:
    init_db()
    log.info("Anonymous chat roulette started")
    watcher_task = asyncio.create_task(idle_chat_watcher())
    try:
        await dp.start_polling(bot)
    finally:
        watcher_task.cancel()
        try:
            await watcher_task
        except asyncio.CancelledError:
            pass
        await bot.session.close()
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
