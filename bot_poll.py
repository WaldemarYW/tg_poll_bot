import asyncio
import logging
import os
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import aiosqlite
from aiogram import Bot, Dispatcher, F, types
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import BaseFilter, Command, CommandStart
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)
from dotenv import load_dotenv
from html import escape
from google_sheets_logger import SheetsReferralEvent, SheetsReferralLogger, NO_NOTE_KEY


# Загружаем переменные из .env (файл должен лежать рядом с bot_poll.py)
load_dotenv()

API_TOKEN = os.getenv("TELEGRAM_API_TOKEN")
MESSAGE_DELAY = 1
DB_PATH = Path(__file__).with_name("bot_data.db")
BOT_USERNAME: Optional[str] = None
NOTE_CREATION_STATE: Dict[int, Dict[str, Optional[str]]] = {}
GOOGLE_SHEETS_SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or os.getenv("GOOGLE_CREDS")
GOOGLE_SHEETS_ENABLED = os.getenv("GOOGLE_SHEETS_ENABLED", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
GOOGLE_SHEETS_TIMEOUT_SEC_RAW = os.getenv("GOOGLE_SHEETS_TIMEOUT_SEC", "5")
try:
    GOOGLE_SHEETS_TIMEOUT_SEC = float(GOOGLE_SHEETS_TIMEOUT_SEC_RAW)
except ValueError:
    GOOGLE_SHEETS_TIMEOUT_SEC = 5.0

SHEETS_LOGGER = SheetsReferralLogger(
    enabled=GOOGLE_SHEETS_ENABLED,
    spreadsheet_id=GOOGLE_SHEETS_SPREADSHEET_ID,
    service_account_json=GOOGLE_SERVICE_ACCOUNT_JSON,
    timeout_sec=GOOGLE_SHEETS_TIMEOUT_SEC,
)

AGE_OPTIONS: Dict[str, str] = {
    "16-24": "16-24",
    "25-30": "25-30",
    "31-40": "31-40",
    "41_plus": "41+",
}

INCOME_OPTIONS: Dict[str, str] = {
    "10-20": "10-20 тис",
    "20-30": "20-30 тис",
    "30-50": "30-50 тис",
    "50+": "50+ тис",
}

DEVICE_OPTIONS: Dict[str, str] = {
    "poll_device_yes": "Так, є",
    "poll_device_no": "Ні, немає",
}

REMINDER_TASKS: Dict[int, asyncio.Task] = {}
REMINDER_EDITORS: set[int] = set()
PHONE_CONTACT_WAITERS: set[int] = set()
PHONE_CONTACT_TIMEOUT_TASKS: Dict[int, asyncio.Task] = {}
CONTACT_WRITE_TEXT = "Написати"
PHONE_CONTACT_TIMEOUT_SEC = 300
DEFAULT_REMINDER_TEXT = (
    "Ти вже сьогодні зможеш, пройти навчання та отримати перші кошти, "
    "навчання багато часу не займе - пиши менеджеру Володимиру👇\n"
    "@hr_volodymyr"
)


class PendingNoteCreationFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return message.from_user.id in NOTE_CREATION_STATE


class ReminderEditFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return message.from_user.id in REMINDER_EDITORS


class PendingPhoneContactFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return message.from_user.id in PHONE_CONTACT_WAITERS


def set_phone_contact_waiter(user_id: int) -> None:
    PHONE_CONTACT_WAITERS.add(user_id)


def clear_phone_contact_waiter(user_id: int) -> None:
    PHONE_CONTACT_WAITERS.discard(user_id)


def cancel_phone_contact_timeout(user_id: int) -> None:
    task = PHONE_CONTACT_TIMEOUT_TASKS.pop(user_id, None)
    if task and not task.done():
        task.cancel()


async def send_with_delay(
    send_method,
    *args,
    delay: float = MESSAGE_DELAY,
    skip_delay: bool = False,
    **kwargs,
):
    """
    Универсальный помощник: мгновенно отправляет первое сообщение, но добавляет
    паузу перед повторными ответами, если skip_delay=False.
    """
    if not skip_delay:
        await asyncio.sleep(delay)
    return await send_method(*args, **kwargs)


def build_start_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Написати менеджеру👨🏻‍💻",
                    callback_data="contact_manager"
                )
            ],
            [
                InlineKeyboardButton(
                    text="Пройти опитування⚡️",
                    callback_data="start_poll"
                )
            ]
        ]
    )


def build_manager_button() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Написати Володимиру✅",
                    url="https://t.me/hr_volodymyr?text=%2B",
                )
            ]
        ]
    )


def build_contact_request_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Поділитися номером", request_contact=True)],
            [KeyboardButton(text=CONTACT_WRITE_TEXT)],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def is_valid_note_url(value: str) -> bool:
    raw = (value or "").strip()
    if not raw:
        return False

    parsed = urlparse(raw)
    if parsed.scheme in {"http", "https"}:
        return bool(parsed.netloc)
    if parsed.scheme == "tg":
        return bool(parsed.path or parsed.netloc)
    return False


async def get_bot_username(bot: Bot) -> str:
    global BOT_USERNAME
    if BOT_USERNAME:
        return BOT_USERNAME

    me = await bot.get_me()
    BOT_USERNAME = me.username or ""
    return BOT_USERNAME


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS groups (
                chat_id INTEGER PRIMARY KEY,
                title TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS referral_settings (
                user_id INTEGER PRIMARY KEY,
                group_id INTEGER,
                FOREIGN KEY(group_id) REFERENCES groups(chat_id)
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS referral_clicks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referred_user_id INTEGER,
                timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                note_id INTEGER,
                group_id INTEGER
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS poll_responses (
                user_id INTEGER PRIMARY KEY,
                referrer_id INTEGER,
                note_id INTEGER,
                age TEXT,
                income TEXT,
                device TEXT,
                notified INTEGER DEFAULT 0,
                reminder_sent INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER NOT NULL,
                group_id INTEGER,
                title TEXT NOT NULL,
                url TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS note_clicks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                note_id INTEGER NOT NULL,
                user_id INTEGER,
                timestamp TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        # Legacy migration: old schema had UNIQUE(referrer_id, referred_user_id),
        # which blocks registering the same user for different notes.
        async with db.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'referral_clicks'"
        ) as cursor:
            row = await cursor.fetchone()
            referral_clicks_sql = row[0] if row and row[0] else ""

        if "UNIQUE(referrer_id, referred_user_id)" in referral_clicks_sql:
            await db.execute("ALTER TABLE referral_clicks RENAME TO referral_clicks_old")
            await db.execute(
                """
                CREATE TABLE referral_clicks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    referrer_id INTEGER,
                    referred_user_id INTEGER,
                    timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                    note_id INTEGER,
                    group_id INTEGER
                )
                """
            )
            old_columns: set[str] = set()
            async with db.execute("PRAGMA table_info(referral_clicks_old)") as cursor:
                async for col_row in cursor:
                    old_columns.add(col_row[1])
            note_expr = "note_id" if "note_id" in old_columns else "NULL AS note_id"
            group_expr = "group_id" if "group_id" in old_columns else "NULL AS group_id"
            await db.execute(
                f"""
                INSERT INTO referral_clicks (id, referrer_id, referred_user_id, timestamp, note_id, group_id)
                SELECT id, referrer_id, referred_user_id, timestamp, {note_expr}, {group_expr}
                FROM referral_clicks_old
                """
            )
            await db.execute("DROP TABLE referral_clicks_old")

        try:
            await db.execute("ALTER TABLE referral_clicks ADD COLUMN note_id INTEGER")
        except sqlite3.OperationalError:
            pass
        try:
            await db.execute("ALTER TABLE poll_responses ADD COLUMN note_id INTEGER")
        except sqlite3.OperationalError:
            pass
        try:
            await db.execute("ALTER TABLE poll_responses ADD COLUMN reminder_sent INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        try:
            await db.execute("ALTER TABLE notes ADD COLUMN group_id INTEGER")
        except sqlite3.OperationalError:
            pass
        try:
            await db.execute("ALTER TABLE referral_clicks ADD COLUMN group_id INTEGER")
        except sqlite3.OperationalError:
            pass
        await db.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_referral_clicks_ref_referred_note
            ON referral_clicks (referrer_id, referred_user_id, IFNULL(note_id, -1))
            """
        )
        try:
            await db.execute("ALTER TABLE poll_responses ADD COLUMN group_id INTEGER")
        except sqlite3.OperationalError:
            pass
        try:
            await db.execute("ALTER TABLE poll_responses ADD COLUMN phone_number TEXT")
        except sqlite3.OperationalError:
            pass
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS clean_launch_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                group_id INTEGER
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS reminder_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                text TEXT NOT NULL
            )
            """
        )
        await db.execute(
            """
            INSERT OR IGNORE INTO reminder_settings (id, text) VALUES (1, ?)
            """,
            (DEFAULT_REMINDER_TEXT,),
        )
        await db.commit()


async def upsert_user(user: types.User):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO users (user_id, username, first_name, last_name)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                last_name=excluded.last_name
            """,
            (user.id, user.username, user.first_name, user.last_name),
        )
        await db.commit()


async def save_group(chat: types.Chat):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO groups (chat_id, title)
            VALUES (?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                title = excluded.title
            """,
            (chat.id, chat.title or "Без назви"),
        )
        await db.commit()


async def fetch_groups() -> List[Tuple[int, str]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT chat_id, title FROM groups ORDER BY title") as cursor:
            rows = await cursor.fetchall()
            return [(row["chat_id"], row["title"]) for row in rows]


async def fetch_group_info(group_id: int) -> Optional[Tuple[int, str]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT chat_id, title FROM groups WHERE chat_id = ?",
            (group_id,),
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return row["chat_id"], row["title"]
    return None


async def fetch_notes(owner_id: int, group_id: int, viewer_id: Optional[int] = None) -> List[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        query = """
            SELECT * FROM notes
            WHERE owner_id = ? AND group_id = ?
        """
        params: List[int] = [owner_id, group_id]

        if viewer_id is not None:
            query += " AND owner_id = ?"
            params.append(viewer_id)

        query += " ORDER BY created_at DESC"

        async with db.execute(query, params) as cursor:
            return await cursor.fetchall()


async def fetch_note(note_id: int) -> Optional[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM notes WHERE id = ?", (note_id,)) as cursor:
            return await cursor.fetchone()


async def create_note(owner_id: int, group_id: int, title: str, url: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "INSERT INTO notes (owner_id, group_id, title, url) VALUES (?, ?, ?, ?)",
            (owner_id, group_id, title, url),
        )
        await db.commit()
        return cursor.lastrowid


async def delete_note(owner_id: int, note_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "DELETE FROM notes WHERE id = ? AND owner_id = ?",
            (note_id, owner_id),
        )
        await db.commit()
        return cursor.rowcount > 0


async def count_note_clicks(note_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM note_clicks WHERE note_id = ?",
            (note_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0


async def set_clean_launch_group(group_id: int):
    await set_user_group(0, group_id)


async def get_clean_launch_group() -> Optional[Tuple[int, str]]:
    return await get_user_group(0)


async def get_display_clean_group(user_id: int) -> Optional[Tuple[int, str]]:
    clean_group = await get_clean_launch_group()
    if clean_group:
        return clean_group
    return await get_user_group(user_id)


async def get_clean_launch_group_id() -> Optional[int]:
    group = await get_clean_launch_group()
    return group[0] if group else None


async def set_user_group(user_id: int, group_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO referral_settings (user_id, group_id)
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET group_id = excluded.group_id
            """,
            (user_id, group_id),
        )
        await db.commit()


async def get_user_group(user_id: int) -> Optional[Tuple[int, str]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT referral_settings.group_id, groups.title
            FROM referral_settings
            JOIN groups ON groups.chat_id = referral_settings.group_id
            WHERE referral_settings.user_id = ?
            """,
            (user_id,),
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return row["group_id"], row["title"]
    return None


async def record_referral_click(
    referrer_id: int,
    referred_user_id: int,
    note_id: Optional[int] = None,
    group_id: Optional[int] = None,
) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            INSERT OR IGNORE INTO referral_clicks (referrer_id, referred_user_id, note_id, group_id)
            VALUES (?, ?, ?, ?)
            """,
            (referrer_id, referred_user_id, note_id, group_id),
        )
        await db.commit()
        return cursor.rowcount > 0


async def record_note_click(note_id: int, user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO note_clicks (note_id, user_id)
            VALUES (?, ?)
            """,
            (note_id, user_id),
        )
        await db.commit()


async def ensure_poll_row(
    user_id: int,
    referrer_id: Optional[int] = None,
    note_id: Optional[int] = None,
    group_id: Optional[int] = None,
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO poll_responses (user_id, referrer_id, note_id, group_id)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                referrer_id = COALESCE(poll_responses.referrer_id, excluded.referrer_id),
                note_id = CASE
                    WHEN excluded.note_id IS NOT NULL THEN excluded.note_id
                    ELSE poll_responses.note_id
                END,
                group_id = CASE
                    WHEN excluded.group_id IS NOT NULL THEN excluded.group_id
                    ELSE poll_responses.group_id
                END
            """,
            (user_id, referrer_id, note_id, group_id),
        )
        await db.commit()


async def update_poll_response(
    user_id: int,
    *,
    age: Optional[str] = None,
    income: Optional[str] = None,
    device: Optional[str] = None,
    phone_number: Optional[str] = None,
):
    referrer_id = await get_referrer_id(user_id)
    await ensure_poll_row(user_id, referrer_id)

    updates = []
    params: List[str] = []
    if age is not None:
        updates.append("age = ?")
        params.append(age)
    if income is not None:
        updates.append("income = ?")
        params.append(income)
    if device is not None:
        updates.append("device = ?")
        params.append(device)
    if phone_number is not None:
        updates.append("phone_number = ?")
        params.append(phone_number)

    if not updates:
        return

    updates.append("updated_at = CURRENT_TIMESTAMP")
    # Do not reset `notified` when device is selected again, to avoid
    # duplicate group notifications on repeated callback presses.
    if device is None:
        updates.append("notified = 0")
    params.append(user_id)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"UPDATE poll_responses SET {', '.join(updates)} WHERE user_id = ?",
            params,
        )
        await db.commit()


async def fetch_poll_response(user_id: int) -> Optional[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT pr.*, u.username, u.first_name, u.last_name
            FROM poll_responses pr
            LEFT JOIN users u ON pr.user_id = u.user_id
            WHERE pr.user_id = ?
            """,
            (user_id,),
        ) as cursor:
            return await cursor.fetchone()


async def get_referrer_id(user_id: int) -> Optional[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT referrer_id FROM poll_responses WHERE user_id = ?",
            (user_id,),
        ) as cursor:
            row = await cursor.fetchone()
            if row and row[0]:
                return row[0]

        async with db.execute(
            "SELECT referrer_id FROM referral_clicks WHERE referred_user_id = ?",
            (user_id,),
        ) as cursor:
            row = await cursor.fetchone()
            if row and row[0]:
                return row[0]
    return None


async def get_referral_stats(user_id: int) -> Dict[str, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT COUNT(*) as cnt FROM referral_clicks WHERE referrer_id = ?",
            (user_id,),
        ) as cursor:
            row_clicks = await cursor.fetchone()
            clicks = row_clicks["cnt"] if row_clicks else 0

        async with db.execute(
            """
            SELECT COUNT(*) as completed
            FROM poll_responses
            WHERE referrer_id = ? AND device IS NOT NULL
            """,
            (user_id,),
        ) as cursor:
            row = await cursor.fetchone()

    completed = row["completed"] if row and row["completed"] else 0

    return {
        "clicks": clicks,
        "completed": completed,
    }


async def get_group_referral_stats(user_id: int, group_id: int) -> Dict[str, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT COUNT(*) as cnt
            FROM referral_clicks
            WHERE referrer_id = ? AND group_id = ?
            """,
            (user_id, group_id),
        ) as cursor:
            row_clicks = await cursor.fetchone()
            clicks = row_clicks["cnt"] if row_clicks else 0

        async with db.execute(
            """
            SELECT COUNT(*) as completed
            FROM poll_responses
            WHERE referrer_id = ? AND group_id = ? AND device IS NOT NULL
            """,
            (user_id, group_id),
        ) as cursor:
            row = await cursor.fetchone()

    completed = row["completed"] if row and row["completed"] else 0
    return {
        "clicks": clicks,
        "completed": completed,
    }


async def get_clean_launch_stats() -> Dict[str, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT COUNT(*) as total FROM poll_responses WHERE referrer_id IS NULL",
        ) as cursor:
            row_total = await cursor.fetchone()
            total = row_total["total"] if row_total else 0

        async with db.execute(
            """
            SELECT COUNT(*) as completed
            FROM poll_responses
            WHERE referrer_id IS NULL AND device IS NOT NULL
            """,
        ) as cursor:
            row_completed = await cursor.fetchone()
            completed = row_completed["completed"] if row_completed else 0

    return {"total": total, "completed": completed}


async def was_notified(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT notified FROM poll_responses WHERE user_id = ?",
            (user_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return bool(row and row[0])


async def try_claim_notification(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            UPDATE poll_responses
            SET notified = 1
            WHERE user_id = ? AND notified = 0 AND device IS NOT NULL
            """,
            (user_id,),
        )
        await db.commit()
        return cursor.rowcount > 0


async def mark_notified(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE poll_responses SET notified = 1 WHERE user_id = ?",
            (user_id,),
        )
        await db.commit()


async def unmark_notified(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE poll_responses SET notified = 0 WHERE user_id = ?",
            (user_id,),
        )
        await db.commit()


async def mark_reminder_sent(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE poll_responses SET reminder_sent = 1 WHERE user_id = ?",
            (user_id,),
        )
        await db.commit()


async def reset_reminder_sent(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE poll_responses SET reminder_sent = 0 WHERE user_id = ?",
            (user_id,),
        )
        await db.commit()


async def get_reminder_text() -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT text FROM reminder_settings WHERE id = 1") as cursor:
            row = await cursor.fetchone()
            return row[0] if row and row[0] else DEFAULT_REMINDER_TEXT


async def set_reminder_text(new_text: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE reminder_settings SET text = ? WHERE id = 1",
            (new_text,),
        )
        await db.commit()


def cancel_reminder_task(user_id: int):
    task = REMINDER_TASKS.pop(user_id, None)
    if task and not task.done():
        task.cancel()


async def remove_contact_keyboard_to_chat(
    bot: Bot,
    chat_id: int,
    text: str = "Добре.",
) -> None:
    await bot.send_message(chat_id, text, reply_markup=ReplyKeyboardRemove())


async def send_manager_contact_to_chat(
    bot: Bot,
    chat_id: int,
    skip_delay: bool = False,
):
    await send_with_delay(
        bot.send_message,
        chat_id=chat_id,
        text=(
            "Надаю вам контакт менеджера Володимира - @hr_volodymyr🧑🏻‍💻 "
            "Відправ йому «+» і він розповість вам про роботу, та буде допомагати в подальшому!🚀"
        ),
        reply_markup=build_manager_button(),
        skip_delay=skip_delay,
    )


async def finalize_phone_contact_wait(
    *,
    bot: Bot,
    user_id: int,
    chat_id: int,
    send_manager: bool = True,
    remove_keyboard_text: Optional[str] = "Добре.",
) -> None:
    cancel_phone_contact_timeout(user_id)
    clear_phone_contact_waiter(user_id)
    if remove_keyboard_text is not None:
        await remove_contact_keyboard_to_chat(bot, chat_id, text=remove_keyboard_text)
    await notify_group_about_poll(bot, user_id)
    if send_manager:
        await send_manager_contact_to_chat(bot, chat_id, skip_delay=True)


async def schedule_phone_contact_timeout(bot: Bot, user_id: int, chat_id: int):
    cancel_phone_contact_timeout(user_id)

    async def timeout_worker():
        try:
            await asyncio.sleep(PHONE_CONTACT_TIMEOUT_SEC)
            if user_id not in PHONE_CONTACT_WAITERS:
                return
            await finalize_phone_contact_wait(
                bot=bot,
                user_id=user_id,
                chat_id=chat_id,
                send_manager=True,
                remove_keyboard_text="Добре.",
            )
        except asyncio.CancelledError:
            pass
        finally:
            PHONE_CONTACT_TIMEOUT_TASKS.pop(user_id, None)

    PHONE_CONTACT_TIMEOUT_TASKS[user_id] = asyncio.create_task(timeout_worker())


async def schedule_reminder(bot: Bot, user_id: int, chat_id: int):
    cancel_reminder_task(user_id)

    async def reminder_worker():
        try:
            await asyncio.sleep(600)
            poll_row = await fetch_poll_response(user_id)
            if not poll_row:
                return
            if poll_row["device"] or poll_row["reminder_sent"]:
                return

            text = await get_reminder_text()
            remind_keyboard = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="Написати менеджеру👨🏻‍💻",
                            url="https://t.me/hr_volodymyr?text=%2B",
                        )
                    ]
                ]
            )
            await bot.send_message(chat_id, text, reply_markup=remind_keyboard)
            await mark_reminder_sent(user_id)
        except asyncio.CancelledError:
            pass

    REMINDER_TASKS[user_id] = asyncio.create_task(reminder_worker())


async def fetch_user_record(user_id: int) -> Optional[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM users WHERE user_id = ?",
            (user_id,),
        ) as cursor:
            return await cursor.fetchone()


def format_user_reference(
    user_row: Optional[aiosqlite.Row],
    user_id: int,
) -> str:
    if user_row and user_row["username"]:
        return f"@{user_row['username']}"

    full_name = ""
    if user_row:
        first = user_row["first_name"] or ""
        last = user_row["last_name"] or ""
        full_name = (first + " " + last).strip()

    if full_name:
        return f"{full_name} (ID: {user_id})"

    return f"ID: {user_id}"


def build_group_lead_message(
    *,
    poll_row: aiosqlite.Row,
    user_row: Optional[aiosqlite.Row],
    user_id: int,
    referrer_row: Optional[aiosqlite.Row],
    referrer_id: Optional[int],
    note_row: Optional[aiosqlite.Row],
    note_id: Optional[int],
) -> str:
    lines = [
        "✴️ НОВА АНКЕТА",
        "",
        f"ℹ️ Користувач: {format_user_reference(user_row, user_id)}",
    ]

    phone_number = poll_row["phone_number"] or ""
    if phone_number:
        lines.extend(
            [
                "",
                f"☎️ Номер телефону: {phone_number}",
            ]
        )

    lines.extend(
        [
            "",
            f"⏳ Вік: {poll_row['age'] or '—'}",
            "",
            f"💰 Бажаний дохід: {poll_row['income'] or '—'}",
            "",
            f"💻 Ноутбук: {poll_row['device'] or '—'}",
        ]
    )

    if note_id and note_row:
        lines.extend(
            [
                "",
                f"🪧 Примітка: {note_row['title']} [{note_id}]",
            ]
        )

    if referrer_id:
        referrer_text = format_user_reference(referrer_row, referrer_id)
    else:
        referrer_text = "чистий запуск"

    lines.extend(
        [
            "",
            f"📥 Реферал від: {referrer_text}",
        ]
    )
    return "\n".join(lines)


async def notify_group_about_poll(bot: Bot, user_id: int):
    poll_row = await fetch_poll_response(user_id)
    if not poll_row or not poll_row["device"]:
        return

    if not await try_claim_notification(user_id):
        return

    referrer_id = poll_row["referrer_id"]
    group_id = poll_row["group_id"]
    group_info: Optional[Tuple[int, str]] = None

    if group_id:
        group_info = await fetch_group_info(group_id)

    if not group_info and referrer_id:
        group_info = await get_user_group(referrer_id)
        if group_info:
            group_id = group_info[0]

    if not group_info and not referrer_id:
        clean_group = await get_clean_launch_group()
        if clean_group:
            group_info = clean_group
            group_id = clean_group[0]

    if not group_info:
        return

    referrer_row = await fetch_user_record(referrer_id)
    user_row = await fetch_user_record(user_id)

    note_id = poll_row["note_id"]
    note_row = None
    if note_id:
        note_row = await fetch_note(note_id)
    message_text = build_group_lead_message(
        poll_row=poll_row,
        user_row=user_row,
        user_id=user_id,
        referrer_row=referrer_row,
        referrer_id=referrer_id,
        note_row=note_row,
        note_id=note_id,
    )

    try:
        await bot.send_message(group_info[0], message_text)
    except Exception:
        # Allow retry in case send failed after we claimed the notification.
        await unmark_notified(user_id)
        raise


async def resolve_group_context(
    *,
    group_id: Optional[int],
    referrer_id: Optional[int],
) -> Tuple[Optional[int], Optional[str]]:
    if group_id:
        group_info = await fetch_group_info(group_id)
        if group_info:
            return group_info[0], group_info[1]

    if referrer_id:
        referrer_group = await get_user_group(referrer_id)
        if referrer_group:
            return referrer_group[0], referrer_group[1]

    return group_id, None


def extract_start_payload(message: types.Message) -> Optional[str]:
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            return parts[1].strip()
    return None


async def handle_referral_payload(payload: Optional[str], user: types.User) -> bool:
    if not payload or not payload.startswith("ref_"):
        return False

    body = payload[4:]
    note_id: Optional[int] = None
    group_id: Optional[int] = None

    if "_note_" in body:
        body, note_part = body.split("_note_", maxsplit=1)
        try:
            note_id = int(note_part)
        except ValueError:
            note_id = None

    if "_group_" in body:
        ref_part, group_part = body.split("_group_", maxsplit=1)
        try:
            group_id = int(group_part)
        except ValueError:
            group_id = None
    else:
        ref_part = body

    try:
        ref_id = int(ref_part)
    except ValueError:
        return False

    if ref_id == user.id:
        return False

    inserted = await record_referral_click(ref_id, user.id, note_id, group_id)

    if inserted:
        resolved_group_id = group_id
        resolved_group_title: Optional[str] = None
        referrer_username: Optional[str] = None
        referred_username: Optional[str] = None
        if resolved_group_id is not None:
            group_info = await fetch_group_info(resolved_group_id)
            if group_info:
                resolved_group_title = group_info[1]
        else:
            referrer_group = await get_user_group(ref_id)
            if referrer_group:
                resolved_group_id, resolved_group_title = referrer_group

        note_title: Optional[str] = NO_NOTE_KEY
        note_url: Optional[str] = None
        if note_id is not None:
            note = await fetch_note(note_id)
            if note:
                note_title = note["title"] or NO_NOTE_KEY
                note_url = note["url"] or ""

        referrer_row = await fetch_user_record(ref_id)
        if referrer_row and referrer_row["username"]:
            referrer_username = referrer_row["username"]

        referred_row = await fetch_user_record(user.id)
        if referred_row and referred_row["username"]:
            referred_username = referred_row["username"]

        await SHEETS_LOGGER.log_referral_click_event(
            SheetsReferralEvent(
                group_id=resolved_group_id,
                group_title=resolved_group_title,
                referrer_id=ref_id,
                referrer_username=referrer_username,
                referred_user_id=user.id,
                referred_username=referred_username,
                note_id=note_id,
                note_title=note_title,
                note_url=note_url,
            )
        )

    if note_id:
        await record_note_click(note_id, user.id)
    await ensure_poll_row(user.id, ref_id, note_id, group_id)
    return True


async def render_ref_dashboard(message: types.Message, user: types.User, *, edit: bool = False):
    bot_username = await get_bot_username(message.bot)
    referral_link = f"https://t.me/{bot_username}?start=ref_{user.id}" if bot_username else "—"

    stats = await get_referral_stats(user.id)
    clean_group_info = await get_display_clean_group(user.id)
    clean_stats = await get_clean_launch_stats()
    groups = await fetch_groups()

    clean_group_line = (
        f"Група для чистого запуску: {escape(clean_group_info[1])} (ID: {clean_group_info[0]})"
        if clean_group_info
        else "Група для чистого запуску: не обрано"
    )

    stats_text = (
        "📊 Загальна статистика:\n"
        f"• Переходи за усіма посиланнями: {stats['clicks']}\n"
        f"• Пройшли тест: {stats['completed']}"
    )

    clean_stats_text = (
        "📈 Чистий запуск:\n"
        f"• Усього стартів: {clean_stats['total']}\n"
        f"• Пройшли тест: {clean_stats['completed']}"
    )

    referral_link_html = f"<code>{escape(referral_link)}</code>"

    lines = [
        "🔗 Ваша реферальна інформація",
        f"Посилання: {referral_link_html}",
        clean_group_line,
        "",
        clean_stats_text,
        "",
        stats_text,
        "",
    ]

    if groups:
        lines.extend(
            [
                "Групи, куди надходитимуть ліди:",
                "Оберіть потрібну групу нижче, щоб отримати її реф-посилання та примітки.",
            ]
        )
    else:
        lines.append(
            "Додайте бота до потрібної групи і надішліть там повідомлення, щоб вона з’явилась у списку."
        )

    buttons = []
    buttons.append(
        [
            InlineKeyboardButton(
                text="📂 Група для чистого запуску", callback_data="open_clean_group_menu"
            )
        ]
    )
    if groups:
        for chat_id, title in groups:
            buttons.append(
                [
                    InlineKeyboardButton(
                        text=title,
                        callback_data=f"group_details:{chat_id}",
                    )
                ]
            )
    buttons.append(
        [InlineKeyboardButton(text="🔔 Нагадування", callback_data="open_reminder_settings")]
    )

    reply_markup = InlineKeyboardMarkup(inline_keyboard=buttons)

    text = "\n".join(lines)

    if edit:
        await message.edit_text(text, reply_markup=reply_markup, parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=reply_markup, parse_mode="HTML")


async def render_group_menu(message: types.Message, *, edit: bool = False):
    groups = await fetch_groups()

    if not groups:
        text = (
            "Поки що немає жодної групи. Додайте бота до потрібного чату та "
            "надішліть там повідомлення, щоб він з’явився у списку."
        )
        keyboard = [[InlineKeyboardButton(text="↩️ Назад", callback_data="close_clean_group_menu")]]
    else:
        text_lines = [
            "Оберіть групу, куди будуть надходити ліди з чистого запуску:",
            "",
        ]
        text = "\n".join(text_lines)
        keyboard = [
            [InlineKeyboardButton(text=title, callback_data=f"set_clean_group:{chat_id}")]
            for chat_id, title in groups
        ]
        keyboard.append([InlineKeyboardButton(text="↩️ Назад", callback_data="close_clean_group_menu")])

    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    if edit:
        await message.edit_text(text, reply_markup=markup)
    else:
        await message.answer(text, reply_markup=markup)


async def render_reminder_settings(message: types.Message, *, edit: bool = False):
    reminder_text = await get_reminder_text()
    lines = [
        "🔔 Поточний текст нагадування:",
        "",
        reminder_text,
        "",
        "Це повідомлення отримають користувачі, які не завершили тест за 10 хвилин.",
    ]
    keyboard = [
        [InlineKeyboardButton(text="✍️ Змінити текст", callback_data="edit_reminder_text")],
        [InlineKeyboardButton(text="↩️ Назад", callback_data="close_reminder_settings")],
    ]
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    if edit:
        await message.edit_text("\n".join(lines), reply_markup=markup)
    else:
        await message.answer("\n".join(lines), reply_markup=markup)


async def render_group_details(
    message: types.Message,
    user: types.User,
    group_id: int,
    *,
    edit: bool = False,
):
    group_info = await fetch_group_info(group_id)
    if not group_info:
        await message.answer("Групу не знайдено. Додайте бота до чату та спробуйте знову.")
        return

    bot_username = await get_bot_username(message.bot)
    referral_link = (
        f"https://t.me/{bot_username}?start=ref_{user.id}_group_{group_info[0]}"
        if bot_username
        else "—"
    )
    stats = await get_group_referral_stats(user.id, group_info[0])

    lines = [
        f"Група: {escape(group_info[1])} (ID: {group_info[0]})",
        "",
        f"Реф-посилання для цієї групи:\n<code>{escape(referral_link)}</code>",
        "",
        "📊 Статистика групи:",
        f"• Переходи: {stats['clicks']}",
        f"• Пройшли тест: {stats['completed']}",
        "",
        "Керування:",
    ]

    keyboard = [
        [
            InlineKeyboardButton(
                text="📝 Примітки", callback_data=f"group_notes:{group_info[0]}"
            )
        ],
        [InlineKeyboardButton(text="↩️ Назад", callback_data="close_group_details")],
    ]
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    text = "\n".join(lines)
    if edit:
        try:
            await message.edit_text(text, reply_markup=markup, parse_mode="HTML")
        except TelegramBadRequest:
            await message.answer(text, reply_markup=markup, parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=markup, parse_mode="HTML")


async def render_group_notes(
    message: types.Message,
    user: types.User,
    group_id: int,
    *,
    edit: bool = False,
    view_note_id: Optional[int] = None,
    page: int = 1,
):
    group_info = await fetch_group_info(group_id)
    if not group_info:
        await message.answer("Групу не знайдено. Поверніться до головного меню.")
        return

    bot_username = await get_bot_username(message.bot)

    safe_group_title = escape(group_info[1])

    notes_page_size = 5
    current_page = max(1, page)

    if view_note_id:
        note = await fetch_note(view_note_id)
        if not note or note["owner_id"] != user.id or note["group_id"] != group_id:
            await message.answer("Примітку не знайдено або немає доступу.")
            return

        clicks = await count_note_clicks(note["id"])
        note_url_raw = note["url"] or ""
        note_url_valid = is_valid_note_url(note_url_raw)
        referral_link = (
            f"https://t.me/{bot_username}?start=ref_{user.id}_group_{group_id}_note_{note['id']}"
            if bot_username
            else "—"
        )
        lines = [
            f"Група: {safe_group_title}",
            f"Назва: {escape(note['title'])}",
            f"Посилання: {escape(note_url_raw) if note_url_raw else '—'}",
            f"Перегляди: {clicks}",
            "",
            f"Реф-посилання для примітки:\n<code>{escape(referral_link)}</code>",
            "",
            "Натисніть кнопку нижче для дій з приміткою.",
        ]
        keyboard: List[List[InlineKeyboardButton]] = []
        if note_url_valid:
            keyboard.append([InlineKeyboardButton(text="🌐 Відкрити примітку", url=note_url_raw)])
        keyboard.append(
            [
                InlineKeyboardButton(
                    text="🗑 Видалити примітку",
                    callback_data=f"delete_note:{group_id}:{note['id']}",
                )
            ]
        )
        keyboard.append(
            [InlineKeyboardButton(text="↩️ Назад", callback_data=f"group_notes:{group_id}:{current_page}")]
        )
        markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
        content = "\n".join(lines)
        if edit:
            try:
                await message.edit_text(content, reply_markup=markup, parse_mode="HTML")
            except TelegramBadRequest:
                await message.answer(content, reply_markup=markup, parse_mode="HTML")
        else:
            await message.answer(content, reply_markup=markup, parse_mode="HTML")
        return

    notes = await fetch_notes(user.id, group_id, viewer_id=user.id)
    total_notes = len(notes)
    total_pages = max(1, (total_notes + notes_page_size - 1) // notes_page_size)
    if current_page > total_pages:
        current_page = total_pages
    start_idx = (current_page - 1) * notes_page_size
    end_idx = start_idx + notes_page_size
    page_notes = notes[start_idx:end_idx]

    if not notes:
        text = (
            f"Для групи «{safe_group_title}» поки немає приміток. Натисніть кнопку нижче, щоб додати першу.\n"
            "Використовуйте примітки для відстеження, де ви розміщуєте реф-посилання."
        )
    else:
        text_lines = [
            f"Примітки для групи «{safe_group_title}»:",
            "",
        ]
        for note in page_notes:
            clicks = await count_note_clicks(note["id"])
            safe_title = escape(note["title"])
            text_lines.append(f"• {safe_title} — {clicks} переходів")
        text_lines.append("")
        text_lines.append(f"Сторінка {current_page}/{total_pages}")
        text_lines.append("")
        text_lines.append("Оберіть примітку для деталей або створіть нову.")
        text = "\n".join(text_lines)

    keyboard = []
    if page_notes:
        for note in page_notes:
            keyboard.append(
                [
                    InlineKeyboardButton(
                        text=note["title"],
                        callback_data=f"group_note_view:{group_id}:{note['id']}:{current_page}",
                    )
                ]
            )
        if total_pages > 1:
            nav_row: List[InlineKeyboardButton] = []
            if current_page > 1:
                nav_row.append(
                    InlineKeyboardButton(
                        text="⬅️",
                        callback_data=f"group_notes:{group_id}:{current_page - 1}",
                    )
                )
            if current_page < total_pages:
                nav_row.append(
                    InlineKeyboardButton(
                        text="➡️",
                        callback_data=f"group_notes:{group_id}:{current_page + 1}",
                    )
                )
            if nav_row:
                keyboard.append(nav_row)
    keyboard.append(
        [InlineKeyboardButton(text="➕ Додати примітку", callback_data=f"add_note:{group_id}")]
    )
    keyboard.append(
        [InlineKeyboardButton(text="↩️ Назад", callback_data=f"group_details:{group_id}")]
    )
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)

    if edit:
        try:
            await message.edit_text(text, reply_markup=markup)
        except TelegramBadRequest:
            await message.answer(text, reply_markup=markup)
    else:
        await message.answer(text, reply_markup=markup)


async def cmd_start(message: types.Message):
    await upsert_user(message.from_user)
    cancel_phone_contact_timeout(message.from_user.id)
    clear_phone_contact_waiter(message.from_user.id)
    payload = extract_start_payload(message)
    handled_referral = await handle_referral_payload(payload, message.from_user)
    group_id = None
    if not handled_referral:
        group_id = await get_clean_launch_group_id()
    await ensure_poll_row(message.from_user.id, group_id=group_id)
    poll_row = await fetch_poll_response(message.from_user.id)
    if not poll_row or not poll_row["device"]:
        await reset_reminder_sent(message.from_user.id)
        await schedule_reminder(message.bot, message.from_user.id, message.chat.id)
    else:
        cancel_reminder_task(message.from_user.id)

    await send_with_delay(
        message.answer,
        "Вітаю! Я бот-помічниця Оля!👩🏻‍💻\n"
        "Я буду скидати вам новини та важливу інформацію⚡️",
        skip_delay=True,
    )

    await send_with_delay(
        message.answer,
        "Зараз ви можете пройти невеличке опитування щоб зрозуміти чи підходить "
        "вам наша вакансія, або одразу звʼязатись з менеджером, який розповість "
        "вам умови праці, та відповість на всі запитання🙌🏻",
        reply_markup=build_start_keyboard(),
    )


async def cmd_poll(message: types.Message):
    await upsert_user(message.from_user)
    cancel_phone_contact_timeout(message.from_user.id)
    clear_phone_contact_waiter(message.from_user.id)
    await send_age_question(message.bot, message.chat.id, skip_delay=True)


async def cmd_ref(message: types.Message):
    await upsert_user(message.from_user)
    cancel_phone_contact_timeout(message.from_user.id)
    clear_phone_contact_waiter(message.from_user.id)
    await render_ref_dashboard(message, message.from_user)


async def handle_contact_manager(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    await send_manager_contact(callback.message, skip_delay=True)
    await callback.answer()


async def handle_poll_callback(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    await send_age_question(callback.message.bot, callback.message.chat.id, skip_delay=True)
    await callback.answer()


async def handle_age_choice(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    _, _, raw_value = callback.data.partition(":")
    age_label = AGE_OPTIONS.get(raw_value, raw_value)
    await update_poll_response(callback.from_user.id, age=age_label)

    await send_with_delay(
        callback.message.answer,
        "Чудово! Адже цей вид занятості підходить для будь-якого віку✨",
        skip_delay=True,
    )
    await send_income_question(callback.message.bot, callback.message.chat.id)
    await callback.answer()


async def handle_income_choice(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    _, _, raw_value = callback.data.partition(":")
    income_label = INCOME_OPTIONS.get(raw_value, raw_value)
    await update_poll_response(callback.from_user.id, income=income_label)

    await send_with_delay(
        callback.message.answer,
        "Це реально і легше, ніж здається!💪",
        skip_delay=True,
    )
    await send_device_question(callback.message.bot, callback.message.chat.id)
    await callback.answer()


async def handle_device_choice(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    selection = DEVICE_OPTIONS.get(callback.data, "Невідомо")
    await update_poll_response(callback.from_user.id, device=selection)
    cancel_reminder_task(callback.from_user.id)
    await mark_reminder_sent(callback.from_user.id)

    if callback.data == "poll_device_no":
        await send_with_delay(
            callback.message.answer,
            "Дякую за інтерес до вакансії!🙌🏻 Для цієї роботи обов’язковий ноутбук чи компʼютер, "
            "тож поки ми не можемо рухатися далі.🤦🏻‍♂️",
            skip_delay=True,
        )
        await send_with_delay(
            callback.message.answer,
            "Проте у нашій компанії діє реферальна програма: ви можете отримати 100 $ бонусу за кожного "
            "запрошеного друга 💰. Головне, щоб ця людина раніше не працювала у нас, після початку роботи "
            "відпрацювала щонайменше 14 днів і за перші 30 днів заробила мінімум 200 $ балансу."
        )
    else:
        await send_with_delay(
            callback.message.answer,
            "Це добре, бо ви самі обираєте зручний для себе темп. Але і розмір виплат буде залежати від того, "
            "скільки часу ви приділяєте цьому💰⌛️",
            skip_delay=True,
        )

    if callback.from_user.username:
        await notify_group_about_poll(callback.message.bot, callback.from_user.id)
        await send_manager_prompt(callback.message)
    else:
        set_phone_contact_waiter(callback.from_user.id)
        await send_phone_contact_prompt(callback.message)
        await schedule_phone_contact_timeout(
            callback.message.bot,
            callback.from_user.id,
            callback.message.chat.id,
        )
    await callback.answer()


async def handle_manager_prompt(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    if callback.from_user.id in PHONE_CONTACT_WAITERS:
        await finalize_phone_contact_wait(
            bot=callback.message.bot,
            user_id=callback.from_user.id,
            chat_id=callback.message.chat.id,
            send_manager=False,
            remove_keyboard_text="Добре.",
        )
    await send_manager_contact(callback.message, skip_delay=True)
    await callback.answer()


async def handle_phone_contact(message: types.Message):
    contact = message.contact
    if not contact or contact.user_id != message.from_user.id:
        await message.answer("Поділіться саме своїм контактом кнопкою нижче або натисніть «Написати».")
        return

    await upsert_user(message.from_user)
    cancel_phone_contact_timeout(message.from_user.id)
    clear_phone_contact_waiter(message.from_user.id)
    await update_poll_response(message.from_user.id, phone_number=contact.phone_number or "")
    await remove_contact_keyboard(message, text="Дякую! Контакт отримано.")

    poll_row = await fetch_poll_response(message.from_user.id)
    if poll_row:
        resolved_group_id, resolved_group_title = await resolve_group_context(
            group_id=poll_row["group_id"],
            referrer_id=poll_row["referrer_id"],
        )
        await SHEETS_LOGGER.update_referral_phone_number(
            group_id=resolved_group_id,
            group_title=resolved_group_title,
            referrer_id=poll_row["referrer_id"],
            referred_user_id=message.from_user.id,
            note_id=poll_row["note_id"],
            phone_number=contact.phone_number or "",
        )

    await notify_group_about_poll(message.bot, message.from_user.id)
    await send_manager_contact(message, skip_delay=True)


async def handle_phone_contact_text(message: types.Message):
    text = (message.text or "").strip()
    if text.startswith("/"):
        return

    if text == CONTACT_WRITE_TEXT:
        await finalize_phone_contact_wait(
            bot=message.bot,
            user_id=message.from_user.id,
            chat_id=message.chat.id,
            send_manager=True,
            remove_keyboard_text="Добре.",
        )
        return

    await message.answer("Натисніть «Поділитися номером» або «Написати».")


async def handle_group_selection(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    _, _, group_id_str = callback.data.partition(":")
    try:
        group_id = int(group_id_str)
    except ValueError:
        await callback.answer("Не вдалося обрати групу", show_alert=True)
        return

    await set_clean_launch_group(group_id)
    await render_ref_dashboard(callback.message, callback.from_user, edit=True)
    await callback.answer("Групу для чистого запуску оновлено")


async def handle_open_group_menu(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    await render_group_menu(callback.message, edit=True)
    await callback.answer()


async def handle_close_group_menu(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    await render_ref_dashboard(callback.message, callback.from_user, edit=True)
    await callback.answer()


async def handle_group_details(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    _, _, group_id_str = callback.data.partition(":")
    try:
        group_id = int(group_id_str)
    except ValueError:
        await callback.answer("Не вдалося відкрити групу", show_alert=True)
        return

    await render_group_details(callback.message, callback.from_user, group_id, edit=True)
    await callback.answer()


async def handle_close_group_details(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    await render_ref_dashboard(callback.message, callback.from_user, edit=True)
    await callback.answer()


async def handle_group_notes(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    parts = callback.data.split(":")
    if len(parts) not in (2, 3):
        await callback.answer("Не вдалося відкрити примітки", show_alert=True)
        return
    try:
        group_id = int(parts[1])
        page = int(parts[2]) if len(parts) == 3 else 1
    except ValueError:
        await callback.answer("Не вдалося відкрити примітки", show_alert=True)
        return

    await render_group_notes(callback.message, callback.from_user, group_id, edit=True, page=page)
    await callback.answer()


async def handle_group_note_view(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    parts = callback.data.split(":")
    if len(parts) not in (3, 4):
        await callback.answer("Не вдалося відкрити примітку", show_alert=True)
        return
    try:
        group_id = int(parts[1])
        note_id = int(parts[2])
        page = int(parts[3]) if len(parts) == 4 else 1
    except ValueError:
        await callback.answer("Не вдалося відкрити примітку", show_alert=True)
        return

    await render_group_notes(
        callback.message,
        callback.from_user,
        group_id,
        edit=True,
        view_note_id=note_id,
        page=page,
    )
    await callback.answer()


async def handle_group_note_add(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    _, _, group_id_str = callback.data.partition(":")
    try:
        group_id = int(group_id_str)
    except ValueError:
        await callback.answer("Не вдалося знайти групу", show_alert=True)
        return

    group_info = await fetch_group_info(group_id)
    if not group_info:
        await callback.answer("Група недоступна", show_alert=True)
        return

    NOTE_CREATION_STATE[callback.from_user.id] = {
        "step": "title",
        "group_id": group_id,
    }
    await callback.message.answer(
        f"Створюємо примітку для групи «{group_info[1]}».\n"
        "Введіть назву примітки. Надішліть /cancel, щоб скасувати створення."
    )
    await callback.answer()


async def handle_group_note_delete(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer("Не вдалося видалити примітку", show_alert=True)
        return
    try:
        group_id = int(parts[1])
        note_id = int(parts[2])
    except ValueError:
        await callback.answer("Не вдалося видалити примітку", show_alert=True)
        return

    note = await fetch_note(note_id)
    if not note or note["owner_id"] != callback.from_user.id or note["group_id"] != group_id:
        await callback.answer("Немає доступу або примітку вже видалено", show_alert=True)
        return

    deleted = await delete_note(callback.from_user.id, note_id)
    if deleted:
        await callback.answer("Примітку видалено")
        await render_group_notes(callback.message, callback.from_user, group_id, edit=True)
    else:
        await callback.answer("Немає доступу або примітку вже видалено", show_alert=True)


async def handle_copy_main_ref(callback: types.CallbackQuery):
    await callback.answer()


async def handle_copy_note_ref(callback: types.CallbackQuery):
    await callback.answer()


async def handle_open_reminder_settings(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    await render_reminder_settings(callback.message, edit=True)
    await callback.answer()


async def handle_close_reminder_settings(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    await render_ref_dashboard(callback.message, callback.from_user, edit=True)
    await callback.answer()


async def handle_edit_reminder_text(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    REMINDER_EDITORS.add(callback.from_user.id)
    await callback.message.answer(
        "Надішліть новий текст нагадування одним повідомленням.\n"
        "Використайте /cancel, щоб скасувати зміну."
    )
    await callback.answer("Очікую нове повідомлення")


async def track_group_presence(message: types.Message):
    await save_group(message.chat)


async def handle_note_input(message: types.Message):
    state = NOTE_CREATION_STATE.get(message.from_user.id)
    if not state:
        return

    text = (message.text or "").strip()
    if text.startswith("/") and text.lower() != "/cancel":
        NOTE_CREATION_STATE.pop(message.from_user.id, None)
        return

    if text.lower() == "/cancel":
        NOTE_CREATION_STATE.pop(message.from_user.id, None)
        await message.answer("Створення примітки скасовано.")
        return

    step = state.get("step")
    if step == "title":
        state["title"] = text
        state["step"] = "url"
        await message.answer("Тепер надішліть посилання для примітки (або '-' якщо воно не потрібне).")
    elif step == "url":
        title = state.get("title")
        if text != "-" and not is_valid_note_url(text):
            await message.answer(
                "Посилання має починатися з http://, https:// або tg://. "
                "Надішліть валідне посилання або '-' якщо його не потрібно."
            )
            return
        url = text if text != "-" else ""
        NOTE_CREATION_STATE.pop(message.from_user.id, None)
        group_id = state.get("group_id")
        if group_id is None:
            await message.answer("Не вдалося визначити групу для примітки. Спробуйте ще раз.")
            return

        note_id = await create_note(
            message.from_user.id,
            group_id,
            title or "Без назви",
            url,
        )
        bot_username = await get_bot_username(message.bot)
        referral_link = (
            f"https://t.me/{bot_username}?start=ref_{message.from_user.id}_group_{group_id}_note_{note_id}"
            if bot_username
            else ""
        )
        group_title: Optional[str] = None
        group_info = await fetch_group_info(group_id)
        if group_info:
            group_title = group_info[1]
        await SHEETS_LOGGER.ensure_note_in_stats(
            group_id=group_id,
            group_title=group_title,
            note_id=note_id,
            note_title=title or "Без назви",
            note_url=url,
            referral_link=referral_link,
        )
        await message.answer(f"Примітку збережено (ID: {note_id}).")
        await render_group_notes(message, message.from_user, group_id)


async def handle_reminder_edit_input(message: types.Message):
    if message.from_user.id not in REMINDER_EDITORS:
        return

    text = (message.text or "").strip()
    if text.lower() == "/cancel":
        REMINDER_EDITORS.discard(message.from_user.id)
        await message.answer("Зміну нагадування скасовано.")
        return

    if not text:
        await message.answer("Повідомлення не може бути порожнім. Спробуйте ще раз або /cancel.")
        return

    REMINDER_EDITORS.discard(message.from_user.id)
    await set_reminder_text(text)
    await message.answer("Текст нагадування оновлено.")
    await render_reminder_settings(message)
async def send_age_question(bot: Bot, chat_id: int, skip_delay: bool = False):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=label, callback_data=f"poll_age:{key}")]
            for key, label in AGE_OPTIONS.items()
        ]
    )
    await send_with_delay(
        bot.send_message,
        chat_id=chat_id,
        text="Скільки вам років?👏",
        reply_markup=keyboard,
        skip_delay=skip_delay,
    )


async def send_income_question(bot: Bot, chat_id: int, skip_delay: bool = False):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=label, callback_data=f"poll_income:{key}")]
            for key, label in INCOME_OPTIONS.items()
        ]
    )
    await send_with_delay(
        bot.send_message,
        chat_id=chat_id,
        text="Скільки ви б хотіли отримувати на місяць?💸",
        reply_markup=keyboard,
        skip_delay=skip_delay,
    )


async def send_device_question(bot: Bot, chat_id: int, skip_delay: bool = False):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=label, callback_data=key)]
            for key, label in DEVICE_OPTIONS.items()
        ]
    )
    await send_with_delay(
        bot.send_message,
        chat_id=chat_id,
        text="Чи є у вас комп'ютер чи ноутбук?",
        reply_markup=keyboard,
        skip_delay=skip_delay,
    )


async def send_manager_prompt(message: types.Message, skip_delay: bool = False):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Так", callback_data="request_manager")]
        ]
    )
    await send_with_delay(
        message.answer,
        "Хочете вже дізнатися подробиці?👌",
        reply_markup=keyboard,
        skip_delay=skip_delay,
    )


async def send_phone_contact_prompt(message: types.Message, skip_delay: bool = False):
    await send_with_delay(
        message.answer,
        "Поділіться контактом та наш менеджер напише вам, або напишіть йому самі.",
        reply_markup=build_contact_request_keyboard(),
        skip_delay=skip_delay,
    )


async def send_manager_contact(message: types.Message, skip_delay: bool = False):
    await send_manager_contact_to_chat(
        message.bot,
        message.chat.id,
        skip_delay=skip_delay,
    )


async def remove_contact_keyboard(message: types.Message, text: str = "Дякую!") -> None:
    await message.answer(text, reply_markup=ReplyKeyboardRemove())


async def main():
    logging.basicConfig(level=logging.INFO)

    if not API_TOKEN:
        raise RuntimeError("Не найден TELEGRAM_API_TOKEN в .env файле")

    await init_db()

    bot = Bot(token=API_TOKEN)
    dp = Dispatcher()

    # Регистрируем хэндлеры
    dp.message.register(handle_note_input, PendingNoteCreationFilter())
    dp.message.register(handle_reminder_edit_input, ReminderEditFilter())
    dp.message.register(cmd_start, CommandStart())
    dp.message.register(cmd_poll, Command("poll"))
    dp.message.register(cmd_ref, Command("ref"))
    dp.message.register(handle_phone_contact, PendingPhoneContactFilter(), F.contact)
    dp.message.register(handle_phone_contact_text, PendingPhoneContactFilter())
    dp.message.register(track_group_presence, F.chat.type.in_({"group", "supergroup"}))

    dp.callback_query.register(handle_contact_manager, F.data == "contact_manager")
    dp.callback_query.register(handle_poll_callback, F.data == "start_poll")
    dp.callback_query.register(handle_age_choice, F.data.startswith("poll_age:"))
    dp.callback_query.register(handle_income_choice, F.data.startswith("poll_income:"))
    dp.callback_query.register(
        handle_device_choice, F.data.in_(list(DEVICE_OPTIONS.keys()))
    )
    dp.callback_query.register(handle_manager_prompt, F.data == "request_manager")
    dp.callback_query.register(handle_group_selection, F.data.startswith("set_clean_group:"))
    dp.callback_query.register(handle_open_group_menu, F.data == "open_clean_group_menu")
    dp.callback_query.register(handle_close_group_menu, F.data == "close_clean_group_menu")
    dp.callback_query.register(handle_group_details, F.data.startswith("group_details:"))
    dp.callback_query.register(handle_close_group_details, F.data == "close_group_details")
    dp.callback_query.register(handle_group_notes, F.data.startswith("group_notes:"))
    dp.callback_query.register(handle_group_note_view, F.data.startswith("group_note_view:"))
    dp.callback_query.register(handle_group_note_add, F.data.startswith("add_note:"))
    dp.callback_query.register(handle_group_note_delete, F.data.startswith("delete_note:"))
    dp.callback_query.register(handle_open_reminder_settings, F.data == "open_reminder_settings")
    dp.callback_query.register(handle_close_reminder_settings, F.data == "close_reminder_settings")
    dp.callback_query.register(handle_edit_reminder_text, F.data == "edit_reminder_text")

    # Запуск бота
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
