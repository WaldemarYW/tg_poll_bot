import asyncio
import logging
import os
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiosqlite
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import BaseFilter, Command, CommandStart
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from dotenv import load_dotenv


# Загружаем переменные из .env (файл должен лежать рядом с bot_poll.py)
load_dotenv()

API_TOKEN = os.getenv("TELEGRAM_API_TOKEN")
MESSAGE_DELAY = 1
DB_PATH = Path(__file__).with_name("bot_data.db")
BOT_USERNAME: Optional[str] = None
NOTE_CREATION_STATE: Dict[int, Dict[str, Optional[str]]] = {}

AGE_OPTIONS: Dict[str, str] = {
    "18-24": "18-24",
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
                    text="Написати менеджеру",
                    callback_data="contact_manager"
                )
            ],
            [
                InlineKeyboardButton(
                    text="Пройти опитування",
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
                    text="Написати Володимиру",
                    url="https://t.me/hr_volodymyr?text=%2B",
                )
            ]
        ]
    )


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
                UNIQUE(referrer_id, referred_user_id)
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
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT OR IGNORE INTO referral_clicks (referrer_id, referred_user_id, note_id)
            VALUES (?, ?, ?)
            """,
            (referrer_id, referred_user_id, note_id),
        )
        await db.commit()


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
):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO poll_responses (user_id, referrer_id, note_id)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                referrer_id = COALESCE(poll_responses.referrer_id, excluded.referrer_id),
                note_id = CASE
                    WHEN excluded.note_id IS NOT NULL THEN excluded.note_id
                    ELSE poll_responses.note_id
                END
            """,
            (user_id, referrer_id, note_id),
        )
        await db.commit()


async def update_poll_response(
    user_id: int,
    *,
    age: Optional[str] = None,
    income: Optional[str] = None,
    device: Optional[str] = None,
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

    if not updates:
        return

    updates.append("updated_at = CURRENT_TIMESTAMP")
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


async def was_notified(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT notified FROM poll_responses WHERE user_id = ?",
            (user_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return bool(row and row[0])


async def mark_notified(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE poll_responses SET notified = 1 WHERE user_id = ?",
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
            await bot.send_message(chat_id, text, reply_markup=build_manager_button())
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


async def notify_group_about_poll(bot: Bot, user_id: int):
    poll_row = await fetch_poll_response(user_id)
    if not poll_row or not poll_row["device"] or await was_notified(user_id):
        return

    referrer_id = poll_row["referrer_id"]
    if not referrer_id:
        return

    group_info = await get_user_group(referrer_id)
    if not group_info:
        return

    referrer_row = await fetch_user_record(referrer_id)
    user_row = await fetch_user_record(user_id)

    lines = [
        "🆕 Нова анкета від ліда",
        f"Користувач: {format_user_reference(user_row, user_id)}",
        f"Вік: {poll_row['age'] or '—'}",
        f"Бажаний дохід: {poll_row['income'] or '—'}",
        f"Ноутбук: {poll_row['device'] or '—'}",
    ]

    note_line = None
    note_id = poll_row["note_id"]
    note_row = None
    if note_id:
        note_row = await fetch_note(note_id)
        if note_row:
            note_line = f"Примітка: {note_row['title']}"
            if note_row["url"]:
                note_line += f" ({note_row['url']})"

    if note_line:
        lines.append(note_line)

    if user_row and user_row["username"]:
        lines.append(f"Профіль користувача: https://t.me/{user_row['username']}")
    else:
        lines.append(f"Профіль користувача: tg://user?id={user_id}")

    if referrer_id:
        lines.append(f"Реферал від: {format_user_reference(referrer_row, referrer_id)}")

    await bot.send_message(group_info[0], "\n".join(lines))
    await mark_notified(user_id)


def extract_start_payload(message: types.Message) -> Optional[str]:
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            return parts[1].strip()
    return None


async def handle_referral_payload(payload: Optional[str], user: types.User):
    if not payload or not payload.startswith("ref_"):
        return

    body = payload[4:]
    note_id = None

    if "_note_" in body:
        ref_part, note_part = body.split("_note_", maxsplit=1)
        try:
            note_id = int(note_part)
        except ValueError:
            note_id = None
    else:
        ref_part = body

    try:
        ref_id = int(ref_part)
    except ValueError:
        return

    if ref_id == user.id:
        return

    await record_referral_click(ref_id, user.id, note_id)
    if note_id:
        await record_note_click(note_id, user.id)
    await ensure_poll_row(user.id, ref_id, note_id)


async def render_ref_dashboard(message: types.Message, user: types.User, *, edit: bool = False):
    bot_username = await get_bot_username(message.bot)
    referral_link = f"https://t.me/{bot_username}?start=ref_{user.id}" if bot_username else "—"

    stats = await get_referral_stats(user.id)
    group_info = await get_user_group(user.id)
    groups = await fetch_groups()

    group_line = (
        f"Поточна група: {group_info[1]} (ID: {group_info[0]})"
        if group_info
        else "Поточна група: не обрано"
    )

    stats_text = (
        "📊 Статистика рефералів:\n"
        f"• Переходи за вашим посиланням: {stats['clicks']}\n"
        f"• Пройшли тест: {stats['completed']}"
    )

    group_prompt = (
        "Натисніть кнопку нижче, щоб обрати групу для сповіщень."
        if groups
        else "Додайте бота до потрібної групи і надішліть у ній повідомлення, щоб вона з’явилась у списку."
    )

    lines = [
        "🔗 Ваша реферальна інформація",
        f"Посилання: {referral_link}",
        group_line,
        "",
        stats_text,
        "",
        group_prompt,
        "",
        "Натисніть кнопку, щоб скопіювати посилання.",
    ]

    buttons = []
    if groups:
        buttons.append(
            [InlineKeyboardButton(text="📂 Обрати групу", callback_data="open_group_menu")]
        )
    if bot_username:
        buttons.append(
            [InlineKeyboardButton(text="📋 Скопіювати посилання", callback_data="copy_main_ref")]
        )
    buttons.append(
        [InlineKeyboardButton(text="📝 Примітки", callback_data="open_notes_menu")]
    )
    buttons.append(
        [InlineKeyboardButton(text="🔔 Нагадування", callback_data="open_reminder_settings")]
    )

    reply_markup = InlineKeyboardMarkup(inline_keyboard=buttons)

    if edit:
        await message.edit_text("\n".join(lines), reply_markup=reply_markup)
    else:
        await message.answer("\n".join(lines), reply_markup=reply_markup)


async def render_group_menu(message: types.Message, *, edit: bool = False):
    groups = await fetch_groups()

    if not groups:
        text = (
            "Поки що немає жодної групи. Додайте бота до потрібного чату та "
            "надішліть там повідомлення, щоб він з’явився у списку."
        )
        keyboard = [[InlineKeyboardButton(text="↩️ Назад", callback_data="close_group_menu")]]
    else:
        text_lines = [
            "Оберіть групу, куди будуть надходити сповіщення про лідів:",
            "",
        ]
        text = "\n".join(text_lines)
        keyboard = [
            [InlineKeyboardButton(text=title, callback_data=f"set_group:{chat_id}")]
            for chat_id, title in groups
        ]
        keyboard.append([InlineKeyboardButton(text="↩️ Назад", callback_data="close_group_menu")])

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


async def render_notes_menu(
    message: types.Message,
    user: types.User,
    *,
    edit: bool = False,
    view_note_id: Optional[int] = None,
):
    bot_username = await get_bot_username(message.bot)
    group_info = await get_user_group(user.id)

    if not group_info:
        text = (
            "Спершу оберіть групу в головному меню, щоб керувати примітками. "
            "Кожна група має власний список приміток і статистику."
        )
        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📂 Обрати групу", callback_data="open_group_menu")],
                [InlineKeyboardButton(text="↩️ Назад", callback_data="close_notes_menu")],
            ]
        )
        if edit:
            await message.edit_text(text, reply_markup=markup)
        else:
            await message.answer(text, reply_markup=markup)
        return

    group_id, group_title = group_info

    if view_note_id:
        note = await fetch_note(view_note_id)
        if not note or note["owner_id"] != user.id or note["group_id"] != group_id:
            await message.answer("Примітку не знайдено або вона належить іншій групі.")
            return

        clicks = await count_note_clicks(note["id"])
        referral_link = (
            f"https://t.me/{bot_username}?start=ref_{user.id}_note_{note['id']}"
            if bot_username
            else "—"
        )
        lines = [
            f"Група: {group_title}",
            f"Назва: {note['title']}",
            f"Посилання: {note['url'] or '—'}",
            f"Перегляди: {clicks}",
            "",
            f"Реф-посилання для цієї примітки:\n{referral_link}",
            "",
            "Натисніть кнопку, щоб скопіювати або відкривати посилання.",
        ]
        keyboard = []
        if note["url"]:
            keyboard.append(
                [InlineKeyboardButton(text="🌐 Відкрити примітку", url=note["url"])]
            )
        if bot_username:
            keyboard.append(
                [
                    InlineKeyboardButton(
                        text="📋 Скопіювати реф-посилання",
                        callback_data=f"copy_note_ref:{note['id']}",
                    )
                ]
            )
        keyboard.append(
            [InlineKeyboardButton(text="🗑 Видалити примітку", callback_data=f"delete_note:{note['id']}")]
        )
        keyboard.append(
            [InlineKeyboardButton(text="↩️ Назад", callback_data="open_notes_menu")]
        )
        markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
        if edit:
            await message.edit_text("\n".join(lines), reply_markup=markup)
        else:
            await message.answer("\n".join(lines), reply_markup=markup)
        return

    notes = await fetch_notes(user.id, group_id, viewer_id=user.id)
    if not notes:
        text = (
            f"Для групи «{group_title}» поки немає приміток. Натисніть кнопку нижче, щоб додати першу.\n"
            "Використовуйте примітки для відстеження, де ви розміщуєте реф-посилання."
        )
    else:
        text_lines = [
            f"Примітки для групи «{group_title}»:",
            "",
        ]
        for note in notes[:5]:
            clicks = await count_note_clicks(note["id"])
            text_lines.append(f"• {note['title']} — {clicks} переходів")
        if len(notes) > 5:
            text_lines.append("... (перегляньте деталі через меню)")
        text_lines.append("")
        text_lines.append("Обери одну з приміток для подробиць.")
        text = "\n".join(text_lines)

    keyboard = []
    if notes:
        keyboard.extend(
            [[InlineKeyboardButton(text=note["title"], callback_data=f"note_view:{note['id']}")]]
            for note in notes
        )
    keyboard.append([InlineKeyboardButton(text="➕ Додати примітку", callback_data="add_note")])
    keyboard.append([InlineKeyboardButton(text="↩️ Назад", callback_data="close_notes_menu")])
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)

    if edit:
        await message.edit_text(text, reply_markup=markup)
    else:
        await message.answer(text, reply_markup=markup)


async def cmd_start(message: types.Message):
    await upsert_user(message.from_user)
    payload = extract_start_payload(message)
    await handle_referral_payload(payload, message.from_user)
    await ensure_poll_row(message.from_user.id)
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
        "Зараз ви можете пройти невеличке опитування чи одразу "
        "звʼязатись з менеджером, який вас введе в курс справи🙌",
        reply_markup=build_start_keyboard(),
    )


async def cmd_poll(message: types.Message):
    await upsert_user(message.from_user)
    await send_age_question(message.bot, message.chat.id, skip_delay=True)


async def cmd_ref(message: types.Message):
    await upsert_user(message.from_user)
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

    await notify_group_about_poll(callback.message.bot, callback.from_user.id)
    await send_manager_prompt(callback.message)
    await callback.answer()


async def handle_manager_prompt(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    await send_manager_contact(callback.message, skip_delay=True)
    await callback.answer()


async def handle_group_selection(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    _, _, group_id_str = callback.data.partition(":")
    try:
        group_id = int(group_id_str)
    except ValueError:
        await callback.answer("Не вдалося обрати групу", show_alert=True)
        return

    await set_user_group(callback.from_user.id, group_id)
    await render_ref_dashboard(callback.message, callback.from_user, edit=True)
    await callback.answer("Групу оновлено")


async def handle_open_group_menu(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    await render_group_menu(callback.message, edit=True)
    await callback.answer()


async def handle_close_group_menu(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    await render_ref_dashboard(callback.message, callback.from_user, edit=True)
    await callback.answer()


async def handle_open_notes_menu(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    await render_notes_menu(callback.message, callback.from_user, edit=True)
    await callback.answer()


async def handle_close_notes_menu(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    await render_ref_dashboard(callback.message, callback.from_user, edit=True)
    await callback.answer()


async def handle_note_view(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    _, _, note_id_str = callback.data.partition(":")
    try:
        note_id = int(note_id_str)
    except ValueError:
        await callback.answer("Не вдалося відкрити примітку", show_alert=True)
        return

    await render_notes_menu(callback.message, callback.from_user, edit=True, view_note_id=note_id)
    await callback.answer()


async def handle_note_add(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    group_info = await get_user_group(callback.from_user.id)
    if not group_info:
        await callback.answer("Спочатку оберіть групу", show_alert=True)
        return

    NOTE_CREATION_STATE[callback.from_user.id] = {
        "step": "title",
        "group_id": group_info[0],
    }
    await callback.message.answer(
        f"Створюємо примітку для групи «{group_info[1]}».\n"
        "Введіть назву примітки. Надішліть /cancel, щоб скасувати створення."
    )
    await callback.answer()


async def handle_note_delete(callback: types.CallbackQuery):
    await upsert_user(callback.from_user)
    _, _, note_id_str = callback.data.partition(":")
    try:
        note_id = int(note_id_str)
    except ValueError:
        await callback.answer("Не вдалося видалити примітку", show_alert=True)
        return

    note = await fetch_note(note_id)
    group_info = await get_user_group(callback.from_user.id)
    if not note or note["owner_id"] != callback.from_user.id:
        await callback.answer("Немає доступу або примітку вже видалено", show_alert=True)
        return
    if not group_info or note["group_id"] != group_info[0]:
        await callback.answer("Ця примітка належить іншій групі", show_alert=True)
        return

    deleted = await delete_note(callback.from_user.id, note_id)
    if deleted:
        await callback.answer("Примітку видалено")
        await render_notes_menu(callback.message, callback.from_user, edit=True)
    else:
        await callback.answer("Немає доступу або примітку вже видалено", show_alert=True)


async def handle_copy_main_ref(callback: types.CallbackQuery):
    bot_username = await get_bot_username(callback.message.bot)
    ref_link = f"https://t.me/{bot_username}?start=ref_{callback.from_user.id}"
    await callback.answer(f"Посилання скопійовано:\n{ref_link}", show_alert=True)


async def handle_copy_note_ref(callback: types.CallbackQuery):
    bot_username = await get_bot_username(callback.message.bot)
    _, _, note_id_str = callback.data.partition(":")
    try:
        note_id = int(note_id_str)
    except ValueError:
        await callback.answer("Не вдалося сформувати посилання", show_alert=True)
        return

    note = await fetch_note(note_id)
    if not note or note["owner_id"] != callback.from_user.id:
        await callback.answer("Немає доступу до цієї примітки", show_alert=True)
        return

    ref_link = f"https://t.me/{bot_username}?start=ref_{callback.from_user.id}_note_{note_id}"
    await callback.answer(f"Посилання для примітки:\n{ref_link}", show_alert=True)


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
        url = text if text != "-" else ""
        NOTE_CREATION_STATE.pop(message.from_user.id, None)
        group_id = state.get("group_id")
        if group_id is None:
            group_info = await get_user_group(message.from_user.id)
            group_id = group_info[0] if group_info else None

        if group_id is None:
            await message.answer("Не вдалося визначити групу для примітки. Спробуйте ще раз.")
            return

        note_id = await create_note(
            message.from_user.id,
            group_id,
            title or "Без назви",
            url,
        )
        await message.answer(f"Примітку збережено (ID: {note_id}).")
        await render_notes_menu(message, message.from_user)


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


async def send_manager_contact(message: types.Message, skip_delay: bool = False):
    await send_with_delay(
        message.answer,
        "Надаю вам контакт менеджера Володимира - @hr_volodymyr🧑🏻‍💻 "
        "Відправ йому «+» і він розповість вам про роботу, та буде допомагати в подальшому!🚀",
        reply_markup=build_manager_button(),
        skip_delay=skip_delay,
    )


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
    dp.message.register(track_group_presence, F.chat.type.in_({"group", "supergroup"}))

    dp.callback_query.register(handle_contact_manager, F.data == "contact_manager")
    dp.callback_query.register(handle_poll_callback, F.data == "start_poll")
    dp.callback_query.register(handle_age_choice, F.data.startswith("poll_age:"))
    dp.callback_query.register(handle_income_choice, F.data.startswith("poll_income:"))
    dp.callback_query.register(
        handle_device_choice, F.data.in_(list(DEVICE_OPTIONS.keys()))
    )
    dp.callback_query.register(handle_manager_prompt, F.data == "request_manager")
    dp.callback_query.register(handle_group_selection, F.data.startswith("set_group:"))
    dp.callback_query.register(handle_open_group_menu, F.data == "open_group_menu")
    dp.callback_query.register(handle_close_group_menu, F.data == "close_group_menu")
    dp.callback_query.register(handle_open_notes_menu, F.data == "open_notes_menu")
    dp.callback_query.register(handle_close_notes_menu, F.data == "close_notes_menu")
    dp.callback_query.register(handle_note_view, F.data.startswith("note_view:"))
    dp.callback_query.register(handle_note_add, F.data == "add_note")
    dp.callback_query.register(handle_note_delete, F.data.startswith("delete_note:"))
    dp.callback_query.register(handle_copy_main_ref, F.data == "copy_main_ref")
    dp.callback_query.register(handle_copy_note_ref, F.data.startswith("copy_note_ref:"))
    dp.callback_query.register(handle_open_reminder_settings, F.data == "open_reminder_settings")
    dp.callback_query.register(handle_close_reminder_settings, F.data == "close_reminder_settings")
    dp.callback_query.register(handle_edit_reminder_text, F.data == "edit_reminder_text")

    # Запуск бота
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
