import asyncio
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiosqlite
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandStart
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from dotenv import load_dotenv


# Загружаем переменные из .env (файл должен лежать рядом с bot_poll.py)
load_dotenv()

API_TOKEN = os.getenv("TELEGRAM_API_TOKEN")
MESSAGE_DELAY = 1
DB_PATH = Path(__file__).with_name("bot_data.db")
BOT_USERNAME: Optional[str] = None

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
                age TEXT,
                income TEXT,
                device TEXT,
                notified INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
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


async def record_referral_click(referrer_id: int, referred_user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT OR IGNORE INTO referral_clicks (referrer_id, referred_user_id)
            VALUES (?, ?)
            """,
            (referrer_id, referred_user_id),
        )
        await db.commit()


async def ensure_poll_row(user_id: int, referrer_id: Optional[int] = None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO poll_responses (user_id, referrer_id)
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                referrer_id = COALESCE(poll_responses.referrer_id, excluded.referrer_id)
            """,
            (user_id, referrer_id),
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
            SELECT
                COUNT(*) as completed,
                SUM(CASE WHEN device = ? THEN 1 ELSE 0 END) as have_device,
                SUM(CASE WHEN device = ? THEN 1 ELSE 0 END) as no_device
            FROM poll_responses
            WHERE referrer_id = ? AND device IS NOT NULL
            """,
            (DEVICE_OPTIONS["poll_device_yes"], DEVICE_OPTIONS["poll_device_no"], user_id),
        ) as cursor:
            row = await cursor.fetchone()

    completed = row["completed"] if row and row["completed"] else 0
    have_device = row["have_device"] if row and row["have_device"] else 0
    no_device = row["no_device"] if row and row["no_device"] else 0

    return {
        "clicks": clicks,
        "completed": completed,
        "have_device": have_device,
        "no_device": no_device,
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

    _, _, ref_id_str = payload.partition("ref_")
    try:
        ref_id = int(ref_id_str)
    except ValueError:
        return

    if ref_id == user.id:
        return

    await record_referral_click(ref_id, user.id)
    await ensure_poll_row(user.id, ref_id)


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
        f"• Завершено опитувань: {stats['completed']}\n"
        f"• Ноутбук: Так – {stats['have_device']}, Ні – {stats['no_device']}"
    )

    group_prompt = (
        "Оберіть групу нижче, щоб отримувати сповіщення."
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
    ]

    keyboard_rows = [
        [InlineKeyboardButton(text=title, callback_data=f"set_group:{chat_id}")]
        for chat_id, title in groups
    ]

    reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard_rows) if keyboard_rows else None

    if edit:
        await message.edit_text("\n".join(lines), reply_markup=reply_markup)
    else:
        await message.answer("\n".join(lines), reply_markup=reply_markup)


async def cmd_start(message: types.Message):
    await upsert_user(message.from_user)
    payload = extract_start_payload(message)
    await handle_referral_payload(payload, message.from_user)

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


async def track_group_presence(message: types.Message):
    await save_group(message.chat)


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

    # Запуск бота
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
