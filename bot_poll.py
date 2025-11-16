import asyncio
import logging
import os

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import CommandStart, Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from dotenv import load_dotenv


# Загружаем переменные из .env (файл должен лежать рядом с bot_poll.py)
load_dotenv()

API_TOKEN = os.getenv("TELEGRAM_API_TOKEN")
MESSAGE_DELAY = 3


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
                    text="написати Володимиру",
                    url="https://t.me/hr_volodymyr?text=%2B",
                )
            ]
        ]
    )


async def cmd_start(message: types.Message):
    """
    Обработка команды /start
    """
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
    """
    Обработка команды /poll
    """
    await send_age_question(message.bot, message.chat.id, skip_delay=True)


async def handle_contact_manager(callback: types.CallbackQuery):
    """
    Ответ на нажатие кнопки «Написати менеджеру»
    """
    await send_manager_contact(callback.message, skip_delay=True)
    await callback.answer()


async def handle_poll_callback(callback: types.CallbackQuery):
    """
    Запуск опроса по кнопке «Пройти опитування»
    """
    await send_age_question(callback.message.bot, callback.message.chat.id, skip_delay=True)
    await callback.answer()


async def handle_age_choice(callback: types.CallbackQuery):
    """
    Обработка выбора возраста
    """
    await send_with_delay(
        callback.message.answer,
        "Чудово! Адже цей вид занятості підходить для будь-якого віку✨",
        skip_delay=True,
    )
    await send_income_question(callback.message.bot, callback.message.chat.id)
    await callback.answer()


async def handle_income_choice(callback: types.CallbackQuery):
    """
    Обработка желаемого дохода
    """
    await send_with_delay(
        callback.message.answer,
        "Це реально і легше, ніж здається!💪",
        skip_delay=True,
    )
    await send_device_question(callback.message.bot, callback.message.chat.id)
    await callback.answer()


async def handle_device_choice(callback: types.CallbackQuery):
    """
    Обработка ответа о наличии компьютера
    """
    if callback.data == "poll_device_no":
        await send_with_delay(
            callback.message.answer,
            "Дякую за інтерес до вакансії!🙌🏻 Для цієї роботи обов’язковий ноутбук чи компʼютер, "
            "тож поки ми не можемо рухатися далі.🤦🏻‍♂️"
            ,
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

    await send_manager_prompt(callback.message)
    await callback.answer()


async def handle_manager_prompt(callback: types.CallbackQuery):
    """
    Ответ на кнопку «Так» в вопросе о подробностях
    """
    await send_manager_contact(callback.message, skip_delay=True)
    await callback.answer()


async def send_age_question(bot: Bot, chat_id: int, skip_delay: bool = False):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="18-24", callback_data="poll_age:18-24")],
            [InlineKeyboardButton(text="25-30", callback_data="poll_age:25-30")],
            [InlineKeyboardButton(text="31-40", callback_data="poll_age:31-40")],
            [InlineKeyboardButton(text="41+", callback_data="poll_age:41_plus")],
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
            [InlineKeyboardButton(text="10-20 тис", callback_data="poll_income:10-20")],
            [InlineKeyboardButton(text="20-30 тис", callback_data="poll_income:20-30")],
            [InlineKeyboardButton(text="30-50 тис", callback_data="poll_income:30-50")],
            [InlineKeyboardButton(text="50+ тис", callback_data="poll_income:50+")],
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
            [InlineKeyboardButton(text="Так, є 👍🏻", callback_data="poll_device_yes")],
            [InlineKeyboardButton(text="Ні, немає 🙅🏻‍♂️", callback_data="poll_device_no")],
        ]
    )
    await send_with_delay(
        bot.send_message,
        chat_id=chat_id,
        text="Чи є у вас комп'ютер чи ноутбук?💻",
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
    """
    Точка входа бота
    """
    logging.basicConfig(level=logging.INFO)

    if not API_TOKEN:
        raise RuntimeError("Не найден TELEGRAM_API_TOKEN в .env файле")

    bot = Bot(token=API_TOKEN)
    dp = Dispatcher()

    # Регистрируем хэндлеры
    dp.message.register(cmd_start, CommandStart())
    dp.message.register(cmd_poll, Command("poll"))
    dp.callback_query.register(handle_contact_manager, F.data == "contact_manager")
    dp.callback_query.register(handle_poll_callback, F.data == "start_poll")
    dp.callback_query.register(handle_age_choice, F.data.startswith("poll_age:"))
    dp.callback_query.register(handle_income_choice, F.data.startswith("poll_income:"))
    dp.callback_query.register(
        handle_device_choice, F.data.in_(["poll_device_yes", "poll_device_no"])
    )
    dp.callback_query.register(handle_manager_prompt, F.data == "request_manager")

    # Запуск бота
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
