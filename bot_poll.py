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


async def cmd_start(message: types.Message):
    """
    Обработка команды /start
    """
    await message.answer(
        "Вітаю! Я бот-помічниця Оля!👩🏻‍💻\n"
        "Я буду скидати вам новини та важливу інформацію⚡️"
    )

    start_keyboard = InlineKeyboardMarkup(
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

    await message.answer(
        "Зараз ви можете пройти невеличке опитування чи одразу "
        "звʼязатись з менеджером, який вас введе в курс справи🙌",
        reply_markup=start_keyboard,
    )


async def cmd_poll(message: types.Message):
    """
    Обработка команды /poll
    """
    await send_predefined_poll(message.bot, message.chat.id)


async def handle_contact_manager(callback: types.CallbackQuery):
    """
    Ответ на нажатие кнопки «Написати менеджеру»
    """
    manager_keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Написати Менеджеру👨🏻‍💻",
                    url="https://t.me/hr_volodymyr?text=%2B",
                )
            ]
        ]
    )

    await callback.message.answer(
        "Надаю вам контакт менеджера Володимира - @hr_volodymyr🧑🏻‍💻 "
        "Відправ йому «+» і він розповість вам про роботу, та буде допомагати "
        "в подальшому!🚀",
        reply_markup=manager_keyboard,
    )

    await callback.answer()


async def handle_poll_callback(callback: types.CallbackQuery):
    """
    Запуск опроса по кнопке «Пройти опитування»
    """
    await send_predefined_poll(callback.message.bot, callback.message.chat.id)
    await callback.answer()


async def send_predefined_poll(bot: Bot, chat_id: int):
    """
    Универсальный помощник по отправке предустановленного опроса
    """
    question = "Какой контент тебе больше всего нравится?"
    options = [
        "Новости",
        "Мемы",
        "Обучение",
        "Всё подряд"
    ]

    await bot.send_poll(
        chat_id=chat_id,
        question=question,
        options=options,
        is_anonymous=False,            # видно, кто голосует
        allows_multiple_answers=True   # можно выбрать несколько
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

    # Запуск бота
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
