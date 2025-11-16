import asyncio
import logging
import os

from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart, Command
from dotenv import load_dotenv


# Загружаем переменные из .env (файл должен лежать рядом с bot_poll.py)
load_dotenv()

API_TOKEN = os.getenv("TELEGRAM_API_TOKEN")


async def cmd_start(message: types.Message):
    """
    Обработка команды /start
    """
    await message.answer(
        "Привет! 😊\n\n"
        "Я бот для опросов.\n"
        "Напиши /poll, и я запущу опрос."
    )


async def cmd_poll(message: types.Message):
    """
    Обработка команды /poll
    """
    question = "Какой контент тебе больше всего нравится?"
    options = [
        "Новости",
        "Мемы",
        "Обучение",
        "Всё подряд"
    ]

    await message.bot.send_poll(
        chat_id=message.chat.id,
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

    # Запуск бота
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
