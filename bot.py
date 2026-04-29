import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from config import BOT_TOKEN
from tg_bot.handlers import router
from tg_bot.wrapper import CianBotWrapper

async def main():
    logging.basicConfig(level=logging.INFO)
    bot = Bot(token="8552621598:AAGsYVn37ufYzvBfHCYEOzfWLB8WiJBHv-M")
    dp = Dispatcher(storage=MemoryStorage())

    bot_wrapper = CianBotWrapper()
    
    dp.include_router(router)
    
    print("Бот запущен...")
    # Передаем wrapper в polling, чтобы он был доступен в хендлерах
    await dp.start_polling(bot, wrapper=bot_wrapper)

if __name__ == "__main__":
    asyncio.run(main())