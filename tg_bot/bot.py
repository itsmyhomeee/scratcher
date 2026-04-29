import asyncio
import sys
from pathlib import Path

# Добавляем родительскую папку в PATH, чтобы импортировать ваши модули
sys.path.append(str(Path(__file__).parent.parent))

from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from config import BOT_TOKEN
from tg_bot.handlers import router
from tg_bot.wrapper import CianBotWrapper 

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

@dp.startup()
async def on_startup():
    bot_wrapper = CianBotWrapper()
    bot["wrapper"] = bot_wrapper
    print("Бот и модели загружены")

dp.include_router(router)

if __name__ == "__main__":
    asyncio.run(dp.start_polling(bot))