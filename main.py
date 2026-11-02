import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent)) 

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from config import BOT_TOKEN
from tg_bot.handlers import router
from tg_bot.wrapper import CianBotWrapper

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher(storage=MemoryStorage())

@dp.startup()
async def on_startup():
    dp["wrapper"] = CianBotWrapper()
    print("✅ Бот и модели загружены")

dp.include_router(router)

if __name__ == "__main__":
    asyncio.run(dp.start_polling(bot))