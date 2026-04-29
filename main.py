import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent)) 

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from tg_bot.handlers import router
from tg_bot.wrapper import CianBotWrapper

bot = Bot(token="8552621598:AAGsYVn37ufYzvBfHCYEOzfWLB8WiJBHv-M")
dp  = Dispatcher(storage=MemoryStorage())

@dp.startup()
async def on_startup():
    bot["wrapper"] = CianBotWrapper()
    print("✅ Бот и модели загружены")

dp.include_router(router)

if __name__ == "__main__":
    asyncio.run(dp.start_polling(bot))