import asyncio
import html
import logging

from aiogram import Router, F
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton

logger = logging.getLogger(__name__)

router = Router()

main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🔍 Найти похожие")],
        [KeyboardButton(text="💰 Оценить стоимость")],
    ],
    resize_keyboard=True
)

class SearchState(StatesGroup):
    waiting_for_text = State()

class PriceState(StatesGroup):
    waiting_for_description = State()

# Команды и кнопки регистрируем раньше состояний, иначе нажатие кнопки
# во время ожидания ввода будет воспринято как текст запроса
@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("🏠 Привет! Я помогу найти жилье или оценить его стоимость.", reply_markup=main_kb)

@router.message(F.text == "🔍 Найти похожие")
async def start_search(message: Message, state: FSMContext):
    await message.answer("📝 Опишите квартиру (например: 2-к, 60м, метро Арбат):")
    await state.set_state(SearchState.waiting_for_text)

@router.message(F.text == "💰 Оценить стоимость")
async def start_price(message: Message, state: FSMContext):
    await message.answer("💰 Опишите параметры квартиры для оценки:")
    await state.set_state(PriceState.waiting_for_description)

@router.message(SearchState.waiting_for_text, F.text)
async def perform_search(message: Message, state: FSMContext, wrapper):
    # 'wrapper' прилетает сюда автоматически из dp.start_polling
    await state.clear()
    try:
        # Модели считают синхронно, поэтому выносим их из цикла событий
        results = await asyncio.to_thread(wrapper.find_similar, message.text)
    except Exception:
        logger.exception("Ошибка поиска")
        return await message.answer("⚠️ Не удалось выполнить поиск. Попробуйте позже.")
    if not results:
        return await message.answer("Ничего не найдено.")

    text = "🔍 <b>Результаты:</b>\n\n"
    for ad in results:
        address = html.escape(str(ad['address']))
        url = html.escape(str(ad['url']))
        text += f"📍 {address}\n💰 {ad['price']:,.0f} ₽\n🔗 {url}\n\n"
    await message.answer(text, parse_mode="HTML", disable_web_page_preview=True)

@router.message(PriceState.waiting_for_description, F.text)
async def perform_price(message: Message, state: FSMContext, wrapper):
    await state.clear()
    try:
        price = await asyncio.to_thread(wrapper.estimate_price, message.text)
    except Exception:
        logger.exception("Ошибка оценки стоимости")
        return await message.answer("⚠️ Не удалось оценить стоимость. Попробуйте позже.")
    await message.answer(f"💎 <b>Оценочная стоимость:</b>\n{price:,.0f} ₽", parse_mode="HTML")
