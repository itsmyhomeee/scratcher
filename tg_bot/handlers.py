from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton

router = Router()

main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🔍 Найти по описанию")],
        [KeyboardButton(text="💰 Оценить стоимость")],
    ],
    resize_keyboard=True
)

class SearchState(StatesGroup):
    waiting_for_text = State()

class PriceState(StatesGroup):
    waiting_for_description = State()

@router.message(F.text == "/start")
async def cmd_start(message: Message):
    await message.answer("🏠 Привет! Я помогу найти жилье или оценить его стоимость.", reply_markup=main_kb)

@router.message(F.text == "🔍 Найти по описанию")
async def start_search(message: Message, state: FSMContext):
    await message.answer("📝 Опишите квартиру (например: 2-к, 60м, метро Арбат):")
    await state.set_state(SearchState.waiting_for_text)

@router.message(SearchState.waiting_for_text)
async def perform_search(message: Message, state: FSMContext, wrapper):
    # 'wrapper' прилетает сюда автоматически из dp.start_polling
    results = wrapper.find_similar(message.text)
    if not results:
        return await message.answer("Ничего не найдено.")
    
    text = "🔍 <b>Результаты:</b>\n\n"
    for ad in results:
        text += f"📍 {ad['address']}\n💰 {ad['price']:,} ₽\n🔗 {ad['url']}\n\n"
    await message.answer(text, parse_mode="HTML", disable_web_page_preview=True)
    await state.clear()

@router.message(F.text == "💰 Оценить стоимость")
async def start_price(message: Message, state: FSMContext):
    await message.answer("💰 Опишите параметры квартиры для оценки:")
    await state.set_state(PriceState.waiting_for_description)

@router.message(PriceState.waiting_for_description)
async def perform_price(message: Message, state: FSMContext, wrapper):
    try:
        price = wrapper.estimate_price(message.text)
        await message.answer(f"💎 <b>Оценочная стоимость:</b>\n{price:,.0f} ₽", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"Ошибка при расчете: {e}")
    await state.clear()