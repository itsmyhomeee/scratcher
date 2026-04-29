from aiogram import Router, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton

router = Router()

main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🔍 Найти похожие")],
        [KeyboardButton(text="💰 Оценить стоимость")],
    ],
    resize_keyboard=True,
)


class SearchState(StatesGroup):
    waiting_for_text = State()


class PriceState(StatesGroup):
    waiting_for_description = State()


# ---------------------------------------------------------------------------
@router.message(F.text == "/start")
async def cmd_start(message: Message):
    await message.answer(
        "🏠 Привет! Я ищу похожие объявления на Циан и оцениваю стоимость недвижимости.\n\n"
        "Выберите действие:",
        reply_markup=main_kb,
    )


# ---------------------------------------------------------------------------
@router.message(F.text == "🔍 Найти похожие")
async def start_search(message: Message, state: FSMContext):
    await message.answer(
        "📝 Опишите желаемую квартиру/комнату:\n"
        "Например: «2-комнатная квартира, 54 м², 5 этаж из 9, рядом с метро Аэропорт»"
    )
    await state.set_state(SearchState.waiting_for_text)


@router.message(SearchState.waiting_for_text)
async def perform_search(message: Message, state: FSMContext, bot):
    query = message.text

    if len(query) < 10:
        await message.answer(
            "❌ Слишком короткое описание. Добавьте больше деталей (метраж, комнаты, район)."
        )
        return

    await message.answer("🔎 Ищу похожие объявления... (обычно 2–3 секунды)")

    wrapper = bot["wrapper"]
    try:
        results = wrapper.find_similar(query)

        if not results:
            await message.answer("😕 Ничего не найдено. Попробуйте изменить описание.")
        else:
            text = "🔍 <b>Похожие объявления:</b>\n\n"
            for i, ad in enumerate(results[:5], 1):
                text += f"{i}. 🏢 {ad['address']}\n"
                text += f"   🛏️ {ad['rooms']} комн. | 📐 {ad['area']} м² | 💰 {ad['price']:,} ₽\n"
                text += f"   🔗 <a href='{ad['url']}'>Смотреть на Циан</a>\n"
                text += f"   📊 Схожесть: {ad['score'] * 100:.1f}%\n\n"

            await message.answer(text, parse_mode="HTML", disable_web_page_preview=True)

    except Exception as e:
        await message.answer(f"⚠️ Ошибка: {str(e)[:200]}")

    await state.clear()


@router.message(F.text == "💰 Оценить стоимость")
async def start_price(message: Message, state: FSMContext):
    await message.answer(
        "💰 <b>Оценка стоимости</b>\n\n"
        "Опишите все параметры квартиры:\n"
        "- Количество комнат\n"
        "- Общая площадь\n"
        "- Этаж / этажность дома\n"
        "- Район / метро\n"
        "- Состояние (черновая/чистовая/евроремонт)",
        parse_mode="HTML",
    )
    await state.set_state(PriceState.waiting_for_description)


@router.message(PriceState.waiting_for_description)
async def perform_price(message: Message, state: FSMContext, bot):
    description = message.text
    await message.answer("🧮 Рассчитываю стоимость...")

    wrapper = bot["wrapper"]
    try:
        price = wrapper.estimate_price(description)

        if price < 1_000_000:
            price_str = f"{price:,.0f} ₽"
        elif price < 1_000_000_000:
            price_str = f"{price / 1_000_000:.1f} млн ₽"
        else:
            price_str = f"{price / 1_000_000_000:.1f} млрд ₽"

        response = (
            f"💎 <b>Предполагаемая стоимость:</b>\n\n"
            f"💰 {price_str}\n\n"
            f"📊 <i>Оценка основана на {len(wrapper.df)} объявлениях Циан</i>"
        )
        await message.answer(response, parse_mode="HTML")

    except Exception as e:
        await message.answer(f"⚠️ Не удалось оценить: {str(e)[:200]}")

    await state.clear()


@router.message()
async def unknown(message: Message):
    await message.answer("Используйте кнопки меню ☝️", reply_markup=main_kb)