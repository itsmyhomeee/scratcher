import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

BASE_DIR = Path(__file__).parent
BOT_TOKEN = os.getenv("BOT_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE")
MINIO_ACCESS_KEY_ID = os.getenv("MINIO_ACCESS_KEY_ID")
MINIO_SECRET_ACCESS_KEY = os.getenv("MINIO_SECRET_ACCESS_KEY")

TOP_K = 5

# СТРОГИЙ список признаков для CatBoost (порядок важен!)
FEATURE_COLUMNS = [
    "square", "new_building", "repair", "lat", "lon", 
    "rooms", "floors_total", "dist_to_center", "house_class", "floor"
]
CAT_FEATURES = ['new_building', 'repair', 'house_class', 'rooms']

# Числовые шкалы из парсера: на них обучена модель.
# Порядок важен: длинный ключ должен идти раньше своей подстроки
REPAIR_RATING = {
    # Вторичка
    "без ремонта":  0.0,
    "косметический": 0.3,
    "евроремонт":   0.7,
    "дизайнерский": 1.0,
    # Новостройка
    "без отделки":        0.0,
    "предчистовая":       0.3,
    "чистовая с мебелью": 1.0,
    "чистовая":           0.7,
}
HOUSE_CLASS = {
    "деревянный":          0.0,
    "панельный":           0.2,
    "блочный":             0.3,
    "сталинский":          0.5,
    "кирпичный":           0.6,
    "кирпично-монолитный": 0.85,
    "монолитный":          1.0,
}
