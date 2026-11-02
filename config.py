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