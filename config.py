import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

BASE_DIR = Path(__file__).parent
BOT_TOKEN = "8552621598:AAGsYVn37ufYzvBfHCYEOzfWLB8WiJBHv-M"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE")

TOP_K = 5

# СТРОГИЙ список признаков для CatBoost (порядок важен!)
FEATURE_COLUMNS = [
    "square", "new_building", "repair", "lat", "lon", 
    "rooms", "floors_total", "dist_to_center", "house_class", "floor"
]
CAT_FEATURES = ['new_building', 'repair', 'house_class', 'rooms']