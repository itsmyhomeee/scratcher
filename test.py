# test_env.py
import os
from pathlib import Path

print("1. Текущая директория:", Path.cwd())

# Проверяем наличие .env файла
env_file = Path('.env')
print(f"2. Файл .env существует: {env_file.exists()}")

# Пробуем загрузить
from dotenv import load_dotenv
load_dotenv()

# Значения не печатаем, только факт, что переменная задана
for name in ["BOT_TOKEN", "OPENAI_API_KEY", "OPENAI_API_BASE", "MINIO_ACCESS_KEY_ID", "MINIO_SECRET_ACCESS_KEY"]:
    print(f"{name}: {'задан' if os.getenv(name) else 'НЕ ЗАДАН'}")
