# test_env.py
import os
from pathlib import Path

print("1. Текущая директория:", Path.cwd())

# Проверяем наличие .env файла
env_file = Path('.env')
print(f"2. Файл .env существует: {env_file.exists()}")

if env_file.exists():
    print("3. Содержимое .env:")
    with open(env_file, 'r') as f:
        print(f.read())
else:
    print("3. Файл .env НЕ НАЙДЕН!")

# Пробуем загрузить
from dotenv import load_dotenv
load_dotenv()

print(f"4. OPENAI_API_KEY из os.getenv: {os.getenv('OPENAI_API_KEY')}")
print(f"5. LLM_API_KEY из os.getenv: {os.getenv('LLM_API_KEY')}")