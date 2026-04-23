import json
import re
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
import pyarrow as pa
import pyarrow.parquet as pq

def clean_price(value) -> float | None:
    """'82000000₽' → 82000000.0"""
    if not value:
        return None
    digits = re.sub(r'[^\d]', '', str(value))
    return float(digits) if digits else None


def clean_square(value) -> float | None:
    """'82,8 м²' → 82.8"""
    if not value:
        return None
    match = re.search(r'\d+[.,]?\d*', str(value))
    return float(match.group().replace(',', '.')) if match else None


def clean_station(value) -> str | None:
    """Пустую строку → None"""
    if not value or str(value).strip() == '':
        return None
    return str(value).strip()


def clean_photos(value) -> int:
    """Список S3 URI → количество фото (0 если null)"""
    if not value:
        return 0
    return len(value)


def load_and_clean(path: str = "cian_results.json") -> pd.DataFrame:
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    rows = []
    for offer_id, data in raw.items():
        rows.append({
            "offer_id":     offer_id,
            "url":          data.get("url"),
            "price":        clean_price(data.get("price")),
            "square":       clean_square(data.get("square")),
            "address":      data.get("address"),
            "station":      clean_station(data.get("station")),
            "new_building": bool(data.get("new_building", False)),
            "photos_count": clean_photos(data.get("photos")),
            "description":  data.get("description"),
        })

    df = pd.DataFrame(rows)
    df["price"]  = pd.to_numeric(df["price"],  errors="coerce")
    df["square"] = pd.to_numeric(df["square"], errors="coerce")
    df["photos_count"] = df["photos_count"].astype(int)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, "cian_data.parquet")

# 2. СХЕМА PANDERA

schema = DataFrameSchema(
    columns={

        "offer_id": Column(
            str,
            checks=Check(
                lambda s: s.str.match(r'^\d+$'),
                error="offer_id должен быть числовой строкой"
            ),
            nullable=False,
            unique=True,
            description="Числовой ID объявления из URL",
        ),

        "url": Column(
            str,
            checks=Check(
                lambda s: s.str.startswith("https://www.cian.ru/"),
                error="URL должен вести на cian.ru"
            ),
            nullable=False,
            unique=True,
            description="Прямая ссылка на объявление",
        ),

        # Цена: от 1 млн до 2 млрд рублей
        # Ниже 1 млн — явная ошибка парсинга для Москвы
        # Выше 2 млрд — выброс / нежилая недвижимость
        "price": Column(
            float,
            checks=[
                Check(lambda s: s.dropna().ge(1_000_000),      error="Цена не может быть меньше 1 000 000 ₽"),
                Check(lambda s: s.dropna().le(2_000_000_000),  error="Цена не может быть больше 2 000 000 000 ₽"),
            ],
            nullable=True,
            description="Цена в рублях (float, без символа ₽)",
        ),

        # Площадь: от 12 до 1000 м²
        # 12 м² — минимальная студия / апартамент
        # 1000 м² — очевидный выброс / пентхаус
        "square": Column(
            float,
            checks=[
                Check(lambda s: s.dropna().ge(12),   error="Площадь не может быть меньше 12 м²"),
                Check(lambda s: s.dropna().le(1000), error="Площадь не может быть больше 1000 м²"),
            ],
            nullable=True,
            description="Общая площадь квартиры в м²",
        ),

        # Адрес: непустая строка содержащая 'Москва'
        "address": Column(
            str,
            checks=[
                Check(lambda s: s.dropna().str.len().gt(5),                         error="Адрес слишком короткий"),
                Check(lambda s: s.dropna().str.contains("Москва", case=False),      error="Адрес должен содержать 'Москва'"),
            ],
            nullable=True,
            description="Полный адрес объявления",
        ),

        # Станция метро: строка или null (не пустая строка)
        "station": Column(
            str,
            checks=Check(
                lambda s: s.dropna().str.len().gt(0),
                error="station не может быть пустой строкой — используй null"
            ),
            nullable=True,
            description="Ближайшая станция метро или null",
        ),

        # Новостройка: строго булево
        "new_building": Column(
            bool,
            nullable=False,
            description="True если год постройки >= 2020",
        ),

        # Количество фото: 0–100
        # Верхний порог 100 — защита от мусора в данных
        "photos_count": Column(
            int,
            checks=[
                Check(lambda s: s.ge(0),   error="Количество фото не может быть отрицательным"),
                Check(lambda s: s.le(100), error="Количество фото не может быть больше 100"),
            ],
            nullable=False,
            description="Количество загруженных фото в MinIO",
        ),

        # Описание: если есть — не короче 10 символов
        "description": Column(
            str,
            checks=Check(
                lambda s: s.dropna().str.len().ge(10),
                error="Описание слишком короткое (меньше 10 символов)"
            ),
            nullable=True,
            description="Текст описания объявления",
        ),
    },

    unique=["offer_id", "url"],
    strict=True,
)


# ==================================================================
# 3. ЗАПУСК ВАЛИДАЦИИ
# ==================================================================

if __name__ == "__main__":
    print("Загружаем и очищаем данные...")
    df = load_and_clean("cian_results.json") 
    print(df[["price", "square"]].head())

    print(f"Загружено записей: {len(df)}")

    print("Запускаем валидацию по схеме Pandera...")
    try:
        validated_df = schema.validate(df, lazy=True)
        print("Все проверки пройдены успешно!")
        print(validated_df.describe())

    except pa.errors.SchemaErrors as e:
        print("Найдены ошибки валидации:\n")
        failure_cases = e.failure_cases[["check", "failure_case", "index"]].drop_duplicates()
        print(failure_cases.to_string(index=False))
