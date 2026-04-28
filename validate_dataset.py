import json
import re
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
import pyarrow as arrow     
import pyarrow.parquet as pq

from parcing import haversine



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
            "new_building": bool(data.get("new_building", False)),
            "photos_count": clean_photos(data.get("photos")),
            "description":  data.get("description"),
            "repair":       data.get("repair"),  
            "lat":          data.get("lat"),       
            "lon":          data.get("lon"),       
            "rooms": data.get("rooms"),
            "floors_total":     data.get("floors_total"),
            "dist_to_center":   data.get("dist_to_center"),
            "house_class":      data.get("house_class"),
            "floor":            data.get("floor")
        })

    df = pd.DataFrame(rows)
    df["price"]        = pd.to_numeric(df["price"],  errors="coerce")
    df["square"]       = pd.to_numeric(df["square"], errors="coerce")
    df["repair"]       = pd.to_numeric(df["repair"], errors="coerce")
    df["lat"]          = pd.to_numeric(df["lat"],    errors="coerce")
    df["lon"]          = pd.to_numeric(df["lon"],    errors="coerce")
    df["floors_total"]   = pd.to_numeric(df["floors_total"],   errors="coerce")
    df["floor"]          = pd.to_numeric(df["floor"],           errors="coerce")
    df["dist_to_center"] = pd.to_numeric(df["dist_to_center"], errors="coerce")
    df['rooms']          = pd.to_numeric(df['rooms'],          errors="coerce")
    df["house_class"]  = pd.to_numeric(df["house_class"],  errors="coerce")
    df["photos_count"] = pd.to_numeric(df["photos_count"], errors="coerce").fillna(0).astype(int)


    # Сохраняем parquet
    table = arrow.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, "cian_data.parquet")
    print("Сохранено → cian_data.parquet")

    return df   # обязательно возвращаем датафрейм


# ==================================================================
# 2. СХЕМА PANDERA
# ==================================================================

schema = DataFrameSchema(
    columns={

        "offer_id": Column(
            str,
            checks=Check(
                lambda s: s.str.match(r'^\d+$'),
                error="offer_id должен быть числовой строкой"
            ),
            nullable=False,

        ),

        "url": Column(
            str,
            checks=Check(
                lambda s: s.str.startswith("https://www.cian.ru/"),
                error="URL должен вести на cian.ru"
            ),
            nullable=False,
        ),

        # Цена: от 1 млн до 2 млрд рублей
        "price": Column(
            float,
            checks=[
                Check(lambda s: s.dropna().ge(1_000_000),     error="Цена не может быть меньше 1 000 000 ₽"),
                Check(lambda s: s.dropna().le(2_000_000_000), error="Цена не может быть больше 2 000 000 000 ₽"),
            ],
            nullable=True,
        ),

        # Площадь: от 12 до 1000 м²
        "square": Column(
            float,
            checks=[
                Check(lambda s: s.dropna().ge(0), error="Площадь не может быть меньше 0 м²"),
                Check(lambda s: s.dropna().le(1000), error="Площадь не может быть больше 1000 м²"),
            ],
            nullable=True,
        ),

        "address": Column(
            str,
            checks=[
                Check(lambda s: s.dropna().str.len().gt(5),                    error="Адрес слишком короткий"),
                Check(lambda s: s.dropna().str.contains("Москва", case=False), error="Адрес должен содержать 'Москва'"),
            ],
            nullable=True,
        ),

        "new_building": Column(bool, nullable=False),

        # Количество фото: 0–10 (лимит парсера)
        "photos_count": Column(
            int,
            checks=[
                Check(lambda s: s.ge(0),  error="Количество фото не может быть отрицательным"),
                Check(lambda s: s.le(10), error="Количество фото не может быть больше 10"),
            ],
            nullable=False,
        ),

        "description": Column(
            str,
            checks=Check(
                lambda s: s.dropna().str.len().ge(10),
                error="Описание слишком короткое"
            ),
            nullable=True,
        ),

        # Скор ремонта: от 0.0 до 1.0
        "repair": Column(
            float,
            checks=[
                Check(lambda s: s.dropna().ge(0.0), error="repair не может быть меньше 0.0"),
                Check(lambda s: s.dropna().le(1.0), error="repair не может быть больше 1.0"),
            ],
            nullable=True,
        ),
        "rooms": Column(
            int,
            checks=[
                Check(lambda s: s.dropna().ge(1), error="rooms не может быть меньше 1"),
                Check(lambda s: s.dropna().le(10), error="rooms не может быть больше 10"),
            ],
            nullable=True,
        ),
        "lat": Column(
            float,
            checks=[
                Check(lambda s: s.dropna().ge(55.0), error="lat выходит за пределы Москвы"),
                Check(lambda s: s.dropna().le(56.0), error="lat выходит за пределы Москвы"),
            ],
            nullable=True,
        ),

        "lon": Column(
            float,
            checks=[
                Check(lambda s: s.dropna().ge(37.0), error="lon выходит за пределы Москвы"),
                Check(lambda s: s.dropna().le(38.0), error="lon выходит за пределы Москвы"),
            ],
            nullable=True,
        ),
        "house_class": Column(
            float,
            checks=[
                Check(lambda s: s.dropna().ge(0.0), error="house_class не может быть меньше 0.0"),
                Check(lambda s: s.dropna().le(1.0), error="house_class не может быть больше 1.0"),
            ],
            nullable=True,
        ),
        "floor": Column(
            int,
            checks=[
                Check(lambda s: s.dropna().ge(1),   error="floor не может быть меньше 1"),
                Check(lambda s: s.dropna().le(150), error="floor не может быть больше 150"),
            ],
            nullable=True,
        ),

        "floors_total": Column(
            int,
            checks=[
                Check(lambda s: s.dropna().ge(1),   error="floors_total не может быть меньше 1"),
                Check(lambda s: s.dropna().le(150), error="floors_total не может быть больше 150"),
            ],
            nullable=True,
        ),
        "dist_to_center": Column(
            float,
            checks=[
                Check(lambda s: s.dropna().ge(0), error="dist_to_center не может быть отрицательным"),]),  
    },

    unique=["offer_id", "url"],
    strict=True,
)


# ==================================================================
# 3. ЗАПУСК
# ==================================================================

if __name__ == "__main__":
    print("Загружаем и очищаем данные...")
    df = load_and_clean("cian_results.json")

    print(f"Загружено записей: {len(df)}")
    print(f"Типы колонок:\n{df.dtypes}\n")

    print("Запускаем валидацию по схеме Pandera...")
    try:
        validated_df = schema.validate(df, lazy=True)
        print("✓ Все проверки пройдены успешно!")
        print(validated_df.describe())

    except pa.errors.SchemaErrors as e:
        print("✗ Найдены ошибки валидации:\n")
        failure_cases = e.failure_cases[["check", "failure_case", "index"]].drop_duplicates()
        print(failure_cases.to_string(index=False))