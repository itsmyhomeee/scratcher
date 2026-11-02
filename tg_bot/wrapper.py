import os
import sys
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from pathlib import Path

# Добавляем корень проекта в path, чтобы импортировать rag и ai_model
sys.path.append(str(Path(__file__).parent.parent))

from rag import RagAgent
from ai_model import RealEstateAI, ApartmentParams
from config import FEATURE_COLUMNS, CAT_FEATURES, REPAIR_RATING, HOUSE_CLASS
from geo import MOSCOW_CENTER, geocode_moscow, haversine, nearby_points


@dataclass
class PriceEstimate:
    params: ApartmentParams
    price: float | None = None
    low: float | None = None
    high: float | None = None
    missing: list[str] = field(default_factory=list)


class CianBotWrapper:
    def __init__(self):
        self.rag = RagAgent()
        self.ai = RealEstateAI()
        
        # Загружаем базу для информации (необязательно, но полезно для логов)
        base_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(base_dir)
        parquet_path = os.path.join(parent_dir, "rag_assets", "metadata.parquet")
        
        if os.path.exists(parquet_path):
            self.df = pd.read_parquet(parquet_path)
            print(f"Бот готов! В базе {len(self.df)} объявлений")
        else:
            print("Предупреждение: metadata.parquet не найден")

    def find_similar(self, query: str, top_k: int = 3) -> list[dict]:
        results_df = self.rag.find_similar(query, k=top_k)
        formatted = []
        for _, row in results_df.iterrows():
            formatted.append({
                "address": row.get("address", "Адрес не указан"),
                "price": row.get("price", 0),
                "description": row.get("description", ""),
                "url": row.get("url", "https://cian.ru"),
                "rooms": row.get("rooms", "?"),
                "area": row.get("square", "?"),
                "score": row.get("similarity_score", 0.0),
            })
        return formatted

    def estimate_price(self, description: str) -> PriceEstimate:
        """Оценивает квартиру по параметрам из описания пользователя."""
        if self.ai.cat_model is None:
            raise RuntimeError("CatBoost модель не загружена")

        params = self.ai.extract_params(description)

        # Площадь и место сильнее всего влияют на цену, без них оценка бессмысленна
        missing = []
        square = params.square if params.square and 5 <= params.square <= 1000 else None
        if square is None:
            missing.append("общую площадь в м²")
        coords = geocode_moscow(params.location) if params.location else None
        if coords is None:
            missing.append("район, улицу или станцию метро в Москве")
        if missing:
            return PriceEstimate(missing=missing, params=params)

        lat, lon = coords
        meta = self.rag.metadata
        floor = params.floor if params.floor and params.floor > 0 else None
        floors_total = params.floors_total if params.floors_total and params.floors_total > 0 else None
        # Этаж модель обучена видеть всегда, поэтому пропуск заменяем типичным значением
        floors_total = floors_total or max(int(meta["floors_total"].median()), floor or 1)
        floor = floor or min(int(meta["floor"].median()), floors_total)
        rooms = params.rooms if params.rooms and 1 <= params.rooms <= 10 else None

        # Категории приводим к тому же виду, что и при обучении в CatBoost.py
        features = pd.DataFrame([{
            "square": square,
            "new_building": int(bool(params.new_building)),
            "repair": str(REPAIR_RATING[params.repair]) if params.repair else "Unknown",
            "lat": point_lat,
            "lon": point_lon,
            "rooms": str(rooms) if rooms else "Unknown",
            "floors_total": floors_total,
            "dist_to_center": haversine(point_lat, point_lon, *MOSCOW_CENTER),
            "house_class": str(HOUSE_CLASS[params.house_type]) if params.house_type else "Unknown",
            "floor": floor,
        } for point_lat, point_lon in nearby_points(lat, lon)])[FEATURE_COLUMNS]

        # В центре цена модели скачет в разы на расстоянии нескольких сотен метров,
        # поэтому одной точке верить нельзя: берём медиану и разброс по соседним
        low, price, high = np.percentile(self.ai.cat_model.predict(features), [25, 50, 75])
        return PriceEstimate(price=float(price), low=float(low), high=float(high), params=params)
