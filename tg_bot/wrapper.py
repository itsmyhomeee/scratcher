import os
import sys
from pathlib import Path

# Добавляем корень проекта в path, чтобы импортировать rag и ai_model
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
from rag import RagAgent
from ai_model import RealEstateAI


class CianBotWrapper:

    def __init__(self):
        self.rag = RagAgent()
        self.ai  = RealEstateAI()


        base_dir    = os.path.dirname(os.path.abspath(__file__))
        parent_dir  = os.path.dirname(base_dir)
        parquet_path = os.path.join(parent_dir, "rag_assets", "metadata.parquet")

        self.df = pd.read_parquet(parquet_path)
        print(f"Бот готов! В базе {len(self.df)} объявлений")

    def find_similar(self, query: str, top_k: int = 3) -> list[dict]:
        """
        Семантический поиск квартир.
        Возвращает список словарей — router.py берёт отсюда данные напрямую.
        """
        results_df = self.rag.find_similar(query, k=top_k)

        formatted = []
        for _, row in results_df.iterrows():
            formatted.append({
                "address":  row.get("address", "Адрес не указан"),
                "price":  row.get("price", 0),
                "description": row.get("description", ""),
                "url": row.get("url", "https://cian.ru"),
                "rooms": row.get("rooms", "?"),       
                "area":        row.get("square", "?"),   
                "score":       row.get("similarity_score", 0.0), 
            })
        return formatted

    def get_CatBoost_answer(self, query: str) -> str:
        return self.ai.ask(query)

    def estimate_price(self, description: str) -> float:
   
        if self.ai.cat_model is None:
            raise RuntimeError("CatBoost модель не загружена")

        # Ищем похожую квартиру и используем её признаки для предсказания
        results_df = self.rag.find_similar(description, k=1)
        if results_df.empty:
            raise ValueError("Не удалось найти похожие объявления для оценки")

        feature_columns = [
            "square", "new_building", "photos_count", "repair",
            "lat", "lon", "rooms", "floors_total",
            "dist_to_center", "house_class", "floor",
        ]
        row  = results_df.iloc[0]
        features = pd.DataFrame([row[feature_columns]])
        price  = self.ai.cat_model.predict(features)[0]
        return float(price)