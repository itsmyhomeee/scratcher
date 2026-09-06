import pandas as pd
from config import BASE_DIR, FEATURE_COLUMNS, CAT_FEATURES
from rag import RagAgent
from ai_model import RealEstateAI

class CianBotWrapper:
    def __init__(self):
        self.rag = RagAgent()
        self.ai = RealEstateAI()

    def find_similar(self, query: str, top_k: int = 3):
        results_df = self.rag.find_similar(query, k=top_k)
        return results_df.to_dict('records')

import os
import sys
import pandas as pd
from pathlib import Path

# Добавляем корень проекта в path, чтобы импортировать rag и ai_model
sys.path.append(str(Path(__file__).parent.parent))

from rag import RagAgent
from ai_model import RealEstateAI
from config import FEATURE_COLUMNS, CAT_FEATURES

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

    def estimate_price(self, description: str) -> float:
        """Метод для оценки стоимости квартиры"""
        if self.ai.cat_model is None:
            raise RuntimeError("CatBoost модель не загружена")

        # 1. Ищем похожую квартиру через RAG
        results_df = self.rag.find_similar(description, k=1)
        if results_df.empty:
            raise ValueError("Аналоги для оценки не найдены")

        row = results_df.iloc[0]

        # 2. Формируем DataFrame строго по колонкам из обучения
        features = pd.DataFrame([row[FEATURE_COLUMNS]])
        
        # 3. Приведение типов (как в CatBoost.py)
        if 'new_building' in features.columns:
            features['new_building'] = pd.to_numeric(features['new_building'], errors='coerce').fillna(0).astype(int)
            
        for col in ['repair', 'house_class', 'rooms']:
            if col in features.columns:
                features[col] = features[col].astype(str).replace(['nan', 'None', '<NA>'], 'Unknown')

        # 4. Предсказание
        prediction = self.ai.cat_model.predict(features)[0]
        return float(prediction)