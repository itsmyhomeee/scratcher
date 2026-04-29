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

def estimate_price(self, description: str) -> float:
        # 1. Поиск наиболее похожего объекта
        results_df = self.rag.find_similar(description, k=1)
        if results_df.empty:
            raise ValueError("Аналоги не найдены")

        row = results_df.iloc[0]

        # 2. Формируем DataFrame строго по списку признаков
        features = pd.DataFrame([row[FEATURE_COLUMNS]])
        
        # 3. Приведение типов (Синхронно с CatBoost.py)
        # new_building -> int
        if 'new_building' in features.columns:
            # Сначала в float, потом в int на случай, если там хранятся float-подобные строки
            features['new_building'] = pd.to_numeric(features['new_building'], errors='coerce').fillna(0).astype(int)
            
        # Категории -> str
        cat_cols_to_str = ['repair', 'house_class', 'rooms']
        for col in cat_cols_to_str:
            if col in features.columns:
                features[col] = features[col].astype(str).replace(['nan', 'None', '<NA>'], 'Unknown')

        # 4. Предсказание
        # Используем .values, чтобы CatBoost не ругался на названия колонок, если они не совпали
        prediction = self.ai.cat_model.predict(features)[0]
        return float(prediction)

        row = results_df.iloc[0]
        # Важно: выбираем только нужные колонки в правильном порядке
        features = pd.DataFrame([row[FEATURE_COLUMNS]])
        
        # Обработка типов для CatBoost
        for col in CAT_FEATURES:
            features[col] = features[col].astype(str).replace(['nan', 'None'], 'Unknown')
            
        return float(self.ai.cat_model.predict(features)[0])