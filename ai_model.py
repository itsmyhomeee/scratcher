import os
from catboost import CatBoostRegressor
from langchain_openai import ChatOpenAI
from config import BASE_DIR, OPENAI_API_KEY, OPENAI_API_BASE

class RealEstateAI:
    def __init__(self):
        self.llm = ChatOpenAI(
            model="gpt-4o-mini",
            openai_api_key=OPENAI_API_KEY,
            openai_api_base=OPENAI_API_BASE,
            temperature=0
        )
        self.cat_model = None
        path = BASE_DIR / "rag_assets/catboost_model.cbm"
        if path.exists():
            self.cat_model = CatBoostRegressor()
            self.cat_model.load_model(str(path))