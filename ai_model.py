import os
import pandas as pd
from catboost import CatBoostRegressor
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from rag import RagAgent

load_dotenv()


FEATURE_COLUMNS = [
    "square", "new_building", "photos_count", "repair",
    "lat", "lon", "rooms", "floors_total",
    "dist_to_center", "house_class", "floor",
]


class RealEstateAI:

    def __init__(self):
        # 1. RAG-поиск
        self.rag_agent = RagAgent()

        # 2. LLM через ProxyAPI
        self.llm = ChatOpenAI(
            model="gpt-4o-mini",
            openai_api_key= 'sk-Raf4dVXNkpR3DST3qajb4cUgk84qRFsb',
            openai_api_base="https://api.proxyapi.ru/openai/v1",
            temperature=0,
        )

        # 3. CatBoost
        base_dir   = os.path.dirname(os.path.abspath(__file__))
        model_path = os.path.join(base_dir, "rag_assets", "catboost_model.cbm")

        self.cat_model = CatBoostRegressor()
        if os.path.exists(model_path):
            self.cat_model.load_model(model_path)
            print("СatBoost загружен")
        else:
            print(f"Модель не найдена: {model_path}")
            self.cat_model = None

    def ask(self, question: str) -> str:
        """Основной метод: поиск → оценка CatBoost → ответ LLM."""

        results_df = self.rag_agent.find_similar(question, k=3)

        if results_df.empty:
            return "К сожалению, в базе нет подходящих объявлений."

        enriched_results = []
        for _, row in results_df.iterrows():
            # --- CatBoost оценка ---
            if self.cat_model is not None:
                missing = [c for c in FEATURE_COLUMNS if c not in row.index]
                if missing:
                    price_block = "Нет данных для оценки модели."
                else:
                    CAT_FEATURES = ['new_building', 'repair', 'house_class']
                    features = pd.DataFrame([row[FEATURE_COLUMNS]])
                    for col in CAT_FEATURES:
                        if col in features.columns:
                            features[col] = features[col].astype(str)
                    predicted_price = self.cat_model.predict(features)[0]
                    real_price  = row["price"]
                    diff_pct  = ((real_price - predicted_price) / predicted_price) * 100
                    status  = "Выгодно (ниже рынка)" if diff_pct < 0 else "Дорого (выше рынка)"
                    price_block = (
                        f" Цена: {real_price:,.0f} руб.\n"
                        f" Оценка модели: {predicted_price:,.0f} руб.\n"
                        f" {status} на {abs(diff_pct):.1f}%"
                    )
            else:
                price_block = f" Цена: {row['price']:,.0f} руб."

            enriched_results.append(
                f"🏠 Адрес: {row['address']}\n"
                f"{price_block}\n"
                f"🔗 {row['url']}"
            )

        context = "\n\n---\n\n".join(enriched_results)

        prompt = (
            f"Ты — эксперт-аналитик по недвижимости. "
            f"Пользователь ищет квартиру по запросу: «{question}»\n\n"
            f"Лучшие найденные варианты:\n{context}\n\n"
            f"Напиши краткий, профессиональный и дружелюбный обзор. "
            f"Акцентируй внимание на самом выгодном варианте. "
            f"Если варианты не совсем соответствуют запросу — вежливо упомяни об этом."
        )

        response = self.llm.invoke(prompt)
        return response.content


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    bot    = RealEstateAI()
    result = bot.ask("Найди квартиру у метро Планерная")
    print("\n--- ОТВЕТ БОТА ---")
    print(result)