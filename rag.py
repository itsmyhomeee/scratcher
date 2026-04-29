import os
import faiss
import pandas as pd
from sentence_transformers import SentenceTransformer


class RagAgent:

    def __init__(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))

        index_path = os.path.join(base_dir, "rag_assets", "apartments.index")
        meta_path  = os.path.join(base_dir, "rag_assets", "metadata.parquet")

        self.index    = faiss.read_index(index_path)
        self.metadata = pd.read_parquet(meta_path)

        # Та же модель, что использовалась при индексации
        self.model = SentenceTransformer('intfloat/multilingual-e5-small')
        print(f"RagAgent готов. Документов в индексе: {self.index.ntotal}")


    def find_similar(self, user_query: str, k: int = 3) -> pd.DataFrame:

        query_vector = self.model.encode(
            ["query: " + user_query],
            normalize_embeddings=True,
        ).astype("float32")

        distances, indices = self.index.search(query_vector, k)

        results = self.metadata.iloc[indices[0]].copy()
        results["similarity_score"] = distances[0]

        return results

    def get_relevant_documents(self, query: str, k: int = 3) -> list[str]:
        """Возвращает список строк для передачи в LLM."""
        results = self.find_similar(query, k=k)

        if results.empty:
            return []

        docs = []
        for _, row in results.iterrows():
            docs.append(
                f"ОПИСАНИЕ: {row['description']}\n"
                f"ЦЕНА: {row['price']:,.0f} руб."
            )
        return docs

    def validate_price(self, apartment_row: pd.Series, catboost_model) -> tuple:
        """Сравнивает реальную цену с предсказанием CatBoost."""
        predicted_price = catboost_model.predict(
            pd.DataFrame([apartment_row])
        )[0]
        actual_price = apartment_row["price"]
        diff = ((actual_price - predicted_price) / predicted_price) * 100
        return predicted_price, diff


if __name__ == "__main__":
    rag = RagAgent()
    query = "Найди квартиру в районе Крылатское"
    found = rag.find_similar(query, k=3)

    for i, (_, row) in enumerate(found.iterrows(), 1):
        print(f"\n--- Вариант №{i} ---")
        print(f"Адрес:    {row['address']}")
        print(f"Цена:     {row['price']:,} руб.")
        print(f"Ссылка:   {row['url']}")
        print(f"Схожесть: {row['similarity_score']:.4f}")
        print(f"Описание: {row['description'][:150]}...")