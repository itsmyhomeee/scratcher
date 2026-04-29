import faiss
import pandas as pd
from sentence_transformers import SentenceTransformer

class RagAgent:
    def __init__(self, index_path="apartments.index", meta_path="metadata.parquet"):
        self.model = SentenceTransformer("rag_assets/my_transformer_model")
        
        self.index = faiss.read_index(index_path)
        
        # 3. Подгружаем метаданные (наш DF с индексами 0..N)
        self.metadata = pd.read_parquet(meta_path)
        
    def find_similar(self, user_query, k=3):
        """
        Находит k самых похожих объявлений по текстовому запросу.
        """
        # Превращаем запрос пользователя в вектор
        query_vector = self.model.encode([user_query]).astype('float32')
        
        # Ищем в FAISS (возвращает расстояния и индексы строк)
        distances, indices = self.index.search(query_vector, k)
        
        # Вытаскиваем соответствующие строки из метаданных
        results = self.metadata.iloc[indices[0]].copy()
        results['similarity_score'] = distances[0]
        
        return results

    def validate_price(self, apartment_row, catboost_model):
        """
        Здесь будет вызываться твоя модель CatBoost для оценки стоимости
        """
        # Предсказываем цену для найденной квартиры
        # apartment_row — это строка из нашего DF
        predicted_price = catboost_model.predict(apartment_row) 
        actual_price = apartment_row['price']
        
        diff = ((actual_price - predicted_price) / predicted_price) * 100
        return predicted_price, diff

# Пример использования:
if __name__ == "__main__":
    rag = RagAgent(
        index_path="rag_assets/apartments.index", 
        meta_path="rag_assets/metadata.parquet"
    )
    query = "Хочу светлую квартиру с евроремонтом рядом с парком"
    
    found_apartments = rag.find_similar(query, k=2)
    
    for i, row in found_apartments.iterrows():
        print(f"\n--- Вариант №{i+1} ---")
        print(f"Адрес: {row['address']}")
        print(f"Цена: {row['price']:,} руб.")
        print(f'Ссылка: {row["url"]}')
        print(f"Описание: {row['description'][:150]}...")