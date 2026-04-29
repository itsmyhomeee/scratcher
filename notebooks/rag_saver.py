import pandas as pd
import faiss
from sentence_transformers import SentenceTransformer
import os


df = pd.read_parquet("D:\VS CODE\scratcher\cian_data.parquet")

#  Очистка и сброс индекса
df = df.reset_index(drop=True)


model = SentenceTransformer('cointegrated/rubert-tiny2')
model.save("rag_assets/my_transformer_model")
df['description'] = df['description'].str.replace('\n', ' ', regex=False)

embeddings = model.encode(df['description'].fillna("").tolist(), show_progress_bar=True)

# Индексация
index = faiss.IndexFlatL2(embeddings.shape[1])
index.add(embeddings.astype('float32'))

os.makedirs("rag_assets", exist_ok=True)

faiss.write_index(index, "rag_assets/apartments.index")
df.to_parquet("rag_assets/metadata.parquet")

print("Готово! Файлы лежат в папке rag_assets.")