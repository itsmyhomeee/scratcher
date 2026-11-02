import pandas as pd
import faiss
from sentence_transformers import SentenceTransformer
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
ASSETS_DIR = BASE_DIR / "rag_assets"

df = pd.read_parquet(BASE_DIR / "cian_data.parquet")

#  Очистка и сброс индекса
df = df.reset_index(drop=True)


# Та же модель, что читает индекс в rag.py
model = SentenceTransformer('intfloat/multilingual-e5-small')
df['description'] = df['description'].str.replace('\n', ' ', regex=False)

embeddings = model.encode(
    ("passage: " + df['description'].fillna("")).tolist(),
    normalize_embeddings=True,
    show_progress_bar=True,
)

# Индексация
index = faiss.IndexFlatIP(embeddings.shape[1])
index.add(embeddings.astype('float32'))

ASSETS_DIR.mkdir(exist_ok=True)

faiss.write_index(index, str(ASSETS_DIR / "apartments.index"))
df.to_parquet(ASSETS_DIR / "metadata.parquet")

print("Готово! Файлы лежат в папке rag_assets.")