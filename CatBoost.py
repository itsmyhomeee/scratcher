import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score, mean_absolute_percentage_error, root_mean_squared_error


df = pd.read_parquet('cian_data.parquet')

df = df.drop(columns=["offer_id", 'url', 'address', 'description', 'photos_count'])

# Список категориальных признаков
cat_features = ['new_building', 'rooms', 'repair', 'house_class']

# Переводим True/False в 0/1
df['new_building'] = df['new_building'].astype(int)


for col in ['rooms', 'repair', 'house_class']:
    df[col] = df[col].astype(str)
    df[col] = df[col].replace(['<NA>', 'nan', 'None'], 'Unknown')
    df[col] = df[col].fillna('Unknown')

y = df["price"]
x = df.drop(columns=["price"])


X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.1, random_state=42)
print(X_train)

model = CatBoostRegressor(
    iterations=500,          
    learning_rate=0.1,
    depth=6,
    loss_function='MAE',   
    random_seed=42,
    verbose=0,     
    cat_features=cat_features
)

# Обучаем
print("Обучаем CatBoost...")
model.fit(X_train, y_train)
print("Обучение закончено.")

# Делаем предсказания на тесте
y_pred = model.predict(X_test)

# Считаем метрики
mae = mean_absolute_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)
mape = mean_absolute_percentage_error(y_test, y_pred)

# Выводим результаты
print(f"\nСредняя абсолютная ошибка (MAE): {mae:.2f}")
print(f"Коэффициент детерминации (R²): {r2:.4f}")
print(f"Средняя относительная ошибка (MAPE): {mape:.2%}")

#model.save_model("rag_assets/catboost_model.cbm")
#print("Модель сохранена в rag_assets/catboost_model.cbm")