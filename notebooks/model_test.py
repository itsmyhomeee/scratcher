import numpy as np
import pandas as pd
from catboost import CatBoostRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score, mean_absolute_percentage_error, root_mean_squared_error
from pathlib import Path

# Подгружаем данные
df = pd.read_parquet(Path(__file__).parent.parent / 'cian_data.parquet')

# Убираем лишние колонки, которые не понадобятся для модели
df = df.drop(columns=["offer_id", 'url', 'address', 'description', 'photos_count'])

# Список категориальных признаков
cat_features = ['new_building', 'rooms', 'repair', 'house_class']

# Переводим True/False в 0/1
df['new_building'] = df['new_building'].astype(int)


for col in ['rooms', 'repair', 'house_class']:
    df[col] = df[col].astype(str)
    df[col] = df[col].replace(['<NA>', 'nan', 'None'], 'Unknown')
    df[col] = df[col].fillna('Unknown')

# Целевая переменная — цена
y = df["price"]
# Все остальное — признаки
x = df.drop(columns=["price"])

# Разбиваем на обучающую и тестовую выборки (10% на тест)
X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.1, random_state=42)

# grid search.py импортирует отсюда данные, поэтому обучаем только при прямом запуске
if __name__ == "__main__":
    # Настраиваем модель CatBoost
    model = CatBoostRegressor(
        iterations=500,          # Количество деревьев
        learning_rate=0.1,
        depth=6,
        loss_function='MAE',     # Средняя абсолютная ошибка (так понятнее для цен)
        random_seed=42,
        verbose=0,               # Чтобы не выводил кучу мусора при обучении
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
