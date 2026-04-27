import pandas as pd
import numpy as np
from catboost import CatBoostRegressor, Pool
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score
from math import radians, sin, cos, sqrt, atan2 


def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))

# Кремль
MOSCOW_CENTER = (55.7520, 37.6175)


# ==================================================================
# 1. ЗАГРУЗКА И ПОДГОТОВКА
# ==================================================================

df = pd.read_parquet("cian_data.parquet")

FEATURES = [
    "square",
    "rooms", 
    "repair",
    "lat",
    "lon",
    "geo_cluster",     # ← новый, категориальный
    "new_building",
    "photos_count",
]
TARGET   = "price"

from sklearn.cluster import KMeans

coords = df[["lat", "lon"]].dropna()
kmeans = KMeans(n_clusters=30, random_state=42)
df.loc[coords.index, "geo_cluster"] = kmeans.fit_predict(coords)

# Передать в CatBoost как категориальный:
cat_features = ["geo_cluster"]

# Убираем строки без таргета
df = df.dropna(subset=[TARGET])
df["dist_to_center"] = df.apply(
    lambda r: haversine(r["lat"], r["lon"], *MOSCOW_CENTER)
    if pd.notna(r["lat"]) else None, axis=1
)
# Убираем выбросы по цене (верхние и нижние 1%)
q_low  = df[TARGET].quantile(0.01)
q_high = df[TARGET].quantile(0.99)
df = df[df[TARGET].between(q_low, q_high)]

X = df[FEATURES]
y = df[TARGET]

print(f"Обучающая выборка: {len(df)} записей")
print(f"Пропуски в фичах:\n{X.isna().sum()}\n")

# ==================================================================
# 2. СПЛИТ
# ==================================================================

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# ==================================================================
# 3. CATBOOST POOL
# Передаём список колонок с пропусками — CatBoost обработает сам
# ==================================================================

cat_features = ["geo_cluster"]  # у нас нет категориальных,

# CatBoost нативно работает с NaN — ничего заполнять не нужно
train_pool = Pool(X_train, y_train)
test_pool  = Pool(X_test,  y_test)

# ==================================================================
# 4. ОБУЧЕНИЕ
# ==================================================================

model = CatBoostRegressor(
    iterations=1000,
    learning_rate=0.05,
    depth=6,
    loss_function="RMSE",
    eval_metric="MAPE",       # удобно для цен — показывает % ошибки
    early_stopping_rounds=50, # остановка если val-метрика не улучшается
    random_seed=42,
    verbose=100,
)

model.fit(
    train_pool,
    eval_set=test_pool,
    use_best_model=True,
)

# ==================================================================
# 5. МЕТРИКИ
# ==================================================================

y_pred = model.predict(X_test)

mae  = mean_absolute_error(y_test, y_pred)
mape = np.mean(np.abs((y_test - y_pred) / y_test)) * 100
r2   = r2_score(y_test, y_pred)

print(f"\n{'='*40}")
print(f"MAE:  {mae:,.0f} ₽")
print(f"MAPE: {mape:.1f}%")
print(f"R²:   {r2:.3f}")
print(f"{'='*40}")

# ==================================================================
# 6. ВАЖНОСТЬ ПРИЗНАКОВ
# ==================================================================

importance = pd.Series(
    model.get_feature_importance(),
    index=FEATURES
).sort_values(ascending=False)

print(f"\nВажность признаков:\n{importance.round(2)}")

# ==================================================================
# 7. СОХРАНЕНИЕ
# ==================================================================