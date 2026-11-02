from sklearn.model_selection import GridSearchCV
from catboost import CatBoostRegressor
from model_test import X_train, y_train, X_test, y_test, cat_features
from sklearn.metrics import mean_absolute_error, r2_score

param_grid = {
    'iterations': [100, 250, 500],     # Количество деревьев
    'learning_rate': [0.01, 0.05, 0.1], # Скорость обучения
    'depth': [4, 6, 8]                  # Глубина деревьев
}

# Базовая модель (без параметров, которые будем подбирать)
base_model = CatBoostRegressor(
    loss_function='MAE',
    random_seed=42,
    verbose=0
)

# Настраиваем перебор с перекрестной проверкой
grid_search = GridSearchCV(
    estimator=base_model,
    param_grid=param_grid,
    cv=3,                              # 3-кратная кросс-валидация
    scoring='neg_mean_absolute_error', # Ориентируемся на MAE
    n_jobs=-1,                         # Задействуем все ядра процессора
    verbose=2
)

print("Начинаем подбор параметров через GridSearchCV...")
# Запускаем перебор, передаем категориальные признаки
grid_search.fit(X_train, y_train, cat_features=cat_features)
print("Подбор параметров завершен.")

# Смотрим, что получилось
print("\nЛучшие параметры:", grid_search.best_params_)
print("Лучшее значение MAE (с минусом, потому так принято в sklearn):", grid_search.best_score_)

# Забираем лучшую модель
best_model = grid_search.best_estimator_

# Предсказываем на тестовой выборке
y_pred_best = best_model.predict(X_test)

# Считаем метрики для лучшей модели
mae_best = mean_absolute_error(y_test, y_pred_best)
r2_best = r2_score(y_test, y_pred_best)

print(f"\nСредняя абсолютная ошибка (MAE) на лучших параметрах: {mae_best:.2f}")
print(f"Коэффициент детерминации (R²) на лучших параметрах: {r2_best:.4f}")