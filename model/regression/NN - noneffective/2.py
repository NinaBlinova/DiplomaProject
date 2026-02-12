import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import numpy as np

# модель с искусственным интелектом доход
df_pred_nn = pd.read_csv(r"C:\Users\blino\DiplomaProject\model\regression\nn_income_predictions.csv")
y_test = df_pred_nn['y_true'].values
y_pred = df_pred_nn['y_pred'].values
# Вычисляем метрики
mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)
print("\nМетрики на сохранённых предсказаниях модели с нейронной сетью:")
print(f"MAE  = {mae:,.2f}")
print(f"RMSE = {rmse:,.2f}")
print(f"R²   = {r2:.4f}")
print("\n")

# модель с искусственным интелектом транзакции
df_pred_nn = pd.read_csv(r"C:\Users\blino\DiplomaProject\model\regression\nn_transactions_predictions.csv")
y_test = df_pred_nn['y_true'].values
y_pred = df_pred_nn['y_pred'].values
# Вычисляем метрики
mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)
print("\nМетрики на сохранённых предсказаниях модели с нейронной сетью:")
print(f"MAE  = {mae:,.2f}")
print(f"RMSE = {rmse:,.2f}")
print(f"R²   = {r2:.4f}")
print("\n")


