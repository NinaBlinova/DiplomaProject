import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import numpy as np

# модель с искусственным интелектом
df_pred_nn = pd.read_csv(r"C:\Users\blino\DiplomaProject\model\regression\nn_tax_predictions.csv")
y_test = df_pred_nn['y_test'].values
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

# метрики для модели с предсказанием дохода
df_pred_income = pd.read_csv(r"C:\Users\blino\DiplomaProject\model\regression\linear_income_modelpredictions.csv")
y_test = df_pred_income['y_test'].values
y_pred = df_pred_income['y_pred'].values
mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)
print("\nМетрики на сохранённых предсказаниях линейной регрессии (доход):")
print(f"MAE  = {mae:,.2f}")
print(f"RMSE = {rmse:,.2f}")
print(f"R²   = {r2:.4f}")
print("\n")

# метрики для модели с предсказанием транзакций
df_pred_transaction = pd.read_csv(
    r"C:\Users\blino\DiplomaProject\model\regression\linear_transactions_modelpredictions.csv")
y_test = df_pred_transaction['y_test'].values
y_pred = df_pred_transaction['y_pred'].values
mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)
print("\nМетрики на сохранённых предсказаниях линейной регрессии (транзакции):")
print(f"MAE  = {mae:,.2f}")
print(f"RMSE = {rmse:,.2f}")
print(f"R²   = {r2:.4f}")
print("\n")

# метрики для модели с предсказанием налога
df_pred_tax = pd.read_csv(r"C:\Users\blino\DiplomaProject\model\regression\linear_tax_modelpredictions.csv")
y_test = df_pred_tax['y_test'].values
y_pred = df_pred_tax['y_pred'].values
mae = mean_absolute_error(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)
print("\nМетрики на сохранённых предсказаниях линейной регрессии (налог):")
print(f"MAE  = {mae:,.2f}")
print(f"RMSE = {rmse:,.2f}")
print(f"R²   = {r2:.4f}")
print("\n")
