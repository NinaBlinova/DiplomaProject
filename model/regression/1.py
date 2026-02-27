# import pandas as pd
# from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
# import numpy as np
#
# # метрики для модели с предсказанием дохода
# df_pred_income = pd.read_csv(r"LightGBM/linear_income_modelpredictions.csv")
# y_test = df_pred_income['y_test'].values
# y_pred = df_pred_income['y_pred'].values
# mae = mean_absolute_error(y_test, y_pred)
# rmse = np.sqrt(mean_squared_error(y_test, y_pred))
# r2 = r2_score(y_test, y_pred)
# print("\nМетрики на сохранённых предсказаниях линейной регрессии (доход):")
# print(f"MAE  = {mae:,.2f}")
# print(f"RMSE = {rmse:,.2f}")
# print(f"R²   = {r2:.4f}")
# print("\n")
#
# # метрики для модели с предсказанием транзакций
# df_pred_transaction = pd.read_csv(
#     r"LightGBM/linear_transactions_modelpredictions.csv")
# y_test = df_pred_transaction['y_test'].values
# y_pred = df_pred_transaction['y_pred'].values
# mae = mean_absolute_error(y_test, y_pred)
# rmse = np.sqrt(mean_squared_error(y_test, y_pred))
# r2 = r2_score(y_test, y_pred)
# print("\nМетрики на сохранённых предсказаниях линейной регрессии (транзакции):")
# print(f"MAE  = {mae:,.2f}")
# print(f"RMSE = {rmse:,.2f}")
# print(f"R²   = {r2:.4f}")
# print("\n")
#
# # метрики для модели с предсказанием налога
# df_pred_tax = pd.read_csv(r"LightGBM/linear_tax_modelpredictions.csv")
# y_test = df_pred_tax['y_test'].values
# y_pred = df_pred_tax['y_pred'].values
# mae = mean_absolute_error(y_test, y_pred)
# rmse = np.sqrt(mean_squared_error(y_test, y_pred))
# r2 = r2_score(y_test, y_pred)
# print("\nМетрики на сохранённых предсказаниях линейной регрессии (налог):")
# print(f"MAE  = {mae:,.2f}")
# print(f"RMSE = {rmse:,.2f}")
# print(f"R²   = {r2:.4f}")
# print("\n")
#
# '''
# 'SZ',  'Самозанятый (НПД)', 'НПД', - 20 процентов
# 'IP6', 'ИП на УСН 6%', 'УСН_6', - 28 процентов
# 'IP15','ИП на УСН 15%', 'УСН_15', - 15 процентов
# 'IPOS','ИП на ОСНО', 'ОСНО', - 20 процентов
# 'IPP', 'ИП на патенте', 'ПАТЕНТ'; - 12 процентов
# '''

from model.AggregationService import AggregationService
from model.ForecastService import ForecastService
from model.TaxDataRepository import TaxDataRepository
from model.YearlyGrowthLoader import YearlyGrowthLoader
from model.YearlyLoader_by_month import YearlyStatsLoader
from model.database import DatabaseEngine
from routes.routes_models import initialize_predictions, get_current_model_info

db = DatabaseEngine()
repository = TaxDataRepository(db)
aggregator = AggregationService()

initialize_predictions()
model_name, model_version = get_current_model_info()
print(model_name)
print(model_version)
median_loader = YearlyStatsLoader(db, repository, aggregator, model_name, model_version)
general_loader = YearlyStatsLoader(db, repository, aggregator, model_name, model_version)

median_loader.load_monthly('median')
general_loader.load_monthly('sum')
loader = YearlyGrowthLoader(db, repository, aggregator, model_name, model_version)
loader.load_general_growth()
loader.load_median_growth()
