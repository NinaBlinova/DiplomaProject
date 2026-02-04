# import joblib
# import pyodbc
# import pandas as pd
# import numpy as np
#
# from sklearn.compose import ColumnTransformer
# from sklearn.linear_model import LinearRegression
# from sklearn.model_selection import train_test_split
# from sklearn.pipeline import Pipeline
# from sklearn.preprocessing import OneHotEncoder
# from sklearn.impute import SimpleImputer
# from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
#
# # =====================
# # Подключение к БД
# # =====================
# conn = pyodbc.connect(
#     "DRIVER={ODBC Driver 17 for SQL Server};"
#     "Server=localhost;"
#     "Database=Taxpayer_Database_DiplomaProject;"
#     "Trusted_Connection=yes;"
# )
#
# query = """
# SELECT
#     m.Year,
#     m.Month,
#     m.TaxType,
#     m.IncomeAmount,
#     m.TaxAmount,
#     m.transactions_count,
#     t.INN,
#     t.TaxpayerType,
#     t.activity_type,
#     t.registration_district,
#     t.has_employees,
#     t.employees_count
# FROM MonthlyTaxData m
# JOIN Taxpayer t ON m.TaxpayerId = t.TaxpayerId
# """
#
# df = pd.read_sql(query, conn)
# conn.close()
#
# # =====================
# # Сортировка и сезонность
# # =====================
# df = df.sort_values(['INN', 'Year', 'Month'])
#
# df['month_sin'] = np.sin(2 * np.pi * df['Month'] / 12)
# df['month_cos'] = np.cos(2 * np.pi * df['Month'] / 12)
#
# # =====================
# # ЛАГОВЫЕ ПРИЗНАКИ
# # =====================
# df['income_lag_1'] = df.groupby('INN')['IncomeAmount'].shift(1)
# df['income_lag_12'] = df.groupby('INN')['IncomeAmount'].shift(12)
# df['transaction_lag_1'] = df.groupby('INN')['transactions_count'].shift(1)
# df['transaction_lag_12'] = df.groupby('INN')['transactions_count'].shift(12)
# df['tax_lag_1'] = df.groupby('INN')['TaxAmount'].shift(1)
#
# # =====================
# # ЛОГ-ТРАНСФОРМАЦИЯ
# # =====================
# df['Income_log'] = np.log1p(df['IncomeAmount'])
# df['Tax_log'] = np.log1p(df['TaxAmount'])
# df['Transaction_log'] = np.log1p(df['transactions_count'])
#
# # Убираем строки без лагов
# df = df.dropna()
#
# # =====================
# # Признаки
# # =====================
# categorical_features = [
#     'TaxType', 'TaxpayerType',
#     'activity_type', 'registration_district'
# ]
#
# numeric_features = [
#     'Year',
#     'month_sin', 'month_cos',
#     'has_employees', 'employees_count',
#     'income_lag_1', 'income_lag_12'
# ]
#
# features = categorical_features + numeric_features
#
# X = df[features]
# y_income = df['Income_log']
# y_tax = df['Tax_log']
# y_transactions = df['Transaction_log']
#
# # =====================
# # Препроцессинг
# # =====================
# categorical_transformer = Pipeline([
#     ('imputer', SimpleImputer(strategy='most_frequent')),
#     ('onehot', OneHotEncoder(handle_unknown='ignore'))
# ])
#
# numeric_transformer = Pipeline([
#     ('imputer', SimpleImputer(strategy='median'))
# ])
#
# preprocessor = ColumnTransformer([
#     ('cat', categorical_transformer, categorical_features),
#     ('num', numeric_transformer, numeric_features)
# ])
#
#
# # =====================
# # Обучение
# # =====================
# def train_model(X, y, name):
#     X_train, X_test, y_train, y_test = train_test_split(
#         X, y, test_size=0.2, random_state=42
#     )
#
#     model = Pipeline([
#         ('preprocessor', preprocessor),
#         ('regressor', LinearRegression())
#     ])
#
#     model.fit(X_train, y_train)
#     y_pred = model.predict(X_test)
#
#     print(f"\n{name}")
#     print("MAE :", mean_absolute_error(y_test, y_pred))
#     print("RMSE:", np.sqrt(mean_squared_error(y_test, y_pred)))
#     print("R²  :", r2_score(y_test, y_pred))
#
#     joblib.dump(model, f"{name}.pkl")
#     print(f"Сохранено: {name}.pkl")
#
#     return model
#
#
# income_model = train_model(X, y_income, "new_income_model")
# tax_model = train_model(X, y_tax, "new_tax_model")
# transactions_model = train_model(X, y_transactions, "new_transactions_model")


import joblib
import pyodbc
import pandas as pd
import numpy as np
import plotly.express as px

# Загрузка моделей
income_model = joblib.load("new_income_model.pkl")
tax_model = joblib.load("new_tax_model.pkl")
transactions_model = joblib.load("new_transactions_model.pkl")

# =====================
# Подключение к БД и загрузка данных
# =====================
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=Taxpayer_Database_DiplomaProject;"
    "Trusted_Connection=yes;"
)

last_data = pd.read_sql("""
SELECT
    t.TaxpayerId,
    t.FullName,
    t.INN,
    t.TaxpayerType,
    t.activity_type,
    t.registration_district,
    t.has_employees,
    t.employees_count,
    m.TaxType,
    m.Year,
    m.Month,
    m.IncomeAmount,
    m.TaxAmount,
    m.transactions_count  -- ВАЖНО: добавляем количество транзакций
FROM MonthlyTaxData m
JOIN Taxpayer t ON m.TaxpayerId = t.TaxpayerId
""", conn)

conn.close()

# Сортировка данных
last_data = last_data.sort_values(['INN', 'Year', 'Month'])

# Берем последние 13 месяцев для расчета лагов (нужен lag_12)
last_records = last_data.groupby('INN').tail(13)

# Определяем год для прогноза
next_year = last_records['Year'].max() + 1

future_rows = []

# =====================
# РЕКУРСИВНЫЙ ПРОГНОЗ С ТРАНЗАКЦИЯМИ
# =====================
for inn, group in last_records.groupby('INN'):
    # Инициализация лаговых значений
    # Берем последние 13 записей, чтобы получить lag_1 и lag_12
    if len(group) >= 13:
        # Для доходов
        last_income = group.iloc[-1]['IncomeAmount']
        income_12_back = group.iloc[-13]['IncomeAmount']

        # Для транзакций
        last_transaction = group.iloc[-1]['transactions_count']
        transaction_12_back = group.iloc[-13]['transactions_count']

        # Для налогов (требует только lag_1 в модели)
        last_tax = group.iloc[-1]['TaxAmount']
    else:
        # Если данных недостаточно, используем последние доступные
        last_income = group.iloc[-1]['IncomeAmount']
        income_12_back = group.iloc[0]['IncomeAmount']

        last_transaction = group.iloc[-1]['transactions_count']
        transaction_12_back = group.iloc[0]['transactions_count']

        last_tax = group.iloc[-1]['TaxAmount']

    base = group.iloc[-1]

    for month in range(1, 13):
        # Сезонные признаки
        month_sin = np.sin(2 * np.pi * month / 12)
        month_cos = np.cos(2 * np.pi * month / 12)

        # Формирование строки для предсказания
        row = {
            'Year': next_year,
            'month_sin': month_sin,
            'month_cos': month_cos,
            'has_employees': base.has_employees,
            'employees_count': base.employees_count,
            'income_lag_1': last_income,
            'income_lag_12': income_12_back,
            'transaction_lag_1': last_transaction,  # Добавляем lag для транзакций
            'transaction_lag_12': transaction_12_back,  # Добавляем lag для транзакций
            'TaxType': base.TaxType,
            'TaxpayerType': base.TaxpayerType,
            'activity_type': base.activity_type,
            'registration_district': base.registration_district
        }

        X_row = pd.DataFrame([row])

        # Прогноз доходов
        pred_log_income = income_model.predict(X_row)[0]
        pred_income = np.expm1(pred_log_income)

        # Прогноз налогов (использует lag_1 от TaxAmount, который мы должны обновлять)
        # В вашей модели используется tax_lag_1, но он не входит в признаки
        # Нужно либо добавить его в row, либо убедиться, что модель его не использует
        pred_log_tax = tax_model.predict(X_row)[0]
        pred_tax = np.expm1(pred_log_tax)

        # Прогноз количества транзакций
        pred_log_transactions = transactions_model.predict(X_row)[0]
        pred_transactions = np.expm1(pred_log_transactions)

        # Обновление лагов для следующего месяца
        income_12_back = last_income
        last_income = max(0, pred_income)

        transaction_12_back = last_transaction
        last_transaction = max(0, pred_transactions)

        last_tax = max(0, pred_tax)

        future_rows.append({
            'INN': inn,
            'TaxpayerId': base.TaxpayerId,
            'FullName': base.FullName,
            'Month': month,
            'Year': next_year,
            'PredictedIncome': max(0, pred_income),
            'PredictedTax': max(0, pred_tax),
            'PredictedTransactions': max(0, pred_transactions),
            'TaxType': base.TaxType,
            'TaxpayerType': base.TaxpayerType,
            'activity_type': base.activity_type
        })

future_df = pd.DataFrame(future_rows)

# =====================
# ВИЗУАЛИЗАЦИЯ
# =====================
# 1. Прогноз доходов по месяцам
monthly_income = future_df.groupby('Month')['PredictedIncome'].sum().reset_index()
monthly_income['Metric'] = 'Income'

# 2. Прогноз налогов по месяцам
monthly_tax = future_df.groupby('Month')['PredictedTax'].sum().reset_index()
monthly_tax['Metric'] = 'Tax'

# 3. Прогноз транзакций по месяцам
monthly_transactions = future_df.groupby('Month')['PredictedTransactions'].sum().reset_index()
monthly_transactions['Metric'] = 'Transactions'

# Объединяем для визуализации
combined_df = pd.concat([
    monthly_income.rename(columns={'PredictedIncome': 'Value'}),
    monthly_tax.rename(columns={'PredictedTax': 'Value'}),
    monthly_transactions.rename(columns={'PredictedTransactions': 'Value'})
])

# График 1: Все метрики вместе
fig1 = px.line(
    combined_df,
    x='Month',
    y='Value',
    color='Metric',
    title=f'Прогноз на {next_year} год: Доходы, Налоги и Количество транзакций',
    markers=True,
    labels={'Value': 'Сумма / Количество', 'Month': 'Месяц'}
)
fig1.show()

# График 2: Только транзакции
fig2 = px.line(
    monthly_transactions,
    x='Month',
    y='PredictedTransactions',
    title=f'Прогноз количества транзакций на {next_year} год',
    markers=True,
    labels={'PredictedTransactions': 'Количество транзакций', 'Month': 'Месяц'}
)
fig2.show()

# График 3: Top-10 налогоплательщиков по прогнозируемым доходам
top_taxpayers = future_df.groupby(['INN', 'FullName'])['PredictedIncome'].sum().reset_index()
top_taxpayers = top_taxpayers.sort_values('PredictedIncome', ascending=False).head(10)

fig3 = px.bar(
    top_taxpayers,
    x='FullName',
    y='PredictedIncome',
    title=f'Top-10 налогоплательщиков по прогнозируемым доходам на {next_year} год',
    labels={'PredictedIncome': 'Прогнозируемый доход', 'FullName': 'Налогоплательщик'}
)
fig3.show()

# =====================
# СОХРАНЕНИЕ РЕЗУЛЬТАТОВ
# =====================
# Сохраняем полный прогноз
future_df.to_csv(f"forecast_{next_year}_full.csv", index=False, encoding='utf-8-sig')

# Сводная таблица по месяцам
summary_monthly = future_df.groupby('Month').agg({
    'PredictedIncome': 'sum',
    'PredictedTax': 'sum',
    'PredictedTransactions': 'sum',
    'INN': 'nunique'
}).reset_index()
summary_monthly = summary_monthly.rename(columns={'INN': 'UniqueTaxpayers'})
summary_monthly.to_csv(f"forecast_{next_year}_monthly_summary.csv", index=False, encoding='utf-8-sig')

# Сводная таблица по типам налогоплательщиков
summary_by_type = future_df.groupby('TaxpayerType').agg({
    'PredictedIncome': 'sum',
    'PredictedTax': 'sum',
    'INN': 'nunique'
}).reset_index()
summary_by_type = summary_by_type.rename(columns={'INN': 'UniqueTaxpayers'})
summary_by_type.to_csv(f"forecast_{next_year}_by_taxpayer_type.csv", index=False, encoding='utf-8-sig')

print(f"Прогноз успешно сгенерирован для {next_year} года!")
print(f"Количество записей: {len(future_df)}")
print(f"Уникальных налогоплательщиков: {future_df['INN'].nunique()}")
print(f"Общий прогнозируемый доход: {future_df['PredictedIncome'].sum():,.2f}")
print(f"Общий прогнозируемый налог: {future_df['PredictedTax'].sum():,.2f}")
print(f"Общее прогнозируемое количество транзакций: {future_df['PredictedTransactions'].sum():,.0f}")
