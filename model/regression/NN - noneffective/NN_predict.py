import pandas as pd
import numpy as np
import joblib
import tensorflow as tf
from sqlalchemy import create_engine

# =========================
# Подключение к БД
# =========================
engine = create_engine(
    "mssql+pyodbc://@localhost/Taxpayer_Database_DiplomaProject?"
    "trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
)

query = """
SELECT
    m.RecordId,
    m.Year,
    m.Month,
    m.TaxType,
    m.IncomeAmount,
    m.TaxAmount,
    m.season,
    m.transactions_count,
    t.FullName,
    t.INN,
    t.TaxpayerType,
    t.activity_type,
    t.registration_district,
    t.has_employees,
    t.employees_count
FROM MonthlyTaxData m
INNER JOIN Taxpayer t
    ON m.TaxpayerId = t.TaxpayerId;
"""

df = pd.read_sql(query, engine)
MIN_INCOME = 5_000
MAX_INCOME = 500_000

MIN_TXN = 1
MAX_TXN = 500

MAX_TAX_RATE = 0.15

# берём список налогоплательщиков
taxpayers = df[[
    'FullName', 'INN',
    'TaxpayerType', 'activity_type',
    'registration_district',
    'has_employees', 'employees_count',
    'TaxType'
]].drop_duplicates()

max_year = df['Year'].max()
next_year = max_year + 1

def get_season(month):
    if month in [12, 1, 2]: return 'winter'
    if month in [3, 4, 5]: return 'spring'
    if month in [6, 7, 8]: return 'summer'
    return 'autumn'

future_rows = []
for _, row in taxpayers.iterrows():
    for month in range(1, 13):
        future_rows.append({
            'FullName': row.FullName,
            'INN': row.INN,
            'Year': next_year,
            'Month': month,
            'season': get_season(month),
            'TaxType': row.TaxType,
            'TaxpayerType': row.TaxpayerType,
            'activity_type': row.activity_type,
            'registration_district': row.registration_district,
            'has_employees': row.has_employees,
            'employees_count': row.employees_count
        })

future_df = pd.DataFrame(future_rows)

engine.dispose()

econ_stub = {
    'Core CPI,seas.adj,,, [CORESA]': 118.0,
    'CPI Price, % y-o-y, not seas. adj.,, [CPTOTSAXNZGY]': 6.5,
    'Real Effective Exchange Rate,,,, [REER]': 101.5,
    'Retail Sales Volume,Index,,, [RETSALESSA]': 110.2,
    'Unemployment rate,Percent,,, [UNEMPSA_]': 4.9
}

for col, value in econ_stub.items():
    future_df[col] = value

# ===== загрузка модели доходов =====
income_model = tf.keras.models.load_model("nn_income_model.keras", compile=False)
income_scaler = joblib.load("scaler_income.joblib")
income_encoder = joblib.load("encoder_income.joblib")

income_num_features = [
    'Year', 'Month',
    'has_employees', 'employees_count',
    'Core CPI,seas.adj,,, [CORESA]',
    'CPI Price, % y-o-y, not seas. adj.,, [CPTOTSAXNZGY]',
    'Real Effective Exchange Rate,,,, [REER]',
    'Retail Sales Volume,Index,,, [RETSALESSA]',
    'Unemployment rate,Percent,,, [UNEMPSA_]'
]

income_cat_features = [
    'season', 'TaxType', 'TaxpayerType',
    'activity_type', 'registration_district'
]

X_income_num = income_scaler.transform(future_df[income_num_features].fillna(0))
X_income_cat = income_encoder.transform(future_df[income_cat_features].fillna("Unknown"))

X_income = np.hstack([X_income_num, X_income_cat])

future_df['PredictedIncome'] = np.expm1(
    income_model.predict(X_income).flatten()
)

# ===== загрузка модели транзакций =====
txn_model = tf.keras.models.load_model("nn_transactions_model.keras", compile=False)
txn_scaler = joblib.load("scaler_transactions.joblib")
txn_encoder = joblib.load("encoder_transactions.joblib")

txn_num_features = [
    'Year', 'Month',
    'IncomeAmount',  # ⚠ заменим предсказанным
    'has_employees', 'employees_count',
    'Core CPI,seas.adj,,, [CORESA]',
    'CPI Price, % y-o-y, not seas. adj.,, [CPTOTSAXNZGY]',
    'Real Effective Exchange Rate,,,, [REER]',
    'Retail Sales Volume,Index,,, [RETSALESSA]',
    'Unemployment rate,Percent,,, [UNEMPSA_]'
]

future_df['IncomeAmount'] = future_df['PredictedIncome']

X_txn_num = txn_scaler.transform(future_df[txn_num_features].fillna(0))
X_txn_cat = txn_encoder.transform(future_df[income_cat_features].fillna("Unknown"))

X_txn = np.hstack([X_txn_num, X_txn_cat])

future_df['PredictedTransactions'] = np.expm1(
    txn_model.predict(X_txn).flatten()
)

# ===== загрузка модели налогов =====
tax_model = tf.keras.models.load_model("nn_tax_model.keras", compile=False)
tax_scaler = joblib.load("scaler_tax.joblib")
tax_encoder = joblib.load("encoder_tax.joblib")

tax_num_features = [
    'Year', 'Month',
    'IncomeAmount',
    'transactions_count',
    'has_employees', 'employees_count',
    'Core CPI,seas.adj,,, [CORESA]',
    'CPI Price, % y-o-y, not seas. adj.,, [CPTOTSAXNZGY]',
    'Real Effective Exchange Rate,,,, [REER]',
    'Retail Sales Volume,Index,,, [RETSALESSA]',
    'Unemployment rate,Percent,,, [UNEMPSA_]'
]

future_df['transactions_count'] = future_df['PredictedTransactions']

X_tax_num = tax_scaler.transform(future_df[tax_num_features].fillna(0))
X_tax_cat = tax_encoder.transform(future_df[income_cat_features].fillna("Unknown"))

X_tax = np.hstack([X_tax_num, X_tax_cat])

future_df['PredictedTaxAmount'] = tax_model.predict(X_tax).flatten()

future_df[[
    'FullName', 'INN',
    'Year', 'Month',
    'PredictedIncome',
    'PredictedTransactions',
    'PredictedTaxAmount'
]].to_csv("1nn_tax_forecast_from_db.csv", index=False)

print("✅ Прогноз успешно сохранён")

import plotly.express as px

# используем именно прогнозы для будущего года
monthly_income_sum = future_df.groupby('Month')['PredictedIncome'].sum().reset_index()

fig_income_sum = px.line(
    monthly_income_sum,
    x='Month',
    y='PredictedIncome',
    title='Прогноз общего дохода по месяцам',
    markers=True
)
fig_income_sum.show()

monthly_income_median = future_df.groupby('Month')['PredictedIncome'].median().reset_index()

fig_income_avg = px.bar(
    monthly_income_median,
    x='Month',
    y='PredictedIncome',
    title='Средний прогнозируемый доход за месяц'
)
fig_income_avg.show()

monthly_tax_sum = future_df.groupby('Month')['PredictedTaxAmount'].sum().reset_index()

fig_tax_sum = px.line(
    monthly_tax_sum,
    x='Month',
    y='PredictedTaxAmount',
    title='Прогноз налоговых поступлений по месяцам',
    markers=True
)
fig_tax_sum.show()

monthly_tax_avg = future_df.groupby('Month')['PredictedTaxAmount'].median().reset_index()

fig_tax_avg = px.bar(
    monthly_tax_avg,
    x='Month',
    y='PredictedTaxAmount',
    title='Средний прогноз налоговых поступлений по месяцам от одного человека'
)
fig_tax_avg.show()
