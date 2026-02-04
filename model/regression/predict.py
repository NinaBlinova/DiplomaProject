import joblib
import pandas as pd
from sqlalchemy import create_engine

income_model = joblib.load("linear_income_model.pkl")
transactions_model = joblib.load("linear_transactions_model.pkl")
tax_model = joblib.load("linear_tax_model.pkl")

engine = create_engine(
    "mssql+pyodbc://@localhost/Taxpayer_Database_DiplomaProject?" +
    "trusted_connection=yes&" +
    "driver=ODBC+Driver+17+for+SQL+Server"
)

taxpayers = pd.read_sql("""
SELECT DISTINCT
    t.TaxpayerId,
    t.FullName,
    t.INN,
    t.TaxpayerType,
    t.activity_type,
    t.registration_district,
    t.has_employees,
    t.employees_count,
    m.TaxType
FROM Taxpayer t
JOIN MonthlyTaxData m
    ON t.TaxpayerId = m.TaxpayerId
""", engine)
max_year = pd.read_sql("SELECT MAX(Year) AS max_year FROM MonthlyTaxData", engine)
engine.dispose()


def get_season(month):
    if month in [12, 1, 2]:
        return 'winter'
    elif month in [3, 4, 5]:
        return 'spring'
    elif month in [6, 7, 8]:
        return 'summer'
    else:
        return 'autumn'


next_year = int(max_year.iloc[0]['max_year']) + 1
print(next_year)

future_rows = []

for _, row in taxpayers.iterrows():
    for month in range(1, 13):
        future_rows.append({
            'TaxpayerId': row.TaxpayerId,
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

features = [
    'Year', 'Month', 'season',
    'TaxType', 'TaxpayerType',
    'activity_type', 'registration_district',
    'has_employees', 'employees_count'
]

X_future = future_df[features]

future_df['PredictedIncome'] = income_model.predict(X_future)
future_df['PredictedTransactions'] = transactions_model.predict(X_future)
future_df['PredictedTax'] = tax_model.predict(X_future)

# защита от отрицательных значений
future_df[['PredictedIncome', 'PredictedTransactions', 'PredictedTax']] = \
    future_df[['PredictedIncome', 'PredictedTransactions', 'PredictedTax']].clip(lower=0)

year_summary = future_df.groupby(
    ['TaxpayerId', 'FullName', 'INN']
).agg(
    TotalIncome=('PredictedIncome', 'sum'),
    TotalTransactions=('PredictedTransactions', 'sum'),
    TotalTax=('PredictedTax', 'sum')
).reset_index()

future_df.to_csv("monthly_tax_forecast_next_year.csv", index=False)
year_summary.to_csv("year_tax_forecast_summary.csv", index=False)

import plotly.express as px

monthly_income = future_df.groupby('Month')['PredictedIncome'].sum().reset_index()

fig_income_sum = px.line(
    monthly_income,
    x='Month',
    y='PredictedIncome',
    title='Прогноз общего дохода по месяцам',
    markers=True
)
fig_income_sum.show()

monthly_income_avg = future_df.groupby('Month')['PredictedIncome'].median().reset_index()

fig_income_avg = px.bar(
    monthly_income_avg,
    x='Month',
    y='PredictedIncome',
    title='Средний прогнозируемый доход за месяц'
)
fig_income_avg.show()

monthly_tax = future_df.groupby('Month')['PredictedTax'].sum().reset_index()

fig_tax = px.line(
    monthly_tax,
    x='Month',
    y='PredictedTax',
    title='Прогноз налоговых поступлений по месяцам',
    markers=True
)
fig_tax.show()

monthly_tax_avg = future_df.groupby('Month')['PredictedTax'].median().reset_index()

fig_tax_avg = px.bar(
    monthly_tax_avg,
    x='Month',
    y='PredictedTax',
    title='Средний прогноз налоговых поступлений по месяцам',
)
fig_tax_avg.show()
