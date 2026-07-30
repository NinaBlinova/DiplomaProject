# model/regression/LinearRegression/train.py

import joblib
from sklearn.linear_model import LinearRegression
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer

engine = create_engine(
    "mssql+pyodbc://@localhost/Taxpayer_Database_DiplomaProject?" +
    "trusted_connection=yes&" +
    "driver=ODBC+Driver+17+for+SQL+Server"
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
engine.dispose()

# print(df.shape)

# 1. Признаки и таргеты
features = [
    'Year', 'Month', 'season',
    'TaxType', 'TaxpayerType', 'activity_type', 'registration_district',
    'has_employees', 'employees_count'
]

target_income = 'IncomeAmount'
target_transactions = 'transactions_count'
target_tax = 'TaxAmount'

X = df[features]

# 2. Типы признаков
categorical_features = [
    'Year', 'season', 'TaxType', 'TaxpayerType',
    'activity_type', 'registration_district'
]

numeric_features = [
    'Month', 'has_employees', 'employees_count'
]

categorical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='most_frequent')),
    ('onehot', OneHotEncoder(handle_unknown='ignore'))
])

numeric_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='median'))
])

preprocessor = ColumnTransformer(
    transformers=[
        ('cat', categorical_transformer, categorical_features),
        ('num', numeric_transformer, numeric_features)
    ]
)


# 3. Функция обучения модели
def train_and_evaluate(X, y, model_name):
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = Pipeline([
        ('preprocessor', preprocessor),
        ('regressor', LinearRegression())
    ])

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    joblib.dump(model, f"{model_name}.pkl")
    pd.DataFrame({'y_test': y_test, 'y_pred': y_pred}).to_csv(f"{model_name}predictions.csv", index=False)
    print(f"The model is safe: {model_name}.pkl")


# 4. Train model
train_and_evaluate(X, df[target_income], "linear_income_model")
train_and_evaluate(X, df[target_transactions], "linear_transactions_model")
train_and_evaluate(X, df[target_tax], "linear_tax_model")
