import joblib
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
import plotly.graph_objects as go
from xgboost import XGBRegressor

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
        ('regressor', XGBRegressor(
            n_estimators=500,
            max_depth=5,
            learning_rate=0.05,
            objective='reg:squarederror',
            random_state=42
        ))
    ])

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    # mae = mean_absolute_error(y_test, y_pred)
    # rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    # r2 = r2_score(y_test, y_pred)
    #
    # print(f"\n Модель: {model_name}")
    # print(f"MAE  = {mae:,.2f}")
    # print(f"RMSE = {rmse:,.2f}")
    # print(f"R²   = {r2:.4f}")

    # уравнение
    # regressor = model.named_steps['regressor']
    #
    # # Получаем имена признаков после preprocessor
    # cat_features = model.named_steps['preprocessor'].transformers_[0][1] \
    #     .named_steps['onehot'].get_feature_names_out(categorical_features)
    # all_features = list(cat_features) + numeric_features
    #
    # coefficients = regressor.coef_
    # intercept = regressor.intercept_
    #
    # equation = "y = " + " + ".join([f"{coef:.2f}*{name}" for coef, name in zip(coefficients, all_features)])
    # equation += f" + {intercept:.2f}"
    #
    # print("Уравнение регрессии по признакам:")
    # print(equation)

    # Визуализация предсказаний

    fig = go.Figure()

    # Случайно выбираем 100 индексов из тестовой выборки
    np.random.seed(42)  # Для воспроизводимости
    n_samples_to_show = 100
    indices = np.random.choice(len(y_test),
                               size=min(n_samples_to_show, len(y_test)),
                               replace=False)

    # Берем выбранные индексы
    y_test_sample = y_test.iloc[indices] if hasattr(y_test, 'iloc') else y_test[indices]
    y_pred_sample = y_pred[indices]

    # Точки реальных vs предсказанных (только 100 записей)
    fig.add_trace(go.Scatter(
        x=y_test_sample,
        y=y_pred_sample,
        mode='markers',
        name='Предсказания (100 случайных)',
        marker=dict(color='blue', size=8, opacity=0.7)
    ))

    # Линия идеального соответствия
    min_val = min(y_test.min(), y_pred.min())
    max_val = max(y_test.max(), y_pred.max())
    fig.add_trace(go.Scatter(
        x=[min_val, max_val],
        y=[min_val, max_val],
        mode='lines',
        name='Идеал',
        line=dict(color='red', dash='dash')
    ))

    fig.update_layout(
        title=f"Реальные vs Предсказанные значения ({model_name})",
        xaxis_title="Реальные значения",
        yaxis_title="Предсказанные значения",
        legend=dict(x=0.02, y=0.98),
        width=700,
        height=500
    )

    fig.show()

    joblib.dump(model, f"{model_name}.pkl")
    pd.DataFrame({'y_test': y_test, 'y_pred': y_pred}).to_csv(f"{model_name}predictions.csv", index=False)
    print(f"Модель сохранена: {model_name}.pkl")


# 4. Обучение моделей
train_and_evaluate(X, df[target_income], "linear_income_model")
train_and_evaluate(X, df[target_transactions], "linear_transactions_model")
train_and_evaluate(X, df[target_tax], "linear_tax_model")
