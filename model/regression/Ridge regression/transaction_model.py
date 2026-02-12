import pandas as pd
import numpy as np
from sqlalchemy import create_engine

from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import Ridge
from sklearn.metrics import r2_score, mean_absolute_error

# =========================
# 1. Загрузка данных из БД
# =========================

engine = create_engine(
    "mssql+pyodbc://@localhost/Taxpayer_Database_DiplomaProject?"
    "trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server"
)

df_tax = pd.read_sql("""
    SELECT 
        m.RecordId,
        m.Year,
        m.Month,
        m.TaxType,
        m.IncomeAmount,
        m.transactions_count,
        m.season,
        t.TaxpayerType,
        t.activity_type,
        t.registration_district,
        t.has_employees,
        t.employees_count
    FROM MonthlyTaxData m
    JOIN Taxpayer t ON m.TaxpayerId = t.TaxpayerId
""", engine)

engine.dispose()

# =========================
# 2. Экономические показатели
# =========================

df_econ = pd.read_csv(r"C:\Users\blino\DiplomaProject\economic_data.csv")
df_econ.replace('..', np.nan, inplace=True)

econ_cols = [
    'Core CPI,seas.adj,,, [CORESA]',
    'CPI Price, % y-o-y, not seas. adj.,, [CPTOTSAXNZGY]',
    'Real Effective Exchange Rate,,,, [REER]',
    'Retail Sales Volume,Index,,, [RETSALESSA]',
    'Unemployment rate,Percent,,, [UNEMPSA_]'
]

df_econ[econ_cols] = df_econ[econ_cols].astype(float)
df_econ['Year'] = pd.to_datetime(df_econ['Time']).dt.year
df_econ['Month'] = pd.to_datetime(df_econ['Time']).dt.month

# =========================
# 3. Объединение данных
# =========================

df = pd.merge(df_tax, df_econ, on=['Year', 'Month'], how='left')

# =========================
# 4. Признаки и таргет
# =========================

target = 'transactions_count'

numeric_features = [
    'Year', 'Month',
    'IncomeAmount',
    'has_employees',
    'employees_count',
    'Core CPI,seas.adj,,, [CORESA]',
    'CPI Price, % y-o-y, not seas. adj.,, [CPTOTSAXNZGY]',
    'Real Effective Exchange Rate,,,, [REER]',
    'Retail Sales Volume,Index,,, [RETSALESSA]',
    'Unemployment rate,Percent,,, [UNEMPSA_]'
]

categorical_features = [
    'season',
    'TaxType',
    'TaxpayerType',
    'activity_type',
    'registration_district'
]

X = df[numeric_features + categorical_features]
y = df[target]

# =========================
# 5. Препроцессинг
# =========================

numeric_transformer = Pipeline(steps=[
    ('scaler', StandardScaler())
])

categorical_transformer = Pipeline(steps=[
    ('encoder', OneHotEncoder(handle_unknown='ignore'))
])

preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features),
        ('cat', categorical_transformer, categorical_features)
    ]
)

# =========================
# 6. Модель Ridge + подбор alpha
# =========================

ridge = Ridge(random_state=42)

model = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('regressor', ridge)
])

param_grid = {
    'regressor__alpha': [0.1, 1, 5, 10, 50, 100]
}

grid = GridSearchCV(
    model,
    param_grid,
    cv=5,
    scoring='r2',
    n_jobs=-1
)

# =========================
# 7. Train / Test
# =========================

X_train, X_test, y_train, y_test = train_test_split(
    X, y,
    test_size=0.2,
    random_state=42
)

grid.fit(X_train, y_train)

best_model = grid.best_estimator_

print("✅ Лучший alpha:", grid.best_params_['regressor__alpha'])

# =========================
# 8. Качество модели
# =========================

y_pred = best_model.predict(X_test)

print("R2 :", r2_score(y_test, y_pred))
print("MAE:", mean_absolute_error(y_test, y_pred))

# =========================
# 9. Анализ влияния факторов
# =========================

# получаем имена признаков
ohe = best_model.named_steps['preprocessor'] \
    .named_transformers_['cat'] \
    .named_steps['encoder']

feature_names = (
    numeric_features +
    ohe.get_feature_names_out(categorical_features).tolist()
)

coef = best_model.named_steps['regressor'].coef_

coef_df = pd.DataFrame({
    'feature': feature_names,
    'coefficient': coef
})

# =========================
# 10. Экономические факторы
# =========================

econ_effect = coef_df[coef_df['feature'].isin(econ_cols)]
econ_effect = econ_effect.sort_values(
    by='coefficient',
    key=abs,
    ascending=False
)

print("\n📊 Влияние экономических показателей:")
print(econ_effect)

econ_effect.to_csv(
    "econ_effect_transactions_ridge.csv",
    index=False
)

print("\n📁 Анализ экономических факторов сохранён")
