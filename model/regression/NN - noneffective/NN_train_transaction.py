import joblib
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import train_test_split

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

# 2. Экономические показатели
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

# 3. Объединение данных
df = pd.merge(df_tax, df_econ, on=['Year', 'Month'], how='left')

# 4. Признаки
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

X_num = df[numeric_features].fillna(0)
X_cat = df[categorical_features].fillna('Unknown')

# 5. Препроцессинг
scaler = StandardScaler()
X_num_scaled = scaler.fit_transform(X_num)

encoder = OneHotEncoder(handle_unknown='ignore', sparse_output=False)
X_cat_encoded = encoder.fit_transform(X_cat)

X = np.hstack([X_num_scaled, X_cat_encoded])

# 6. Таргет (лог-трансформация)
y = np.log1p(df['transactions_count'].clip(0, 500))

# 7. Train / Test split
X_train, X_test, y_train, y_test = train_test_split(
    X, y,
    test_size=0.2,
    random_state=42
)

# 8. Модель
model = Sequential([
    Dense(64, activation='relu', input_shape=(X.shape[1],)),
    Dense(32, activation='relu'),
    Dense(1)
])

model.compile(
    optimizer='adam',
    loss='mae',        # ⚠ лучше для счётчиков
    metrics=['mae']
)

# 9. Обучение
model.fit(
    X_train,
    y_train,
    validation_split=0.2,
    epochs=80,
    batch_size=32,
    verbose=1
)

# 10. Сохранение
model.save("nn_transactions_model.keras")
joblib.dump(scaler, "scaler_transactions.joblib")
joblib.dump(encoder, "encoder_transactions.joblib")

print("Модель транзакций сохранена")

# 11. Предсказания
y_pred_log = model.predict(X_test).flatten()
y_pred = np.expm1(y_pred_log)
y_true = np.expm1(y_test)

pd.DataFrame({
    'y_true': y_true,
    'y_pred': y_pred
}).to_csv("nn_transactions_predictions.csv", index=False)

print("Предсказания транзакций сохранены")
