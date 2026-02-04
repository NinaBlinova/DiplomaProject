import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import train_test_split
import pyodbc

# 1. Загрузка налоговых данных
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=Taxpayer_Database_DiplomaProject;"
    "Trusted_Connection=yes;"
)

query = """
SELECT
    m.RecordId,
    m.Year,
    m.Month,
    m.TaxType,
    m.TaxAmount,
    m.season,
    t.TaxpayerType,
    t.activity_type,
    t.registration_district,
    t.has_employees,
    t.employees_count
FROM MonthlyTaxData m
INNER JOIN Taxpayer t
    ON m.TaxpayerId = t.TaxpayerId;
"""

df_tax = pd.read_sql(query, conn)
conn.close()

# 2. Загрузка экономических показателей
df_econ = pd.read_csv(r"C:\Users\blino\DiplomaProject\economic_data.csv")
df_econ.replace('..', np.nan, inplace=True)
numeric_econ_cols = [
    'Core CPI,seas.adj,,, [CORESA]',
    'CPI Price, % y-o-y, not seas. adj.,, [CPTOTSAXNZGY]',
    'Real Effective Exchange Rate,,,, [REER]',
    'Retail Sales Volume,Index,,, [RETSALESSA]',
    'Unemployment rate,Percent,,, [UNEMPSA_]'
]

df_econ[numeric_econ_cols] = df_econ[numeric_econ_cols].astype(float)
df_econ['Year'] = pd.to_datetime(df_econ['Time']).dt.year
df_econ['Month'] = pd.to_datetime(df_econ['Time']).dt.month

# print(df_econ)

# Объединяем налоговые и экономические данные
df = pd.merge(df_tax, df_econ, on=['Year', 'Month'], how='left')

# 3. Признаки
categorical_features = ['season', 'TaxType', 'TaxpayerType', 'activity_type', 'registration_district']
numeric_features = ['Year', 'Month', 'has_employees', 'employees_count', 'Core CPI,seas.adj,,, [CORESA]',
                    'CPI Price, % y-o-y, not seas. adj.,, [CPTOTSAXNZGY]', 'Real Effective Exchange Rate,,,, [REER]',
                    'Retail Sales Volume,Index,,, [RETSALESSA]', 'Unemployment rate,Percent,,, [UNEMPSA_]']

X_cat = df[categorical_features].fillna('Unknown')
X_num = df[numeric_features].fillna(0)
# Таргет
y = df['TaxAmount'].values

# 4. Преобразование признаков
# One-Hot для категориальных
encoder = OneHotEncoder(handle_unknown='ignore', sparse_output=False)
X_cat_encoded = encoder.fit_transform(X_cat)
print(X_cat_encoded)

# Масштабирование числовых признаков
scaler = StandardScaler()
X_num_scaled = scaler.fit_transform(X_num)
print('------')
print(X_num_scaled)

# Объединяем признаки для нейросети
X_nn = np.hstack([X_num_scaled, X_cat_encoded])

# 5. Разделение на train/test
X_train, X_test, y_train, y_test = train_test_split(X_nn, y, test_size=0.2, random_state=42)

# 6. Создание нейросети
model = Sequential([
    Dense(128, activation='relu', input_shape=(X_nn.shape[1],)),
    Dropout(0.2),
    Dense(64, activation='relu'),
    Dense(1)  # прогноз TaxAmount
])

model.compile(optimizer='adam', loss='mse', metrics=['mae'])
model.summary()

# 7. Обучение
history = model.fit(
    X_train, y_train,
    validation_split=0.2,
    epochs=100,
    batch_size=32
)

# 8. Предсказания
y_pred = model.predict(X_test).flatten()

# 9. Сохранение модели и предсказаний
model.save("nn_tax_model.h5")
pd.DataFrame({'y_test': y_test, 'y_pred': y_pred}).to_csv("nn_tax_predictions.csv", index=False)

print("Нейросеть обучена и сохранена. Предсказания сохранены.")
