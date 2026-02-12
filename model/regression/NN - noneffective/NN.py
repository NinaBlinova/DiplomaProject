import joblib
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import train_test_split
import pyodbc

# 1. Загрузка налоговых данных
engine = create_engine(
    "mssql+pyodbc://@localhost/Taxpayer_Database_DiplomaProject?" +
    "trusted_connection=yes&" +
    "driver=ODBC+Driver+17+for+SQL+Server"
)

taxpayers = pd.read_sql("""
    SELECT 
    m.RecordId, 
    m.Year, 
    m.Month, 
    m.TaxType, 
    m.TaxAmount, 
    m.IncomeAmount,
    m.season, 
    m.transactions_count,
    t.TaxpayerType, 
    t.activity_type, 
    t.registration_district, 
    t.has_employees, 
    t.employees_count 
    FROM MonthlyTaxData m 
    INNER JOIN Taxpayer t ON m.TaxpayerId = t.TaxpayerId;
""", engine)
engine.dispose()
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
df = pd.merge(taxpayers, df_econ, on=['Year', 'Month'], how='left')

# 3. Признаки
categorical_features = ['season', 'TaxType', 'TaxpayerType', 'activity_type', 'registration_district']
numeric_features = ['Year', 'Month', 'IncomeAmount', 'transactions_count', 'has_employees', 'employees_count',
                    'Core CPI,seas.adj,,, [CORESA]',
                    'CPI Price, % y-o-y, not seas. adj.,, [CPTOTSAXNZGY]', 'Real Effective Exchange Rate,,,, [REER]',
                    'Retail Sales Volume,Index,,, [RETSALESSA]', 'Unemployment rate,Percent,,, [UNEMPSA_]']

X_cat = df[categorical_features].fillna('Unknown')
X_num = df[numeric_features].fillna(0)
# Таргет
y_tax = df['TaxAmount'].values

# 4. Преобразование признаков
# One-Hot для категориальных
encoder = OneHotEncoder(handle_unknown='ignore', sparse_output=False)
X_cat_encoded = encoder.fit_transform(X_cat)
print(X_cat_encoded)

# Масштабирование числовых признаков
scaler = StandardScaler()
X_num_scaled = scaler.fit_transform(X_num)

# Объединяем признаки для нейросети
X_nn = np.hstack([X_num_scaled, X_cat_encoded])

# 5. Разделение на train/test
idx_train, idx_test = train_test_split(
    np.arange(len(df)),
    test_size=0.2,
    random_state=42
)

X_train = X_nn[idx_train]
X_test = X_nn[idx_test]


def build_model(input_dim):
    model = Sequential([
        Dense(128, activation='relu', input_shape=(input_dim,)),
        Dropout(0.2),
        Dense(64, activation='relu'),
        Dense(1)
    ])
    model.compile(
        optimizer='adam',
        loss='mse',
        metrics=['mae']
    )
    return model


# модель налогов
model_tax = build_model(X_nn.shape[1])

model_tax.fit(
    X_train,
    y_tax[idx_train],
    validation_split=0.2,
    epochs=100,
    batch_size=32,
    verbose=1
)

model_tax.save("nn_tax_model.keras")
joblib.dump(encoder, "encoder_tax.joblib")
joblib.dump(scaler, "scaler_tax.joblib")

pd.DataFrame({
    'y_true': y_tax[idx_test],
    'y_pred': model_tax.predict(X_test).flatten()
}).to_csv("nn_tax_predictions.csv", index=False)

print("Модель и процессинг сохранены")
