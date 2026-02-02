import random
from faker import Faker
import pyodbc

fake = Faker('ru_RU')
Faker.seed(42)
random.seed(42)

# Подключение к MS SQL
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=Taxpayer_Database_DiplomaProject;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

# Получаем всех самозанятых
cursor.execute("""
    SELECT
        t.TaxpayerId,
        t.activity_type,
        t.registration_district,
        ISNULL(t.employees_count, 0) AS employees_count
    FROM dbo.Taxpayer t
    WHERE t.TaxpayerType = 'IPP'
""")
sz_taxpayers = cursor.fetchall()
print(f"Найдено налогоплательщиков: {len(sz_taxpayers)}")

# print(sz_taxpayers)

# Коэффициенты сезонности для разных сфер
seasonal_factors = {
    'TRADE': {'Зима': 1.2, 'Весна': 1.0, 'Лето': 0.9, 'Осень': 1.1},  # Зима - выше (новогодние покупки)
    'SERVICES': {'Зима': 1.1, 'Весна': 1.0, 'Лето': 0.8, 'Осень': 1.2},  # Осень - ремонт к зиме
    'IT': {'Зима': 1.0, 'Весна': 1.0, 'Лето': 0.7, 'Осень': 1.1},  # Лето - отпуска
    'FREELANCE': {'Зима': 1.0, 'Весна': 1.1, 'Лето': 0.6, 'Осень': 1.2},  # Осень/Весна - активность
    'PRODUCTION': {'Зима': 0.9, 'Весна': 1.1, 'Лето': 1.0, 'Осень': 1.0},  # Весна - стройсезон
    'FOOD': {'Зима': 1.1, 'Весна': 1.0, 'Лето': 1.3, 'Осень': 0.9},  # Лето - сезон кафе
    'LOGISTICS': {'Зима': 1.3, 'Весна': 1.0, 'Лето': 0.8, 'Осень': 1.2},  # Зима - доставка, Осень - заказы
    'EDUCATION': {'Зима': 1.0, 'Весна': 0.8, 'Лето': 0.4, 'Осень': 1.5}  # Осень - начало учебы
}


# Функция определения сезона
def get_season(month):
    if month in [12, 1, 2]:
        return 'Зима'
    elif month in [3, 4, 5]:
        return 'Весна'
    elif month in [6, 7, 8]:
        return 'Лето'
    else:  # 9, 10, 11
        return 'Осень'


def get_workers_factor(workers_count):
    if workers_count == 0:
        return 0.9  # сам работает
    elif 1 <= workers_count <= 2:
        return 1.0
    elif 3 <= workers_count <= 5:
        return 1.15
    elif 6 <= workers_count <= 10:
        return 1.3
    else:
        return 1.5


# Средние доходы по сферам деятельности (в рублях в месяц)
potential_income_by_activity = {
    'TRADE': 1200000,
    'SERVICES': 1000000,
    'IT': 1500000,
    'FREELANCE': 1100000,
    'PRODUCTION': 1300000,
    'FOOD': 1400000,
    'LOGISTICS': 1250000,
    'EDUCATION': 1000000
}

avg_incomes_by_activity = {
    'TRADE': {2023: 90000, 2024: 95000, 2025: 100000},
    'SERVICES': {2023: 85000, 2024: 90000, 2025: 95000},
    'IT': {2023: 100000, 2024: 110000, 2025: 120000},
    'FREELANCE': {2023: 90000, 2024: 95000, 2025: 100000},
    'PRODUCTION': {2023: 88000, 2024: 93000, 2025: 98000},
    'FOOD': {2023: 92000, 2024: 97000, 2025: 102000},
    'LOGISTICS': {2023: 95000, 2024: 100000, 2025: 105000},
    'EDUCATION': {2023: 90000, 2024: 95000, 2025: 100000}
}

# Коэффициенты по районам (дорогие vs дешевые районы)
district_factors = {
    "Приморский": 1.3, "Центральный": 1.4, "Петроградский": 1.3,
    "Василеостровский": 1.2, "Адмиралтейский": 1.1,
    "Фрунзенский": 1.0, "Московский": 1.0, "Невский": 1.0,
    "Красногвардейский": 0.9, "Кировский": 0.9, "Калининский": 0.9,
    "Выборгский": 0.9, "Красносельский": 0.9, "Петродворцовый": 1.1,
    "Курортный": 1.2, "Пушкинский": 1.0, "Колпинский": 0.8,
    "Кронштадтский": 0.8
}

# Настройки генерации
years = range(2023, 2026)  # только 23-25
months = range(1, 13)
batch_size = 100000  # оптимальный размер батча


def generate_monthly_record(taxpayer_id, activity_type, district, workers_count, year, month):
    season = get_season(month)
    district_factor = district_factors.get(district, 1.0)
    workers_factor = get_workers_factor(workers_count)

    # ФАКТИЧЕСКИЙ доход (не влияет на налог!)
    base_income = avg_incomes_by_activity[activity_type][year]
    income = random.uniform(0.9, 1.1) * base_income
    income *= seasonal_factors[activity_type][season]
    income *= district_factors.get(district, 1.0)

    if random.random() < 0.01:
        income = 0
        transactions = 0
    else:
        transactions = max(1, int(income / random.uniform(2000, 3000)))

    adjusted_potential_income = (
            potential_income_by_activity[activity_type]
            * district_factor
            * workers_factor
    )

    # ФИКСИРОВАННЫЙ налог по патенту
    annual_tax = adjusted_potential_income * 0.06
    monthly_tax = round(annual_tax / 12, 2)

    return (
        taxpayer_id,
        year,
        month,
        'IPP',  # тип налога
        round(income, 2),
        monthly_tax,
        season,
        transactions
    )


# Вставка пакетами
batch = []
count = 0
cursor.fast_executemany = True

print("Генерация данных о доходах и налогах...")

for taxpayer_id, activity_type, district, workers in sz_taxpayers:
    for year in years:
        for month in months:
            batch.append(generate_monthly_record(taxpayer_id, activity_type, district, workers, year, month))

            if len(batch) >= batch_size:
                try:
                    cursor.executemany("""
                        INSERT INTO MonthlyTaxData
                        (TaxpayerId, Year, Month, TaxType, IncomeAmount, TaxAmount, season, transactions_count)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, batch)
                    conn.commit()
                    count += len(batch)
                    print(f"Вставлено записей: {count}")
                    batch = []
                except Exception as e:
                    print(f"Ошибка при вставке: {e}")
                    conn.rollback()
                    batch = []

# Вставляем оставшиеся записи
if batch:
    try:
        cursor.executemany("""
            INSERT INTO MonthlyTaxData
            (TaxpayerId, Year, Month, TaxType, IncomeAmount, TaxAmount, season, transactions_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, batch)
        conn.commit()
        count += len(batch)
    except Exception as e:
        print(f"Ошибка при вставке остатка: {e}")
        conn.rollback()

print('Генерация ИПП завершена')
