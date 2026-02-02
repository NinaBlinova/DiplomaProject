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
    WHERE t.TaxpayerType = 'IPOS'
""")
sz_taxpayers = cursor.fetchall()
print(f"Найдено налогоплательщиков: {len(sz_taxpayers)}")

# print(sz_taxpayers)

# Коэффициенты сезонности для разных сфер
# Коэффициенты сезонности для разных сфер (для доходов)
seasonal_factors = {
    'TRADE': {'Зима': 1.2, 'Весна': 1.0, 'Лето': 0.9, 'Осень': 1.1},
    'SERVICES': {'Зима': 1.1, 'Весна': 1.0, 'Лето': 0.8, 'Осень': 1.2},
    'IT': {'Зима': 1.0, 'Весна': 1.0, 'Лето': 0.7, 'Осень': 1.1},
    'FREELANCE': {'Зима': 1.0, 'Весна': 1.1, 'Лето': 0.6, 'Осень': 1.2},
    'PRODUCTION': {'Зима': 0.9, 'Весна': 1.1, 'Лето': 1.0, 'Осень': 1.0},
    'FOOD': {'Зима': 1.1, 'Весна': 1.0, 'Лето': 1.3, 'Осень': 0.9},
    'LOGISTICS': {'Зима': 1.3, 'Весна': 1.0, 'Лето': 0.8, 'Осень': 1.2},
    'EDUCATION': {'Зима': 1.0, 'Весна': 0.8, 'Лето': 0.4, 'Осень': 1.5}
}


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


# Средние доходы по сферам деятельности (в рублях в месяц)
avg_incomes_by_activity = {
    'TRADE': {2023: 500000, 2024: 550000, 2025: 600000},  # Торговля - высокие обороты
    'SERVICES': {2023: 400000, 2024: 450000, 2025: 500000},  # Услуги
    'IT': {2023: 600000, 2024: 700000, 2025: 800000},  # IT - высокие доходы
    'FREELANCE': {2023: 300000, 2024: 350000, 2025: 400000},  # Фриланс
    'PRODUCTION': {2023: 450000, 2024: 500000, 2025: 550000},  # Производство
    'FOOD': {2023: 350000, 2024: 400000, 2025: 450000},  # Общепит
    'LOGISTICS': {2023: 400000, 2024: 450000, 2025: 500000},  # Логистика
    'EDUCATION': {2023: 300000, 2024: 350000, 2025: 400000}  # Обучение
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

# Процент расходов от дохода для разных сфер (в среднем 60-90%)
expense_ratios_by_activity = {
    'TRADE': 0.75,  # Торговля: закуп товаров, аренда склада
    'SERVICES': 0.65,  # Услуги: материалы, зарплаты
    'IT': 0.40,  # IT: низкие расходы, в основном зарплаты
    'FREELANCE': 0.30,  # Фриланс: минимальные расходы
    'PRODUCTION': 0.80,  # Производство: сырье, оборудование
    'FOOD': 0.85,  # Общепит: продукты, аренда, зарплаты
    'LOGISTICS': 0.70,  # Логистика: ГСМ, ремонт, зарплаты
    'EDUCATION': 0.60  # Обучение: аренда, материалы
}

# Настройки генерации
years = range(2023, 2026)  # только 23-25
months = range(1, 13)
batch_size = 100000  # оптимальный размер батча


def generate_monthly_record(taxpayer_id, activity_type, district, workers_count, year, month):
    season = get_season(month)

    # Базовый средний доход для сферы и года
    avg_monthly = avg_incomes_by_activity[activity_type][year]

    # Применяем сезонный коэффициент
    seasonal_factor = seasonal_factors[activity_type][season]
    # Применяем районный коэффициент
    district_factor = district_factors.get(district, 1.0)
    workers_factor = get_workers_factor(workers_count)

    # Общий коэффициент
    total_factor = seasonal_factor * district_factor * workers_factor
    avg_monthly = avg_monthly * total_factor

    # Основной доход с вариацией ±%
    monthly_income = random.uniform(0.95 * avg_monthly, 1.05 * avg_monthly)

    # Определяем, есть ли доход в этом месяце (75% - есть доход, 25% - нет - 0.25)
    has_income = random.random() >= 0.0000005

    if not has_income:
        monthly_income = 0
        transactions_count = 0
    else:
        # Генерация количества транзакций в зависимости от дохода
        # Примерно 1 транзакция на каждые 1000-3000 рублей дохода
        base_transactions = int(monthly_income / random.uniform(1500, 2500))
        transactions_count = max(1, base_transactions + random.randint(-3, 3))

    # Аномалии в доходе (~1% случаев)
    anomaly_chance = random.random()
    if anomaly_chance < 0.003 and has_income:
        # Очень высокий доход
        monthly_income *= random.uniform(3, 5)
        transactions_count = int(transactions_count * random.uniform(1.5, 2))
    elif 0.003 <= anomaly_chance < 0.0025 and has_income:
        # Очень низкий доход (но не ноль)
        monthly_income *= random.uniform(0.1, 0.3)
        transactions_count = max(1, int(transactions_count * random.uniform(0.3, 0.7)))

    # Налог
    if monthly_income > 0:
        tax_rate = 0.2
        tax_amount = round(monthly_income * tax_rate, 2)

        # Аномалии в налоге (~0.02% случаев)
        if random.random() < 0.0002:
            # Неправильный расчет налога
            wrong_rate = random.choice([0, 0.5, 0.1, 0.15])
            tax_amount = round(monthly_income * wrong_rate, 2)
    else:
        tax_amount = 0

    return (taxpayer_id, year, month, 'IPOS',
            round(monthly_income, 2),
            round(tax_amount, 2),
            season,
            transactions_count)


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
