import random
from faker import Faker
import pyodbc

fake = Faker('ru_RU')
Faker.seed(42)
random.seed(42)

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=Taxpayer_Database_DiplomaProject;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()
cursor.fast_executemany = True

# Справочники (общие для всех типов)

ACTIVITY_TYPES = ['TRADE', 'SERVICES', 'IT', 'FREELANCE',
                  'PRODUCTION', 'FOOD', 'LOGISTICS', 'EDUCATION']

DISTRICTS = [
    "Адмиралтейский", "Василеостровский", "Выборгский", "Калининский",
    "Кировский", "Колпинский", "Красногвардейский", "Красносельский",
    "Кронштадтский", "Курортный", "Московский", "Невский",
    "Петроградский", "Петродворцовый", "Приморский", "Пушкинский",
    "Фрунзенский", "Центральный"
]

SEASONAL_FACTORS = {
    'TRADE': {'Зима': 1.2, 'Весна': 1.0, 'Лето': 0.9, 'Осень': 1.1},
    'SERVICES': {'Зима': 1.1, 'Весна': 1.0, 'Лето': 0.8, 'Осень': 1.2},
    'IT': {'Зима': 1.0, 'Весна': 1.0, 'Лето': 0.7, 'Осень': 1.1},
    'FREELANCE': {'Зима': 1.0, 'Весна': 1.1, 'Лето': 0.6, 'Осень': 1.2},
    'PRODUCTION': {'Зима': 0.9, 'Весна': 1.1, 'Лето': 1.0, 'Осень': 1.0},
    'FOOD': {'Зима': 1.1, 'Весна': 1.0, 'Лето': 1.3, 'Осень': 0.9},
    'LOGISTICS': {'Зима': 1.3, 'Весна': 1.0, 'Лето': 0.8, 'Осень': 1.2},
    'EDUCATION': {'Зима': 1.0, 'Весна': 0.8, 'Лето': 0.4, 'Осень': 1.5},
}

SEASONAL_FACTORS_EXPENSES = {
    'TRADE': {'Зима': 1.1, 'Весна': 1.0, 'Лето': 0.9, 'Осень': 1.0},
    'SERVICES': {'Зима': 1.0, 'Весна': 1.1, 'Лето': 0.8, 'Осень': 1.1},
    'IT': {'Зима': 1.0, 'Весна': 1.0, 'Лето': 0.8, 'Осень': 1.0},
    'FREELANCE': {'Зима': 1.0, 'Весна': 1.0, 'Лето': 0.7, 'Осень': 1.1},
    'PRODUCTION': {'Зима': 0.9, 'Весна': 1.2, 'Лето': 1.0, 'Осень': 1.0},
    'FOOD': {'Зима': 1.2, 'Весна': 1.0, 'Лето': 1.2, 'Осень': 0.9},
    'LOGISTICS': {'Зима': 1.2, 'Весна': 1.0, 'Лето': 0.9, 'Осень': 1.1},
    'EDUCATION': {'Зима': 1.0, 'Весна': 0.9, 'Лето': 0.5, 'Осень': 1.3},
}

DISTRICT_FACTORS = {
    "Приморский": 1.3, "Центральный": 1.4, "Петроградский": 1.3,
    "Василеостровский": 1.2, "Адмиралтейский": 1.1,
    "Фрунзенский": 1.0, "Московский": 1.0, "Невский": 1.0,
    "Красногвардейский": 0.9, "Кировский": 0.9, "Калининский": 0.9,
    "Выборгский": 0.9, "Красносельский": 0.9, "Петродворцовый": 1.1,
    "Курортный": 1.2, "Пушкинский": 1.0, "Колпинский": 0.8,
    "Кронштадтский": 0.8,
}

EXPENSE_RATIOS = {
    'TRADE': 0.75, 'SERVICES': 0.65, 'IT': 0.40, 'FREELANCE': 0.30,
    'PRODUCTION': 0.80, 'FOOD': 0.85, 'LOGISTICS': 0.70, 'EDUCATION': 0.60,
}

# Средние доходы по типам налогоплательщиков
AVG_INCOMES = {
    'SZ': {
        'TRADE': {2023: 48000, 2024: 52000, 2025: 56000},
        'SERVICES': {2023: 45000, 2024: 50000, 2025: 54000},
        'IT': {2023: 60000, 2024: 65000, 2025: 70000},
        'FREELANCE': {2023: 50000, 2024: 55000, 2025: 60000},
        'PRODUCTION': {2023: 47000, 2024: 51000, 2025: 55000},
        'FOOD': {2023: 52000, 2024: 56000, 2025: 60000},
        'LOGISTICS': {2023: 55000, 2024: 60000, 2025: 65000},
        'EDUCATION': {2023: 46000, 2024: 50000, 2025: 54000},
    },
    'IP6': {
        'TRADE': {2023: 90000, 2024: 95000, 2025: 100000},
        'SERVICES': {2023: 85000, 2024: 90000, 2025: 95000},
        'IT': {2023: 100000, 2024: 110000, 2025: 120000},
        'FREELANCE': {2023: 90000, 2024: 95000, 2025: 100000},
        'PRODUCTION': {2023: 88000, 2024: 93000, 2025: 98000},
        'FOOD': {2023: 92000, 2024: 97000, 2025: 102000},
        'LOGISTICS': {2023: 95000, 2024: 100000, 2025: 105000},
        'EDUCATION': {2023: 90000, 2024: 95000, 2025: 100000},
    },
    'IP15': {
        'TRADE': {2023: 115000, 2024: 117000, 2025: 120000},
        'SERVICES': {2023: 110000, 2024: 112000, 2025: 115000},
        'IT': {2023: 120000, 2024: 120000, 2025: 120000},
        'FREELANCE': {2023: 110000, 2024: 112000, 2025: 115000},
        'PRODUCTION': {2023: 115000, 2024: 117000, 2025: 120000},
        'FOOD': {2023: 110000, 2024: 113000, 2025: 115000},
        'LOGISTICS': {2023: 112000, 2024: 115000, 2025: 118000},
        'EDUCATION': {2023: 110000, 2024: 112000, 2025: 115000},
    },
    'IPOS': {
        'TRADE': {2023: 500000, 2024: 550000, 2025: 600000},
        'SERVICES': {2023: 400000, 2024: 450000, 2025: 500000},
        'IT': {2023: 600000, 2024: 700000, 2025: 800000},
        'FREELANCE': {2023: 300000, 2024: 350000, 2025: 400000},
        'PRODUCTION': {2023: 450000, 2024: 500000, 2025: 550000},
        'FOOD': {2023: 350000, 2024: 400000, 2025: 450000},
        'LOGISTICS': {2023: 400000, 2024: 450000, 2025: 500000},
        'EDUCATION': {2023: 300000, 2024: 350000, 2025: 400000},
    },
    'IPP': {  # фактический доход (не влияет на налог)
        'TRADE': {2023: 90000, 2024: 95000, 2025: 100000},
        'SERVICES': {2023: 85000, 2024: 90000, 2025: 95000},
        'IT': {2023: 100000, 2024: 110000, 2025: 120000},
        'FREELANCE': {2023: 90000, 2024: 95000, 2025: 100000},
        'PRODUCTION': {2023: 88000, 2024: 93000, 2025: 98000},
        'FOOD': {2023: 92000, 2024: 97000, 2025: 102000},
        'LOGISTICS': {2023: 95000, 2024: 100000, 2025: 105000},
        'EDUCATION': {2023: 90000, 2024: 95000, 2025: 100000},
    },
}

# Потенциальный доход для патентной системы (фиксированный)
POTENTIAL_INCOME_IPP = {
    'TRADE': 1200000, 'SERVICES': 1000000, 'IT': 1500000,
    'FREELANCE': 1100000, 'PRODUCTION': 1300000, 'FOOD': 1400000,
    'LOGISTICS': 1250000, 'EDUCATION': 1000000,
}

YEARS = range(2023, 2026)
MONTHS = range(1, 13)
BATCH_SIZE = 100000


# Вспомогательные функции

def get_season(month):
    if month in (12, 1, 2): return 'Зима'
    if month in (3, 4, 5):  return 'Весна'
    if month in (6, 7, 8):  return 'Лето'
    return 'Осень'


def get_workers_factor(workers_count):
    if workers_count == 0:              return 0.9
    if workers_count <= 2:              return 1.0
    if workers_count <= 5:              return 1.15
    if workers_count <= 10:             return 1.3
    return 1.5


# Генераторы записей по типу

def generate_record_sz(taxpayer_id, activity, district, workers, year, month):
    """Самозанятые — НПД 6%, нет сотрудников, 25% месяцев без дохода."""
    season = get_season(month)
    base = AVG_INCOMES['SZ'][activity][year]
    base *= SEASONAL_FACTORS[activity][season] * DISTRICT_FACTORS.get(district, 1.0)

    has_income = random.random() >= 0.25
    if not has_income:
        return (taxpayer_id, year, month, 'NPD', 0.0, 0.0, season, 0)

    income = random.uniform(0.75 * base, 1.25 * base)
    transactions = max(1, int(income / random.uniform(1500, 2500)) + random.randint(-3, 3))

    chance = random.random()
    if chance < 0.003:
        income *= random.uniform(3, 5)
        transactions = int(transactions * random.uniform(1.5, 2))
    elif chance < 0.006:
        income *= random.uniform(0.1, 0.3)
        transactions = max(1, int(transactions * random.uniform(0.3, 0.7)))

    tax = round(income * 0.06, 2)
    if random.random() < 0.0002:
        tax = round(income * random.choice([0, 0.05, 0.10, 0.15]), 2)

    return (taxpayer_id, year, month, 'NPD', round(income, 2), tax, season, transactions)


def generate_record_ip6(taxpayer_id, activity, district, workers, year, month):
    """ИП УСН 6% — налог с полного дохода."""
    season = get_season(month)
    base = AVG_INCOMES['IP6'][activity][year]
    base *= (SEASONAL_FACTORS[activity][season]
             * DISTRICT_FACTORS.get(district, 1.0)
             * get_workers_factor(workers))

    income = random.uniform(0.8 * base, 1.2 * base)
    transactions = max(1, int(income / random.uniform(5000, 15000)) + random.randint(-2, 2))

    chance = random.random()
    if chance < 0.01:
        income *= random.uniform(1.5, 3.0)
        transactions = int(transactions * random.uniform(1.2, 1.5))
    elif chance < 0.02:
        income *= random.uniform(0.3, 0.6)
        transactions = max(1, int(transactions * random.uniform(0.3, 0.6)))

    tax = round(income * 0.06, 2)
    if random.random() < 0.0002:
        tax = round(income * random.choice([0, 0.05, 0.10, 0.15]), 2)

    return (taxpayer_id, year, month, 'IP6', round(income, 2), tax, season, transactions)


def generate_record_ip15(taxpayer_id, activity, district, workers, year, month):
    """ИП УСН 15% — налог с разницы доходы минус расходы."""
    season = get_season(month)
    base = AVG_INCOMES['IP15'][activity][year]
    base *= (SEASONAL_FACTORS[activity][season]
             * DISTRICT_FACTORS.get(district, 1.0)
             * get_workers_factor(workers))

    income = random.uniform(0.8 * base, 1.2 * base)
    transactions = max(1, int(income / random.uniform(10000, 30000)) + random.randint(-2, 2))

    avg_exp = EXPENSE_RATIOS[activity]
    exp_ratio = random.uniform(avg_exp * 0.85, avg_exp * 1.15)
    exp_ratio *= SEASONAL_FACTORS_EXPENSES[activity][season]
    exp_ratio = min(exp_ratio, 0.75)
    taxable = max(0, income - income * exp_ratio)
    tax = round(taxable * 0.15, 2)

    chance = random.random()
    if chance < 0.01:
        income *= random.uniform(1.5, 3.0)
        taxable = income * random.uniform(0.4, 0.6)
        tax = round(taxable * 0.15, 2)
        transactions = int(transactions * random.uniform(1.2, 1.5))
    elif chance < 0.02:
        income *= random.uniform(0.3, 0.6)
        taxable = max(0, income * random.uniform(0.05, 0.15))
        tax = round(taxable * 0.15, 2)
        transactions = max(1, int(transactions * random.uniform(0.3, 0.6)))
    elif chance < 0.022:
        tax = round(taxable * random.choice([0, 0.05, 0.10, 0.20, 0.30]), 2)

    return (taxpayer_id, year, month, 'IP15', round(income, 2), tax, season, transactions)


def generate_record_ipos(taxpayer_id, activity, district, workers, year, month):
    """ИП ОСНО — налог 20% с дохода."""
    season = get_season(month)
    base = AVG_INCOMES['IPOS'][activity][year]
    base *= (SEASONAL_FACTORS[activity][season]
             * DISTRICT_FACTORS.get(district, 1.0)
             * get_workers_factor(workers))

    income = random.uniform(0.95 * base, 1.05 * base)
    transactions = max(1, int(income / random.uniform(1500, 2500)) + random.randint(-3, 3))

    chance = random.random()
    if chance < 0.003:
        income *= random.uniform(3, 5)
        transactions = int(transactions * random.uniform(1.5, 2))
    elif chance < 0.006:
        income *= random.uniform(0.1, 0.3)
        transactions = max(1, int(transactions * random.uniform(0.3, 0.7)))

    tax = round(income * 0.20, 2)
    if random.random() < 0.0002:
        tax = round(income * random.choice([0, 0.05, 0.10, 0.15]), 2)

    return (taxpayer_id, year, month, 'IPOS', round(income, 2), tax, season, transactions)


def generate_record_ipp(taxpayer_id, activity, district, workers, year, month):
    """ИП патент — налог фиксированный (6% от потенциального дохода / 12)."""
    season = get_season(month)
    base = AVG_INCOMES['IPP'][activity][year]
    income = (random.uniform(0.9, 1.1) * base
              * SEASONAL_FACTORS[activity][season]
              * DISTRICT_FACTORS.get(district, 1.0))

    if random.random() < 0.01:
        return (taxpayer_id, year, month, 'IPP', 0.0, 0.0, season, 0)

    transactions = max(1, int(income / random.uniform(2000, 3000)))

    pot = (POTENTIAL_INCOME_IPP[activity]
           * DISTRICT_FACTORS.get(district, 1.0)
           * get_workers_factor(workers))
    monthly_tax = round(pot * 0.06 / 12, 2)

    return (taxpayer_id, year, month, 'IPP', round(income, 2), monthly_tax, season, transactions)


# Диспетчер: тип → функция генерации
GENERATORS = {
    'SZ': generate_record_sz,
    'IP6': generate_record_ip6,
    'IP15': generate_record_ip15,
    'IPOS': generate_record_ipos,
    'IPP': generate_record_ipp,
}

# 1. Генерация налогоплательщиков

taxpayer_distribution = {
    'SZ': 60, 'IP6': 15, 'IP15': 10, 'IPOS': 10, 'IPP': 5
}
total_records = 30000
record_counts = {t: int(total_records * p / 100)
                 for t, p in taxpayer_distribution.items()}
record_counts['SZ'] += total_records - sum(record_counts.values())

print("Распределение по типам:")
for t, c in record_counts.items():
    print(f"  {t}: {c} ({c / total_records * 100:.1f}%)")

passport_base, inn_base = 1000000000, 100000000000
records, idx = [], 0

for t_type, type_count in record_counts.items():
    for i in range(type_count):
        full_name = f"{fake.last_name()} {fake.first_name()} {fake.middle_name()}"
        has_employees = random.choice([0, 1]) if t_type != 'SZ' else 0
        records.append((
            full_name,
            str(passport_base + idx),
            str(inn_base + idx),
            t_type,
            DISTRICTS[idx % len(DISTRICTS)],
            random.choice(ACTIVITY_TYPES),
            has_employees,
            random.randint(1, 20) if has_employees else None,
        ))
        idx += 1
        if len(records) >= 1000:
            cursor.executemany("""
                INSERT INTO Taxpayer
                (FullName, PassportNumber, INN, TaxpayerType,
                 registration_district, activity_type,
                 has_employees, employees_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, records)
            conn.commit()
            records = []

if records:
    cursor.executemany("""
        INSERT INTO Taxpayer
        (FullName, PassportNumber, INN, TaxpayerType,
         registration_district, activity_type,
         has_employees, employees_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, records)
    conn.commit()

print(f"Создано налогоплательщиков: {idx}")

# 2. Генерация ежемесячных данных для всех типов

for t_type, gen_func in GENERATORS.items():
    extra_col = ", ISNULL(t.employees_count, 0) AS employees_count" if t_type != 'SZ' else ", 0 AS employees_count"
    cursor.execute(f"""
        SELECT t.TaxpayerId, t.activity_type, t.registration_district{extra_col}
        FROM dbo.Taxpayer t
        WHERE t.TaxpayerType = '{t_type}'
    """)
    taxpayers = cursor.fetchall()
    print(f"\n[{t_type}] Найдено: {len(taxpayers)} налогоплательщиков")

    batch, count = [], 0
    for row in taxpayers:
        taxpayer_id, activity, district, workers = row
        for year in YEARS:
            for month in MONTHS:
                batch.append(gen_func(taxpayer_id, activity, district, workers, year, month))
                if len(batch) >= BATCH_SIZE:
                    try:
                        cursor.executemany("""
                            INSERT INTO MonthlyTaxData
                            (TaxpayerId, Year, Month, TaxType,
                             IncomeAmount, TaxAmount, season, transactions_count)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, batch)
                        conn.commit()
                        count += len(batch)
                        print(f"  Вставлено: {count}")
                        batch = []
                    except Exception as e:
                        print(f"  Ошибка вставки: {e}")
                        conn.rollback()
                        batch = []

    if batch:
        try:
            cursor.executemany("""
                INSERT INTO MonthlyTaxData
                (TaxpayerId, Year, Month, TaxType,
                 IncomeAmount, TaxAmount, season, transactions_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
            conn.commit()
            count += len(batch)
        except Exception as e:
            print(f"  Ошибка вставки остатка: {e}")
            conn.rollback()

    print(f"  [{t_type}] Итого вставлено: {count}")

cursor.close()
conn.close()
print("\nГенерация завершена!")
