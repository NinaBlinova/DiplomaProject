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
    WHERE t.TaxpayerType = 'IP15'
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

# Коэффициенты сезонности для расходов (могут отличаться!)
seasonal_factors_expenses = {
    'TRADE': {'Зима': 1.1, 'Весна': 1.0, 'Лето': 0.9, 'Осень': 1.0},
    'SERVICES': {'Зима': 1.0, 'Весна': 1.1, 'Лето': 0.8, 'Осень': 1.1},
    'IT': {'Зима': 1.0, 'Весна': 1.0, 'Лето': 0.8, 'Осень': 1.0},
    'FREELANCE': {'Зима': 1.0, 'Весна': 1.0, 'Лето': 0.7, 'Осень': 1.1},
    'PRODUCTION': {'Зима': 0.9, 'Весна': 1.2, 'Лето': 1.0, 'Осень': 1.0},
    'FOOD': {'Зима': 1.2, 'Весна': 1.0, 'Лето': 1.2, 'Осень': 0.9},
    'LOGISTICS': {'Зима': 1.2, 'Весна': 1.0, 'Лето': 0.9, 'Осень': 1.1},
    'EDUCATION': {'Зима': 1.0, 'Весна': 0.9, 'Лето': 0.5, 'Осень': 1.3}
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


# Средние доходы по сферам деятельности для ИП с налогом 15% с расходов (руб/мес)
avg_incomes_by_activity = {
    'TRADE': {2023: 115000, 2024: 117000, 2025: 120000},  # Торговля
    'SERVICES': {2023: 110000, 2024: 112000, 2025: 115000},  # Услуги
    'IT': {2023: 120000, 2024: 120000, 2025: 120000},  # IT
    'FREELANCE': {2023: 110000, 2024: 112000, 2025: 115000},  # Фриланс
    'PRODUCTION': {2023: 115000, 2024: 117000, 2025: 120000},  # Производство
    'FOOD': {2023: 110000, 2024: 113000, 2025: 115000},  # Общепит
    'LOGISTICS': {2023: 112000, 2024: 115000, 2025: 118000},  # Логистика
    'EDUCATION': {2023: 110000, 2024: 112000, 2025: 115000}  # Обучение
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

    # 1. ГЕНЕРАЦИЯ ДОХОДА
    base_income = avg_incomes_by_activity[activity_type][year]

    # Применяем сезонность и район
    income_factor = seasonal_factors[activity_type][season] * district_factors.get(district, 1.0) * get_workers_factor(
        workers_count)
    base_income_adjusted = base_income * income_factor

    # Вариация дохода ±20%
    monthly_income = random.uniform(0.8 * base_income_adjusted, 1.2 * base_income_adjusted)

    # 2. ГЕНЕРАЦИЯ РАСХОДОВ
    # Берем средний процент расходов для этой сферы
    avg_expense_ratio = expense_ratios_by_activity[activity_type]

    # Вариация процента расходов ±15%
    min_expense_ratio = avg_expense_ratio * 0.85  # например, 0.75 * 0.85 = 0.6375
    max_expense_ratio = avg_expense_ratio * 1.15  # например, 0.75 * 1.15 = 0.8625

    # Случайный процент расходов в этом диапазоне
    expense_ratio = random.uniform(min_expense_ratio, max_expense_ratio)

    # Применяем сезонность к расходам
    expense_season_factor = seasonal_factors_expenses[activity_type][season]
    expense_ratio = expense_ratio * expense_season_factor

    # Ограничиваем: расходы не могут превышать 75% дохода
    expense_ratio = min(expense_ratio, 0.75)

    # Рассчитываем сумму расходов
    monthly_expenses = monthly_income * expense_ratio

    # 3. РАСЧЕТ НАЛОГООБЛАГАЕМОЙ БАЗЫ И НАЛОГА
    taxable_base = monthly_income - monthly_expenses

    # Гарантируем, что налогооблагаемая база не отрицательная
    if taxable_base < 0:
        taxable_base = 0

    # Налог 15%
    tax_rate = 0.15
    tax_amount = round(taxable_base * tax_rate, 2)

    # 4. ГЕНЕРАЦИЯ КОЛИЧЕСТВА ТРАНЗАКЦИЙ
    if monthly_income > 0:
        # Для ИП транзакций меньше (крупнее сделки)
        base_transactions = int(monthly_income / random.uniform(10000, 30000))
        transactions_count = max(1, base_transactions + random.randint(-2, 2))
    else:
        transactions_count = 0

    # 5. АНОМАЛИИ
    anomaly_chance = random.random()

    if anomaly_chance < 0.01 and monthly_income > 0:
        # Аномально высокий доход
        monthly_income *= random.uniform(1.5, 3.0)
        # При высоком доходе расходы могут быть ниже в процентах
        expense_ratio = random.uniform(0.4, 0.6)
        monthly_expenses = monthly_income * expense_ratio
        taxable_base = monthly_income - monthly_expenses
        tax_amount = round(taxable_base * tax_rate, 2)
        transactions_count = int(transactions_count * random.uniform(1.2, 1.5))

    elif 0.01 <= anomaly_chance < 0.02 and monthly_income > 0:
        # Аномально низкий доход или высокие расходы
        monthly_income *= random.uniform(0.3, 0.6)
        expense_ratio = random.uniform(0.85, 0.95)  # Расходы почти равны доходу
        monthly_expenses = monthly_income * expense_ratio
        taxable_base = monthly_income - monthly_expenses
        if taxable_base < 0:
            taxable_base = 0
        tax_amount = round(taxable_base * tax_rate, 2)
        transactions_count = max(1, int(transactions_count * random.uniform(0.3, 0.6)))

    elif 0.02 <= anomaly_chance < 0.022:
        # Неправильный расчет налога
        wrong_tax_rate = random.choice([0, 0.05, 0.10, 0.20, 0.30])
        tax_amount = round(taxable_base * wrong_tax_rate, 2)

    return (taxpayer_id, year, month, 'IP15',
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
