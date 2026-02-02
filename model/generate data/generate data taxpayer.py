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

# Type taxpayers
'''
'SZ',  'Самозанятый (НПД)', 'НПД', - 20 процентов
'IP6', 'ИП на УСН 6%', 'УСН_6', - 28 процентов
'IP15','ИП на УСН 15%', 'УСН_15', - 15 процентов
'IPOS','ИП на ОСНО', 'ОСНО', - 20 процентов
'IPP', 'ИП на патенте', 'ПАТЕНТ'; - 12 процентов
'''

# Field of activity
'''
TRADE        — Торговля (розница / опт)
SERVICES     — Услуги (салоны, ремонт, клининг)
IT           — IT и цифровые услуги
FREELANCE    — Фриланс (дизайн, тексты, маркетинг)
PRODUCTION   — Производство
FOOD         — Общепит
LOGISTICS    — Логистика и доставка
EDUCATION    — Обучение и курсы
'''

# Процентное распределение типов налогоплательщиков
taxpayer_distribution = {
    'SZ': 20,  # 20%
    'IP6': 28,  # 28%
    'IP15': 15,  # 15%
    'IPOS': 20,  # 20%
    'IPP': 12  # 12%
}

# Общее количество записей (около 30 000)
total_records = 30000

# Рассчитываем точное количество для каждого типа
record_counts = {}
for t_type, percentage in taxpayer_distribution.items():
    record_counts[t_type] = int(total_records * percentage / 100)

# Проверяем, что сумма равна total_records
actual_total = sum(record_counts.values())
# Корректируем последний тип, если есть расхождение
if actual_total != total_records:
    record_counts['IPP'] += total_records - actual_total

# Field of activity
activity_types = ['TRADE', 'SERVICES', 'IT', 'FREELANCE',
                  'PRODUCTION', 'FOOD', 'LOGISTICS', 'EDUCATION']

# Районы СпБ
districts = [
    "Адмиралтейский", "Василеостровский", "Выборгский", "Калининский",
    "Кировский", "Колпинский", "Красногвардейский", "Красносельский",
    "Кронштадтский", "Курортный", "Московский", "Невский",
    "Петроградский", "Петродворцовый", "Приморский", "Пушкинский",
    "Фрунзенский", "Центральный"
]

passport_base = 1000000000
inn_base = 100000000000
records = []
idx = 0

print("Распределение по типам налогоплательщиков:")
for t_type, count in record_counts.items():
    print(f"{t_type}: {count} записей ({count / total_records * 100:.1f}%)")

# Генерация записей для каждого типа
for t_type, type_count in record_counts.items():
    for i in range(type_count):
        full_name = f"{fake.last_name()} {fake.first_name()} {fake.middle_name()}"
        passport = str(passport_base + idx)
        inn = str(inn_base + idx)

        # Для самозанятых (SZ) не может быть сотрудников
        has_employees = random.choice([0, 1]) if t_type != 'SZ' else 0
        employees_count = random.randint(1, 20) if has_employees else None

        # Равномерно распределяем по районам
        district = districts[idx % len(districts)]

        records.append((
            full_name,
            passport,
            inn,
            t_type,
            district,
            random.choice(activity_types),
            has_employees,
            employees_count
        ))
        idx += 1

        # Пакетная вставка каждые 1000 записей
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

# Вставка оставшихся записей
if records:
    cursor.executemany("""
        INSERT INTO Taxpayer
        (FullName, PassportNumber, INN, TaxpayerType, 
         registration_district, activity_type,
         has_employees, employees_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, records)
    conn.commit()

cursor.close()
conn.close()

print(f"\nГенерация {total_records} налогоплательщиков завершена!")
print(f"Создано записей: {idx}")