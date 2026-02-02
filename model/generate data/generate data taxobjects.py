import random
from faker import Faker
import pyodbc
from datetime import datetime

fake = Faker('ru_RU')
random.seed(42)

# Подключение к MS SQL
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=DiplomaProject;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

# ---- Данные о ценах за кв.м ----
apartment_prices = {
    (2000, 2004): (10000, 30000),
    (2005, 2008): (55000, 60000),
    (2009, 2009): (45000, 50000),
    (2010, 2014): (50000, 55000),
    (2014, 2015): (55000, 60000),
    (2016, 2019): (55000, 70000),
    (2020, 2020): (65000, 70000),
    (2021, 2021): (70000, 80000),
    (2022, 2022): (110000, 120000),
    (2023, 2023): (130000, 140000),
    (2024, 2024): (150000, 200000),
}

average_salaries = {
    2020: 51220,
    2021: 56990,
    2022: 66390,
    2023: 73709,
    2024: 84500,
    2025: 91500
}


# ---- Функции ----
def random_apartment_price(year):
    for (start, end), (low, high) in apartment_prices.items():
        if start <= year <= end:
            return random.randint(low, high)
    return 60000


def random_salary(year):
    avg = average_salaries.get(year, 60000)
    return round(avg * random.uniform(0.8, 1.2), 2)


def random_car_engine():
    return round(random.uniform(1.0, 5.0), 1)


def random_other_object():
    objects = [
        'Ювелирные изделия', 'Картины', 'Антиквариат', 'Яхта',
        'Драгоценные камни', 'Компьютерное оборудование',
        'Патент', 'Авторское право', 'Оборудование для бизнеса'
    ]
    value = round(random.uniform(100000, 5000000), 2)
    return random.choice(objects), value


def random_land_price(year, area):
    if 1991 <= year <= 1999:
        price_per_m2 = random.randint(500, 3000)
    elif 2000 <= year <= 2009:
        price_per_m2 = random.randint(3000, 15000)
    elif 2010 <= year <= 2019:
        price_per_m2 = random.randint(15000, 50000)
    elif 2020 <= year <= 2025:
        price_per_m2 = random.randint(50000, 200000)
    else:
        price_per_m2 = 10000
    return price_per_m2 * area


# ---- Получаем всех налогоплательщиков ----
cursor.execute("SELECT taxpayer_id, taxpayer_type FROM taxpayer")
taxpayers = cursor.fetchall()

batch_size = 10000
batch = []

for t in taxpayers:
    taxpayer_id, t_type = t

    # ---- Физлица ----
    if t_type == 'individual':
        # Квартира
        year = random.randint(2000, 2024)
        price_per_m2 = random_apartment_price(year)
        area = random.randint(30, 120)
        total_price = price_per_m2 * area
        acq_date = fake.date_between_dates(date_start=datetime(year, 1, 1), date_end=datetime(year, 12, 31))
        batch.append(('property', f'Квартира {area} м²', total_price, acq_date, taxpayer_id))

        # Зарплата за каждый месяц с 2020 по 2025
        for year_salary in range(2020, 2026):
            for month in range(1, 13):
                salary = random_salary(year_salary)
                acq_date = datetime(year_salary, month, random.randint(1, 28))
                batch.append(('income', f'Зарплата {year_salary}-{month:02d}', salary, acq_date, taxpayer_id))

        # Машина с вероятностью 30%
        if random.random() < 0.3:
            engine = random_car_engine()
            batch.append(('vehicle', 'Личный автомобиль', engine, None, taxpayer_id))

    else:  # ИП, ООО, АО и др.
        # Доход за каждый месяц с 2020 по 2025
        for year_income in range(2020, 2026):
            for month in range(1, 13):
                base_income = average_salaries.get(year_income, 60000)
                income = round(base_income * random.uniform(1.5, 3.0), 2)
                acq_date = datetime(year_income, month, random.randint(1, 28))
                batch.append(('income', f'Доход {year_income}-{month:02d}', income, acq_date, taxpayer_id))

        # Иногда есть квартиры и машины
        if random.random() < 0.3:
            year_ap = random.randint(2000, 2024)
            price_per_m2 = random_apartment_price(year_ap)
            area = random.randint(50, 300)
            total_price = price_per_m2 * area
            acq_date = fake.date_between_dates(date_start=datetime(year_ap, 1, 1), date_end=datetime(year_ap, 12, 31))
            batch.append(('property', f'Квартира {area} м²', total_price, acq_date, taxpayer_id))
        if random.random() < 0.4:
            engine = random_car_engine()
            batch.append(('vehicle', 'Служебный автомобиль', engine, None, taxpayer_id))

    # ---- Земельный участок для 47% налогоплательщиков ----
    if random.random() < 0.47:
        land_year = random.randint(1991, 2025)
        land_area = random.randint(500, 5000)
        land_price = random_land_price(land_year, land_area)
        acq_date = fake.date_between_dates(date_start=datetime(land_year, 1, 1), date_end=datetime(land_year, 12, 31))
        batch.append(('property', f'Земельный участок {land_area} м²', land_price, acq_date, taxpayer_id))

    if random.random() < 0.1:
        obj_name, obj_value = random_other_object()
        batch.append(('other', obj_name, obj_value, None, taxpayer_id))

    # ---- Вставка пакетами ----
    if len(batch) >= batch_size:
        cursor.executemany("""
            INSERT INTO taxobject (object_type, description_tax, taxable_base, acquisition_date, taxpayer_id)
            VALUES (?, ?, ?, ?, ?)
        """, batch)
        conn.commit()
        print(f"Вставлено {len(batch)} объектов")
        batch = []

# Вставляем остатки
if batch:
    cursor.executemany("""
        INSERT INTO taxobject (object_type, description_tax, taxable_base, acquisition_date, taxpayer_id)
        VALUES (?, ?, ?, ?, ?)
    """, batch)
    conn.commit()
    print(f"Вставлено {len(batch)} объектов")

cursor.close()
conn.close()
print("Генерация объектов налоговой базы завершена!")
